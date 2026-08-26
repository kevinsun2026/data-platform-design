"""Tests for the AES-256-GCM credential encryption.

These tests pin the contract that
:mod:`aidp_datasource.services.credential_service` ships in
Task 14:

- Plaintext credentials are never persisted — the
  :func:`CredentialService.encrypt` output is a separate
  :class:`EncryptedCredentials` with a ciphertext that does
  not contain the plaintext.
- :func:`decrypt` is the inverse of :func:`encrypt` for any
  valid input.
- AAD tampering (different ``tenant_id`` / ``datasource_id`` /
  ``kind``) causes a GCM auth failure, not a silent
  mis-decryption.
- A non-32-byte key is rejected at construction time (or at
  ``get_credential_key`` time when the env var is loaded).
- The wire format is the same as the audit service's
  payload encryption (``12-byte nonce + AAD + ``v1`` key
  version + ``AES-256-GCM`` algorithm).
"""

from __future__ import annotations

import base64

import pytest
from aidp_common.errors import UpstreamError
from aidp_datasource.schemas import CredentialsPayload
from aidp_datasource.services.credential_service import (
    ALGORITHM,
    ENV_VAR,
    KEY_VERSION,
    CredentialService,
    EncryptedCredentials,
    default_credential_service,
    get_credential_key,
    reset_credential_key_cache,
    set_default_credential_service,
)

# A second 32-byte key, distinct from the conftest default, so
# tests can verify key mismatches cause a decryption failure.
_OTHER_KEY = b"\x02" * 32


@pytest.fixture
def svc() -> CredentialService:
    """A fresh :class:`CredentialService` bound to the conftest key."""
    reset_credential_key_cache()
    return CredentialService()


@pytest.fixture
def alt_svc() -> CredentialService:
    """A :class:`CredentialService` bound to a *different* 32-byte key."""
    return CredentialService(key=_OTHER_KEY)


# ---------------------------------------------------------------------------
# Round-trip
# ---------------------------------------------------------------------------


def test_credential_encrypted_at_rest(svc: CredentialService) -> None:
    """``encrypt`` output is distinct from the plaintext, ``decrypt`` round-trips."""
    # Use a long, fixed plaintext so the "plaintext is not a
    # substring of the ciphertext" assertion is deterministic.
    # A single-byte plaintext like ``"p"`` collides with the
    # AES output roughly once in every four runs (the
    # ciphertext carries ~80 bytes of pseudo-random data and
    # each byte has a 1/256 chance of equalling ``0x70``).
    plain = CredentialsPayload(
        username="u-very-long",
        password="hunter2-not-very-secret-but-long-enough",
        extra={"key": "value-12345"},
    )
    encrypted = svc.encrypt(
        plain,
        tenant_id="tenant-a",
        datasource_id="ds-1",
        kind="postgresql",
    )
    assert encrypted.ciphertext != plain.password.encode()
    assert plain.password.encode() not in encrypted.ciphertext
    assert svc.decrypt(
        ciphertext=encrypted.ciphertext,
        nonce=encrypted.nonce,
        tenant_id="tenant-a",
        datasource_id="ds-1",
        kind="postgresql",
    ) == plain


def test_ciphertext_differs_for_different_inputs(svc: CredentialService) -> None:
    """Two calls with the same plaintext produce different ciphertexts (fresh nonces)."""
    plain = CredentialsPayload(username="u", password="p")
    a = svc.encrypt(plain, tenant_id="t", datasource_id="d", kind="postgresql")
    b = svc.encrypt(plain, tenant_id="t", datasource_id="d", kind="postgresql")
    assert a.ciphertext != b.ciphertext
    assert a.nonce != b.nonce


def test_decrypt_round_trips_extra_dict(svc: CredentialService) -> None:
    """The ``extra`` dict survives encrypt → decrypt."""
    plain = CredentialsPayload(
        username="u",
        password="p",
        extra={"auth": "KERBEROS", "service_name": "orcl"},
    )
    encrypted = svc.encrypt(
        plain, tenant_id="t", datasource_id="d", kind="oracle"
    )
    out = svc.decrypt(
        ciphertext=encrypted.ciphertext,
        nonce=encrypted.nonce,
        tenant_id="t",
        datasource_id="d",
        kind="oracle",
    )
    assert out.extra == {"auth": "KERBEROS", "service_name": "orcl"}


# ---------------------------------------------------------------------------
# AAD binding
# ---------------------------------------------------------------------------


def test_decrypt_with_wrong_tenant_id_fails(svc: CredentialService) -> None:
    """A row encrypted for tenant A must not decrypt as tenant B."""
    plain = CredentialsPayload(username="u", password="p")
    enc = svc.encrypt(
        plain, tenant_id="tenant-a", datasource_id="ds-1", kind="postgresql"
    )
    with pytest.raises(UpstreamError) as exc_info:
        svc.decrypt(
            ciphertext=enc.ciphertext,
            nonce=enc.nonce,
            tenant_id="tenant-b",  # tamper
            datasource_id="ds-1",
            kind="postgresql",
        )
    assert "authentication failed" in str(exc_info.value).lower()


def test_decrypt_with_wrong_datasource_id_fails(svc: CredentialService) -> None:
    """A row encrypted for one id must not decrypt as another."""
    plain = CredentialsPayload(username="u", password="p")
    enc = svc.encrypt(
        plain, tenant_id="t", datasource_id="ds-a", kind="postgresql"
    )
    with pytest.raises(UpstreamError):
        svc.decrypt(
            ciphertext=enc.ciphertext,
            nonce=enc.nonce,
            tenant_id="t",
            datasource_id="ds-b",  # tamper
            kind="postgresql",
        )


def test_decrypt_with_wrong_kind_fails(svc: CredentialService) -> None:
    """A row encrypted for PG must not decrypt as MySQL."""
    plain = CredentialsPayload(username="u", password="p")
    enc = svc.encrypt(plain, tenant_id="t", datasource_id="d", kind="postgresql")
    with pytest.raises(UpstreamError):
        svc.decrypt(
            ciphertext=enc.ciphertext,
            nonce=enc.nonce,
            tenant_id="t",
            datasource_id="d",
            kind="mysql",  # tamper
        )


# ---------------------------------------------------------------------------
# Key mismatch
# ---------------------------------------------------------------------------


def test_decrypt_with_wrong_key_fails(
    svc: CredentialService, alt_svc: CredentialService
) -> None:
    """A row encrypted with key A must not decrypt with key B."""
    plain = CredentialsPayload(username="u", password="p")
    enc = svc.encrypt(plain, tenant_id="t", datasource_id="d", kind="postgresql")
    with pytest.raises(UpstreamError):
        alt_svc.decrypt(
            ciphertext=enc.ciphertext,
            nonce=enc.nonce,
            tenant_id="t",
            datasource_id="d",
            kind="postgresql",
        )


# ---------------------------------------------------------------------------
# Key loading
# ---------------------------------------------------------------------------


def test_invalid_key_length_rejected() -> None:
    """A non-32-byte key is rejected at construction time."""
    with pytest.raises(UpstreamError):
        CredentialService(key=b"\x00" * 16)


def test_get_credential_key_reads_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """``get_credential_key`` reads ``AIDP_DATASOURCE_CREDENTIAL_KEY`` and caches."""
    key_bytes = b"\xab" * 32
    monkeypatch.setenv(ENV_VAR, base64.urlsafe_b64encode(key_bytes).decode("ascii"))
    reset_credential_key_cache()
    try:
        assert get_credential_key() == key_bytes
        # Second call returns the cached value (no re-read).
        assert get_credential_key() == key_bytes
    finally:
        reset_credential_key_cache()


def test_get_credential_key_rejects_missing_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """A missing env var raises :class:`UpstreamError`."""
    monkeypatch.delenv(ENV_VAR, raising=False)
    reset_credential_key_cache()
    with pytest.raises(UpstreamError) as exc_info:
        get_credential_key()
    assert ENV_VAR in str(exc_info.value)


def test_get_credential_key_rejects_bad_base64(monkeypatch: pytest.MonkeyPatch) -> None:
    """A non-base64 env var raises :class:`UpstreamError`."""
    monkeypatch.setenv(ENV_VAR, "not-base64!@#")
    reset_credential_key_cache()
    with pytest.raises(UpstreamError):
        get_credential_key()


def test_get_credential_key_rejects_wrong_length(monkeypatch: pytest.MonkeyPatch) -> None:
    """An env var that decodes to a non-32-byte key is rejected."""
    monkeypatch.setenv(ENV_VAR, base64.urlsafe_b64encode(b"\x01" * 16).decode("ascii"))
    reset_credential_key_cache()
    with pytest.raises(UpstreamError) as exc_info:
        get_credential_key()
    assert "32 bytes" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------


def test_default_credential_service_uses_env_key(
    svc: CredentialService,
) -> None:
    """``default_credential_service`` is a singleton that uses the env key."""
    set_default_credential_service(svc)
    try:
        assert default_credential_service() is svc
    finally:
        set_default_credential_service(None)


# ---------------------------------------------------------------------------
# Wire format
# ---------------------------------------------------------------------------


def test_encrypted_payload_envelope() -> None:
    """The on-disk envelope matches the audit service's contract."""
    assert ALGORITHM == "AES-256-GCM"
    assert KEY_VERSION == "v1"
    enc = EncryptedCredentials(
        ciphertext=b"x" * 32,
        nonce=b"n" * 12,
        aad="t:d:k",
        key_version="v1",
    )
    assert len(enc.nonce) == 12
    assert enc.key_version == "v1"


def test_encrypted_payload_rejects_bad_nonce_length() -> None:
    """The dataclass refuses a non-12-byte nonce at construction time."""
    with pytest.raises(ValueError):
        EncryptedCredentials(
            ciphertext=b"x" * 16,
            nonce=b"n" * 8,  # wrong
            aad="t:d:k",
            key_version="v1",
        )


# ---------------------------------------------------------------------------
# Decrypt edge cases
# ---------------------------------------------------------------------------


def test_decrypt_rejects_short_nonce(svc: CredentialService) -> None:
    """A nonce that is not 12 bytes is rejected at the API layer."""
    plain = CredentialsPayload(username="u", password="p")
    enc = svc.encrypt(plain, tenant_id="t", datasource_id="d", kind="postgresql")
    with pytest.raises(UpstreamError) as exc_info:
        svc.decrypt(
            ciphertext=enc.ciphertext,
            nonce=b"x" * 8,  # wrong
            tenant_id="t",
            datasource_id="d",
            kind="postgresql",
        )
    assert "nonce" in str(exc_info.value).lower()


def test_decrypt_rejects_empty_identity(svc: CredentialService) -> None:
    """Empty identity strings are rejected before the GCM call."""
    plain = CredentialsPayload(username="u", password="p")
    enc = svc.encrypt(plain, tenant_id="t", datasource_id="d", kind="postgresql")
    with pytest.raises(ValueError):
        svc.decrypt(
            ciphertext=enc.ciphertext,
            nonce=enc.nonce,
            tenant_id="",
            datasource_id="d",
            kind="postgresql",
        )


def test_encrypt_rejects_empty_identity(svc: CredentialService) -> None:
    """Empty identity strings are rejected before the GCM call."""
    plain = CredentialsPayload(username="u", password="p")
    with pytest.raises(ValueError):
        svc.encrypt(plain, tenant_id="", datasource_id="d", kind="postgresql")
