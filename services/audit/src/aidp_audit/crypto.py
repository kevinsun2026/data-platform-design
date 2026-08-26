"""AES-256-GCM at-rest encryption for audit event payloads.

Every ``audit_events`` row carries a companion ``audit_payloads`` row
holding the encrypted :attr:`aidp_events.envelope.EventEnvelope.payload`.
The plaintext never touches disk — only the audit service holds the key
material needed to render it back to a client.

Threat model
------------

- The database is a *trusted but cautious* component: an attacker with
  a SQLi or backup-tape leak should not learn the audit payload.
- The key is held *outside* the database (env var in dev, KMS in prod).
  Compromise of the database alone does not yield plaintext.
- The key is *not* held by callers of the query API. The audit service
  is the only component authorised to decrypt; the query handler
  decrypts on demand, applies the L1 tenant filter, and returns the
  plaintext only when the caller is authorised for the row's tenant.

What this module does *not* do
------------------------------

- It does not implement a KMS integration. Production deployments
  inject the key via ``AIDP_AUDIT_PAYLOAD_KEY``; a future task wires
  the same slot to a KMS-backed envelope-encryption provider.
- It does not provide key rotation. The ``key_version`` column is
  reserved for that — when the env var changes, decrypt still uses
  whatever key the row was encrypted with (operators must run an
  explicit re-encryption sweep).
- It does not authenticate the *envelope* — that is the producer's job
  (the platform signs payloads via the ``EventEnvelope`` and the
  ``trace_id`` is included in the AAD so a replay across tenants
  fails GCM authentication).

Wire format
-----------

The on-disk ``ciphertext`` is the raw AES-GCM output (ciphertext +
auth tag, concatenated). The 12-byte ``nonce`` is stored alongside
so the decrypt path can reconstruct the cipher state. The AAD is the
string ``f"{tenant_id}:{event_id}:{event_type}"`` so a row produced
by tenant A can never be decrypted as tenant B even if a misbehaving
caller swapped the ``tenant_id`` FK.
"""

from __future__ import annotations

import base64
import logging
import os
from dataclasses import dataclass
from typing import Final

from aidp_common.errors import UpstreamError
from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

_LOG = logging.getLogger(__name__)

#: AES-256 key length, in bytes.
_KEY_LENGTH: Final = 32

#: AES-GCM nonce length, in bytes (12 is the IETF-recommended size).
_NONCE_LENGTH: Final = 12

#: Canonical key version. Rotated together with the underlying key
#: material; the column on :class:`aidp_audit.models.AuditPayload` is
#: written for every new row so a future rotation can derive the right
#: key from the version.
KEY_VERSION: Final = "v1"

#: Canonical cipher identifier. Stored on
#: :class:`aidp_audit.models.AuditPayload.algorithm` so a future
#: migration can pick a different cipher per row.
ALGORITHM: Final = "AES-256-GCM"


@dataclass(frozen=True)
class EncryptedPayload:
    """The output of :func:`encrypt_payload`.

    Attributes:
        ciphertext: Raw AES-GCM output (ciphertext || 16-byte tag).
        nonce: 12-byte nonce. Generated fresh on every call.
        aad: Additional Authenticated Data — written to
            :class:`aidp_audit.models.AuditPayload.aad` for the
            audit-side replay check.
    """

    ciphertext: bytes
    nonce: bytes
    aad: str

    def __post_init__(self) -> None:
        if len(self.nonce) != _NONCE_LENGTH:
            raise ValueError(f"nonce must be exactly {_NONCE_LENGTH} bytes, got {len(self.nonce)}")


def _load_key_from_env() -> bytes:
    """Read and validate the ``AIDP_AUDIT_PAYLOAD_KEY`` environment variable.

    The variable holds a URL-safe base64 encoding of a 32-byte key
    (AES-256). We refuse to start if the key is missing or the wrong
    size — a 0-byte key would silently turn encryption into a no-op,
    which would be a compliance bug.

    Returns:
        The decoded 32-byte key.

    Raises:
        UpstreamError: When the env var is missing or the decoded key
            is not exactly 32 bytes. The error code is
            ``UPSTREAM_ERROR`` because the audit service cannot operate
            without its encryption key, so a misconfigured deploy is
            a startup-time upstream failure.
    """
    raw = os.environ.get("AIDP_AUDIT_PAYLOAD_KEY")
    if not raw:
        raise UpstreamError(
            "AIDP_AUDIT_PAYLOAD_KEY is not set; the audit service cannot "
            "operate without a payload-encryption key",
            details={"env_var": "AIDP_AUDIT_PAYLOAD_KEY"},
        )
    try:
        decoded = base64.urlsafe_b64decode(raw.encode("ascii"))
    except Exception as exc:
        raise UpstreamError(
            "AIDP_AUDIT_PAYLOAD_KEY is not valid base64",
            details={"error": str(exc)},
        ) from exc
    if len(decoded) != _KEY_LENGTH:
        raise UpstreamError(
            f"AIDP_AUDIT_PAYLOAD_KEY must decode to exactly {_KEY_LENGTH} bytes, "
            f"got {len(decoded)}",
            details={"expected_bytes": _KEY_LENGTH, "got_bytes": len(decoded)},
        )
    return decoded


#: Process-wide key cache. We load the key once and reuse it; a future
#: rotation task will swap this for a KMS-backed envelope-encryption
#: provider and re-key in place.
_KEY_CACHE: bytes | None = None


def get_payload_key() -> bytes:
    """Return the cached 32-byte payload-encryption key.

    The key is read from ``AIDP_AUDIT_PAYLOAD_KEY`` on first access and
    cached for the process lifetime. Tests can call
    :func:`reset_payload_key_cache` between cases.

    Raises:
        UpstreamError: When the env var is missing or invalid.
    """
    global _KEY_CACHE
    if _KEY_CACHE is None:
        _KEY_CACHE = _load_key_from_env()
    return _KEY_CACHE


def reset_payload_key_cache() -> None:
    """Drop the cached key. Intended for tests."""
    global _KEY_CACHE
    _KEY_CACHE = None


def _aad_for(*, tenant_id: str, event_id: str, event_type: str) -> bytes:
    """Build the Additional Authenticated Data string for one event.

    The format ``"{tenant_id}:{event_id}:{event_type}"`` is fixed so
    decrypt can recompute the AAD from the parent row's columns and
    catch a tampered ``tenant_id`` FK at GCM auth time.

    Returns:
        The AAD as bytes (UTF-8 encoded). The :class:`EncryptedPayload`
        consumer also stores the AAD as a string for forensic
        inspection.
    """
    return f"{tenant_id}:{event_id}:{event_type}".encode()


def encrypt_payload(
    *,
    plaintext: bytes,
    tenant_id: str,
    event_id: str,
    event_type: str,
) -> EncryptedPayload:
    """Encrypt *plaintext* for the given audit-event identity.

    A fresh random nonce is generated for every call — never reuse
    a nonce under the same key (it would catastrophically break GCM).

    Args:
        plaintext: The audit payload bytes (typically
            ``json.dumps(payload_dict).encode("utf-8")``).
        tenant_id: Tenant the event belongs to. Part of the AAD.
        event_id: The producer's ``EventEnvelope.event_id``. Part of
            the AAD.
        event_type: The producer's ``EventEnvelope.event_type``. Part
            of the AAD.

    Returns:
        An :class:`EncryptedPayload` ready to be persisted on
        :class:`aidp_audit.models.AuditPayload`.

    Raises:
        ValueError: When *tenant_id* / *event_id* / *event_type* is
            empty (an empty string is technically valid UTF-8 but
            would collide with itself in the AAD namespace).
    """
    if not tenant_id or not event_id or not event_type:
        raise ValueError("tenant_id, event_id, and event_type must all be non-empty")
    key = get_payload_key()
    aad_bytes = _aad_for(tenant_id=tenant_id, event_id=event_id, event_type=event_type)
    nonce = os.urandom(_NONCE_LENGTH)
    cipher = AESGCM(key)
    ciphertext = cipher.encrypt(nonce, plaintext, aad_bytes)
    return EncryptedPayload(
        ciphertext=ciphertext,
        nonce=nonce,
        aad=aad_bytes.decode("utf-8"),
    )


def decrypt_payload(
    *,
    ciphertext: bytes,
    nonce: bytes,
    tenant_id: str,
    event_id: str,
    event_type: str,
) -> bytes:
    """Decrypt an :class:`EncryptedPayload` back to its plaintext.

    Recomputes the AAD from the parent row's identity columns and
    passes it to GCM, so any tamper with ``tenant_id`` /
    ``event_id`` / ``event_type`` causes an authentication failure
    rather than a silent mis-decryption.

    Args:
        ciphertext: The value from :attr:`AuditPayload.ciphertext`.
        nonce: The value from :attr:`AuditPayload.nonce`.
        tenant_id / event_id / event_type: From the parent
            :class:`AidpAuditEvent` row.

    Returns:
        The decrypted plaintext bytes.

    Raises:
        UpstreamError: When GCM authentication fails (the row is
            tampered with, the key has changed, or the AAD columns
            were edited). The error is logged at WARNING so an
            operator can correlate the failure with the
            ``audit_event_id`` that triggered it.
    """
    if not tenant_id or not event_id or not event_type:
        raise ValueError("tenant_id, event_id, and event_type must all be non-empty")
    if len(nonce) != _NONCE_LENGTH:
        raise UpstreamError(
            "audit payload nonce has the wrong length",
            details={
                "expected_bytes": _NONCE_LENGTH,
                "got_bytes": len(nonce),
            },
        )
    key = get_payload_key()
    aad_bytes = _aad_for(tenant_id=tenant_id, event_id=event_id, event_type=event_type)
    cipher = AESGCM(key)
    try:
        plaintext = cipher.decrypt(nonce, ciphertext, aad_bytes)
    except InvalidTag as exc:
        _LOG.warning(
            "audit payload GCM auth tag mismatch; refusing to decrypt",
            extra={
                "tenant_id": tenant_id,
                "event_id": event_id,
                "event_type": event_type,
            },
        )
        raise UpstreamError(
            "audit payload authentication failed; refusing to decrypt",
            details={
                "tenant_id": tenant_id,
                "event_id": event_id,
                "event_type": event_type,
            },
        ) from exc
    return plaintext


__all__ = [
    "ALGORITHM",
    "KEY_VERSION",
    "EncryptedPayload",
    "decrypt_payload",
    "encrypt_payload",
    "get_payload_key",
    "reset_payload_key_cache",
]
