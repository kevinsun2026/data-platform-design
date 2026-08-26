"""AES-256-GCM at-rest encryption for datasource credentials.

Every :class:`aidp_datasource.models.Datasource` row carries a
companion encrypted blob — the ``credentials_ciphertext`` /
``credentials_nonce`` / ``credentials_aad`` columns. The plaintext
(:class:`aidp_datasource.schemas.CredentialsPayload`) never touches
disk: only the datasource service holds the key material needed to
render it back into a ``Connector``.

Threat model
------------

- The database is a *trusted but cautious* component: an attacker
  with a SQLi or backup-tape leak should not learn the credentials
  to a tenant's production database.
- The key is held *outside* the database (env var in dev, KMS in
  prod). Compromise of the database alone does not yield
  plaintext.
- The key is *not* held by callers of the query API. The datasource
  service is the only component authorised to decrypt; the
  :func:`aidp_datasource.services.datasource_service.get_decrypted_connection`
  helper decrypts on demand, applies the L1 tenant filter, and
  returns the plaintext only when the caller is authorised for the
  row's tenant.

What this module does *not* do
------------------------------

- It does not implement a KMS integration. Production deployments
  inject the key via ``AIDP_DATASOURCE_CREDENTIAL_KEY``; a future
  task wires the same slot to a KMS-backed envelope-encryption
  provider.
- It does not provide key rotation. The ``key_version`` column on
  :class:`aidp_datasource.models.Datasource` is reserved for that —
  when the env var changes, decrypt still uses whatever key the
  row was encrypted with (operators must run an explicit
  re-encryption sweep).
- It does not authenticate the *envelope* — that is the connector
  service's job (the AAD includes the datasource id and kind so a
  replay across rows fails GCM authentication).

Wire format
-----------

The on-disk ``ciphertext`` is the raw AES-GCM output (ciphertext +
auth tag, concatenated). The 12-byte ``nonce`` is stored alongside
so the decrypt path can reconstruct the cipher state. The AAD is
the string ``f"{tenant_id}:{datasource_id}:{kind}"`` so a row
produced for tenant A can never be decrypted as tenant B even if
a misbehaving caller swapped the ``tenant_id`` FK.

The format is intentionally identical to the audit service's
:mod:`aidp_audit.crypto` payload format so the same KMS integration
eventually serves both services.
"""

from __future__ import annotations

import base64
import json
import logging
import os
from dataclasses import dataclass
from typing import Final

from aidp_common.errors import UpstreamError
from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from aidp_datasource.schemas import CredentialsPayload

_LOG = logging.getLogger(__name__)

#: AES-256 key length, in bytes.
_KEY_LENGTH: Final = 32

#: AES-GCM nonce length, in bytes (12 is the IETF-recommended size).
_NONCE_LENGTH: Final = 12

#: Canonical key version. Rotated together with the underlying key
#: material; the column on
#: :class:`aidp_datasource.models.Datasource` is written for every
#: new row so a future rotation can derive the right key from the
#: version.
KEY_VERSION: Final = "v1"

#: Canonical cipher identifier. Stored on
#: :class:`aidp_datasource.models.Datasource.credentials_key_version`
#: so a future migration can pick a different cipher per row.
ALGORITHM: Final = "AES-256-GCM"

#: Environment variable that carries the base64-encoded 32-byte key.
ENV_VAR: Final = "AIDP_DATASOURCE_CREDENTIAL_KEY"


@dataclass(frozen=True)
class EncryptedCredentials:
    """The output of :func:`CredentialService.encrypt`.

    Attributes:
        ciphertext: Raw AES-GCM output (ciphertext || 16-byte tag).
        nonce: 12-byte nonce. Generated fresh on every call.
        aad: Additional Authenticated Data — written to
            :class:`aidp_datasource.models.Datasource.credentials_aad`
            for the replay check.
        key_version: Key version used to encrypt the current row.
            Reserved for the day a key rotation runs.
    """

    ciphertext: bytes
    nonce: bytes
    aad: str
    key_version: str

    def __post_init__(self) -> None:
        if len(self.nonce) != _NONCE_LENGTH:
            raise ValueError(
                f"nonce must be exactly {_NONCE_LENGTH} bytes, got {len(self.nonce)}"
            )


def _load_key_from_env() -> bytes:
    """Read and validate the ``AIDP_DATASOURCE_CREDENTIAL_KEY`` environment variable.

    The variable holds a URL-safe base64 encoding of a 32-byte key
    (AES-256). We refuse to start if the key is missing or the wrong
    size — a 0-byte key would silently turn encryption into a no-op,
    which would be a compliance bug.

    Returns:
        The decoded 32-byte key.

    Raises:
        UpstreamError: When the env var is missing or the decoded key
            is not exactly 32 bytes. The error code is
            ``UPSTREAM_ERROR`` because the datasource service cannot
            operate without its encryption key, so a misconfigured
            deploy is a startup-time upstream failure.
    """
    raw = os.environ.get(ENV_VAR)
    if not raw:
        raise UpstreamError(
            f"{ENV_VAR} is not set; the datasource service cannot "
            "operate without a credential-encryption key",
            details={"env_var": ENV_VAR},
        )
    try:
        decoded = base64.urlsafe_b64decode(raw.encode("ascii"))
    except Exception as exc:
        raise UpstreamError(
            f"{ENV_VAR} is not valid base64",
            details={"error": str(exc)},
        ) from exc
    if len(decoded) != _KEY_LENGTH:
        raise UpstreamError(
            f"{ENV_VAR} must decode to exactly {_KEY_LENGTH} bytes, "
            f"got {len(decoded)}",
            details={"expected_bytes": _KEY_LENGTH, "got_bytes": len(decoded)},
        )
    return decoded


#: Process-wide key cache. We load the key once and reuse it; a future
#: rotation task will swap this for a KMS-backed envelope-encryption
#: provider and re-key in place.
_KEY_CACHE: bytes | None = None


def get_credential_key() -> bytes:
    """Return the cached 32-byte credential-encryption key.

    The key is read from ``AIDP_DATASOURCE_CREDENTIAL_KEY`` on first
    access and cached for the process lifetime. Tests can call
    :func:`reset_credential_key_cache` between cases.

    Raises:
        UpstreamError: When the env var is missing or invalid.
    """
    global _KEY_CACHE
    if _KEY_CACHE is None:
        _KEY_CACHE = _load_key_from_env()
    return _KEY_CACHE


def reset_credential_key_cache() -> None:
    """Drop the cached key. Intended for tests."""
    global _KEY_CACHE
    _KEY_CACHE = None


def _aad_for(*, tenant_id: str, datasource_id: str, kind: str) -> bytes:
    """Build the Additional Authenticated Data string for one row.

    The format ``"{tenant_id}:{datasource_id}:{kind}"`` is fixed so
    decrypt can recompute the AAD from the parent row's identity
    columns and catch a tampered ``tenant_id`` / ``datasource_id`` /
    ``kind`` at GCM auth time.

    Returns:
        The AAD as bytes (UTF-8 encoded). The
        :class:`EncryptedCredentials` consumer also stores the AAD as
        a string for forensic inspection.
    """
    return f"{tenant_id}:{datasource_id}:{kind}".encode()


def _serialise_credentials(credentials: CredentialsPayload) -> bytes:
    """Serialise a :class:`CredentialsPayload` to canonical bytes.

    The format is JSON with sorted keys, UTF-8 encoded, no extra
    whitespace. Sorting is not strictly required for GCM (which
    works on bytes) but it makes the on-disk ciphertext
    deterministic given the same plaintext — useful for forensic
    diffing in the test suite.

    Returns:
        The serialised credentials bytes.
    """
    return json.dumps(
        credentials.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _deserialise_credentials(plaintext: bytes) -> CredentialsPayload:
    """Inverse of :func:`_serialise_credentials`.

    Raises:
        ValueError: When the plaintext is not valid JSON or does not
            match the :class:`CredentialsPayload` schema.
    """
    decoded = json.loads(plaintext.decode("utf-8"))
    if not isinstance(decoded, dict):
        raise ValueError("decrypted credential payload is not a JSON object")
    return CredentialsPayload.model_validate(decoded)


class CredentialService:
    """Encrypt / decrypt datasource credentials.

    The service is intentionally tiny: the constructor takes the
    32-byte key (so tests can inject a fixture), the default
    factory :func:`default_credential_service` reads it from the
    process env via :func:`get_credential_key`. The two methods —
    :meth:`encrypt` and :meth:`decrypt` — are the only public
    surface; everything else is module-private.

    Concurrency
    -----------

    The service is stateless after construction (it only holds the
    raw 32-byte key), so a single instance is safe to share across
    coroutines. The :func:`default_credential_service` helper
    memoises one per process for the common path.
    """

    def __init__(self, key: bytes | None = None) -> None:
        """Initialise the service with an explicit *key* (or env var).

        Args:
            key: The 32-byte AES-256 key. When ``None`` the key is
                loaded from the process env (via
                :func:`get_credential_key`). Production code passes
                ``None``; tests pass a fixture so the env can be
                left unset.

        Raises:
            UpstreamError: When *key* is omitted and the env var is
                missing, or when the supplied *key* is the wrong
                size.
        """
        if key is None:
            key = get_credential_key()
        if len(key) != _KEY_LENGTH:
            raise UpstreamError(
                "credential key must be exactly 32 bytes",
                details={"expected_bytes": _KEY_LENGTH, "got_bytes": len(key)},
            )
        self._key = key

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def encrypt(
        self,
        credentials: CredentialsPayload,
        *,
        tenant_id: str,
        datasource_id: str,
        kind: str,
    ) -> EncryptedCredentials:
        """Encrypt *credentials* for the given row identity.

        A fresh random nonce is generated for every call — never
        reuse a nonce under the same key (it would catastrophically
        break GCM).

        Args:
            credentials: The plaintext credentials.
            tenant_id: Tenant the datasource belongs to. Part of the
                AAD.
            datasource_id: The new row's id (a UUID4 string).
                Part of the AAD.
            kind: The datasource kind (``"postgresql"`` / ...). Part
                of the AAD.

        Returns:
            An :class:`EncryptedCredentials` ready to be persisted on
            :class:`aidp_datasource.models.Datasource`.

        Raises:
            ValueError: When any of the identity strings is empty.
        """
        if not tenant_id or not datasource_id or not kind:
            raise ValueError(
                "tenant_id, datasource_id, and kind must all be non-empty"
            )
        aad_bytes = _aad_for(
            tenant_id=tenant_id, datasource_id=datasource_id, kind=kind
        )
        nonce = os.urandom(_NONCE_LENGTH)
        cipher = AESGCM(self._key)
        ciphertext = cipher.encrypt(nonce, _serialise_credentials(credentials), aad_bytes)
        return EncryptedCredentials(
            ciphertext=ciphertext,
            nonce=nonce,
            aad=aad_bytes.decode("utf-8"),
            key_version=KEY_VERSION,
        )

    def decrypt(
        self,
        *,
        ciphertext: bytes,
        nonce: bytes,
        tenant_id: str,
        datasource_id: str,
        kind: str,
    ) -> CredentialsPayload:
        """Decrypt an :class:`EncryptedCredentials` back to its plaintext.

        Recomputes the AAD from the parent row's identity columns
        and passes it to GCM, so any tamper with ``tenant_id`` /
        ``datasource_id`` / ``kind`` causes an authentication
        failure rather than a silent mis-decryption.

        Args:
            ciphertext: The value from
                :attr:`Datasource.credentials_ciphertext`.
            nonce: The value from
                :attr:`Datasource.credentials_nonce`.
            tenant_id / datasource_id / kind: From the parent
                :class:`aidp_datasource.models.Datasource` row.

        Returns:
            The decrypted :class:`CredentialsPayload`.

        Raises:
            ValueError: When the identity strings are empty.
            UpstreamError: When the GCM authentication fails
                (tampered, key mismatch, AAD drift) or the nonce
                length is wrong.
        """
        if not tenant_id or not datasource_id or not kind:
            raise ValueError(
                "tenant_id, datasource_id, and kind must all be non-empty"
            )
        if len(nonce) != _NONCE_LENGTH:
            raise UpstreamError(
                "credential nonce has the wrong length",
                details={
                    "expected_bytes": _NONCE_LENGTH,
                    "got_bytes": len(nonce),
                },
            )
        aad_bytes = _aad_for(
            tenant_id=tenant_id, datasource_id=datasource_id, kind=kind
        )
        cipher = AESGCM(self._key)
        try:
            plaintext = cipher.decrypt(nonce, ciphertext, aad_bytes)
        except InvalidTag as exc:
            _LOG.warning(
                "credential GCM auth tag mismatch; refusing to decrypt",
                extra={
                    "tenant_id": tenant_id,
                    "datasource_id": datasource_id,
                    "kind": kind,
                },
            )
            raise UpstreamError(
                "credential authentication failed; refusing to decrypt",
                details={
                    "tenant_id": tenant_id,
                    "datasource_id": datasource_id,
                    "kind": kind,
                },
            ) from exc
        try:
            return _deserialise_credentials(plaintext)
        except (ValueError, json.JSONDecodeError) as exc:
            raise UpstreamError(
                "decrypted credential payload is not a valid CredentialsPayload",
                details={"error": str(exc)},
            ) from exc


#: Process-wide service singleton. Tests can replace it via
#: :func:`set_default_credential_service`; the default factory
#: reads the key from the env var on first use.
_DEFAULT: CredentialService | None = None


def default_credential_service() -> CredentialService:
    """Return the process-wide :class:`CredentialService`.

    Built lazily on first call so the env var is read only after
    pytest fixtures have had a chance to seed it.
    """
    global _DEFAULT
    if _DEFAULT is None:
        _DEFAULT = CredentialService()
    return _DEFAULT


def set_default_credential_service(service: CredentialService | None) -> None:
    """Override the process-wide service (used by tests)."""
    global _DEFAULT
    _DEFAULT = service


__all__ = [
    "ALGORITHM",
    "ENV_VAR",
    "KEY_VERSION",
    "CredentialService",
    "EncryptedCredentials",
    "default_credential_service",
    "get_credential_key",
    "reset_credential_key_cache",
    "set_default_credential_service",
]
