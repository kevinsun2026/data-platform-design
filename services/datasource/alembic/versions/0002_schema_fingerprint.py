"""add fingerprint column to datasource_schemas

Revision ID: 0002
Revises: 0001
Create Date: 2026-08-26 11:00:00.000000

Adds the :attr:`aidp_datasource.models.DatasourceSchema.fingerprint`
column that the schema service uses to detect upstream schema
drift. The column is a 64-character SHA-256 hex digest (or the
empty string for snapshots taken before the migration was
applied; the schema service treats ``""`` as "no baseline").

The migration is intentionally additive:

- ``nullable=False`` so every new row carries a fingerprint
  the moment :class:`aidp_datasource.services.schema_service`
  starts writing snapshots.
- ``server_default=""`` so an ``INSERT`` that omits the column
  (e.g. a backfill script or a hand-written SQL test fixture)
  still succeeds and the schema service can later compute the
  fingerprint on the next sync.
- Existing rows are backfilled to ``""`` via the
  ``server_default`` on add-column; the schema service then
  overwrites them on the next sync. We do *not* backfill the
  hash by replaying the source DB — that would require a live
  connection to the upstream data warehouse, which the migration
  path deliberately avoids.

Upgrade / downgrade are mirrors of each other so a rollback
removes the column without touching the rest of the table.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# ---------------------------------------------------------------------------
# Upgrade
# ---------------------------------------------------------------------------


def upgrade() -> None:
    """Add the :attr:`DatasourceSchema.fingerprint` column.

    The column is sized at 64 chars (SHA-256 hex). We use
    ``String`` (not ``CHAR(64)``) so the value can be ``""``
    without a trailing-pad mismatch on engines that compare
    ``CHAR`` strictly.
    """
    op.add_column(
        "datasource_schemas",
        sa.Column(
            "fingerprint",
            sa.String(length=64),
            nullable=False,
            server_default="",
        ),
    )


# ---------------------------------------------------------------------------
# Downgrade
# ---------------------------------------------------------------------------


def downgrade() -> None:
    """Drop the :attr:`DatasourceSchema.fingerprint` column."""
    op.drop_column("datasource_schemas", "fingerprint")
