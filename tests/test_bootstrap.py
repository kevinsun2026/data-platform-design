"""Bootstrap placeholder so ``task test`` has at least one passing test on the
empty monorepo. Subsequent Phase 1 tasks add real coverage alongside each
service / library; this file can be deleted once ``tests/`` has substantive
content.
"""


def test_bootstrap() -> None:
    """Trivial assertion that always holds — represents the empty repo state."""
    assert True
