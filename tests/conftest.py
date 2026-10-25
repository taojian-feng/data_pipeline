from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from gridlake.config import Settings
from gridlake.contracts import load_contracts
from gridlake.sources.eia import load_balancing_authorities

REPO = Path(__file__).resolve().parents[1]
START = datetime(2026, 10, 1, tzinfo=UTC)


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        home=tmp_path / "gridlake", catalog_type="local", eia_api_key=None, openlineage_url=None
    )


@pytest.fixture(scope="session")
def contracts():
    return load_contracts(REPO / "contracts")


@pytest.fixture(scope="session")
def bas():
    return load_balancing_authorities(REPO / "seeds")
