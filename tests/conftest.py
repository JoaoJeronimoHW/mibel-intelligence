"""
Shared pytest configuration.

1. Isolation: several tests DELETE from prices_day_ahead and the download
   tests write raw files. Every test therefore gets its own temporary
   database and data directories; data/mibel.duckdb and data/raw are never
   touched by the test suite.
2. Loud failures: the tests are also runnable as scripts and report failure
   by returning False. pytest ignores return values, so a returned False is
   turned into a real test failure here.
"""

import functools
import importlib
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

RAW_DIR_MODULES = {
    'src.data.omie_ingest': 'omie',
    'src.data.entsoe_ingest': 'entsoe',
    'src.data.weather_ingest': 'weather',
}


@pytest.fixture(autouse=True)
def isolated_data(tmp_path, monkeypatch):
    from src.utils import db_utils
    monkeypatch.setattr(db_utils, 'DB_PATH', tmp_path / 'test.duckdb')

    raw = tmp_path / 'raw'
    for module_name, sub in RAW_DIR_MODULES.items():
        try:
            module = importlib.import_module(module_name)
        except ImportError:
            continue
        (raw / sub).mkdir(parents=True, exist_ok=True)
        monkeypatch.setattr(module, 'RAW_DIR', raw / sub)
        if hasattr(module, 'COMBINED_DIR'):
            monkeypatch.setattr(module, 'COMBINED_DIR', raw / sub / 'combined')

    from src.data import load_to_db, build_panel
    monkeypatch.setattr(load_to_db, 'RAW_DIR', raw)
    (tmp_path / 'processed').mkdir()
    monkeypatch.setattr(build_panel, 'PROCESSED_DIR', tmp_path / 'processed')
    yield tmp_path


def pytest_collection_modifyitems(items):
    for item in items:
        test_fn = item.obj

        @functools.wraps(test_fn)
        def wrapper(*args, __test_fn=test_fn, **kwargs):
            if __test_fn(*args, **kwargs) is False:
                pytest.fail("test reported failure by returning False")

        item.obj = wrapper
