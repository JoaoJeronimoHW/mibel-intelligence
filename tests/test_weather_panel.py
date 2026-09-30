"""
Weather aggregation to country level: levels from 'main' cities, dispersion
(sd, range) from all of a country's cities; and the downloader's
skip-existing / no-partial-file behaviour. Offline.

Run: python -m pytest tests/test_weather_panel.py
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.timezone_utils import create_hour_index
from src.data import load_to_db, weather_ingest
from src.data.build_panel import build_main_panel
from src.data.weather_locations import LOCATIONS


def weather_raw(hours, location, value):
    return pd.DataFrame({
        'timestamp': hours, 'temperature_c': value, 'wind_speed_10m': 1.0,
        'wind_speed_100m': value, 'wind_direction_100m': 3.0, 'solar_radiation': 4.0,
        'dni': 5.0, 'diffuse_radiation': 6.0, 'cloud_cover': 7.0,
        'location': location, 'latitude': 0.0, 'longitude': 0.0})


def test_levels_use_main_cities_and_dispersion_uses_all(isolated_data):
    assert LOCATIONS['Madrid']['role'] == 'main' and LOCATIONS['Valencia']['role'] == 'reference'
    start = end = '2022-06-15'
    hours = create_hour_index(start, end)['timestamp']
    # ES: two main cities (10, 20) and one reference city (30); PT: one main city
    for loc, value in [('Madrid', 10.0), ('Barcelona', 20.0), ('Valencia', 30.0), ('Lisbon', 15.0)]:
        target = isolated_data / 'raw' / 'weather' / f'weather_{loc}_x.parquet'
        weather_raw(hours, loc, value).to_parquet(target, index=False)
    load_to_db.load_all_data()

    panel = build_main_panel(start, end, ['ES', 'PT', 'FR'])
    es = panel[panel['country'] == 'ES'].iloc[0]
    pt = panel[panel['country'] == 'PT'].iloc[0]
    fr = panel[panel['country'] == 'FR']

    assert es['temperature_c'] == pytest.approx(15.0)        # mean of main cities only
    assert es['temperature_c_sd'] == pytest.approx(10.0)     # sd of 10, 20, 30
    assert es['temperature_c_range'] == pytest.approx(20.0)  # 30 - 10
    assert es['wind_speed_100m_sd'] == pytest.approx(10.0)
    assert es['cloud_cover_sd'] == pytest.approx(0.0)
    assert pt['temperature_c'] == pytest.approx(15.0)
    assert pd.isna(pt['temperature_c_sd'])                   # one city: sd undefined
    assert pt['temperature_c_range'] == pytest.approx(0.0)
    assert fr['temperature_c'].isna().all()                  # no FR files written


def test_download_skips_existing_and_never_saves_partial_locations(isolated_data, monkeypatch):
    monkeypatch.setattr(weather_ingest, 'LOCATIONS', {
        'Done': {'lat': 0, 'lon': 0}, 'Flaky': {'lat': 0, 'lon': 0}})
    monkeypatch.setattr(weather_ingest.time, 'sleep', lambda s: None)
    raw = weather_ingest.RAW_DIR
    existing = raw / 'weather_Done_2022-01-01_2023-12-31.parquet'
    weather_raw(pd.date_range('2022-01-01', periods=2, freq='h', tz='UTC'), 'Done', 1.0) \
        .to_parquet(existing, index=False)

    calls = []

    def fake_download(name, lat, lon, start, end):
        calls.append((name, start))
        if start.startswith('2023'):  # second yearly chunk fails
            return pd.DataFrame()
        return weather_raw(pd.date_range(start, periods=2, freq='h', tz='UTC'), name, 1.0)

    monkeypatch.setattr(weather_ingest, 'download_historical_weather', fake_download)
    weather_ingest.download_all_locations('2022-01-01', '2023-12-31')

    assert all(name == 'Flaky' for name, _ in calls), "existing file must be skipped"
    assert not (raw / 'weather_Flaky_2022-01-01_2023-12-31.parquet').exists()
