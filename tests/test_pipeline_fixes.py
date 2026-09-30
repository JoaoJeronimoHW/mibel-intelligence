"""
Regression tests for the defects in docs/PIPELINE_MANUAL.md, Part F.
Each test names the manual issue it guards. All run offline against the
temporary database and directories provided by conftest.py.

Run: python -m pytest tests/test_pipeline_fixes.py
"""

import sys
from datetime import date
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / 'mlops' / 'models' / 'training'))

from src.utils import db_utils
from src.utils.timezone_utils import create_hour_index, add_time_features, market_window_utc
from src.data import load_to_db
from src.data.build_panel import build_main_panel


def omie_raw(days: dict) -> pd.DataFrame:
    """Raw OMIE-shaped frame: {date: n_hours}; price = 100*day_index + hour."""
    rows = []
    for i, (day, n_hours) in enumerate(days.items()):
        for concept in ['PRICE_SP', 'PRICE_PT', 'ENER_IB']:
            row = {'DATE': day, 'CONCEPT': concept}
            for h in range(1, 26):
                row[f'H{h}'] = float(100 * i + h) if h <= n_hours else np.nan
            rows.append(row)
    return pd.DataFrame(rows)


def write_raw(tmp_path, name, df):
    target = tmp_path / 'raw' / name
    target.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(target, index=False)


# D-01 / D-03 / D-04 --------------------------------------------------------

def test_omie_hours_map_to_true_utc_including_dst_days():
    raw = omie_raw({date(2022, 3, 27): 23, date(2022, 10, 30): 25, date(2022, 6, 15): 24})
    long = load_to_db.omie_wide_to_long(raw)
    es = long[long['country'] == 'ES'].set_index('timestamp')['price_eur_mwh']

    assert len(es) == 23 + 25 + 24, "ENER_IB must be excluded and every real hour kept"
    def utc(s):
        return pd.Timestamp(s, tz='UTC')
    # Spring: H1 = 00:00 CET = 23:00 UTC the day before; H3 = 03:00 CEST = 01:00 UTC
    assert es[utc('2022-03-26 23:00')] == 1
    assert es[utc('2022-03-27 01:00')] == 3
    # Autumn: H1 = 00:00 CEST = 22:00 UTC the day before. H3 (02:00 CEST) and
    # H4 (02:00 CET) share a wall clock but are distinct UTC hours; H25 is kept
    assert es[utc('2022-10-29 22:00')] == 101
    assert es[utc('2022-10-30 00:00')] == 103
    assert es[utc('2022-10-30 01:00')] == 104
    assert es[utc('2022-10-30 22:00')] == 125
    # Summer: H1 = 00:00 CEST = 22:00 UTC the day before
    assert es[utc('2022-06-14 22:00')] == 201


def test_stored_timestamps_do_not_depend_on_session_timezone(isolated_data):
    write_raw(isolated_data, 'omie/day_ahead_prices_x.parquet',
              omie_raw({date(2022, 6, 15): 24}))
    load_to_db.load_all_data()
    # Reloading onto existing rows must work (DuckDB rejects delete+reinsert
    # of the same key in one transaction) and leave the same contents
    load_to_db.load_all_data()
    assert db_utils.get_row_count('prices_day_ahead') == 2 * 24

    conn = duckdb.connect(str(db_utils.DB_PATH), read_only=True)
    conn.execute("SET TimeZone = 'Europe/Lisbon'")
    first = conn.execute("SELECT epoch(MIN(timestamp)) FROM prices_day_ahead").fetchone()[0]
    conn.close()
    assert first == pd.Timestamp('2022-06-14 22:00', tz='UTC').timestamp()


# D-10 / D-09 -----------------------------------------------------------------

def test_weather_and_flow_loaders_match_columns_by_name(isolated_data):
    ts = pd.date_range('2022-06-15', periods=3, freq='h', tz='UTC')
    weather = pd.DataFrame({  # ingest order, which differs from the table order
        'timestamp': ts, 'temperature_c': [20.0, 21, 22], 'wind_speed_10m': 1.0,
        'wind_speed_100m': 2.0, 'wind_direction_100m': 3.0, 'solar_radiation': 4.0,
        'dni': 5.0, 'diffuse_radiation': 6.0, 'cloud_cover': 7.0,
        'location': 'Madrid', 'latitude': 40.4, 'longitude': -3.7,
    })
    write_raw(isolated_data, 'weather/weather_Madrid_x.parquet', weather)
    # A combined file must not be loaded on top of the per-location file
    write_raw(isolated_data, 'weather/weather_all_x.parquet', weather)
    flows = pd.DataFrame({'timestamp': ts, 'flow_mw': [500.0, 600, 700],
                          'from_country': 'ES', 'to_country': 'FR'})
    write_raw(isolated_data, 'entsoe/cross_border_flows_ES_FR_x.parquet', flows)

    load_to_db.load_all_data()

    w = db_utils.execute_query("SELECT location, temperature_c, latitude FROM weather ORDER BY timestamp")
    assert list(w['location']) == ['Madrid'] * 3
    assert list(w['temperature_c']) == [20, 21, 22]
    assert w['latitude'].iloc[0] == pytest.approx(40.4)
    f = db_utils.execute_query("SELECT country_from, country_to, flow_mw FROM cross_border_flows ORDER BY timestamp")
    assert list(f['country_from']) == ['ES'] * 3 and list(f['flow_mw']) == [500, 600, 700]


# D-13 / D-08 / D-06 ----------------------------------------------------------

def test_panel_window_is_consistent_across_sources(isolated_data):
    start, end = '2022-06-15', '2022-06-16'
    hours = create_hour_index(start, end)['timestamp']
    days = {date(2022, 6, 15): 24, date(2022, 6, 16): 24}
    write_raw(isolated_data, 'omie/day_ahead_prices_x.parquet', omie_raw(days))
    for loc in ['Madrid', 'Lisbon']:
        write_raw(isolated_data, f'weather/weather_{loc}_x.parquet', pd.DataFrame({
            'timestamp': hours, 'temperature_c': 20.0, 'wind_speed_10m': 1.0,
            'wind_speed_100m': 2.0, 'wind_direction_100m': 3.0, 'solar_radiation': 4.0,
            'dni': 5.0, 'diffuse_radiation': 6.0, 'cloud_cover': 7.0,
            'location': loc, 'latitude': 0.0, 'longitude': 0.0}))
    write_raw(isolated_data, 'entsoe/cross_border_flows_ES_FR_x.parquet', pd.DataFrame({
        'timestamp': hours, 'flow_mw': 1.0, 'from_country': 'ES', 'to_country': 'FR'}))
    load_to_db.load_all_data()

    panel = build_main_panel(start, end, ['ES', 'PT'])

    assert len(panel) == 2 * 48
    assert panel['price_eur_mwh'].notna().all()
    # The final market day's weather and flows are present (old '<' end bug)
    assert panel['temperature_c'].notna().all()
    assert panel['ES_to_FR_mw'].notna().all()
    # Each country got its own prices (compound-key merge)
    es = panel[panel['country'] == 'ES']['price_eur_mwh'].to_numpy()
    assert es[0] == 1 and es[-1] == 124


def test_hour_index_and_policy_flag_use_market_days():
    assert len(create_hour_index('2022-01-01', '2023-12-31')) == 17_520
    start, _ = market_window_utc('2022-06-15', '2022-06-15')
    flags = add_time_features(pd.DataFrame({'timestamp': pd.DatetimeIndex(
        [start - pd.Timedelta(hours=1), start]).tz_convert('UTC')}))
    assert list(flags['is_iberian_exception']) == [False, True]
    assert list(flags['hour']) == [23, 0], "calendar features are in market local time"


# D-14 ------------------------------------------------------------------------

def test_feature_engineering_survives_empty_weather_and_keeps_countries_apart():
    from feature_engineering import PriceFeatureEngineer

    ts = pd.date_range('2022-01-01', periods=24 * 30, freq='h', tz='UTC')
    panel = pd.concat([
        pd.DataFrame({'timestamp': ts, 'country': c, 'price_eur_mwh': base + np.arange(len(ts)),
                      'temperature_c': np.nan})
        for c, base in [('ES', 0.0), ('PT', 1e6)]
    ], ignore_index=True)
    panel = add_time_features(panel)

    fe = PriceFeatureEngineer()
    out = fe.create_all_features(panel)

    assert len(out) == 2 * (len(ts) - 168), "only the 168h warm-up per country is dropped"
    pt = out[out['country'] == 'PT']
    assert (pt['price_lag_168h'] >= 1e6).all(), "PT lags must never contain ES prices"

    with pytest.raises(ValueError):
        fe.create_all_features(panel.head(100))
