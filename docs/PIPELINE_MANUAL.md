# MIBEL Intelligence — Build Manual

**An assembly manual for the Iberian electricity-market data pipeline: what was built, in what
order, how each piece works, why it was done that way, what broke along the way, and what the
finished object actually is.**

Written against the repository state of 2026-09-14 (branch `main`, HEAD `85291bf`).

---

## 0. How to read this manual

An IKEA manual has three virtues: it is ordered, it is honest about which screw goes where, and
it shows you the wrong way next to the right way. This document keeps those virtues and adds the
two things IKEA leaves out — **why** each step exists, and **what happened when the step went
wrong**.

It is organised as:

| Part | Contents |
|---|---|
| **A** | The brief — the economics question, the parts list, the tools |
| **B** | Architecture decisions — the six choices that determine everything downstream |
| **C** | The build, step by step — ten steps, each with *what / how / why / what broke* |
| **D** | The issue log — 18 technical problems, each with symptom, diagnosis, fix, and residual risk |
| **E** | What was actually achieved — with numbers, measured from the artefacts on disk |
| **F** | Known defects and unfinished work, ranked |
| **G** | Rebuild checklist |

Throughout, code is cited as `path:line` so you can open the exact spot.

A note on tone: Part D and Part F are deliberately unflattering. A manual that only describes the
happy path is a brochure. The defects listed are the ones that would bite a reviewer, a
co-author, or you in six months.

---

# PART A — THE BRIEF

## A1. The economic question

On **15 June 2022** Spain and Portugal were granted a derogation from EU electricity market rules
— the **"Iberian Exception"** (*excepción ibérica*). Gas used for electricity generation had its
price capped (starting around €40/MWh of gas, escalating over time), with the difference
reimbursed to gas generators via a levy on the consumers who benefit from the lower pool price.
The derogation ran until **31 December 2023**.

This is an unusually clean natural experiment:

- **It is a sharp, dated intervention.** A marginal-cost cap on the price-setting technology in a
  marginal-pricing auction should mechanically compress the pool price whenever gas is marginal,
  and do nothing when it is not.
- **It is geographically bounded.** Spain and Portugal are treated; France, Germany, Italy and the
  rest of the European coupled market are not. That is a ready-made donor pool for
  difference-in-differences or synthetic control.
- **It has a known leakage channel.** A cheaper Iberian pool makes Spanish exports to France more
  attractive, so part of the Iberian consumer subsidy is captured by French buyers. Cross-border
  flow data measures that directly.
- **It has a structural interpretation.** The cap changes the *shape of the supply curve*, not
  just its level. Bid-curve (offer-stack) data lets you reconstruct the merit order and test
  whether generators re-optimised their bidding.

So the project needs, at hourly resolution: **prices** (treated and control countries),
**cross-border flows**, **generation mix**, **weather** (the exogenous driver of renewable supply
and of demand), and optionally **bid curves**.

## A2. What "done" looks like

The target artefact is an **hourly country panel**:

```
one row per (hour, country)
columns: price, weather, flows, calendar features, treatment flag
```

That shape is chosen because it is exactly what panel estimators want. `is_iberian_exception`
becomes the treatment indicator `D_it`; `country` is the unit fixed effect; `timestamp` (and the
derived calendar columns) is the time fixed effect. Nothing downstream has to reshape anything.

## A3. Parts list — the data sources

| Source | What it gives | Coverage | Auth | Cost of acquisition |
|---|---|---|---|---|
| **OMIE** (Iberian market operator) | Day-ahead marginal price, Spain & Portugal; bid/offer curves | ES, PT | none | prices: minutes; bid curves: ~24–48 h |
| **ENTSO-E Transparency Platform** | Day-ahead prices for all EU bidding zones; physical cross-border flows; actual generation by fuel | 15 countries configured | API key (register + email request) | ~1–2 h for 5 years |
| **Open-Meteo Archive** (ERA5 reanalysis) | Hourly temperature, wind at 10 m and 100 m, shortwave radiation, DNI, diffuse radiation, cloud cover | 7 Iberian cities | none | minutes |

Why three sources rather than one: ENTSO-E alone *could* supply Iberian prices, but OMIE is the
authoritative primary source for MIBEL and is the only route to bid curves. Open-Meteo is used
instead of ENTSO-E's generation figures for the weather instrument because reanalysis data is
gap-free and revision-free, whereas TSO-reported generation is patchy and restated.

## A4. Tools — and why each

From `requirements.txt`, all versions pinned:

- **pandas 2.2.0 / numpy 1.26.3** — the working representation.
- **duckdb 0.10.0** — the analytical store. Reasoning in §B3.
- **OMIEData 0.4.0.0** — community client for OMIE's file endpoints. Chosen over writing a
  scraper: OMIE publishes fixed-width/CSV files with idiosyncratic encodings that vary by year,
  and the library already absorbs that churn.
- **entsoe-py 0.6.5** — wraps ENTSO-E's XML API and returns pandas objects with timezone-aware
  indices.
- **pytz 2024.1** — explicit timezone database. Pinned because DST rules change, and an
  unpinned tz database makes historical results irreproducible.
- **python-dotenv** — keeps the ENTSO-E key out of the source tree.
- **tqdm** — progress bars. Not decoration: a download that takes 90 minutes with no output is
  indistinguishable from a hung one, and you will kill it.
- **black / ruff** — formatting and linting.

---

# PART B — THE SIX ARCHITECTURE DECISIONS

Everything in Part C follows from these. They are listed first because each one is a *constraint
that pays for itself later*, and the payoff is only visible if you know the constraint was
deliberate.

## B1. Three independent, idempotent stages

```
   ingest                 load                   build
data/raw/*.parquet  →  data/mibel.duckdb  →  data/processed/*.parquet
```

- **Ingest** (`src/data/*_ingest.py`) — talk to APIs, write raw Parquet, *transform nothing*.
- **Load** (`src/data/load_to_db.py`) — reshape, standardise, insert into typed tables.
- **Build** (`src/data/build_panel.py`) — query, join, add features, write the panel.

**Why three and not one.** The three stages have wildly different failure modes and costs.
Ingest is slow, network-bound and rate-limited — you want to run it once and never again.
Loading is cheap and gets re-run every time you discover a transformation bug (which, per Part D,
was often). Panel construction is cheap and gets re-run every time you change the analysis
window. Fusing them would mean that fixing a one-character bug in the hour mapping costs you
another two-hour download.

**Why idempotent.** Each stage can be re-run over the same inputs and produce the same outputs.
Ingest writes to deterministic filenames and overwrites. Load clears its target table first
(`load_to_db.py:48`, `:169`, `:203`) and uses `INSERT OR IGNORE`. Build always writes
`main_panel_{start}_{end}.parquet`. This means "just run it again" is always a safe response to a
failure, which is the single most valuable property a pipeline can have when you are debugging it
at midnight.

## B2. Raw Parquet, kept immutable

Every ingest function saves the API response, in the API's own shape, to
`data/raw/<source>/`, Snappy-compressed, before anything touches it.

**Why.** Three reasons, in increasing order of importance:

1. **Re-download is expensive.** Two hours for ENTSO-E, a day for bid curves.
2. **Sources are mutable.** OMIE restates published prices; ENTSO-E backfills. A raw file dated
   today is a record of what the source said today. Without it you cannot explain why last
   month's numbers differ from this month's.
3. **You will get the transformation wrong.** Part D documents four separate occasions on which
   the OMIE wide→long transformation was wrong. Each time, the fix cost seconds because the raw
   files were untouched. Had ingestion transformed in place, each fix would have cost a
   re-download.

Snappy rather than gzip: roughly 3× faster to decompress, and these files are re-read constantly
during debugging. Disk is free; your attention is not.

## B3. DuckDB as the analytical store

| | pandas in memory | SQLite | **DuckDB** |
|---|---|---|---|
| Layout | all resident in RAM | row-oriented | **columnar** |
| 50 M-row aggregation | 4+ GB RAM, slow | full row scan | **reads only needed columns** |
| Server process | — | none | **none, one file** |
| pandas interop | native | manual | **`SELECT * FROM df` directly** |
| Parquet | via reader | none | **queries Parquet in place** |

**Why not just pandas.** The bid-curve table is the sizing constraint: ~24 h/day × 365 d × 5 y ×
hundreds of offers per hour is comfortably 50 M+ rows. Holding that in a DataFrame to compute
"mean offered quantity below €60/MWh by month" is absurd; in a columnar store it touches two
columns and finishes in seconds.

**Why not SQLite or Postgres.** SQLite is row-oriented — the same aggregation scans every byte of
every row. Postgres solves that but adds a server, a user, a port and a backup story to a
single-researcher project. DuckDB is the only option that is simultaneously columnar and a single
file you can copy, delete, or `.gitignore`.

**Why not Parquet alone, no database.** Because the panel build needs *joins* across sources with
different keys and different granularities, and SQL expresses that far more legibly than a chain
of `merge` calls. The database is also where deduplication is enforced declaratively, via primary
keys, rather than remembered procedurally.

> This decision has a sting in its tail. See **Issue D-01** — DuckDB's handling of
> timezone-aware timestamps silently changed the meaning of every timestamp in the database.

## B4. Normalise timezones once, at the boundary

European electricity data is a timezone minefield:

- OMIE publishes in Iberian market local time (**CET/CEST**, i.e. UTC+1/UTC+2) on a fixed
  `H1…H25` grid.
- ENTSO-E returns timezone-aware timestamps; `entsoe-py` wants tz-aware inputs.
- Open-Meteo returns UTC if you ask (`timezone: 'UTC'` in `weather_ingest.py`) and local time if
  you don't.
- Spain and Portugal observe DST, so two days a year have 23 hours and two have 25.
- Spain and Portugal are *in different timezones from each other* (Madrid is CET, Lisbon is WET —
  one hour apart) despite sharing a market.

**The rule adopted:** convert to UTC exactly once, at the point of ingestion, and let nothing
downstream ever see a local time. `src/utils/timezone_utils.py` is the single place the rule is
implemented: `normalize_to_utc()`, `handle_dst_transitions()`, `create_hour_index()`,
`add_time_features()`.

**Why this rule.** UTC has no DST, so every day has exactly 24 hours, and arithmetic on
timestamps is arithmetic on a line rather than on a line with two holes and two overlaps in it.
The alternative — carrying local times and converting at each join — means every join is a place
the bug can live.

> **The rule was adopted and then, in practice, broken.** The database does not contain UTC.
> See **D-01**. The *design* is right; the *implementation* leaks. This is documented rather than
> quietly fixed because the leak is visible in the published numbers, and anyone rebuilding the
> panel needs to know about it.

## B5. Build the time skeleton first, join onto it

`build_main_panel()` does **not** start from the data and reshape it. It starts from
`create_hour_index(start, end)` — a gapless hourly `DatetimeIndex` — and **left-joins** every
source onto that skeleton (`build_panel.py:196` onward).

**Why.** Consider the alternative: join prices to weather to flows, inner or outer. An hour
missing from the price feed simply *isn't in the output*. Your panel is then unbalanced in a way
nothing announces, `len(df)` looks plausible, and a fixed-effects regression silently drops the
affected periods. Building the skeleton first converts *missing rows* into *present rows with
NaN*, which is the difference between a data-quality problem you can count and one you can't see.

It is also what makes the row count a test: 2 countries × 17,520 hours must equal 35,040 rows,
exactly, or something is wrong. It does (§E4).

## B6. Long format in the database, wide only at the edges

Tables are keyed `(timestamp, country)` or `(timestamp, country, technology)` — long format, one
measurement per row. Wide format appears only twice, both times at a boundary:

- On the way *in*: OMIE ships wide (`H1…H25` columns), immediately melted on load.
- On the way *out*: cross-border flows are pivoted to one column per country pair
  (`ES_to_FR_mw`), because a panel row is a country-hour and a flow is a property of that
  country-hour.

**Why long in the middle.** Adding a country or a fuel type in long format is adding rows.
In wide format it is a schema migration. Long format also makes the primary key meaningful:
`PRIMARY KEY (timestamp, country)` is a real integrity constraint that catches duplicate loads;
in wide format there is no equivalent.

---

# PART C — THE BUILD, STEP BY STEP

Each step gives **what** it does, **how** it does it, **why** that way, and **what broke**.
Issue codes in bold (**D-nn**) point to the full write-up in Part D.

---

## Step 1 — Environment and secrets

**What.** A virtualenv with pinned dependencies, and an ENTSO-E API key held in `.env`.

**How.**
```bash
python3 -m venv venv
venv\Scripts\activate          # Windows
pip install -r requirements.txt
python create_env.py           # prompts for the key, writes .env
```
`create_env.py` is eleven lines: prompt, write `ENTSOE_API_KEY=…`, confirm. `.env` is listed in
`.gitignore`; `entsoe_ingest.py` reads it via `load_dotenv()` at import time, and
`get_entsoe_client()` raises a *specific, actionable* error if the key is absent
(`entsoe_ingest.py:66-79`).

**Why a separate `create_env.py`.** Because the alternative — "create a file called `.env` in the
project root" in a README — is the single most common setup failure in a Python project, and
because a script cannot accidentally paste the key into a shell history file that gets committed.

**Why `get_entsoe_client()` is a function, not a module-level client.** Centralised key loading,
one place to add error handling, and no network object constructed at import time (which would
make `import entsoe_ingest` fail on a machine without a key, breaking the tests).

**What broke.** Getting the key is not instant: ENTSO-E requires registration *and* an email to
`transparency@entsoe.eu` requesting API access, answered by a human. In practice that gate is
what pushed the project to build and validate the whole ES/PT path on OMIE alone first — which,
as it turned out, is where the project stopped (**D-15**).

---

## Step 2 — OMIE ingestion

**What.** Download MIBEL day-ahead marginal prices for Spain and Portugal.
Module: `src/data/omie_ingest.py` (495 lines).

**How.**

`download_day_ahead_prices(start, end)` constructs an `OMIEMarginalPriceFileImporter`, calls
`read_to_dataframe()`, and writes `data/raw/omie/day_ahead_prices_{start}_{end}.parquet`.

The returned frame is **wide and multi-concept**:

```
DATE | CONCEPT | H1 | H2 | ... | H24 | H25
```

with `CONCEPT ∈ {PRICE_SP, PRICE_PT, ENER_IB, ENER_IB_BILLAT}` — so one calendar day is four
rows, and the hour is a *column*, not a value. `H25` exists solely to carry the extra hour of the
autumn DST fold and is `NaN` on all 363 other days of the year.

`batch_download_prices()` wraps this in a chunking loop (default six months), with
`time.sleep(2)` between chunks and a `try/except` that **logs and continues** rather than
aborting.

**Why chunk.** Three reasons: a single five-year request may time out server-side; a failure
halfway through loses only one chunk; and chunk boundaries give you natural resume points.

**Why `except: log; continue` rather than `raise`.** Because a download loop that dies on the
seventh of eight quarters and discards the first six is worse than useless. The failure is
recorded and the loop proceeds; you re-run, and the already-present chunks are simply overwritten
with identical content.

**Why `time.sleep()` at all.** OMIE is a public service with no published rate limit, which means
the limit is enforced by 403s rather than by documentation. Two seconds costs nothing on a
download measured in minutes, and removes the entire class of "why am I suddenly banned" failure.

**Bid curves** (`download_bid_curves`, `download_all_bid_curves`,
`download_bid_curves_sample`) are implemented but gated off. The library fetches **one hour at a
time**: 5 years × 365 days × 24 hours ≈ 43,800 requests ≈ 24–48 hours of wall clock. The design
response is a three-tier entry point — prices only, then a 7-day 24-hour sample, then the full
run — selected automatically in `__main__` by inspecting which files already exist
(`omie_ingest.py:441-495`). See **D-16**.

**What broke.**
- The wide `H1…H25` shape with a `CONCEPT` discriminator is not obvious from the docs and had to
  be reverse-engineered from a downloaded file (**D-02**).
- `H25` and the 23-hour spring day are the source of the DST damage (**D-03**).
- The `__main__` block calls `input()` on the third run, which deadlocks under any
  non-interactive runner (**D-17**).

---

## Step 3 — ENTSO-E ingestion

**What.** Day-ahead prices for the donor-pool countries, physical cross-border flows, and
(optionally) generation by fuel. Module: `src/data/entsoe_ingest.py` (362 lines).

**How.**

- `COUNTRY_CODES` maps 15 country names to two-letter codes.
- `download_day_ahead_prices(code, start, end)` builds tz-aware bounds with
  `pd.Timestamp(date, tz='Europe/Brussels')` — the API *requires* tz-aware input — calls
  `client.query_day_ahead_prices()`, reshapes the returned Series to
  `[timestamp, price_eur_mwh, country]`, and immediately does
  `df['timestamp'].dt.tz_convert('UTC')` (`entsoe_ingest.py:114`).
- `download_cross_border_flows(from, to, …)` handles the fact that `entsoe-py` returns a Series
  for some pairs and a DataFrame for others, normalising both to
  `[timestamp, flow_mw, from_country, to_country]` (`entsoe_ingest.py:158-171`).
- `download_all_countries_prices()` loops countries × 3-month chunks, `sleep(1)` between chunks
  and `sleep(2)` between countries, saving one file per country plus a combined file.
- `download_all_entsoe_data()` is the entry point: 12 donor countries, then the four directed
  flow pairs ES↔PT and ES↔FR.

**Why `Europe/Brussels` for the request bounds.** It is the reference zone of the ENTSO-E
platform. The bounds are a *request parameter*, not data — the returned index is converted to UTC
one line later — so the choice affects only which hours you ask for, and asking in the platform's
own zone avoids off-by-one at the edges.

**Why flows are directed and downloaded both ways.** ENTSO-E reports *physical* flows as a
non-negative quantity per direction, not a signed net. ES→FR and FR→ES are separate series, and
net exports must be computed as the difference. Downloading only one direction would make export
leakage look like it never reverses.

**Why `NoMatchingDataError` returns an empty frame rather than raising.** Not every country has
every series for every window; a missing quarter for Norway should not abort the other eleven
countries.

**Why a per-country file *and* a combined file.** The per-country files are the resumable unit;
the combined file is the convenient one. The load step globs `prices_*.parquet`, which matches
both — harmless only because that insert is `INSERT OR IGNORE` (**D-09**).

**What broke.** Nothing, because **this step was never run** — the API key gate (Step 1) was never
passed, `data/raw/entsoe/` is empty, and every downstream consequence of that follows in
**D-15**.

---

## Step 4 — Weather ingestion

**What.** Hourly ERA5 reanalysis weather for seven Iberian locations.
Module: `src/data/weather_ingest.py` (192 lines).

**How.** `LOCATIONS` pins seven coordinate pairs — Madrid, Barcelona, Seville, Bilbao (Spain);
Lisbon, Porto, Faro (Portugal). For each, `download_historical_weather()` calls the Open-Meteo
archive endpoint with eight hourly variables and `timezone: 'UTC'`, then builds a DataFrame with
explicit column renaming (`temperature_2m` → `temperature_c`, `shortwave_radiation` →
`solar_radiation`, `direct_normal_irradiance` → `dni`).

`download_all_locations()` chunks by year and sleeps 1 s between calls: 7 locations × 6 years =
42 requests against a 10,000/day limit.

**Why these seven locations.** Renewable output is a *spatial* phenomenon. Wind in Galicia and
wind in Andalusia are weakly correlated; a single national weather point would be a bad
instrument. The seven span the meteorological regimes: Atlantic north (Bilbao, Porto),
Mediterranean east (Barcelona), continental interior (Madrid), and the southern solar belt
(Seville, Faro), with Lisbon for the Portuguese load centre.

**Why wind at 100 m as well as 10 m.** 10 m is the standard meteorological reference height;
100 m is turbine hub height. Wind power goes as the cube of speed and speed increases with height,
so 10 m wind is a systematically biased proxy for generation. Both are stored so the choice can be
revisited.

**Why DNI separately from shortwave radiation.** Shortwave (global horizontal) radiation drives
fixed-tilt PV; direct normal irradiance drives tracking PV and concentrated solar, of which Spain
has a lot. They diverge sharply on hazy days.

**Why `timezone: 'UTC'` explicitly.** Because the default is local time, and a silently-local
weather series joined to a nominally-UTC price series is a 1–2 hour misalignment that will look
like a genuine lead-lag relationship in a regression. It is the exact class of bug that **B4**
exists to prevent.

**Why chunk by year rather than request 6 years at once.** Partly recovery granularity, mostly
response size: six years of eight hourly variables in a single JSON body is a large parse with no
partial-progress signal.

**What broke.** Same as Step 3 — **this step was never run**. `data/raw/weather/` is empty. The
consequences cascade all the way to the ML training set (**D-14**).

---

## Step 5 — Database schema

**What.** Five tables and eight indexes. Module: `src/utils/db_schema.py` (149 lines), with
connection management in `src/utils/db_utils.py` (83 lines).

**How.** `SCHEMA_SQL` is one string of `CREATE TABLE IF NOT EXISTS` / `CREATE INDEX IF NOT
EXISTS` statements, split on `;` and executed one at a time by `create_schema()`.

```sql
prices_day_ahead   (timestamp, country, price_eur_mwh, energy_mwh)   PK (timestamp, country)
generation         (timestamp, country, technology, generation_mw)   PK (timestamp, country, technology)
cross_border_flows (timestamp, country_from, country_to, flow_mw)    PK (timestamp, country_from, country_to)
weather            (timestamp, location, lat, lon, 8 variables)      PK (timestamp, location)
bid_curves         (timestamp, country, generator_id, price, qty, bid_type, technology)   -- no PK
```

`db_utils.get_connection(readonly)` is the only place a connection is opened; `execute_query()`
wraps query-and-fetch in a context manager.

**Why every table is keyed on `(timestamp, …)`.** The primary key *is* the deduplication
mechanism. Chunked downloads overlap at boundaries; re-running a load re-inserts rows. With a real
PK plus `INSERT OR IGNORE`, duplicate suppression is declarative and cannot be forgotten. Without
it, every loader needs its own `drop_duplicates` and one of them will be missing.

**Why `bid_curves` has no primary key.** There is genuinely no unique key: the same generator can
submit several offers at the same price in the same hour, and those are distinct economic objects.
Forcing a key would silently delete real bids.

**Why two-letter ISO codes rather than country names.** Compact, standard, join-friendly, and —
more practically — the code is what both ENTSO-E and every downstream chart expects, so it removes
a mapping table.

**Why indexes on `timestamp` and `country`.** Every analytical query in the project filters on a
date window and usually on a country. The index turns a full table scan into a range seek.

**Why a single connection module.** DuckDB is single-writer. Two concurrent write connections to
the same file produce a lock error that reports as a permissions problem, which is a miserable
thing to debug from a Jupyter kernel you forgot was still open.

**What broke.** The `TIMESTAMP` column type is timezone-*naive*, and the loaders insert
timezone-*aware* pandas timestamps into it. That single type mismatch is the root cause of the
project's most serious data defect — **D-01**.

---

## Step 6 — Loading raw files into the database

**What.** Read raw Parquet, reshape, insert. Module: `src/data/load_to_db.py` (267 lines), plus
three standalone rescue scripts written during debugging.

**How — the OMIE path**, which is where all the difficulty lives (`load_to_db.py:22-113`):

1. Glob `data/raw/omie/day_ahead_prices_*.parquet`.
2. `DELETE FROM prices_day_ahead WHERE country IN ('ES','PT')` — clear before reload, so the step
   is idempotent.
3. For each file, split on `CONCEPT` into `PRICE_SP` and `PRICE_PT`.
4. Melt wide → long: for hour column `H{n}`, `n ∈ 1…24`, emit one row at `date + (n-1) hours`,
   localised to UTC (`load_to_db.py:66-92`).
5. Drop `NaN` prices.
6. `INSERT OR IGNORE INTO prices_day_ahead SELECT * FROM df_long`.

The other three loaders are simpler: ENTSO-E prices are already long and only need column
selection and reordering; weather and flows are cleared and bulk-inserted.

**Why `DELETE` then insert, rather than upsert.** The raw files are the source of truth and the
transformation is cheap. A full reload is a few seconds and guarantees the table reflects the
current version of the transformation code — which matters enormously when the transformation code
is the thing you keep fixing.

**Why `INSERT OR IGNORE` on top of a `DELETE`.** Belt and braces. The `DELETE` handles
re-running the whole loader; `OR IGNORE` handles overlap *within* a single run, i.e. two raw files
whose date ranges touch.

**Why `range(1, 25)` — i.e. why `H25` is deliberately excluded.** Because under the "timestamps
are UTC" rule a calendar day has exactly 24 hours, and there is no UTC slot for a 25th. This is
the *correct* decision given the rule; it is wrong only because the rule itself is violated
elsewhere. See **D-03**.

**What broke.** A great deal. In rough chronological order:

- The wide→long transformation was wrong, then wrong differently, three times (**D-02**).
- Duplicate `(timestamp, country)` keys appeared around 30 October (**D-04**), which is what
  prompted `diagnose_duplicates.py`.
- Console output crashed on Windows before any data was written (**D-05**).
- Three loaders build DataFrames whose **column order does not match the table** and then do
  `SELECT *` (**D-10**). These have never fired only because their sources were never downloaded.
- Row-by-row `iterrows()` plus per-row `INSERT` is roughly three orders of magnitude slower than
  necessary (**D-11**).

The three rescue scripts at the repository root are the fossil record of this:
`diagnose_duplicates.py` (locate the duplicate), `fix_and_reload_data.py` (first corrected
transformation, vectorised dedupe), `fix_midnight_hour.py` (final version, row-by-row with
`INSERT OR IGNORE`, ASCII-only output). **`fix_midnight_hour.py` is the script that actually
produced the current database** — not `src/data/load_to_db.py`. They are not quite equivalent
(**D-12**).

---

## Step 7 — Building the panel

**What.** Turn database tables into the analysis-ready hourly country panel.
Module: `src/data/build_panel.py` (298 lines).

**How** (`build_main_panel`, `build_panel.py:175-298`):

1. **Skeleton.** `create_hour_index(start, end)` → gapless hourly UTC index. `end` is inclusive of
   the whole final day: `end + 1 day − 1 hour` (`timezone_utils.py:115-116`).
2. **Prices.** `build_price_panel()` queries `prices_day_ahead` over the window, then
   `fix_timestamp_dtype()` coerces to `datetime64[ns, UTC]`.
3. **Weather.** `build_weather_panel()` queries city-level weather and averages to country level —
   Madrid + Barcelona + Seville + Bilbao → `ES`; Lisbon + Porto + Faro → `PT`.
4. **Flows.** `build_flows_panel()` pivots long flows to wide `{from}_to_{to}_mw` columns.
5. **Merge.** For each country: copy the skeleton, stamp `country`, left-join that country's
   prices on `timestamp`, left-join country weather if ES/PT. Concatenate.
6. **Flows join** onto the concatenated frame (same values for every country).
7. **`add_time_features()`** — `hour`, `day_of_week`, `month`, `year`, `quarter`, `day_of_year`,
   `is_weekend`, `is_iberian_exception`.
8. Sort by `(country, timestamp)`, log diagnostics, write
   `data/processed/main_panel_{start}_{end}.parquet`.

**Why a per-country loop rather than one big merge.** A single merge on `['timestamp','country']`
would work, but the loop makes the per-country cardinality obvious and gives a natural place to
attach country-specific sources (weather exists only for ES and PT). It also made the
cross-country contamination bug impossible to miss during debugging (**D-08**).

**Why simple averaging for weather rather than population or capacity weighting.** The docstring
calls it "weighted averages… based on population" but the code is a plain `mean`
(`build_panel.py:99-124`). Plain mean is the *defensible default*: the correct weights for a price
regression are installed renewable capacity by region, which is a separate dataset that was not
acquired. An unweighted mean across regime-spanning locations is a transparent approximation; a
population weighting would be a different arbitrary choice dressed up as a principled one.
**The docstring is wrong and should be corrected** (**D-18**).

**Why flows are joined to every country's rows.** ES→FR flow is a property of the hour, and both
the Spanish and French rows of that hour want it — Spain as export, France as import. Storing it
once per country-hour is redundant but makes every regression specification a column selection
rather than a join.

**Why `is_iberian_exception` is computed here rather than stored.** It is a *research design*
choice, not a fact about the world. Keeping it in the panel builder means changing the treatment
window (e.g. to test the December 2022 cap escalation) is a one-line edit, not a database
migration.

**Why `fix_timestamp_dtype()` exists at all.** Because DuckDB hands back `datetime64[us]` and the
skeleton is `datetime64[ns, UTC]`, and pandas will merge those into all-NaN without complaint.
Full story in **D-06**.

**What broke.**
- The dtype mismatch above, which produced a fully-NaN price column and cost an entire debugging
  session and a dedicated script, `debug_timestamp_merge.py` (**D-06**).
- Spain and Portugal came back with *identical* prices, which looked exactly like a broken merge
  and was in fact correct economics (**D-07**).
- The price query uses `timestamp <= '{end} 23:59:59'` while weather and flows use
  `timestamp < '{end}'` — an inconsistency that silently drops the final day of weather and flows
  (**D-13**).
- Debug `print()` calls remain in `build_price_panel()` (`build_panel.py:62-68`), dumping the full
  head of the frame on every build.

---

## Step 8 — Exploratory analysis

**What.** `notebooks/01_exploratory_analysis.ipynb` (22 cells) and `notebooks/full_panel.ipynb`
(22 cells).

**How.** The first notebook was written against the 7-day pilot panel; the second against the full
two-year panel, and is the one that produced the published figures. Analyses:

1. **Price time series**, ES and PT, with 15 June 2022 marked and the treatment window shaded.
2. **Price duration curves** — prices sorted descending against percentile. The natural object for
   a price-cap study: a cap should *flatten the left tail* (the expensive hours) and leave the
   right tail alone. A mean comparison cannot distinguish "cap bit in 5% of hours, hard" from
   "everything drifted down".
3. **Hourly profiles** with ±1 s.d. bands, peak window 18:00–22:00 highlighted.
4. **Market coupling** — ES vs PT overlaid, plus the ES−PT spread, to identify price splitting.
5. **Weekday/weekend** distributions — a demand-side sanity check.
6. **Rolling 24 h volatility** and hour-on-hour changes.
7. **Summary dashboard** — distribution, key statistics, and the headline aggregates.

**Why duration curves get their own section.** Because they are the diagnostic that matches the
theory. Under marginal pricing with a cap on the marginal fuel, the *shape* of the distribution is
the treatment effect.

**Why the ES−PT spread is plotted separately.** Its magnitude is the market-coupling test: a
spread that is zero almost always means the interconnector is unconstrained and MIBEL clears as
one zone; the non-zero hours are exactly the hours the ES–PT interconnection binds.

**What broke.** Cell 3 of the first notebook is a panicked
`np.array_equal(spain_prices, portugal_prices)` check, and cell 14 goes back to the raw Parquet to
confirm the source data itself has identical values. That is **D-07** being diagnosed in public.

---

## Step 9 — Power BI export

**What.** Five CSVs in `data/powerbi_exports/`, produced by `notebooks/full_panel.ipynb`
cells 6–7, consumed by `dashboard.pbix`.

| File | Grain | Rows |
|---|---|---|
| `mibel_prices.csv` | hour × country, plus display columns | 35,040 |
| `monthly_summary.csv` | country × month: mean, median, min, max, sd, hours | 48 |
| `hourly_patterns.csv` | country × hour-of-day: mean, sd, count | 48 |
| `period_comparison.csv` | country × treatment flag | 4 |
| `waterfall_data.csv` | pre / change / during | 3 |

The export adds presentation columns the panel deliberately lacks: `date`, `time`, `month_name`,
`weekday_name`, and `period` as a readable label.

**Why CSV and not Parquet.** Power BI's Parquet support requires a dataflow or a gateway; CSV is a
first-class source everywhere. 35,040 rows is 3.9 MB — the performance argument for Parquet does
not apply at this size.

**Why pre-aggregate at all, instead of letting Power BI aggregate the fact table.** Because the
aggregations are *analysis decisions* — which window counts as "pre", how the waterfall is
decomposed — and analysis decisions belong in versioned Python, not in DAX measures inside a
binary `.pbix`. The dashboard then only presents.

**Why the display columns are added at export rather than kept in the panel.** `month_name` is a
localisation artefact, not data. Keeping it out of the panel keeps the panel a clean matrix.

---

## Step 10 — ML feature engineering

**What.** `mlops/models/training/feature_engineering.py` — a `PriceFeatureEngineer` class turning
the panel into a supervised learning matrix for day-ahead price forecasting.

**How.** Four feature families, then a time-ordered split:

- **Lags** at 1, 2, 3, 24, 48, 168 hours — recent momentum, same-hour-yesterday,
  same-hour-two-days-ago, same-hour-last-week.
- **Rolling statistics** over 24 h and 168 h windows — mean, sd, min, max — each computed on
  `.shift(1)` first.
- **Cyclic calendar encoding** — sin/cos of hour, day-of-week, month; plus `is_weekend` and
  `is_peak_hour` (18:00–21:00).
- **Policy features** — binary `iberian_exception` and `days_since_policy`.
- **Split** — last 30 days held out, chronologically, per country.

**Why those lag choices.** Electricity prices have three superimposed periodicities: an
autoregressive component at 1–3 h, a diurnal cycle at 24 h, and a weekly cycle at 168 h (weekend
demand). Lag 48 distinguishes a genuine daily cycle from a one-day shock.

**Why `.shift(1)` before every rolling window.** Without it, the window at time *t* includes the
price at time *t* — the target. The model would score brilliantly in backtest and uselessly in
production. This is the single most common leak in time-series ML, and the `shift(1)` is the
defence.

**Why cyclic encoding rather than integer or one-hot hour.** Integer hour tells a linear model
that 23 is twenty-three units from 0, when it is one hour away. One-hot fixes the ordering but
spends 24 columns and learns nothing about adjacency. `(sin, cos)` puts the hours on a circle in
two columns, so 23 and 0 are neighbours by construction.

**Why a chronological split, never a random one.** A random split lets the model see Tuesday and
Thursday while predicting Wednesday. Realistic evaluation requires the test set to be strictly in
the future of the training set — that is the question being asked.

**Why `days_since_policy` in addition to the binary flag.** The binary flag models a level shift.
`days_since_policy` allows a trend: market adaptation, the scheduled escalation of the gas cap,
and gradual re-optimisation of bidding are all dynamics a step function cannot express.

**What broke.** The script runs, reports success, and writes **two empty files**.
`create_all_features()` ends with an unrestricted `df.dropna()`
(`feature_engineering.py:216`); the panel's five weather columns are 100% NaN because Step 4 was
never run; therefore every row is dropped. `train_data.parquet` and `test_data.parquet` on disk
are **0 rows × 39 columns**. See **D-14** — this is the most consequential silent failure in the
project.

---

# PART D — THE ISSUE LOG

Eighteen problems. Each gets **Symptom → Diagnosis → Fix → Why that fix → Residual risk**.
They are ordered by severity, not chronology.

---

## D-01 — The database is not in UTC *(severity: critical, unresolved)*

**Symptom.** Everything about the data looked right, and two facts didn't fit. First, the
spring-forward days were missing an hour at **01:00**, not at 02:00 or 03:00 where Iberian DST
actually happens. Second, the average hourly price profile peaks at hour **21**, which is a
plausible Iberian evening peak in *local* time but would be 23:00 local if the column were really
UTC — implausible.

**Diagnosis.** The chain is short and every link is individually reasonable:

1. `db_schema.py` declares `timestamp TIMESTAMP` — timezone-**naive**.
2. The loaders build timezone-**aware** UTC pandas timestamps and insert them.
3. DuckDB 0.10.0, asked to put an aware value into a naive column, converts it to the **session
   time zone** and drops the offset.
4. The session time zone defaults to the operating system's. This machine is in Portugal, so:

```
SELECT value FROM duckdb_settings() WHERE name='TimeZone';  -->  Europe/Lisbon
```

Europe/Lisbon is UTC+0 in winter and UTC+1 in summer, and its DST transition occurs at
**01:00 UTC** — which is precisely where the missing hours are.

Verified end-to-end against the raw files. Raw OMIE for 2022-03-27 (spring forward) has
`H1=258.17, H2=242.01, … H23=242.90, H24=NaN`. In the database:

```
2022-03-27 00:00  258.17     <- H1: 00:00 UTC -> 00:00 WET
       (01:00 absent)        <- 01:00 WET does not exist; the clock jumps
2022-03-27 02:00  242.01     <- H2: 01:00 UTC -> 02:00 WEST
...
2022-03-27 23:00  242.90     <- H23
       2022-03-28 00:00 absent  <- would come from the 27th's H24, which is NaN
```

And on the autumn fold, 2022-10-30, raw `H1=139.17, H2=105.10, H3=100.25`:

```
2022-10-30 01:00  139.17     <- H1: 00:00 UTC -> 01:00 WEST
       (105.10 never stored) <- H2: 01:00 UTC -> 01:00 WET, same wall clock,
                                 primary-key collision, INSERT OR IGNORE drops it
2022-10-30 02:00  100.25     <- H3
```

The same pattern repeats for 2023-10-29 (`H2=1.38`, lost).

**The compound error.** OMIE's `H{n}` is hour *n−1* in **Iberian market local time** (CET/CEST),
not UTC. The loader treats it as UTC without converting; DuckDB then shifts it again into Lisbon
local time. Net effect on the panel's `timestamp` column:

| Season | Panel label minus true UTC |
|---|---|
| Winter (CET / WET) | **+1 hour** |
| Summer (CEST / WEST) | **+3 hours** |

The column is therefore neither UTC nor any consistent local time. As a happy accident, panel
`hour` equals Iberian local hour in winter and local hour + 1 in summer — which is why the hourly
profiles look economically sensible and why nothing screamed.

**Fix.** *Not fixed.* What the fix requires:

1. Store the hour index in OMIE's own convention explicitly: build the naive timestamp, then
   `tz_localize('Europe/Madrid', ambiguous=<explicit>, nonexistent='shift_forward')`, then
   `tz_convert('UTC')`.
2. Change the column type to `TIMESTAMPTZ`, **or** issue `SET TimeZone='UTC'` in
   `db_utils.get_connection()` — belt and braces, do both.
3. Use `H25` on fold days as the second (WET) occurrence of the repeated local hour, rather than
   discarding it.
4. Reload and rebuild. Raw files are untouched, so this costs seconds, not a re-download — which
   is decision **B2** paying for itself.

**Why it is documented rather than silently patched.** The published figures in
`data/powerbi_exports/` and `dashboard.pbix` were computed on the current labelling. Changing the
labels without saying so would put two sets of numbers into circulation with no way to tell them
apart.

**Residual risk if left.** (a) Not reproducible — the same code on a UTC or Madrid machine
produces different timestamps from the same inputs; (b) any future merge with genuinely-UTC
ENTSO-E or Open-Meteo data is misaligned by 1–3 hours, which is exactly the size of a spurious
lead-lag effect; (c) the treatment flag turns on about an hour before local midnight on 15 June
2022; (d) four prices per country (two fold collisions, two unread `H25` values) are permanently
absent.

**Lesson.** "Everything is UTC" is a property you must **assert and test**, not a property you get
by writing `tz_localize('UTC')` in the right places. The storage layer has opinions.

---

## D-02 — OMIE ships wide, multi-concept, one-indexed *(resolved)*

**Symptom.** The first load produced a table that was empty; then one with a quarter of the
expected rows; then one where every price was offset by an hour.

**Diagnosis.** Three separate mistakes stacked in the same function:

1. The frame is **wide** (`H1…H25` columns), not long. Nothing to melt on until you notice.
2. It is **multi-concept**: one calendar day is four rows discriminated by
   `CONCEPT ∈ {PRICE_SP, PRICE_PT, ENER_IB, ENER_IB_BILLAT}`. Loading without filtering mixes
   prices with traded energy volumes in the same column.
3. It is **one-indexed in the Spanish convention**: `H1` is the hour 00:00–01:00, so `H{n}` maps
   to hour offset `n−1`. Mapping `H1 → 01:00` shifts the entire dataset by one hour, which is
   invisible in aggregate statistics and fatal for hourly profiles.

**Fix.** Filter on `CONCEPT`, iterate `H1…H24`, map with `i = n − 1` (`load_to_db.py:66-92`).
`fix_and_reload_data.py:44-46` carries the comment that marks the moment the off-by-one was
understood:

```python
# CRITICAL: H1 = hour 0 (00:00), H2 = hour 1 (01:00), etc.
# So we use i directly, not i+1
```

**Why this took three attempts.** Each wrong version produced output that passed a casual check.
Wrong concept filter → plausible row count, wrong magnitudes. Off-by-one → plausible everything,
except that the hourly profile peaks in the wrong place, and you have to *know* the Iberian peak
is around 21:00 to notice.

**Residual risk.** Low. The mapping is right *within OMIE's own convention*; it is the conversion
out of that convention that is missing (**D-01**).

---

## D-03 — DST creates 23- and 25-hour days *(partly resolved)*

**Symptom.** Two days a year, the hour columns don't fill the grid: spring has 23 populated
columns with `H24 = NaN`; autumn has 25, with `H25` populated only on that one day.

**Diagnosis.** Local wall-clock time is not a uniform grid. A fixed 24-column layout cannot
represent it, so OMIE uses a 25-column layout with a hole in spring and a spare in autumn.
Verified directly:

```
2022-03-26 (normal)  H1..H24 populated, H25 NaN     -> 24 hours
2022-03-27 (spring)  H1..H23 populated, H24/H25 NaN -> 23 hours
2022-10-30 (autumn)  H1..H25 populated              -> 25 hours
```

**Fix.** Two mechanisms:

- **Design-level:** work in UTC, where every day has 24 hours (**B4**), and build the panel from a
  gapless UTC skeleton so missing hours surface as NaN rather than as absent rows (**B5**).
- **Code-level:** iterate `range(1, 25)` and discard `H25`, on the reasoning that under a UTC grid
  there is no slot for a 25th hour.

**Why discarding `H25` is the right decision and still produces a wrong result.** It *is* correct
under the stated rule. But because the rule is violated (**D-01**), `H25` was the only surviving
copy of a real market price — the fold's second occurrence of the repeated hour — and both copies
are now gone: one dropped as an `H25`, the other dropped by the PK collision. Two real prices lost
per country per year.

**Residual risk.** Four empty grid slots and four discarded prices per country in the current
two-year panel. On 35,040 rows that is 0.02% — immaterial for averages, but it is a *systematic*
loss concentrated on exactly the four days a reviewer will check.

---

## D-04 — Duplicate primary keys on the DST fold *(resolved, lossily)*

**Symptom.** `INSERT` into `prices_day_ahead` failed with a primary-key violation. The failing key
was always around **30 October**.

**Diagnosis.** Two candidate causes, and both were real:

1. **Overlapping chunk files.** `batch_download_prices` chunks by calendar offset and the ranges
   touch at the boundaries, so the same day appears in two raw files.
2. **The DST fold** (via **D-01**): two distinct UTC hours map to the same stored wall-clock
   timestamp, producing a genuine collision on `(timestamp, country)` from a *single* file.

`diagnose_duplicates.py` was written to distinguish them — it re-melts every raw file, tags each
row with its source filename, groups by `(timestamp, country)` and prints which files each
duplicate came from, with a hard-coded special case for 30 October 2022.

**Fix.** `INSERT OR IGNORE`, backed by the primary key (`load_to_db.py:109`,
`fix_midnight_hour.py:71`). Cause 1 is fully solved — the duplicate is byte-identical, so ignoring
it is exactly right. Cause 2 is *suppressed, not solved*: the second value is a different, real
price, and ignoring it discards data.

**Why `OR IGNORE` and not `OR REPLACE`.** For overlapping files the two values are identical, so
the choice is arbitrary and `IGNORE` is cheaper. (`fix_midnight_hour.py`'s closing banner claims
it used `INSERT OR REPLACE`; the code says `INSERT OR IGNORE`. The banner is wrong — see
**D-12**.)

**Residual risk.** Cause 2 is unresolved and is a sub-case of **D-01**.

---

## D-05 — Windows console encoding kills the loader *(resolved)*

**Symptom.** The load script printed two lines and died:

```
UnicodeEncodeError: 'charmap' codec can't encode character '✓' in position 3
```

No data was written. Preserved in `load_output.txt`.

**Diagnosis.** Windows consoles default to code page **cp1252**, which has no ✓, ✗, ⚠, 📊 or any
other character above U+00FF. Python's `print()` encodes to the console encoding and raises. The
crash happened at `print("   ✓ Cleared")` — *after* `DELETE FROM prices_day_ahead` and its commit.
So the failure mode was: **table emptied, nothing reloaded, script dead**, from a decorative
character in a status message.

**Fix.** Three layers:

1. `_tmp_nonascii.txt` at the repository root is the output of a scan that located every offending
   character by line and code point (`36: [(13, '0x274c')]`, `104: [(21, '0x2713')]`, …).
2. `fix_midnight_hour.py` was rewritten with ASCII-only markers: `[OK]`, `[WARN]`, `[ERROR]`.
3. Anything still emitting Unicode is run with `PYTHONIOENCODING=utf-8`.

**Why not just set `PYTHONIOENCODING` globally and keep the ✓.** Because it is an environment
variable, which means it is a thing you have to remember, on every machine, in every runner, in
every CI job. ASCII markers are a thing you cannot forget. The script that must never fail is the
one that got the ASCII treatment.

**Residual risk.** Low, but present: `src/data/load_to_db.py` and `diagnose_pipeline.py` still
print ✓/❌ and will still crash on a fresh Windows console.

**Lesson.** Decorative output is code. It runs, and it can take your data with it. Order your
destructive operations so that a crash in a status message cannot leave the database
half-destroyed.

---

## D-06 — Silent all-NaN merges from timestamp dtype mismatch *(resolved)*

**Symptom.** The panel built, the row count was exactly right, and `price_eur_mwh` was **100%
NaN**. No error, no warning.

**Diagnosis.** Three representations of "the same instant" were in play:

| Origin | dtype |
|---|---|
| DuckDB `fetchdf()` | `datetime64[us]`, naive |
| `create_hour_index()` | `datetime64[ns, UTC]`, `pytz.UTC` |
| `pd.Timestamp(tz='UTC')` | `datetime64[ns, UTC]`, `datetime.timezone.utc` |

`pandas.merge` on keys with different dtypes does not raise — it finds no matches and returns NaN.
With a left join onto a complete skeleton, the output has exactly the expected number of rows, so
every shape-based check passes.

`debug_timestamp_merge.py` (122 lines) was written to isolate it: insert known rows, read them
back, print `dtype`, `repr()` and the raw `.value` nanosecond integer for both sides, compare
element-wise, attempt the merge, and for each unmatched row find the nearest timestamp on the
other side and report the gap in seconds. That last step is what converts "the merge failed" into
"the merge failed by *exactly* this much, or by zero — in which case it's a type problem".

**Fix.** `fix_timestamp_dtype()` (`build_panel.py:33-41`) — a three-line function applied
immediately after every database read and before every merge (`build_panel.py:60, 206, 215, 220,
237`):

```python
df[col] = pd.to_datetime(df[col], utc=True).astype('datetime64[ns, UTC]')
```

and, in `normalize_to_utc()`, the deliberate round-trip `tz_localize(None).tz_localize(UTC)`
(`timezone_utils.py:46-48`) to force `pytz.UTC` specifically rather than an
equivalent-but-different tzinfo object.

**Why a helper rather than fixing it at the source.** Because there are three sources, each with
its own quirk, and any of them can change under a library upgrade. One coercion function applied
at every boundary is a single place to audit.

**Why it looks redundant and isn't.** `build_price_panel` calls it, then `build_main_panel` calls
it again on the same frame (`build_panel.py:206`), and again on the filtered per-country slice
(`:237`). That is scar tissue, and it is cheap scar tissue: the alternative is another all-NaN
column and another lost evening.

**Residual risk.** None for correctness. The redundant calls are noise.

**Lesson.** The dangerous bugs are the ones with no error message, and `merge` producing NaN is
the archetype. Anything that joins should be followed by an assertion on *match rate*, not just on
row count.

---

## D-07 — Spain and Portugal have identical prices *(not a bug)*

**Symptom.** `np.array_equal(spain_prices, portugal_prices)` returned `True` for the pilot week.
Every sign of a broken merge: the country filter must be leaking.

**Diagnosis.** It is correct. MIBEL is a **coupled market**. Whenever the Spain–Portugal
interconnector is not congested, the market clears as a single zone and both countries take the
same marginal price. Prices *split* only in the hours the interconnection binds.

Confirmed at two levels: cell 14 of `01_exploratory_analysis.ipynb` goes back to the raw OMIE
Parquet and shows that `PRICE_SP` and `PRICE_PT` are identical **in the source file**, so no code
of this project could be responsible. Measured over the full two-year panel:

- correlation **0.9953**
- **95.9%** of hours have *exactly* equal prices
- mean absolute spread **€0.94/MWh**
- ES above PT in **102** hours; PT above ES in **613** hours

**Fix.** None needed. The investigation became a finding: the 4.1% of splitting hours are the
hours the ES–PT interconnector constrains, and the asymmetry (PT dearer six times as often as ES)
says the binding direction is overwhelmingly Spain-to-Portugal.

**Why it is in this log anyway.** Because two debugging sessions and two notebook cells went into
it. A domain fact that *looks* like a bug costs exactly as much as a bug. The correct defence is
to write down the expected relationship between sources *before* looking at the merge output.

---

## D-08 — Cross-country contamination in the per-country merge *(resolved)*

**Symptom.** Suspicion, during the D-07 investigation, that the per-country join could attach one
country's prices to another country's rows.

**Diagnosis.** The join is `panel.merge(country_prices, on='timestamp', how='left')` — **on
`timestamp` alone**. It is only safe because `country_prices` was pre-filtered to a single country
immediately above (`build_panel.py:233`). If that filter were ever dropped or reordered, the merge
would fan out silently and multiply the row count.

**Fix.** A verification column: `country_prices['country_check'] = country` before the merge,
dropped after (`build_panel.py:239-244`). Its presence in the merged frame proves the join found
rows from the intended country.

**Why not merge on `['timestamp','country']` directly, which is unconditionally safe.** It would
be better. The `country_check` pattern is a debugging scaffold that survived into the source. Both
work; the compound key expresses the intent, whereas the check column merely tests it.

**Residual risk.** Low — the filter and the merge are three lines apart — but it is a fragile
construction that should be replaced by the compound-key join.

---

## D-09 — Glob patterns that match their own aggregates *(resolved by accident)*

**Symptom.** None observed.

**Diagnosis.** `download_all_countries_prices` writes per-country files
`prices_{ES}_{start}_{end}.parquet` **and** a combined `prices_all_countries_{start}_{end}.parquet`
into the same directory. `load_entsoe_prices` globs `prices_*.parquet`, which matches both, so
every row is inserted twice. The same pattern exists for weather: per-location files plus
`weather_all_*.parquet`, globbed as `weather_*.parquet`.

**Fix.** For prices the insert is `INSERT OR IGNORE` and the primary key absorbs it — correct by
accident, not by design. For **weather** the insert is a bare `INSERT INTO`
(`load_to_db.py:177`), so the duplicate load would raise a primary-key violation. That has never
happened only because weather was never downloaded (**D-15**).

**Why the combined file exists at all.** Convenience for ad-hoc analysis. The right fix is to put
it in a different directory, or name it so the loader's glob cannot match it, or make the loader's
glob explicit. Relying on the primary key to clean up after a glob you know is too broad is
working by coincidence.

---

## D-10 — `SELECT *` inserts with mismatched column order *(latent, unfired)*

**Symptom.** None — the affected code paths have never run.

**Diagnosis.** `INSERT INTO table SELECT * FROM df` matches columns **by position**, not by name.
Two loaders build frames whose column order does not match the table:

**Weather** (`load_to_db.py:177`):
```
DataFrame: timestamp, temperature_c, wind_speed_10m, wind_speed_100m,
           wind_direction_100m, solar_radiation, dni, diffuse_radiation,
           cloud_cover, location, latitude, longitude
Table:     timestamp, location, latitude, longitude, temperature_c, ...
```
Position 2 is `temperature_c` (double) going into `location` (varchar), and `location`
('Madrid') eventually lands on a double column, where the cast fails.

**Cross-border flows** (`load_to_db.py:217`) — after the rename, the frame is
`timestamp, flow_mw, country_from, country_to` against a table of
`timestamp, country_from, country_to, flow_mw`. Same class of error.

The ENTSO-E price loader **does** get it right — it explicitly reselects
`df[['timestamp','country','price_eur_mwh']]` and appends `energy_mwh`
(`load_to_db.py:139-141`). That is the pattern the other two should follow.

**Fix.** Not applied. The correction is one line per loader: name the columns in the `SELECT`, or
reindex the frame to the table's column order before inserting.

**Why it matters more than it looks.** `VARCHAR(2)` in DuckDB does not enforce length, so some
mis-ordered inserts will *succeed* and write garbage rather than raising. An error is a good
outcome here; silence is not.

---

## D-11 — Row-by-row inserts *(known, accepted)*

**Symptom.** Loading two years of prices takes minutes rather than the second or so the data
volume warrants.

**Diagnosis.** `fix_midnight_hour.py` — the script that actually built the current database —
iterates `price_spain.iterrows()`, loops `range(1, 25)` inside that, and issues **one
parameterised `INSERT` per hour**: ~35,000 individual statements for two years, each with its own
round trip. `load_to_db.py` is better (it builds a long DataFrame and bulk-inserts) but still
constructs it via `iterrows()` and a list of dicts.

**Fix.** Not applied; accepted as a deliberate trade during debugging. The row-by-row form gives
per-row error attribution, which is what you want when you are trying to find *which* row
collides. The vectorised form — `melt` the hour columns, build the timestamp arithmetically,
insert once — is what you want when the transformation is known-good.

**Why it is in the log.** Because it is the right trade *while debugging* and the wrong one now,
and the reason the slow version is still in the critical path is simply that it was the one that
finally worked. That is how slow code becomes permanent.

---

## D-12 — Three loaders, and the one in `src/` isn't the one that ran *(unresolved)*

**Symptom.** `python -m src.data.load_to_db` and `python fix_midnight_hour.py` both "load OMIE
prices", and they are not the same program.

**Diagnosis.** Four scripts can populate `prices_day_ahead`:

| Script | Status | Differences |
|---|---|---|
| `src/data/load_to_db.py` | documented entry point | deletes only ES/PT; bulk insert; Unicode output (**D-05**) |
| `load_omie_to_db.py` | superseded | deletes whole table; verbose; Unicode output |
| `fix_and_reload_data.py` | superseded | pandas-level `drop_duplicates` instead of PK |
| `fix_midnight_hour.py` | **what actually ran** | deletes whole table; row-by-row `INSERT OR IGNORE`; ASCII-only |

They differ in delete scope, deduplication mechanism and insert strategy. `fix_midnight_hour.py`'s
closing banner says it used `INSERT OR REPLACE`; it used `INSERT OR IGNORE`. Under the fold
collision (**D-04**) those two have *different outcomes* — `REPLACE` would keep the second price,
`IGNORE` keeps the first — so the banner misdescribes which of two real prices survived.

**Fix.** Not applied. The correct resolution: fold the fixes from `fix_midnight_hour.py` into
`src/data/load_to_db.py`, delete the three root-level scripts (git remembers them), and correct
the banner.

**Why it matters.** Provenance. Right now, reproducing the database requires knowing that the
documented command is not the one that was used. That is exactly the failure the whole
"reproducible pipeline" framing exists to prevent.

---

## D-13 — Inconsistent end-date semantics across sources *(unresolved)*

**Symptom.** None observed — weather and flows are empty.

**Diagnosis.** Within one function, three different window conventions:

| Source | Predicate | `end_date='2023-12-31'` yields |
|---|---|---|
| Skeleton (`create_hour_index`) | `end + 1 day − 1 hour` | through 2023-12-31 **23:00** |
| Prices (`build_price_panel`) | `timestamp <= '{end} 23:59:59'` | through 2023-12-31 **23:59** ✓ |
| Weather (`build_weather_panel`) | `timestamp < '{end}'` | through 2023-12-**30** 23:00 ✗ |
| Flows (`build_flows_panel`) | `timestamp < '{end}'` | through 2023-12-**30** 23:00 ✗ |

Weather and flows would silently lose the final 24 hours of any window.

**Fix.** Not applied. One convention, defined once, applied by every query — the natural choice
being half-open `[start, end)` everywhere, matching `create_hour_index`.

**Related.** All three queries interpolate dates into SQL with f-strings. With hard-coded literals
this is safe, but it is the wrong habit and DuckDB supports parameters.

---

## D-14 — The ML training set is empty and says it succeeded *(severity: high, unresolved)*

**Symptom.** `feature_engineering.py` runs to completion, prints `✓ Created N features` and
`✅ Feature engineering complete!`, and writes `train_data.parquet` and `test_data.parquet`. Both
files are **0 rows × 39 columns**.

**Diagnosis.** `create_all_features()` ends with an unrestricted drop
(`feature_engineering.py:216`):

```python
df = df.dropna()
```

The intent is to remove the leading rows where the 168-hour lag and rolling windows are undefined
— about a week per country, entirely reasonable. But `dropna()` with no `subset` drops a row if
**any** column is NaN, and the panel carries five weather columns (`temperature_c`,
`wind_speed_100m`, `solar_radiation`, `dni`, `cloud_cover`) that are **100% NaN** because Step 4
was never run. Every row qualifies. The frame empties, and the subsequent `print` statements
report on it cheerfully.

**Why nothing caught it.** The script prints `Dropped {n} rows` and `Final dataset: {n} rows` —
which would have said `Final dataset: 0 rows` — but the numbers are cosmetic, not assertions.
The train/test split then runs on an empty frame, `spain['timestamp'].max()` is `NaT`, comparisons
against `NaT` are `False`, both splits are empty, and `to_parquet` happily writes zero rows.

**Fix.** Not applied. Three changes, each independently sufficient to have caught it:

1. `df.dropna(subset=self.feature_names + ['price_eur_mwh'])` — drop only on columns the model
   actually uses.
2. An assertion: `assert len(df) > 0, "feature engineering produced no rows"`.
3. A column-level NaN audit before the drop, warning on any column above (say) 50% missing.

**Why it is the most instructive failure in the project.** Every individual piece is correct.
`dropna()` after lag construction is textbook. Leaving weather in the panel schema is right.
Running feature engineering before the weather download is a reasonable order. The defect lives
*between* the pieces — and the reporting was written to describe success rather than to verify it.

**Lesson.** A pipeline stage that cannot fail loudly will fail quietly. Every stage that reduces
row count should assert a lower bound.

---

## D-15 — Two of three sources were never acquired *(scope, unresolved)*

**Symptom.** `data/raw/entsoe/` and `data/raw/weather/` are empty directories.

**Diagnosis.** ENTSO-E is gated on a human-approved API key (Step 1). Open-Meteo has no gate and
would take about five minutes — it was simply never run, most likely because the ES/PT-only path
was working and momentum carried forward.

**Consequences, in order of severity:**

1. **No donor pool.** Difference-in-differences and synthetic control both require untreated
   units. Without France, Germany, Italy et al., the design collapses to a pre/post comparison in
   the treated units — which cannot separate the policy from the simultaneous collapse in European
   gas prices over the same window. This is the single largest gap between the project's stated
   goal and its current state.
2. **No leakage measurement.** The ES→FR flow series is the entire empirical content of the
   "subsidy leakage to France" question.
3. **No weather controls** — and, via **D-14**, an empty ML training set.
4. **No generation mix**, so no merit-order analysis.

**Fix.** Steps 3 and 4 as written. The code is complete and the design accommodates them: the
panel builder already queries weather and flows, `build_main_panel` already defaults to eight
countries, and the schema already has the tables. This is execution, not development — with the
caveat that **D-10** must be fixed first or both loads will fail, and **D-01** must be fixed or
the merge will be misaligned by 1–3 hours.

---

## D-16 — Bid curves: correctly scoped out *(deliberate)*

**Symptom.** The most analytically valuable dataset in the project is implemented and disabled.

**Diagnosis.** `OMIESupplyDemandCurvesImporter` fetches **one hour at a time**. Five years is
~43,800 requests at ~2 s each: 24–48 hours of uninterrupted downloading, over a public endpoint,
with no resume-from-partial support in the library.

**Fix.** A three-tier progressive entry point:

1. **First run** — prices and generation only (~10–20 min).
2. **Second run** — a 7-day, 24-hour bid-curve sample starting **15 June 2022**, the treatment
   date (~1 h).
3. **Third run** — the full download, behind an explicit flag, four logged warnings, and a
   10-second `Ctrl+C` window (`omie_ingest.py:395-408`).

`__main__` selects the tier automatically by inspecting which files already exist
(`omie_ingest.py:441-495`).

**Why the sample starts on the treatment date.** Because it is the week that matters. If you can
only afford one week of the expensive dataset, take the week that straddles the intervention.

**Why this is a good decision and not a failure.** The economics of acquisition are part of the
design. A 24-hour download that you have not proved you can parse is a 24-hour download you will
do twice. Ship the parser against a sample, *then* spend the day.

**Residual risk.** None incurred. The `bid_curves` table and indexes exist and are empty, so the
extension is a download, not a migration.

---

## D-17 — `input()` in `__main__` *(unresolved)*

**Symptom.** On the third and later runs, `python -m src.data.omie_ingest` stops and waits
(`omie_ingest.py:475-495`):

```python
choice = input("\nEnter choice (1/2/3): ").strip()
```

**Diagnosis.** Interactive prompts in a module entry point make the module unrunnable from cron,
CI, a notebook with no stdin, or a background process — where it does not error, it **hangs**.
`download_2year_dataset.py:25` has the same pattern, as an "are you sure" gate before a 2–3 hour
download.

**Fix.** Not applied. `argparse` with explicit flags (`--bid-curves`, `--full`, `--yes`),
defaulting to the safe behaviour.

**Why it was written this way.** It is a genuine safety feature — the prompt guards a 24-hour
download and an overwrite. The mistake is the *mechanism*, not the intent: a flag is equally safe
and does not block automation.

---

## D-18 — Documentation drift *(unresolved, cosmetic but corrosive)*

**Symptom.** Following the README does not work.

**Diagnosis.** Accumulated drift:

| Says | Is |
|---|---|
| `src/utils/dbutils.py`, `dbschema.py`, `timezoneutils.py` | `db_utils.py`, `db_schema.py`, `timezone_utils.py` |
| `prices_dayahead`, `crossborder_flows`, `bidcurves` | `prices_day_ahead`, `cross_border_flows`, `bid_curves` |
| README example: `from src.utils.dbutils import execute_query` | fails with `ModuleNotFoundError` |
| README: panel covers 2019–2024, 8 countries, ~420,000 rows | 2022–2023, 2 countries, 35,040 rows |
| `build_weather_panel` docstring: "weighted averages… based on population" | unweighted `mean` |
| `fix_midnight_hour.py` banner: "used INSERT OR REPLACE" | used `INSERT OR IGNORE` |
| README references a `docs/` directory | did not exist until this document created it |

**Why it matters more than it looks.** Every one of these is trivially fixable, and collectively
they are the difference between a repository someone can run and one they abandon. The
underscore-name discrepancy in particular means the README's *only* code examples fail on the
first line.

**Fix.** A README pass against the actual tree, and a `pytest` run in CI so the examples are
executable rather than aspirational.

---

# PART E — WHAT WAS ACTUALLY ACHIEVED

Measured from the artefacts on disk, not from documentation.

## E1. The pipeline

Five pipeline modules plus three utility modules — roughly 2,000 lines — alongside 12 test
functions across 8 test files (~1,000 lines), and a 401-line end-to-end diagnostic harness
(`diagnose_pipeline.py`) that checks ten layers: directory structure, installed packages, raw
files, database connectivity, schema presence, row counts and null rates, panel files, module
importability, critical-function behaviour, and a ranked list of recommended actions.

That diagnostic is worth calling out as an achievement in its own right. It is the tool that turns
"it doesn't work" into "layer 6 passes, layer 7 fails, here is the row count", and most of Part D
was found with it.

## E2. Data acquired

| Source | Status | Volume |
|---|---|---|
| OMIE day-ahead prices | **complete** | 8 quarterly Parquet files, 2022-01-01 → 2023-12-31, ~480 KB |
| OMIE bid curves | not run — deliberate (**D-16**) | — |
| ENTSO-E | **not run** (**D-15**) | — |
| Open-Meteo | **not run** (**D-15**) | — |

## E3. The database

`data/mibel.duckdb`, 5.3 MB, five tables:

| Table | Rows |
|---|---|
| `prices_day_ahead` | **35,032** |
| `weather` | 0 |
| `generation` | 0 |
| `cross_border_flows` | 0 |
| `bid_curves` | 0 |

`prices_day_ahead` holds 17,516 hours each for ES and PT, spanning 2022-01-01 00:00 to
2023-12-31 23:00. The complete grid is 17,520; the four absent hours are `2022-03-27 01:00`,
`2022-03-28 00:00`, `2023-03-26 01:00`, `2023-03-27 00:00` — both spring DST transitions, per
**D-01**/**D-03**.

## E4. The panel

`data/processed/main_panel_2022-01-01_2023-12-31.parquet`, 304 KB:

- **35,040 rows** = 2 countries × 17,520 hours — the skeleton is complete and exactly balanced,
  which is decision **B5** working as designed.
- **16 columns**: `timestamp`, `country`, `price_eur_mwh`, 5 weather columns (all NaN),
  8 calendar/policy columns.
- **0.023% missing prices** (4 hours per country), all attributable to **D-01**.
- No flow columns — the flows table is empty, so the pivot produced nothing and the merge was a
  silent no-op.

A 7-day pilot panel (`main_panel_2022-06-15_2022-06-22.parquet`) also exists — the artefact
against which the merge machinery was debugged before being run at scale. Building a one-week
version first is the reason **D-06** was found in minutes rather than in a two-year run.

## E5. Empirical findings

All figures computed from the delivered panel. **Caveat:** these are pre/post comparisons within
the treated units, not causal estimates — there is no control group (**D-15**), and European gas
prices fell sharply over the same window. Read them as descriptive.

**Price levels, Spain:**

| Period | Mean €/MWh | Median | Min | Max | s.d. | Hours |
|---|---|---|---|---|---|---|
| Pre-exception (1 Jan – 14 Jun 2022) | **211.80** | 206.66 | 1.03 | 700.00 | 67.45 | 3,958 |
| During (15 Jun 2022 – 31 Dec 2023) | **102.66** | 105.00 | 0.00 | 300.00 | 47.84 | 13,558 |

Portugal is within €1/MWh of Spain in both periods (212.03 pre, 103.59 during).

**The distributional signature.** The mean falls 52%, but the interesting numbers are in the
tails:

- The maximum falls from **€700** to **€300/MWh** — and exactly 10 hours sit at precisely 300.00.
  A hard ceiling appearing in the support of the distribution is what a cap *looks like*, and it
  is visible without any model.
- The standard deviation falls **29%** (67.45 → 47.84). The cap compressed dispersion, not just
  the level.
- The minimum reaches **€0.00** during the treatment window, so the floor was not lifted — the
  compression is one-sided, which is precisely what a cap on the marginal technology predicts.

**Market coupling.** Correlation 0.9953; **95.9%** of hours have exactly identical ES and PT
prices; mean absolute spread €0.94/MWh; ES above PT in 102 hours, PT above ES in 613. MIBEL is a
single price zone for all practical purposes, and when it splits it is almost always Portugal
paying more.

**Diurnal structure.** Averaged over two years, the peak is hour **21** (€159.3/MWh) and the
trough hour **15** (€102.8/MWh) — a peak-to-trough spread of €56.5/MWh. The 15:00 trough is the
solar signature: midday PV output pushes the marginal unit down the merit order. *Note that, per
**D-01**, these hour labels are Iberian local time in winter and local + 1 in summer, not UTC.*

## E6. Downstream artefacts

- **Two analysis notebooks**, 44 cells: time series with the intervention marked, price duration
  curves with percentile annotations, hourly profiles with dispersion bands, coupling and spread
  analysis, weekday/weekend comparison, rolling volatility, and a summary dashboard.
- **Five Power BI extracts** (3.9 MB total) and `dashboard.pbix`.
- **A feature-engineering module** producing 39 columns — 6 lags, 8 rolling statistics, 6 cyclic
  calendar encodings, 2 binary flags, 2 policy features — with leak-free `shift(1)` construction
  and a chronological 30-day holdout. *The module is correct; its output is empty* (**D-14**).

## E7. What this is, honestly

**A production-quality ingestion, storage and panel-construction pipeline for MIBEL day-ahead
prices**, delivering a complete, balanced, analysis-ready two-year hourly panel for Spain and
Portugal, with a working analysis and visualisation layer on top and a real diagnostic harness
underneath.

It is **not yet** the causal-inference dataset described in the brief. The three things standing
between here and there are: the timestamp defect (**D-01**), the two unacquired sources
(**D-15**), and the silent failure in feature engineering (**D-14**). All three are known, all
three are localised, and none requires re-downloading anything that has already been downloaded.

---

# PART F — KNOWN DEFECTS, RANKED

| # | Defect | Severity | Effort | Ref |
|---|---|---|---|---|
| 1 | Timestamps are Europe/Lisbon wall time, not UTC; 1 h off in winter, 3 h in summer | **critical** | half a day | D-01 |
| 2 | ML training set is empty; the script reports success | **high** | 1 hour | D-14 |
| 3 | ENTSO-E and Open-Meteo never downloaded → no donor pool, no controls, no leakage measure | **high** | 3 hours | D-15 |
| 4 | Weather and flow loaders will fail or corrupt on `SELECT *` column-order mismatch | **high** (latent) | 30 min | D-10 |
| 5 | Four scripts can load prices; the documented one is not the one that ran | medium | 2 hours | D-12 |
| 6 | DST fold discards two real prices per country per year | medium | folded into #1 | D-03/04 |
| 7 | Inconsistent end-date semantics silently drop the final day of weather and flows | medium | 15 min | D-13 |
| 8 | `input()` in module entry points blocks automation | medium | 30 min | D-17 |
| 9 | Unicode output still crashes on Windows consoles in two files | medium | 15 min | D-05 |
| 10 | README references wrong module and table names; examples fail on line 1 | medium | 1 hour | D-18 |
| 11 | Row-by-row inserts, ~1000× slower than necessary | low | 1 hour | D-11 |
| 12 | Debug `print()` calls left in `build_price_panel` | low | 5 min | Step 7 |
| 13 | Per-country merge relies on a prior filter rather than a compound key | low | 10 min | D-08 |
| 14 | Glob patterns match their own aggregate files | low | 15 min | D-09 |
| 15 | Dates interpolated into SQL by f-string | low | 20 min | D-13 |

**Suggested order.** #1 first — every other number in the project depends on the timestamps being
what they claim. Then #4 (it blocks #3), then #3, then #2. Those four take the project from
"a clean price panel" to "the dataset the brief describes".

---

# PART G — REBUILD CHECKLIST

From an empty directory to the current state:

```bash
# 1. Environment
python3 -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
python create_env.py                    # ENTSO-E key -> .env

# 2. Verify the skeleton before downloading anything
python diagnose_pipeline.py             # expect failures in sections 3, 6, 7

# 3. Ingest  (Steps 3 and 4 are currently unrun - see D-15)
python download_2year_dataset.py        # OMIE, 8 quarterly chunks
# python -m src.data.entsoe_ingest      # needs D-10 fixed first
# python -m src.data.weather_ingest     # needs D-10 fixed first

# 4. Schema + load
python -m src.utils.db_schema           # creates 5 tables + 8 indexes
python fix_midnight_hour.py             # NOTE: this, not load_to_db.py - see D-12

# 5. Verify the load
python diagnose_duplicates.py           # expect: no duplicates
python diagnose_pipeline.py             # sections 1-6 should now pass

# 6. Build the panel
python -c "from src.data.build_panel import build_main_panel; \
           build_main_panel('2022-01-01','2023-12-31',['ES','PT'])"
# expect exactly 35,040 rows

# 7. Analysis
jupyter notebook notebooks/full_panel.ipynb     # figures + Power BI exports

# 8. Tests
python -m pytest tests/
```

**Verification gates.** Do not proceed past a step whose gate fails:

| After | Check | Expected |
|---|---|---|
| Ingest | `ls data/raw/omie/*.parquet` | 8 files |
| Load | `SELECT COUNT(*) FROM prices_day_ahead` | 35,032 |
| Load | duplicate check in `fix_midnight_hour.py` | none |
| Panel | `len(panel)` | 35,040 exactly |
| Panel | `panel.price_eur_mwh.isna().mean()` | < 0.001 |
| Panel | `panel.groupby('country').size()` | 17,520 each |

---

## Closing note — what this project actually teaches

Three lessons, in the order they were learned the hard way.

**1. Keep the raw bytes.** Decision **B2** was the cheapest insurance in the project. The OMIE
transformation was wrong three times (**D-02**) and the timestamp handling is still wrong
(**D-01**); every one of those fixes costs seconds instead of hours purely because the downloaded
files were never modified. When the fix for the most serious defect in the project is "re-run the
load", you have built the right thing.

**2. Silent failures are the expensive ones.** Every genuinely costly bug here failed *quietly*:
`merge` returning all-NaN with a perfect row count (**D-06**); `dropna()` emptying a frame while
the script prints a tick (**D-14**); a storage layer rewriting every timestamp with no warning
(**D-01**). The loud failures — the encoding crash, the primary-key violation — were fixed in
minutes. The defence is not more care; it is **assertions on invariants** at every stage boundary:
match rates after joins, lower bounds on row counts, an explicit check that the timestamp column
means what the docstring says.

**3. Domain knowledge is a debugging tool.** Identical Spanish and Portuguese prices looked exactly
like a broken merge and were a textbook consequence of market coupling (**D-07**). A price profile
peaking at hour 21 looked fine and was the thread that unravelled **D-01**. You cannot debug
electricity market data without knowing what electricity markets do — and the corollary is that
the invariants worth asserting are usually economic ones, not just structural ones.

---

*Manual compiled 2026-09-14 against commit `85291bf`. All figures in Part E measured directly
from `data/mibel.duckdb`, `data/processed/main_panel_2022-01-01_2023-12-31.parquet`,
`data/raw/omie/*.parquet` and `data/powerbi_exports/*.csv`.*
