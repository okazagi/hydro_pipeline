# RFGLS backfill notes

Note written **2026-05-11**, covering work done **2026-05-04**.

## What we did

Filled in 2026 RFGLS gaps in `hydro_data_April.db` from `RFGLS_backfill/2026_RFGLS-2026_05_04_16_22_08_UTC.csv` (Jan 1 → May 4, 2026, 35,613 rows).

### Phases run

1. **Transform** (`RFGLS_backfill/transform_backfill.R`):
   - Used `config/master_key.json` (GLS active logger 21535700) for sensor mappings — picks up our corrected entries (no `_B`, soil temp at 20 cm, typo fixes).
   - Converted °F → °C, in → cm.
   - Clipped to DB's last RFGLS timestamp **2026-04-27 08:40:00** (dropped 2,100 rows past that).
   - Dropped 25,134 battery-only off-cadence rows (5-min ticks between 20-min reads), matching live `licor_transform.R` behavior.
   - Output: `RFGLS_backfill/rfgls_backfill_combined.csv` — 8,379 rows × 13 cols, 9 sensors at 100 %.

2. **Compare** (`RFGLS_backfill/compare_to_db.R`):
   - DB had 8,360 rows in 2026; backfill had 8,379 (19 backfill-only timestamps, mostly daily 08:40 in April — real DB ingest gaps).
   - On shared timestamps: AirTemp, RH, Rain, Battery match 100 % (corr = 1.0).
   - Dewpoint: backfill had 6 extras (otherwise corr = 1.0).
   - **WC gaps in DB**: WC_5cm had only 2,022/8,360, WC_20cm and WC_50cm only 1,524/8,360. Backfill had all 8,360 (corr = 1.0 where overlap existed).
   - **WC outage windows**: 2026-01-01 → 2026-03-13 (5,136 rows) and 2026-03-18 → 2026-04-06 (1,350 rows), plus smaller daily gaps. Three months of silently-missing WC values in the live ingest.
   - **SoilTemp_C_20cm in DB**: 0 rows. **SoilTemp_C_5cm in DB**: 8,360 rows. Confirmed mislabel — DB.SoilTemp_C_5cm vs backfill.SoilTemp_C_20cm: corr = 1.0, median |diff| = 0.0022, max |diff| = 0.0067.
   - Output: `metadata/rfgls_backfill_vs_db_overlap.csv`.

3. **Replace** (`RFGLS_backfill/replace_db_with_backfill.R`):
   - Backed up DB → `old_db/hydro_data_April_pre_rfgls_backfill.db`.
   - Transactional DELETE of all RFGLS rows where `Date_UTC >= '2026-01-01'` + INSERT of all 8,379 backfill rows conformed to master schema.
   - Set `Station_ID = '5'` (numeric code per RF-station convention) for all 8,379. Was previously `'RFGLS'` after the 2025-09-25 pipeline switch.
   - `SoilTemp_C_5cm` legacy column cleared (data correctly placed in `SoilTemp_C_20cm`).

### Decisions made

- **Option 2 replacement strategy**: where backfill had a value, write it (vs pure gap-fill).
- **Mislabel fix**: SoilTemp_C_5cm 2026 values cleared and rewritten into SoilTemp_C_20cm.
- **Station_ID cleanup**: flipped post-2025-09-25 rows from `'RFGLS'` to `'5'` while we were touching them. Larger dual-coding cleanup across all stations still pending.
- **Scope**: 2026 only. Did not touch pre-2026 RFGLS rows (likely affected by the same pre-2024 broken-transform issue identified during the RFSPV audit — to be addressed in a future station-by-station backfill).

### Post-write state (verified)

```
2026 RFGLS: 8,379 rows
  AirTemp_C, RH, Rain_cm, WC5/20/50cm, SoilTemp_C_20cm, Battery: 8,379 (100%)
  Dewpoint_C: 8,376 (3 NAs in source)
  SoilTemp_C_5cm: 0  (cleared)
  Station_ID: '5' for all 8,379
```

## Still pending for RFGLS

- **Pre-2026 audit**: RFGLS has 270k+ rows back to 2015-08-14 under `Station_ID='5'`. Per the RFSPV findings, pre-2024 values are likely from the broken old transform. Would need a full historical backfill (raw HOBOware exports for 2015–2025) to verify and replace if needed.
- **Live pipeline gap diagnosis**: the WC outage 2026-01-01 → 2026-03-13 means the live LiCor ingest dropped WC channels for ~10 weeks without alerting. Worth investigating why (sensor_key mismatch? sensor offline at LiCor side that then recovered? transform error?) to prevent recurrence.
- **`SoilTemp_C_5cm` legacy column cleanup at other stations**: same mislabel pattern likely exists for stations that haven't been backfilled yet.
