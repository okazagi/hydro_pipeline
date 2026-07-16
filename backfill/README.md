# Backfill Tools

Everything needed to backfill historical data into the pipeline database.
Run these scripts manually as needed — none of them are part of the daily automated pipeline.

## Structure

```
backfill/
├── scripts/        Active backfill scripts (run from project root with here())
├── data/
│   ├── RFSPV/      Raw LiCOR JSON exports by year (2016–2026) + supporting scripts
│   └── RFGLS/      RFGLS backfill CSV + transform/compare scripts
├── R/              Helper functions sourced by backfill scripts
│   ├── functions_clean.R
│   └── functions_import.R
└── rfgls_backfill_notes.md   Notes on RFGLS sensor history and corrections
```

## Scripts

| Script | Purpose |
|---|---|
| `rfspv_backfill_transform.R` | Transforms raw RFSPV year-by-year JSON exports into master schema |
| `rfspv_backfill_inventory.R` | Audits RFSPV sensor serial tenure and coverage gaps |
| `usgs_historical_backfill.R` | Fetches and loads historical USGS data |
| `usgs_RoaringForkAspen_backfill.R` | Backfill specific to Roaring Fork @ Aspen gauge |
| `nwcc_historical_backfill.R` | Fetches and loads historical NWCC/SNOTEL data |
| `import_old_data.R` | Imports pre-pipeline legacy CSVs into the database |
| `indepass_transform.R` | Transforms raw Independence Pass data to master schema |
| `indepass_clean_split.R` | Cleans and splits Independence Pass source data |
| `indepass_qc.R` | QC pass for Independence Pass historical data |

## How to run

All scripts use `here()` which roots to the project directory (`roaring-fork-hydro/`).
Run from there:

```bash
cd /home/okazagi/services/roaring-fork-hydro
conda run -n rfh Rscript backfill/scripts/rfspv_backfill_transform.R
```

## Notes

- `usgs_RoaringForkAspen_backfill.R` references `current_pipeline/usgs_transform.R`
  which has been moved to `roaring-fork-hydro-archive/current_pipeline/`. If re-running
  this script, update that source() path to `scripts/usgs_transform.R`.
- RFSPV and RFGLS backfill data is already loaded into the live `hydro_data.db`.
  Re-running transform scripts will regenerate CSVs; re-running the DB load will
  upsert (INSERT OR REPLACE) so no data loss risk.
