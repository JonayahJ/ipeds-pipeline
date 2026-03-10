# IPEDS Completions Pipeline

A data engineering portfolio project built with **dbt**, **PostgreSQL**, and **Apache Superset**.

## Overview

This pipeline ingests IPEDS (Integrated Postsecondary Education Data System) completions and institutional data from the National Center for Education Statistics (NCES) and transforms it into clean, tested, analysis-ready models exploring racial equity in postsecondary completions.

**Data source:** IPEDS 2023-24
- Completions by program (6-digit CIP code), award level, race/ethnicity, and gender
- Institutional directory — name, location, control type, sector, and classification

## Stack

| Tool | Purpose |
|------|---------|
| PostgreSQL 16 | Local data warehouse |
| dbt 1.11 | Data transformation and testing |
| Apache Superset | Visualization and dashboarding |
| DBeaver | Database GUI |
| Python 3.11 | Environment and tooling |

## Project Structure
```
ipeds-pipeline/
├── data/                              # Raw source files (not tracked in git)
├── ipeds_dbt/
│   ├── models/
│   │   ├── sources.yml                # Source definitions
│   │   ├── schema.yml                 # Model documentation and tests
│   │   ├── stg_completions_2024.sql   # Staged completions model
│   │   ├── stg_institutions_2024.sql  # Staged institutional directory
│   │   └── mart_completions_by_race.sql  # Mart — completions by race and institution
│   └── dbt_project.yml
└── README.md
```

## Data Files

| File | Table | Rows | Description |
|------|-------|------|-------------|
| c2024_a.csv | raw_completions_2024 | 307,707 | Completions by program, award level, race/ethnicity, gender |
| hd2024.csv | raw_institutions_2024 | 6,072 | Institutional directory |

Downloaded from: https://nces.ed.gov/ipeds/datacenter/DataFiles.aspx

## Models

### `stg_completions_2024`
Cleans and renames the raw IPEDS completions table. Filters out zero-completion rows and selects key race/ethnicity and gender columns.

**Tests:** not_null on key fields, accepted_values on majornum

### `stg_institutions_2024`
Cleans the institutional directory. Decodes control type, institution level, sector, and institution size from numeric codes to plain English labels. Converts IPEDS 1/2 flag convention to proper booleans for HBCU, tribal, hospital, and medical flags.

### `mart_completions_by_race`
Joins completions to institutions. Adds CIP family groupings, award level labels, race percentage calculations, students of color totals, and a white/black completion gap calculation.

**Key design decisions:**
- `cipcode = '99'` excluded — aggregate row, not a real program
- Territories (PR, GU, VI, MP, AS) included but flagged with `is_territory`
- No minimum completion threshold hardcoded — filter at the BI layer
- CIP family derived from 2-digit CIP prefix

## Getting Started
```bash
# 1. Clone the repo
git clone https://github.com/JonayahJ/ipeds-pipeline.git

# 2. Create the database
createdb ipeds_dev

# 3. Download IPEDS data files from NCES and place in /data
#    - c2024_a.csv (Completions)
#    - hd2024.csv (Institutional Directory)

# 4. Load raw data
psql ipeds_dev -c "CREATE TABLE raw_completions_2024 (...);"
psql ipeds_dev -c "\copy raw_completions_2024 FROM 'data/c2024_a.csv' CSV HEADER;"

psql ipeds_dev -c "CREATE TABLE raw_institutions_2024 (...);"
psql ipeds_dev -c "\copy raw_institutions_2024 FROM 'data/hd2024.csv' CSV HEADER;"

# 5. Activate Python environment and run dbt
source ~/.venvs/datawork/bin/activate
cd ipeds_dbt
dbt run
dbt test
```

## About

Built as a freelance portfolio project demonstrating end-to-end data engineering: raw data ingestion, multi-source joins, transformation, testing, documentation, and visualization. Focus area is racial equity in postsecondary completions across institution types, program families, and award levels.
