# IPEDS Completions Pipeline

A data engineering portfolio project built with **dbt**, **PostgreSQL**, and Python.

## Overview

This pipeline ingests IPEDS (Integrated Postsecondary Education Data System) completions data from the National Center for Education Statistics (NCES) and transforms it into clean, tested, analysis-ready models.

**Data source:** IPEDS Completions 2023-24 — Awards and degrees conferred by program (6-digit CIP code), award level, race/ethnicity, and gender.

## Stack

| Tool | Purpose |
|------|---------|
| PostgreSQL 16 | Local data warehouse |
| dbt 1.11 | Data transformation and testing |
| DBeaver | Database GUI |
| Python 3.11 | Environment and tooling |

## Project Structure
```
ipeds-pipeline/
├── data/               # Raw source files (not tracked in git)
├── ipeds_dbt/
│   ├── models/
│   │   ├── sources.yml           # Source definitions
│   │   ├── schema.yml            # Model documentation and tests
│   │   └── stg_completions_2024.sql  # Staged completions model
│   └── dbt_project.yml
└── README.md
```

## Models

### `stg_completions_2024`
Cleans and renames the raw IPEDS completions table. Filters out zero-completion rows and selects key race/ethnicity and gender columns.

**Tests:** not_null on key fields, accepted_values on majornum

## Getting Started
```bash
# 1. Clone the repo
git clone https://github.com/JonayahJ/ipeds-pipeline.git

# 2. Create the database
createdb ipeds_dev

# 3. Load raw data (download from NCES IPEDS Data Center)
psql ipeds_dev < load_raw.sql

# 4. Run dbt
cd ipeds_dbt
dbt run
dbt test
```

## About

Built as a freelance portfolio project demonstrating end-to-end data engineering skills: raw data ingestion, transformation, testing, and documentation.
