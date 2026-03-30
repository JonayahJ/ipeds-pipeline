# IPEDS Completions Pipeline

A data engineering portfolio project built with **dbt**, **PostgreSQL**, and **Apache Superset**.

## Overview

This pipeline ingests IPEDS (Integrated Postsecondary Education Data System) completions and institutional data from the National Center for Education Statistics (NCES) and transforms it into clean, tested, analysis-ready models exploring racial equity in postsecondary completions.

**Guiding question:** Are students of color being concentrated in lower-credential pathways, and what does that mean for long-term economic mobility?

**Data source:** IPEDS 2023-24
- Completions by program (6-digit CIP code), award level, race/ethnicity, and gender
- Institutional directory — name, location, control type, sector, and classification

## Live Dashboard

**[Black Student Credential Attainment: Program, Tier, and Geography — 2023-24](https://data.thinkhalcyon.com)**

An interactive Superset dashboard exploring racial equity in postsecondary completions across institution types, program families, credential tiers, and geography.

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
│   │   ├── staging/
│   │   │   ├── sources.yml                # Source definitions
│   │   │   ├── schema.yml                 # Model documentation and tests
│   │   │   ├── stg_completions_2024.sql   # Staged completions model
│   │   │   └── stg_institutions_2024.sql  # Staged institutional directory
│   │   └── marts/
│   │       └── mart_completions_by_race.sql  # Mart — completions by race and institution
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
Joins completions to institutions. Adds CIP family groupings, award level labels, award level tiers, race percentage calculations, students of color totals, white/black completion gap, and ISO state codes for geographic visualization.

**Key design decisions:**
- `cipcode = '99'` excluded — aggregate row, not a real program
- Territories (PR, GU, VI, MP, AS) included but flagged with `is_territory`
- No minimum completion threshold hardcoded — filter at the BI layer
- CIP family derived from 2-digit CIP prefix
- Award level codes decoded to plain English labels and grouped into six credential tiers
- `award_level_sort` column added for correct non-alphabetical ordering in BI tools
- `state_iso` column added in `US-XX` format for Superset Country Map compatibility
- Materialized as a table (not a view) for performance and portability

## Dashboard

The Superset dashboard tells a five-part data story:

| Step | Story Beat | Chart |
|------|-----------|-------|
| 1 | Credential landscape overview | Total Completions by Program Family |
| 2 | Who completes what by credential tier | Completions by Credential Tier and Race |
| 2 | Who completes what by program | Completions by Race and Program Family |
| 3 | Where are gaps widest | White-Black Completion Gap by Program Family |
| 3 | Black rate vs population parity | Black Student Completion Rate by Field Relative to Population Share |
| 4 | Geographic distribution | Black Student Share of Total Completions by State |
| 4 | Geographic + credential tier | Black Completions by Credential Tier by State |
| 4 | Geographic + program family | Black Completions by Program Family — All States |
| 5 | Program x credential concentration | Concentration of Black Completions Across Fields and Degree Types |

**Key findings:**
- TBD

## Getting Started

```bash
# 1. Clone the repo
git clone https://github.com/JonayahJ/ipeds-pipeline.git

# 2. Create the database
createdb ipeds_dev

# 3. Download IPEDS data files from NCES and place in /data
#    - c2024_a.csv (Completions)
#    - hd2024.csv (Institutional Directory)
#    Download from: https://nces.ed.gov/ipeds/datacenter/DataFiles.aspx

# 4. Load raw data (see SETUP.md for full CREATE TABLE schemas)
psql ipeds_dev < load_completions.sql
psql ipeds_dev < load_institutions.sql

# 5. Activate Python environment and run dbt
source ~/.venvs/datawork/bin/activate
cd ipeds_dbt
dbt run
dbt test
```

## About

Built as a freelance data engineering portfolio project under **Think Halcyon, LLC**, demonstrating end-to-end pipeline work: raw data ingestion, multi-source joins, transformation, testing, documentation, and visualization. Focus area is racial equity in postsecondary completions across institution types, program families, credential tiers, and geography.

**Think Halcyon** is a data consultancy focused on... TBD. Learn more at [thinkhalcyon.com](https://thinkhalcyon.com).