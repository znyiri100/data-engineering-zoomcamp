# Week 4: Analytics Engineering with dbt

## Overview

Week 4 of the Data Engineering Zoomcamp focused on **dbt (data build tool)** — an open-source analytics engineering framework that transforms data inside your data warehouse. This module covered the **T in ELT** (Extract, Load, Transform), turning raw warehouse data into clean, analytics-ready models.

The project involved building a dbt pipeline using **NYC Yellow & Green Taxi data (2019–2020)** to learn analytics engineering fundamentals, data modeling concepts, and real-world dbt workflows.

## Project Repository

🔗 **dbt Project**: [taxi_rides_ny](https://github.com/znyiri100/taxi_rides_ny)

This repository contains the complete dbt project with both local (DuckDB) and cloud (BigQuery) deployment configurations.

## Setup Options

### ☁️ Cloud Setup
- **Stack**: BigQuery + dbt Cloud
- **Cost**: Free tier available (dbt Cloud Developer), BigQuery pricing varies
- **Prerequisite**: BigQuery data from Module 3

### 💻 Local Setup
- **Stack**: DuckDB + dbt Core
- **Cost**: Free
- **Challenge**: Working with 100+ million rows on a 16GB notebook required DuckDB optimization to handle out-of-memory errors

## Key Learnings

✔️ Built scalable transformation pipelines with dbt  
✔️ Modeled raw data into clean, analytics-ready layers  
✔️ Enforced data quality through automated testing  
✔️ Mapped and managed model dependencies and lineage  
✔️ Derived business insights from NYC taxi revenue data  
✔️ Delivered solutions in both cloud and local setups  

## Technical Highlights

### Project Structure
- **models/staging**: Raw data transformation and cleaning
- **models/marts**: Business logic and reporting tables (e.g., `fct_monthly_zone_revenue`)
- **macros**: Custom dbt macros with dialect-specific logic for DuckDB/BigQuery
- **local_download.py**: Script to seed local DuckDB with data

### Deployment Workflows

#### Local Deployment (DuckDB)
```bash
# Download and prepare data
python local_download.py

# Run dbt
dbt deps
dbt build --target dev

# Query results
duckdb taxi_rides_ny.duckdb "SELECT * FROM prod.fct_monthly_zone_revenue LIMIT 10"
```

#### Cloud Deployment (BigQuery)
```bash
# Set GCP project
export GCP_PROJECT_ID="your-gcp-project-id"

# Authenticate
gcloud auth application-default login

# Run dbt
dbt deps
dbt build --target prod
```

## Challenges & Solutions

The dataset was intentionally large (100+ million rows) to highlight the limitations of local environments and teach important engineering lessons about:
- Scale and infrastructure tradeoffs
- Memory optimization techniques
- When to use local vs. cloud solutions

Successfully troubleshooting the out-of-memory errors on a 16GB notebook required DuckDB optimization and insights from fellow course participants.

## Resources

- 📝 **LinkedIn Post**: [Week 4 Completion Summary](https://www.linkedin.com/posts/znyiri_dataengineering-dbt-analyticsengineering-activity-7429174005154152448-MV9z?utm_source=share&utm_medium=member_desktop&rcm=ACoAAAH8bmUBWjaIC4CIzVGn3MSJ2Tnw9AN0MVw)
- 💻 **GitHub Repository**: [taxi_rides_ny dbt project](https://github.com/znyiri100/taxi_rides_ny)
- 🎓 **Course**: [DataTalks.Club Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp)

## Technologies Used

- **dbt Core / dbt Cloud**: Data transformation framework
- **BigQuery**: Cloud data warehouse
- **DuckDB**: Local analytical database
- **Python**: Data preparation and scripting
- **NYC TLC Data**: Yellow & Green taxi trip records (2019-2020)

---

**Tags**: #DataEngineering #dbt #AnalyticsEngineering #BigQuery #DuckDB #ELT #DataTalksClub
