# TPC-A Airline Practice Set

<div align="center">
  <img src="https://readme-typing-svg.demolab.com?font=Fira+Code&weight=700&size=28&pause=1000&color=00C2FF&center=true&vCenter=true&width=820&lines=TPC-A+Airline+Practice+Set;Python+%2B+PySpark+Data+Pipeline;Bronze+%E2%86%92+Silver+%E2%86%92+Gold;Business+Analytics+and+Insights" alt="TPC-A Airline Practice Set" />
</div>

<div align="center">
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python" />
  <img src="https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white" alt="PySpark" />
  <img src="https://img.shields.io/badge/Jupyter-F37626?style=for-the-badge&logo=jupyter&logoColor=white" alt="Jupyter" />
  <img src="https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=sqlite&logoColor=white" alt="SQL" />
  <img src="https://img.shields.io/badge/Data-Lakehouse-0EA5E9?style=for-the-badge" alt="Data Lakehouse" />
</div>

<p align="center">
  A modern airline data engineering and analytics project built around the TPC-A benchmark pattern, using a medallion-style architecture to transform raw airline data into clean, trusted, and business-ready insights.
</p>

<div align="center">
  <img src="https://img.shields.io/badge/Architecture-Medallion%20Bronze%20%2F%20Silver%20%2F%20Gold-8B5CF6?style=flat-square" alt="Architecture" />
  <img src="https://img.shields.io/badge/Focus-Data%20Quality%20%26%20Reconciliation-10B981?style=flat-square" alt="Data quality" />
  <img src="https://img.shields.io/badge/Domain-Airline%20Operations-EC4899?style=flat-square" alt="Domain" />
</div>

## Overview

This repository demonstrates a complete airline analytics workflow using a layered data pipeline:

- Bronze: raw ingestion and setup
- Silver: cleaning, validation, deduplication, and quarantine handling
- Gold: business-ready dimensional and fact tables for analysis
- SQL / BI: exploration and business queries for reporting and decision-making

The project is designed to model how real-world airline datasets are processed in a data engineering environment, with emphasis on:

- data quality enforcement
- referential integrity checks
- invalid record quarantine
- canonical business dimensions
- reporting-ready fact tables

## Project Architecture

<div align="center">
  <table>
    <tr>
      <td align="center" width="33%">
        <img src="https://img.shields.io/badge/Bronze-Raw%20Ingestion-2563EB?style=for-the-badge" alt="Bronze layer" />
        <br />Source data ingestion and schema setup
      </td>
      <td align="center" width="33%">
        <img src="https://img.shields.io/badge/Silver-Clean%20%26%20Validated-22C55E?style=for-the-badge" alt="Silver layer" />
        <br />Normalization, cleaning, and quality rules
      </td>
      <td align="center" width="33%">
        <img src="https://img.shields.io/badge/Gold-Business%20Ready-EC4899?style=for-the-badge" alt="Gold layer" />
        <br />Dimensional tables and booking facts
      </td>
    </tr>
  </table>
</div>

## Repository Structure

```text
TPC-A-Airline-Practice-Set/
├── bronze/
│   ├── 00_setup.ipynb
│   └── 01_ingestion.ipynb
├── silver/
│   ├── airline_clean.py
│   ├── airline_route_clean.py
│   ├── booking_clean.py
│   ├── flight_booking_clean.py
│   ├── nation_clean.py
│   ├── passenger_clean.py
│   ├── region_clean.py
│   └── route_clean.py
├── gold/
│   ├── gold_facts_and_dim.py
│   └── Data Quality and Reconciliation.ipynb
├── SQLBusiness Analysis/
│   └── business_quries.ipynb
├── explorations/
│   └── code_review.ipynb
├── README.md
└── .gitignore
```

## Key Components

### 1) Bronze Layer
The bronze layer handles raw ingestion and initial setup for airline, route, booking, and other operational datasets. This stage preserves the original source records and prepares them for downstream transformations.

### 2) Silver Layer
The silver layer performs:

- data standardization
- trimming and type conversion
- duplicate detection
- invalid record quarantine
- referential integrity validation
- incremental merge logic into curated tables

The cleaning scripts in `silver/` include processing for:

- airlines
- routes
- passengers
- regions
- nations
- bookings
- flight bookings

### 3) Gold Layer
The gold layer creates analysis-ready outputs:

- `dim_passenger`
- `dim_route`
- `dim_airline`
- `dim_geography`
- `fact_bookings`
- KPI views for annual booking performance

This stage is designed for reporting, dashboarding, and SQL-driven business analysis.

### 4) SQL Business Analysis
The `SQLBusiness Analysis` notebook focuses on business questions and analytical queries based on the cleaned and enriched datasets.

## Business Value

This project supports airline business use cases such as:

- booking trend analysis
- revenue and discount analysis
- passenger geography segmentation
- airline route performance measurement
- fare and capacity evaluation
- operational quality checks
- KPI tracking over time

## Tech Stack

- Python
- PySpark
- Jupyter Notebook
- SQL
- Delta-style table patterns
- Data validation and reconciliation workflows

## Typical Workflow

1. Raw data is loaded into the Bronze layer.
2. Silver scripts clean and validate each business entity.
3. Faulty records are quarantined instead of silently discarded.
4. Valid records are merged into trusted silver tables.
5. Gold tables are materialized for business analysis.
6. KPI views and SQL analytics are used for reporting and insights.

## Data Quality Principles

This repo emphasizes robust data quality through:

- strict null handling
- duplicate prevention
- type validation
- referential integrity checks
- reconciliation reports between gold and silver results

## Getting Started

### Prerequisites

- Python 3.x
- Apache Spark / PySpark
- Jupyter Notebook
- Access to a Spark-compatible environment or Databricks-like setup

### Run the pipeline

Open the notebook files in the `bronze/` folder first, then execute the transformation scripts in the `silver/` folder, and finally run the gold pipeline:

```bash
python gold/gold_facts_and_dim.py
```

> The exact execution flow can vary depending on your environment (local Spark, Databricks, or cloud analytics workspace).

## Example Insights Generated

The gold layer can help answer questions like:

- Which airline has the highest annual revenue?
- Which route contributes most to bookings?
- Which passenger segments drive the most travel?
- How do discounts affect net booking revenue?
- How do booking KPIs evolve over time?

## Why This Project Matters

This repository is a practical demonstration of how a modern data engineering pipeline can turn raw operational data into trustworthy analytics assets. It reflects key concepts used in real-world warehouse and lakehouse architectures, including data validation, lineage, trusted layers, and business intelligence readiness.

## License

This project is intended for learning and practice purposes. Please check repository permissions before reusing in production or commercial environments.

---

<div align="center">
  <sub>Built with ❤️ for airline analytics, data engineering, and business intelligence.</sub>
</div>
