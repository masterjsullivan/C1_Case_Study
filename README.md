# Compass One Healthcare Case Study: Hospital Retail Strategy

## Project Overview
**Client:** Two hospital retail sites (currently operated by separate companies) located 1 mile apart.  
**Objective:** Analyze Point of Sale (POS) data to provide recommended adjustments to operations, pricing, and menu mix to improve the bottom line prior to operational takeover.

This repository contains the **ETL (Extract, Transform, Load) pipeline** used to process raw POS data, standardize it, and load it into a relational database (SQLite) for downstream analysis.

## Business Goals
As outlined in the strategic brief, this project aims to:
1.  **Analyze Purchasing Habits:** Identify trends in time of day, product mix, and seasonality.
2.  **Compare Sites:** Evaluate Revenue, Check Averages, Items per Transaction, and Growth rates.
3.  **Optimize Menu:** Streamline offerings and identify opportunities for expansion.
4.  **Strategic Recommendations:** Suggest operational changes while respecting the client's goal of pricing healthy items lower to encourage employee wellness.

## 📂 Repository Structure
```text
├── Data/
│   ├── POS_Data.xlsx          # Raw source data
│   ├── dim_categories.xlsx    # Static dimension table for margin mapping
│   └── C1_case_study.db       # Output SQLite database
├── pipeline.py                # Main ETL script
├── nutriscore.py              # Contains NutriScoreEstimator class
├── analysis.py                # Notebook for ad-hoc analysis
├── whiteboard.ipynb           # Notebook for WIP code development
├── requirements.txt           # Python dependencies
└── README.md                  # Project documentation
