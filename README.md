# Uber-DataEngineering
🚖 Uber Data Analysis | Data Engineering Project

This project demonstrates an end-to-end data engineering pipeline for analyzing Uber trip data. Raw data is ingested from Google Cloud Storage, processed using an ETL pipeline built with Mage running on a Compute Engine VM, loaded into BigQuery for analytics, and finally visualized using Looker.

## Architecture

![Architecture Diagram](architecture.jpg)

## Project Structure

```
.
├── analytic-table-query.txt # Example query for BigQuery analytics table
├── architecture.jpg         # System architecture diagram
├── data_model.jpeg          # Data model diagram
├── LICENSE                  # Project license
├── README.md                # This file
├── data/
│   └── uber_data.csv        # Raw Uber trip data
└── mage_pipeline/
    ├── extract.py           # Mage script for extracting data from GCS
    ├── transform.py         # Mage script for transforming data
    └── load.py              # Mage script for loading data into BigQuery
```

## Technologies Used

*   **Cloud Provider:** Google Cloud Platform (GCP)
*   **Data Lake:** Google Cloud Storage (GCS)
*   **ETL Tool:** Mage AI ([mage_pipeline/](mage_pipeline/))
*   **Compute:** Google Compute Engine (GCE) for Mage VM
*   **Data Warehouse:** Google BigQuery
*   **BI Tool:** Looker

## Visualisation

Vous pouvez explorer le rapport interactif sur Looker Studio :
[Voir le rapport Uber Data Analysis](https://lookerstudio.google.com/reporting/850c4671-dc48-4b7e-a855-194e3e2c3b4b)
