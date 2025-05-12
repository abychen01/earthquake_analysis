# Earthquake Analysis Project using Azure Databricks

## Overview

This project is an automated data pipeline for analyzing global earthquake data using PySpark in Azure Databricks. The pipeline is structured in the medallion architecture style (Bronze, Silver, Gold) and is orchestrated using Databricks Workflows. Each stage of the pipeline writes data to an Azure Data Lake Storage (ADLS) Gen2 container.

---

## Architecture

* **Bronze Layer**: Raw earthquake data is ingested from the USGS (United States Geological Survey) GeoJSON API.
* **Silver Layer**: Raw JSON is cleaned and transformed into structured columns, adding derived date/time fields.
* **Gold Layer**: Enriched with geolocation metadata using reverse geocoding (country extraction), and saved in analytics-ready format.

---

## Data Source

* **USGS Earthquake GeoJSON API**: [https://earthquake.usgs.gov/fdsnws/event/1/](https://earthquake.usgs.gov/fdsnws/event/1/)
* Time window: Fetches data for the previous day using dynamic date logic.

---

## Technologies Used

* Azure Databricks (Notebooks & Workflows)
* Azure Data Lake Storage Gen2 (Bronze, Silver, Gold containers)
* PySpark (Spark SQL & DataFrames)
* Python Libraries: `requests`, `json`, `reverse_geocoder`

---

## File Structure

* **Bronze Notebook**: `Ingest earthquake JSON data`
* **Silver Notebook**: `Transform and clean JSON data`
* **Gold Notebook**: `Add geolocation and enrich with country info`

---

## Notebook Details
<br>
  
### 1. Bronze Notebook

**Purpose**: Fetch yesterday's earthquake data and store as raw JSON in Bronze container.

**Key Steps:**

* Define ADLS paths for bronze, silver, gold tiers.
* Call USGS GeoJSON API with start and end dates.
* Validate API response and convert to JSON.
* Write data to ADLS using `dbutils.fs.put()`.
* Output metadata (paths, dates) via `dbutils.jobs.taskValues.set()`.

**Output**: JSON file in `abfss://bronze@...` named like `2025-05-10_earthquake_data.json`

<br><br>

### 2. Silver Notebook

**Purpose**: Clean raw JSON and structure it into a proper schema using DataFrames.

**Key Steps:**

* Read JSON file saved in Bronze container.
* Extract relevant fields (lat, long, magnitude, place, etc.).
* Handle null values and convert timestamps.
* Add human-readable date and time fields.
* Save structured data in Delta format to Silver container.

**Output**: Delta table in `abfss://silver@.../earthquake_events_silver`

<br><br>

### 3. Gold Notebook

**Purpose**: Add location-based enrichment (Country) to the Silver dataset.

**Key Steps:**

* Read Silver Delta table (last 2 days of data).
* Use `reverse_geocoder` to derive country code from latitude & longitude.
* UDF applied to Spark DataFrame.
* Save enriched data to Gold container.

**Output**: Delta table in `abfss://gold@.../EarthquakeData`

---

## Automation

* **Databricks Workflow**: Orchestrates execution of Bronze → Silver → Gold notebooks.
* **Task Value Sharing**: Paths and metadata are passed between notebooks using `dbutils.jobs.taskValues`.
<br>
<img width="1414" alt="image" src="https://github.com/user-attachments/assets/a9f59ec4-b60e-494e-aba9-e38ec887986f" />


---

## Future Enhancements

* Add alerts or visualization integration with Power BI.
* Validate schema and enforce schema evolution.
* Implement deduplication or idempotency logic.

---

## Authors

* Created by: Abey Abraham
* Last updated: 05-12-2025

---

## Disclaimer

This project is built using publicly available earthquake data from the USGS. Ensure you comply with data licensing and API usage guidelines.
