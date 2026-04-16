# Game Data Transformation with dbt & Docker

This project is a modern **Data Pipeline** designed to process Mobile Game data from Firebase/Google Analytics 4 on the BigQuery platform. It leverages **dbt** for transformation logic and **Docker** for containerized orchestration.

---

## Data Architecture

The data is organized into logical layers to optimize performance and cost:

1.  **Source Layer (Raw)**: Raw GA4/Firebase data stored in BigQuery.
2.  **Flatten Layer (`event_flatten_raw`)**: Flattens complex nested JSON structures (event_params, user_properties) into a tabular format.
3.  **Base Layer (`event_base`)**: Standardizes key dimensions like device info, location, and campaigns, while handling A/B Testing logic.



---

## Tech Stack
- **dbt-bigquery**: Core tool for building transformation logic using SQL.
- **Docker**: Encapsulates the runtime environment to ensure consistency across all machines.
- **Google BigQuery**: Data Warehouse for storage and massive parallel processing.
- **Git/GitHub**: Version control for the codebase.

##  Setup & Operation Guide

### 1. Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed.
- A Google Cloud Service Account JSON key (`.json`) with BigQuery Admin/Data Editor permissions.

### 2. Security Configuration
For security reasons, sensitive files are not stored on Git. You need to recreate the following structure locally:
1. Create a `data/` folder and copy your key file: `data/annoying-puzzle-dbt.json`.
2. Ensure the `profiles.yml` file in the root directory has the correct connection details for your project.

### 3. Build & Run

**Build Image (Run this when you change SQL code or Dockerfile):**
```bash
docker build -t dbt-game-app .
```
**Execute Full Pipeline (Transform Data):**
```bash
docker run --rm dbt-game-app run
```
**Data Quality Check (Testing):**
```bash
docker run --rm dbt-game-app test
```
**View Lineage Graph (Documentation)**
```bash
docker run --rm -p 8080:8080 dbt-game-app docs generate serve --port 8080
```
Access via: http://localhost:8080
## Key Advantages

* **Incremental Logic**: Uses the `is_incremental` macro so dbt only scans new data (last 2 days), saving up to 90% of BigQuery processing costs.
* **Surrogate Key**: Generates unique IDs using MD5 to prevent data duplication during pipeline reruns.
* **Modular Code**: Uses the `ref()` function, allowing dbt to automatically understand dependencies without manual configuration.

---

## Daily Workflow
1.  Modify SQL logic in VS Code.
2.  Run `docker build -t dbt-game-app .` to update the Container Image.
3.  Run `docker run --rm dbt-game-app run` to verify results in BigQuery.
4.  `git push` the code to GitHub after successful testing.

---
*Project maintained by [sonnt32](https://github.com/sonnt32)*

