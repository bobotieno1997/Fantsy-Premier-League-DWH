# Fanstasy Premier League

![FPL_logo](https://github.com/bobotieno1997/FPL/blob/9b4eddd462aee2402433df7c01296e20d24cbda3/Others/FPL-Statement-Lead.webp)

This repository contains the SQL and Python code for managing the Fantasy Premier League (FPL) dataset. Data is accessed via RESTful API endpoints provided by FPL and ingested using Python scripts. During ingestion, little transformations are done before loading the data to postgres instance hosted on Aiven.

---
## The Architecture
The Medallion architecture is the selected solution approach since data is only available at season level i.e is the current season is 2024/2025 then only data for that season is made available.
![Architecture](https://github.com/bobotieno1997/Fantsy-Premier-League-DWH/blob/5106904ca942be83605a357b4138906b229918ba/project_files/Architecture/overview_architecture%20.jpg)

### Bronze Layer
The bronze layer stands as the landing zone for all incoming dataset.The main idea is for the data to be available for processing before loading the it to the silver layer.The bronze layer should always then loaded the loaded during everyrun.
Click [here](https://github.com/bobotieno1997/Fantsy-Premier-League-DWH/tree/main/dags/01_Bronze) to view the airflow DAGs that are used to load data to the bronze database.
Here is a view of airflow webserver UI with successfull DAG runs and tables
![Airflow]()

Proof of data in the tables
![Database Query]()

### silver Layer
The bronze layer stands as the core datawarehouse. It is designed to store history data, i.e data related to previous season. How this layer is updated is different from the bronze layer, to preserve history only new updated records from the bronze layer are added incrementally.This applies to all tables in this layer apart from silver.future_games_info that needs to be truncated after every run.
Indexing is done in this layer for tables that are heavily queried.
Click [here](https://github.com/bobotieno1997/Fantsy-Premier-League-DWH/tree/main/dags/02_Silver/Scripts) to view the stored procedure scripts that are used to update data to the silver layer tables.

Proof of data in the tables
![Database Query]()

### Gold Layer
The gold layer is the reporting layer.Views are created from the bronze layer to ansewer business questions. Creating virtual table help in ensuring space is utilized appropriately in the database.
It adopts a star schema for efficient data quering.
The processed data in the data warehouse layer is optimized for analytical workloads and data visualization, enabling insightful exploration and reporting for FPL enthusiasts and analysts.
Click [here](https://github.com/bobotieno1997/Fantsy-Premier-League-DWH/tree/main/dags/03_Gold) to view the view scripts that are used to update data to the silver layer tables.

Proof of data in the tables
![Database Query]()

## Technologies Used:
- Postgres Database
- Python Programming 
- Docker
- Apache Airflow

## 📂 Repository Structure (Key Documents)
```
├───config
├───dags
│   ├───00_Initialization --Database and Schema creation scripts
│   ├───01_Bronze
│   │   ├───Scripts       -- Scripts to load data to bronze layer
│   ├───02_Silver
│   │   ├───Scripts
│   │   │   ├───01_teams    -- Store Procedure to update teams silver layer table
│   │   │   ├───02_players  -- Store Procedure to update player silver layer table
│   │   │   ├───03_games    -- Store Procedure to update games silver layer table
│   │   │   └───04_stats   -- Store Procedure to update stats silver layer table
│   │   └───__pycache__
│   └───03_Gold            -- Scripts to create gold layer views
├───logs
├───plugins
└───project_files
    ├───Architecture      -- Architecture Images and dat flow
    └───Documentation     -- naming convension

```