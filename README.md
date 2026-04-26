# Apache Airflow Data Orchestration Portfolio 🚀

This repository contains a collection of production-grade Airflow DAGs demonstrating advanced data engineering concepts, including resource optimization, priority scheduling, and automated ETL workflows.

## 🌟 Featured Projects

### 1. Resource Management with Airflow Pools
**Concept:** Concurrency Control & Priority Weighting.
- **Problem:** Preventing system crashes when running multiple heavy tasks simultaneously.
- **Solution:** Implemented `spark_pool` to limit concurrent tasks and used `priority_weight` (1-10) to ensure critical data processes are executed first.
- **Tools:** PythonOperator, Airflow Pools, Resource Isolation.

### 2. SQL Orchestration & Database Automation
**Concept:** Relational Data Management.
- **Objective:** Automating schema creation and data selection using `SQLExecuteQueryOperator`.
- **Integrations:** PostgreSQL, Docker-compose.

## 🛠️ Tech Stack
- **Orchestration:** Apache Airflow
- **Containerization:** Docker & Docker-compose (Astro CLI)
- **Programming:** Python
- **Databases:** PostgreSQL

## 🚀 How to Run
Every project is Dockerized. To run any DAG:
1. Install Astro CLI.
2. Clone this repo.
3. Run `astro dev start`.
