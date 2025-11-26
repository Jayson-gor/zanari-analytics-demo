# Zanari Analytics Data Platform 🚀

A modern, containerized analytics stack for batch and real-time ETL, CDC, and BI, supporting ERP and retail data with dimensional modeling.

## Key Features
- **Batch ETL**: Automated loading from ERP Excel and Postgres to ClickHouse
- **Real-Time Streaming**: CDC via Kafka, Debezium, and Python consumers
- **Dimension Sync**: Airflow DAGs for batch/faker dimension population
- **Business Intelligence**: Superset dashboards on star schema
- **Full Stack**: Postgres, ClickHouse, Airflow, dbt, Superset, pgAdmin, Jupyter, Kafka, Debezium

## Technology Stack
| Component         | Technology                | Purpose                        |
|-------------------|--------------------------|--------------------------------|
| Orchestration     | Apache Airflow           | Workflow scheduling            |
| Batch ETL         | Python, pandas           | ERP Excel/Postgres to ClickHouse|
| CDC/Streaming     | Kafka, Debezium, Python  | Real-time fact table updates   |
| Database          | Postgres, ClickHouse     | Source & analytics DBs         |
| BI                | Apache Superset          | Dashboards & analytics         |
| Management        | pgAdmin, Jupyter         | DB admin, notebooks            |
| Containerization  | Docker Compose           | Environment isolation          |

## Project Structure
```
zanari-analytics-data/
├── airflow/                 # Airflow DAGs (ETL, dimension sync)
├── batch-loading-scripts/   # Python batch ETL scripts
├── streaming-consumer/      # Kafka to ClickHouse consumer
├── dbt/                     # dbt models
├── postgres/                # Star schema, init scripts
├── click-house/             # ClickHouse configs
├── superset/                # Superset dashboards
├── docker/                  # Docker configs
├── docker-compose.yaml      # Main stack definition
├── requirements.txt         # Python dependencies
└── README.md                # This documentation
```

## Data Flow
```
ERP Excel/Postgres → Airflow/ETL → ClickHouse (batch)
ERP/Postgres → Debezium → Kafka → Python Consumer → ClickHouse (streaming)
Dimensions → Airflow DAG/faker → ClickHouse
ClickHouse → Superset (BI)
```

## Real-Time & Batch Processing
- **Batch ETL**: Load ERP data via Python scripts and Airflow DAGs
- **CDC/Streaming**: Debezium captures changes, streams via Kafka, Python consumer loads facts to ClickHouse
- **Dimension Tables**: Airflow DAG syncs dimensions from ERP or generates fake records for demo/testing
- **Star Schema**: Fact and dimension tables in ClickHouse for analytics

## Tools Used
- **Airflow**: DAGs for ETL, dimension sync, scheduling
- **Python**: Batch ETL, streaming consumer, faker logic
- **Kafka & Debezium**: Real-time CDC from Postgres
- **ClickHouse**: High-performance analytics DB
- **Superset**: BI dashboards
- **pgAdmin**: Database management
- **Jupyter**: Data exploration
- **Docker Compose**: All services containerized

## Demo/Testing
- All tools/services run in containers
- Real-time streaming and batch ETL validated
- Dimension tables auto-populated with faker if source missing
- Superset dashboards show live analytics

---

*Built with ❤️ by a passionate data engineer*

