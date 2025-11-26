# GitHub Repository Description

## Short Description (for GitHub repo description field):
End-to-end weather data ETL pipeline using Airflow, dbt, PostgreSQL, and Superset. Demonstrates modern data engineering practices with Docker containerization, automated orchestration, and interactive BI dashboards.

## Topics/Tags to Add:
- airflow
- dbt
- etl-pipeline
- weather-data
- docker
- superset
- postgresql
- data-engineering
- python
- sql
- business-intelligence
- data-pipeline
- orchestration
- docker-compose
- data-transformation
- analytics
- dashboard
- modern-data-stack
- kenya
- weatherstack-api

## Detailed Features for GitHub:

### 🚀 Key Features
- **Automated Data Ingestion**: Scheduled weather data collection from WeatherStack API
- **Modern Data Stack**: Airflow + dbt + PostgreSQL + Superset integration  
- **Container-First**: Full Docker Compose setup with service orchestration
- **Data Transformation**: SQL-based transformations using dbt with staging and mart layers
- **Interactive Dashboards**: Real-time weather analytics with Apache Superset
- **Production Ready**: Comprehensive security guidelines and deployment documentation
- **Monitoring & Observability**: Built-in Airflow monitoring and error handling

### 🛠️ Technologies Used
- **Orchestration**: Apache Airflow 3.0.5
- **Transformation**: dbt (Data Build Tool)  
- **Database**: PostgreSQL 14.19
- **BI/Analytics**: Apache Superset
- **Containerization**: Docker & Docker Compose
- **Languages**: Python, SQL
- **API**: WeatherStack API

### 💼 Perfect For
- Data engineering portfolio projects
- Learning modern data stack technologies
- Understanding ETL/ELT pipeline development
- Demonstrating container orchestration skills
- Exploring business intelligence with Superset

### 🎯 Use Cases
- Weather monitoring for Kenyan cities
- Historical weather trend analysis  
- Real-time weather dashboard creation
- Data engineering skill demonstration
- Educational and learning purposes

# Zanari Analytics Data Platform Setup Guide

## Prerequisites
- Docker & Docker Compose
- Git
- Python 3.11 (for local scripts)

## Passwords & Environment Variables
- Use strong passwords (min 16 characters) for all services
- Example credentials for demo/testing:
  - Postgres: `erp_user` / `erp_pass`
  - pgAdmin: `pgadmin@demo.com` / `pgadmin_pass`
  - Superset: `admin` / `admin`
  - Airflow: `admin` / `admin`
- Edit `.env` and `docker-compose.yaml` to set these values
- For production, generate secure secrets and never use defaults

## Quick Setup Steps
1. **Clone the repository:**
   ```bash
   git clone https://github.com/Jayson-gor/zanari-analytics-demo.git
   cd zanari-analytics-data
   ```
2. **Create and edit your `.env` file:**
   ```bash
   cp .env.example .env
   # Edit .env and set all passwords and API keys
   ```
3. **Start all services:**
   ```bash
   docker-compose up -d
   ```
4. **Check ClickHouse data volumes:**
   ```bash
   docker-compose exec clickhouse clickhouse-client --query "SELECT name AS table, total_rows AS rows FROM system.tables WHERE database = 'default' AND name IN ('fact_sales','fact_purchases','fact_financials','dim_customer','dim_supplier','dim_truck','dim_product','dim_depot','dim_bank','dim_account','dim_date') ORDER BY name"
   ```
5. **Generate demo data with faker:**
   ```bash
   docker run --rm \
     -e PG_DSN="dbname=erp_db user=erp_user password=erp_pass host=db port=5432" \
     -e KAFKA_BOOTSTRAP="kafka:9092" \
     -e KAFKA_TOPIC="erp.erp.sales" \
     -v "$PWD/batch-loading-scripts:/app" \
     --network zanari-analytics-data_my-network \
     python:3.11 bash -c "pip install psycopg2-binary confluent-kafka && python /app/erp_faker.py"
   ```
6. **Monitor logs and tails:**
   ```bash
   docker-compose logs -f airflow
   docker-compose logs -f streaming-consumer
   docker-compose logs -f clickhouse
   # Or tail specific log files in containers
   docker-compose exec airflow tail -f /opt/airflow/logs/*/*/*/*.log
   ```

## Real-Time & Batch Data Flow
- Batch ETL: ERP Excel/Postgres → Airflow/ETL → ClickHouse
- CDC/Streaming: Postgres → Debezium → Kafka → Python Consumer → ClickHouse
- Dimensions: Airflow DAG/faker → ClickHouse
- BI: ClickHouse → Superset

## All Tools Are Working
- All services run in Docker Compose
- Real-time streaming, batch ETL, CDC, and BI dashboards are validated
- Dimension tables auto-populate with faker if source is missing
- Data volumes and logs can be checked as above

---
*For more details, see the main README.md and individual service docs.*
