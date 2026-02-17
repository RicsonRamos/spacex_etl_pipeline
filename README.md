````markdown
# SpaceX ETL Pipeline

[![Python](https://img.shields.io/badge/python-3.11-blue)](https://www.python.org/)
[![Prefect](https://img.shields.io/badge/prefect-2.0-orange)](https://www.prefect.io/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

---

## Overview

This repository contains a **modular ETL pipeline** that extracts SpaceX data from the public API, transforms it using **Polars**, and loads it into **PostgreSQL**. The pipeline is orchestrated with **Prefect**, enabling retries, logging, and cloud monitoring.  

It is designed for **scalable, maintainable, and testable** data ingestion workflows, suitable for production-ready ETL systems.

---

## Architecture

### ETL Workflow

```mermaid
graph TD
    A[SpaceX API] --> B[Extract (Polars)]
    B --> C[Transform (Polars)]
    C --> D[Load (PostgreSQL)]
    D --> E[Prefect Cloud Monitoring]
````

**PNG version:**
![ETL Flow](docs/diagrams/etl_flow.png)

---

### Dependencies

```mermaid
graph LR
    Python --> Polars
    Python --> SQLAlchemy
    SQLAlchemy --> PostgreSQL
    Prefect --> Python
    Prefect --> PostgreSQL
```

**PNG version:**
![Dependencies](docs/diagrams/dependencies.png)

---

### Database Model (ER Diagram)

```mermaid
erDiagram
    ROCKETS {
        int id PK
        string name
    }
    LAUNCHES {
        int id PK
        int rocket_id FK
        date date
    }
    MISSIONS {
        int id PK
        int launch_id FK
        string name
    }
    ROCKETS ||--o{ LAUNCHES : has
    LAUNCHES ||--o{ MISSIONS : contains
```

**PNG version:**
![ER Model](docs/diagrams/er_model.png)

---

### Prefect Orchestration Flow

```mermaid
graph TD
    A[ETL Deployment] --> B[Extract Task]
    B --> C[Transform Task]
    C --> D[Load Task]
    D --> E[Retries & Alerts]
    E --> F[Prefect Cloud UI]
```

**PNG version:**
![Prefect Flow](docs/diagrams/prefect_flow.png)

---

## Features

* **Extract:** Fetches raw SpaceX API data using Polars for high-performance ingestion.
* **Transform:** Cleans and normalizes raw data into structured tables.
* **Load:** Inserts transformed data into PostgreSQL with transactional safety.
* **Orchestration:** Prefect provides task retries, logging, and cloud monitoring.
* **Testing:** Pytest integration ensures database connectivity and data correctness.
* **Configurable:** Environment variables control database credentials and Prefect API keys.

---

## Getting Started

### Requirements

* Python 3.11+
* PostgreSQL 15+
* Prefect Cloud account (optional but recommended)
* Git

### Installation

```bash
git clone https://github.com/RicsonRamos/spacex_etl_pipeline.git
cd spacex_etl_pipeline
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Configuration

Create a `.env` file:

```env
POSTGRES_USER=postgres
POSTGRES_PASSWORD=admin
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=spacex_db

PREFECT_API_KEY=<your_prefect_api_key>
PREFECT_API_URL=<your_prefect_api_url>
DATABASE_URL=postgresql://postgres:admin@localhost:5432/spacex_db
```

---

## Running the ETL

**Locally:**

```bash
python -m src.main
```

**Using Prefect Cloud:**

```bash
prefect deployment apply deployments/etl_deployment.yaml
prefect flow run "spacex_etl_pipeline"
```

---

## Testing

```bash
pytest tests/ -v --maxfail=1 --disable-warnings
```

* Ensures **database connection**, API availability, and data consistency.
* CI/CD pipelines can use these tests for automated validation.

---

## Project Structure

```
spacex_etl_pipeline/
├─ src/
│  ├─ extract.py       # Data extraction logic
│  ├─ transform.py     # Data transformation logic
│  ├─ load.py          # Database loading logic
│  ├─ main.py          # ETL orchestration
│  └─ settings.py      # Environment variables & configuration
├─ tests/              # Pytest tests
├─ requirements.txt
├─ pyproject.toml
└─ README.md
```

---

## Contributing

* Fork the repository
* Create a feature branch (`git checkout -b feature/xyz`)
* Make your changes with tests
* Submit a pull request

---

## License

This project is licensed under the **MIT License**. See [LICENSE](LICENSE) for details.

---

## References

* [SpaceX API](https://github.com/r-spacex/SpaceX-API)
* [Polars](https://www.pola.rs/)
* [SQLAlchemy](https://www.sqlalchemy.org/)
* [Prefect](https://www.prefect.io/)

---

Developed by Ricson Ramos
Data Analyst & Software Engineer