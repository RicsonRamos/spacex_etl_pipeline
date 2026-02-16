# SpaceX High-Reliability ETL Pipeline

## 1. Business Context and Objective

Space exploration generates massive volumes of complex and highly volatile data. For mission analysts, inconsistent data can lead to incorrect conclusions about launch feasibility.

This project implements an **End-to-End** data pipeline that extracts real data from the SpaceX API, processes it using **Polars** for maximum performance, and loads it **idempotently** into a **PostgreSQL** database—ensuring the database acts as a reliable **Single Source of Truth**.

---

## 2. System Architecture

The system was designed following the **Separation of Concerns** principle:

- **Execution Plane:** Dockerized environment isolating Python 3.12 and PostgreSQL 16.
- **Control Plane:** Orchestration via **Prefect Cloud**, managing retries, alerts, and scheduling.
- **Data Layer:** PostgreSQL with strict schema enforcement and strong typing.

---

## 3. Senior-Level Technical Decisions

### Data Engineering and Idempotency

Unlike simple pipelines that only insert data, this project uses an **Upsert (ON CONFLICT DO UPDATE)** strategy.

- **Why?** Ensures pipeline re-runs do not duplicate records and that corrections from the source API are automatically reflected in the database without manual intervention.

---

### Strong Schema Enforcement

The loader was refactored to eliminate generic data types (`TEXT`).

- **Decision:** Dates are stored as `TIMESTAMPTZ` and success flags as `BOOLEAN`.
- **Impact:** 100% reduction in BI tool conversion errors and improved indexing performance.

---

### Security and Network Isolation

The `docker-compose.yml` file defines isolated internal networks.

- **Decision:** The PostgreSQL database runs in a `backend_network` with no internet access. Only the ETL container can access both the external API and the database.
- **Impact:** Protection against brute-force attacks on the database.

---

### Observability and Structured Logging

Implemented `structlog` with Prefect metadata injection (`flow_run_id`).

- **Insight:** In production failures, structured logs allow precise tracing of which task and which endpoint (e.g., `rockets`) caused the issue, reducing **MTTR (Mean Time To Repair)**.

---

## 4. Project KPIs (Key Performance Indicators)

| Metric | Value/Status | Technical Impact |
|--------|-------------|-----------------|
| **Idempotency Rate** | 100% | Zero duplicate records on re-runs |
| **Resilience (Retries)** | 3 attempts | Tolerance to intermittent API network failures |
| **Docker Build Time** | ~45s | Optimized via `.dockerignore` (reduced build context) |
| **Test Coverage** | High | Unit tests for Extract, Transform, and Load |

---

## 5. Repository Structure

```text
├── .github/workflows/  # CI/CD (GitHub Actions)
├── src/
│   ├── config/         # Secret management via Pydantic Settings
│   ├── extract/        # API clients and Pydantic schemas
│   ├── transform/      # Business logic with Polars
│   ├── load/           # Idempotent loader with SQLAlchemy
│   └── flows/          # Prefect orchestration
├── tests/              # Test suite (Pytest + Mocks)
├── docker-compose.yml  # Infrastructure as Code
└── pyproject.toml      # Modern dependency management
```

## 6. How to Run

Prerequisites
- Docker and Docker Compose
- Prefect Cloud account (optional for local runs)

Step-by-Step
1. Clone the repository.
2. Create a .env file based on .env.example.
3. Start the stack:
```bash 
docker-compose up -d --build
```
4. Run the tests to validate integrity:
```bash 
docker-compose run --rm etl_app pytest tests/

```
## 7. Engineering Insights

During development, we identified that the SpaceX API contains deeply nested fields. The decision to use Polars instead of Pandas was driven by scalability concerns: Polars handles large memory workloads more efficiently and provides an expression-based syntax that makes schema transformations (flattening struct fields into columns) more readable and performant.

Developed by Ricson Ramos
Data Analyst & Software Engineer