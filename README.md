# Education BI — Full Stack Analytics Project

A 3-month BI pipeline covering:
- **Phase 1** SQL ETL → Star/Constellation schema data warehouse (SQL Server) ✅
- **Phase 2** Python ETL → Pivoted fact tables (this project) 🔄
- **Phase 3** Dashboards → Flask + Chart.js (in progress)
- **Phase 4** PDF export → Jinja2 + WeasyPrint (planned)
- **Phase 5** Chatbot knowledge base → PDF ingestion + embeddings (planned)

## Project Structure

```
education_bi/
├── config/
│   └── settings.py          # All config from .env — import from here
├── db/
│   └── connection.py        # get_engine(), get_connection(), test_connection()
├── etl/
│   ├── pipeline.py          # Orchestrator — run this to execute ETL
│   ├── extractors/
│   │   └── source.py        # All SELECT from OLTP source
│   ├── transformers/
│   │   ├── service_revenue.py  # Unpivot months 1-12
│   │   └── service_tranche.py  # Unpivot TR1/TR2/Tranche cols
│   └── loaders/
│       └── warehouse.py     # Surrogate key resolution + bulk INSERT
├── dashboards/              # Phase 2: Flask + Chart.js
├── reports/                 # Phase 3: PDF generation
├── chatbot/                 # Phase 4: Chatbot knowledge base
├── utils/
│   ├── logger.py            # Loguru setup
│   └── helpers.py           # Shared utilities
└── tests/
    └── etl/
        └── test_transformers.py  # Unit tests (no DB needed)
```

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure connection
cp .env.example .env
# Edit .env with your SQL Server credentials

# 3. Run the full ETL pipeline
python -m etl.pipeline

# 4. Run only one table
python -m etl.pipeline --table revenue
python -m etl.pipeline --table tranche

# 5. Run tests
pytest tests/ -v
```

## Environment Variables (.env)

| Variable | Description | Example |
|---|---|---|
| DB_SERVER | SQL Server hostname | `localhost` |
| DB_PORT | Port | `1433` |
| DB_NAME | Database name | `educated-demo-db` |
| DB_USER | SQL login (leave blank for Windows auth) | `sa` |
| DB_PASSWORD | Password | `secret` |
| DB_DRIVER | ODBC driver name | `ODBC Driver 17 for SQL Server` |
| ETL_BATCH_SIZE | Rows per insert batch | `500` |
