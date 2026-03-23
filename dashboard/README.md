# Educated BI — Analytics Dashboard
## Axis 1: Academic Performance · Axis 3: Attendance & Engagement

---

## Stack
| Layer    | Technology                        |
|----------|-----------------------------------|
| Frontend | React 18 + Chart.js 4 + Vite      |
| Backend  | Flask 3 + pyodbc + python-dotenv  |
| Database | SQL Server (educated-demo-db)     |

---

## Setup

### 1. Backend (Flask API)

```bash
cd dashboard/
pip install -r requirements.txt

# Copy .env from your ETL project (same DB settings)
cp ../education_bi/.env .env

python app.py
# Runs on http://localhost:5000
```

### 2. Frontend (React)

```bash
npm install
npm run dev
# Runs on http://localhost:3000
```

> The Vite dev server proxies `/api/*` requests to Flask on port 5000.
> If Flask is unavailable the dashboard automatically falls back to mock data.

---

## API Endpoints

### Axis 1 — Academic Performance
| Method | Endpoint                            | Description                            |
|--------|-------------------------------------|----------------------------------------|
| GET    | `/api/axis1/kpis`                   | Overall KPIs (avg score, pass rate...) |
| GET    | `/api/axis1/avg-by-subject`         | Average score per subject              |
| GET    | `/api/axis1/score-distribution`     | Score histogram (10 bins)              |
| GET    | `/api/axis1/avg-by-school-year`     | Year-over-year comparison              |
| GET    | `/api/axis1/pass-fail-by-subject`   | Pass vs fail stacked by subject        |

### Axis 3 — Attendance & Engagement
| Method | Endpoint                            | Description                            |
|--------|-------------------------------------|----------------------------------------|
| GET    | `/api/axis3/kpis`                   | Presence rate, late rate, hours        |
| GET    | `/api/axis3/absence-rate-by-month`  | Monthly absence trend                  |
| GET    | `/api/axis3/teacher-hours-by-month` | Monthly teacher hours                  |
| GET    | `/api/axis3/top-absent-students`    | Top 10 most absent students            |
| GET    | `/api/axis3/attendance-vs-score`    | Scatter: absence rate vs avg score     |

All endpoints accept optional `?school_year_key=N` filter.

### Shared
| Method | Endpoint          | Description                 |
|--------|-------------------|-----------------------------|
| GET    | `/api/school-years` | All school years for filter |

---

## Dashboard Features

- **School year filter** — all charts update simultaneously
- **Mock data fallback** — works without a database connection
- **Sticky header** with axis navigation
- **KPI cards** with semantic color coding
- **Insight strip** — computed highlights below each section
- **Dark theme** throughout (deep navy #0F172A)

---

## Project Structure

```
dashboard/
├── app.py              ← Flask backend (all SQL queries)
├── requirements.txt    ← Python dependencies
├── index.html          ← HTML entry point
├── vite.config.js      ← Vite + proxy config
├── package.json        ← npm dependencies
└── src/
    ├── main.jsx        ← React root
    └── Dashboard.jsx   ← Full dashboard (all axes, charts, components)
```
