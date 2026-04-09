"""
dashboard/app.py — reuses project db/connection.py directly.
Run from inside education_bi/ :  python dashboard/app.py
"""
import sys, os
import urllib.request
import json as _json
import datetime as _dt
import datetime as _dt2
from collections import defaultdict

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from db.connection import get_engine, test_connection
from loguru import logger
from flask import Flask, jsonify, request
from flask_cors import CORS
from sqlalchemy import text

app = Flask(__name__)
CORS(app)

def query(sql):
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(sql))
        cols = list(result.keys())
        return [dict(zip(cols, row)) for row in result.fetchall()]

def wf(alias="er"):
    """Filter by school year using SchoolYearPeriodOid GUID"""
    k = request.args.get("school_year_key")
    # If k is a numeric string, we need to get the corresponding GUID from DimSchoolYear
    if k and k.isdigit():
        # Query to get the GUID for the given SchoolYearKey
        result = query(f"SELECT SchoolYearOid FROM dbo.DimSchoolYear WHERE SchoolYearKey = {int(k)}")
        if result and result[0].get("SchoolYearOid"):
            guid = result[0]["SchoolYearOid"]
            return f"AND {alias}.SchoolYearPeriodOid = '{guid}'"
    elif k and not k.isdigit():
        # If it's already a GUID string
        return f"AND {alias}.SchoolYearPeriodOid = '{k}'"
    return ""

@app.before_request
def _once():
    app.before_request_funcs[None].remove(_once)
    test_connection()

# ══════════════════════════════════════════════════════════════════════════════
# SHARED / UTILITY
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/school-years")
def school_years():
    """Return school years with SchoolYearKey as int and SchoolYearOid as GUID"""
    engine = get_engine()
    query_sql = text("""
        SELECT 
            SchoolYearKey,
            SchoolYearOid,
            Description,
            CASE 
                WHEN GETDATE() BETWEEN StartDate AND EndDate THEN 1
                ELSE 0
            END AS IsCurrent
        FROM dbo.DimSchoolYear
        WHERE SchoolYearKey <> 1
        ORDER BY SchoolYearKey
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query_sql)
        school_years = [
            {
                "SchoolYearKey": row.SchoolYearKey,
                "SchoolYearOid": str(row.SchoolYearOid),  # Convert GUID to string
                "YearLabel": row.Description,
                "IsCurrent": bool(row.IsCurrent)
            }
            for row in result
        ]
    
    return jsonify(school_years)

# ══════════════════════════════════════════════════════════════════════════════
# AXIS 1 — Academic Performance Overview
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/axis1/kpis")
def axis1_kpis():
    rows = query(f"""
        SELECT COUNT(DISTINCT er.StudentOid) AS total_students,
               ROUND(AVG(CAST(f.Average AS FLOAT)),2) AS avg_score,
               ROUND(100.0*SUM(CASE WHEN f.Average>=10 THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),1) AS pass_rate,
               ROUND(100.0*SUM(CASE WHEN f.Average<10 THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),1) AS fail_rate,
               MAX(f.Average) AS max_score,
               MIN(f.Average) AS min_score
        FROM dbo.FactStudentEvaluation f
        JOIN dbo.DimEvaluationReport er
            ON f.EvaluationReportKey = er.EvaluationReportKey
        WHERE er.StudentOid IS NOT NULL
          AND f.Average IS NOT NULL
          {wf()}
    """)
    return jsonify(rows[0] if rows else {})

@app.route("/api/axis1/avg-by-subject")
def axis1_avg_by_subject():
    return jsonify(query(f"""
        SELECT c.Description AS subject,
               ROUND(AVG(CAST(f.Average AS FLOAT)),2) AS avg_score,
               COUNT(*) AS n_evaluations,
               ROUND(100.0*SUM(CASE WHEN f.Average>=10 THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),1) AS pass_rate,
               SUM(CASE WHEN f.Average>=10 THEN 1 ELSE 0 END) AS pass_count,
               SUM(CASE WHEN f.Average<10 THEN 1 ELSE 0 END) AS fail_count
        FROM dbo.FactStudentEvaluation f
        JOIN dbo.DimContent c ON f.ContentKey = c.ContentKey
        JOIN dbo.DimEvaluationReport er
            ON f.EvaluationReportKey = er.EvaluationReportKey
        WHERE er.StudentOid IS NOT NULL
          AND f.Average IS NOT NULL
          AND c.ContentKey <> 1
          {wf()}
        GROUP BY c.Description
        ORDER BY avg_score DESC
    """))

@app.route("/api/axis1/score-distribution")
def axis1_score_distribution():
    return jsonify(query(f"""
        SELECT CASE 
            WHEN f.Average < 2 THEN '0-2'
            WHEN f.Average < 4 THEN '2-4'
            WHEN f.Average < 6 THEN '4-6'
            WHEN f.Average < 8 THEN '6-8'
            WHEN f.Average < 10 THEN '8-10'
            WHEN f.Average < 12 THEN '10-12'
            WHEN f.Average < 14 THEN '12-14'
            WHEN f.Average < 16 THEN '14-16'
            WHEN f.Average < 18 THEN '16-18'
            ELSE '18-20' END AS bin,
            COUNT(*) AS count
        FROM dbo.FactStudentEvaluation f
        JOIN dbo.DimEvaluationReport er
            ON f.EvaluationReportKey = er.EvaluationReportKey
        WHERE er.StudentOid IS NOT NULL
          AND f.Average IS NOT NULL
          {wf()}
        GROUP BY CASE 
            WHEN f.Average < 2 THEN '0-2'
            WHEN f.Average < 4 THEN '2-4'
            WHEN f.Average < 6 THEN '4-6'
            WHEN f.Average < 8 THEN '6-8'
            WHEN f.Average < 10 THEN '8-10'
            WHEN f.Average < 12 THEN '10-12'
            WHEN f.Average < 14 THEN '12-14'
            WHEN f.Average < 16 THEN '14-16'
            WHEN f.Average < 18 THEN '16-18'
            ELSE '18-20' END
        ORDER BY MIN(f.Average)
    """))

@app.route("/api/axis1/avg-by-school-year")
def axis1_avg_by_school_year():
    return jsonify(query("""
        SELECT sy.Description AS year_label,
               sy.SchoolYearKey,
               ROUND(AVG(CAST(f.Average AS FLOAT)),2) AS avg_score,
               ROUND(100.0*SUM(CASE WHEN f.Average>=10 THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),1) AS pass_rate,
               COUNT(DISTINCT er.StudentOid) AS student_count
        FROM dbo.FactStudentEvaluation f
        JOIN dbo.DimEvaluationReport er
            ON f.EvaluationReportKey = er.EvaluationReportKey
        JOIN dbo.DimSchoolYear sy
            ON er.SchoolYearPeriodOid = sy.SchoolYearOid
        WHERE er.StudentOid IS NOT NULL
          AND f.Average IS NOT NULL
          AND sy.SchoolYearKey <> 1
        GROUP BY sy.Description, sy.SchoolYearKey
        ORDER BY sy.SchoolYearKey
    """))

@app.route("/api/axis1/pass-fail-by-subject")
def axis1_pass_fail_by_subject():
    return jsonify(query(f"""
        SELECT TOP 12 c.Description AS subject,
               SUM(CASE WHEN f.Average>=10 THEN 1 ELSE 0 END) AS pass_count,
               SUM(CASE WHEN f.Average< 10 THEN 1 ELSE 0 END) AS fail_count,
               COUNT(*) AS total
        FROM dbo.FactStudentEvaluation f
        JOIN dbo.DimContent c ON f.ContentKey=c.ContentKey
        JOIN dbo.DimEvaluationReport er ON f.EvaluationReportKey=er.EvaluationReportKey
        WHERE er.StudentOid IS NOT NULL
          AND f.Average IS NOT NULL 
          AND c.ContentKey<>1 
          {wf()}
        GROUP BY c.Description ORDER BY total DESC"""))

# ══════════════════════════════════════════════════════════════════════════════
# AXIS 2 — Temporal Progression & Trends
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/axis2/progression-by-student")
def axis2_progression_by_student():
    student_key = request.args.get("student_key")
    w = f"AND er.StudentOid = '{student_key}'" if student_key else ""
    return jsonify(query(f"""
        SELECT sy.Description AS year_label, sy.SchoolYearKey,
               s.FirstName+' '+s.LastName AS student_name,
               ROUND(AVG(CAST(f.Average AS FLOAT)),2) AS avg_score,
               COUNT(*) AS n_evals
        FROM dbo.FactStudentEvaluation f
        JOIN dbo.DimEvaluationReport er ON f.EvaluationReportKey = er.EvaluationReportKey
        JOIN dbo.DimSchoolYear sy ON er.SchoolYearPeriodOid = sy.SchoolYearOid
        JOIN dbo.DimStudent s ON er.StudentOid = s.StudentOid
        WHERE er.StudentOid IS NOT NULL
          AND s.StudentOid IS NOT NULL
          AND f.Average IS NOT NULL 
          AND sy.SchoolYearKey <> 1 
          {w}
        GROUP BY sy.Description, sy.SchoolYearKey, s.FirstName, s.LastName
        ORDER BY sy.SchoolYearKey"""))

@app.route("/api/axis2/year-over-year-by-subject")
def axis2_yoy_by_subject():
    return jsonify(query("""
        SELECT sy.Description AS year_label, sy.SchoolYearKey,
               c.Description AS subject,
               ROUND(AVG(CAST(f.Average AS FLOAT)),2) AS avg_score
        FROM dbo.FactStudentEvaluation f
        JOIN dbo.DimEvaluationReport er ON f.EvaluationReportKey = er.EvaluationReportKey
        JOIN dbo.DimSchoolYear sy ON er.SchoolYearPeriodOid = sy.SchoolYearOid
        JOIN dbo.DimContent    c  ON f.ContentKey=c.ContentKey
        WHERE er.StudentOid IS NOT NULL
          AND f.Average IS NOT NULL
          AND sy.SchoolYearKey <> 1 
          AND c.ContentKey<>1
        GROUP BY sy.Description, sy.SchoolYearKey, c.Description
        ORDER BY sy.SchoolYearKey, c.Description"""))

@app.route("/api/axis2/stability-index")
def axis2_stability_index():
    return jsonify(query(f"""
        SELECT TOP 20
            s.FirstName+' '+s.LastName AS student_name,
            COUNT(DISTINCT er.SchoolYearPeriodOid) AS years_active,
            ROUND(AVG(CAST(f.Average AS FLOAT)),2)  AS avg_score,
            ROUND(MAX(CAST(f.Average AS FLOAT)) - MIN(CAST(f.Average AS FLOAT)),2) AS score_range,
            ROUND(STDEV(CAST(f.Average AS FLOAT)),2) AS score_stddev
        FROM dbo.FactStudentEvaluation f
        JOIN dbo.DimEvaluationReport er ON f.EvaluationReportKey = er.EvaluationReportKey
        JOIN dbo.DimStudent s ON er.StudentOid = s.StudentOid
        WHERE er.StudentOid IS NOT NULL
          AND s.StudentOid IS NOT NULL
          AND f.Average IS NOT NULL
        GROUP BY s.FirstName, s.LastName
        HAVING COUNT(DISTINCT er.SchoolYearPeriodOid)>=2 AND COUNT(*)>=5
        ORDER BY score_stddev ASC"""))

@app.route("/api/axis2/regression-detection")
def axis2_regression():
    return jsonify(query("""
        WITH history AS (
            SELECT er.StudentOid,
                   AVG(CAST(f.Average AS FLOAT)) AS hist_avg,
                   MAX(er.SchoolYearPeriodOid) AS latest_year_oid
            FROM dbo.FactStudentEvaluation f
            JOIN dbo.DimEvaluationReport er ON f.EvaluationReportKey = er.EvaluationReportKey
            WHERE er.StudentOid IS NOT NULL
              AND f.Average IS NOT NULL
            GROUP BY er.StudentOid
        ),
        latest AS (
            SELECT er.StudentOid,
                   AVG(CAST(f.Average AS FLOAT)) AS latest_avg
            FROM dbo.FactStudentEvaluation f
            JOIN dbo.DimEvaluationReport er ON f.EvaluationReportKey = er.EvaluationReportKey
            JOIN history h ON er.StudentOid = h.StudentOid
                          AND er.SchoolYearPeriodOid = h.latest_year_oid
            WHERE f.Average IS NOT NULL
            GROUP BY er.StudentOid
        )
        SELECT TOP 15
            s.FirstName+' '+s.LastName AS student_name,
            ROUND(h.hist_avg,2)        AS historical_avg,
            ROUND(l.latest_avg,2)      AS latest_avg,
            ROUND(l.latest_avg - h.hist_avg,2) AS delta,
            CASE WHEN l.latest_avg - h.hist_avg < -2 THEN 'Regression'
                 WHEN l.latest_avg - h.hist_avg >  2 THEN 'Improvement'
                 ELSE 'Stable' END     AS trend_label
        FROM history h
        JOIN latest  l ON h.StudentOid = l.StudentOid
        JOIN dbo.DimStudent s ON h.StudentOid = s.StudentOid
        WHERE s.StudentOid IS NOT NULL
        ORDER BY delta ASC"""))

@app.route("/api/axis2/semester-comparison")
def axis2_semester():
    return jsonify(query("""
        SELECT sy.Description AS year_label,
               er.ReportName AS report_name,
               ROUND(AVG(CAST(f.Average AS FLOAT)),2) AS avg_score,
               COUNT(DISTINCT er.StudentOid) AS student_count
        FROM dbo.FactStudentEvaluation f
        JOIN dbo.DimEvaluationReport er ON f.EvaluationReportKey = er.EvaluationReportKey
        JOIN dbo.DimSchoolYear sy ON er.SchoolYearPeriodOid = sy.SchoolYearOid
        WHERE er.StudentOid IS NOT NULL
          AND f.Average IS NOT NULL
          AND sy.SchoolYearKey <> 1 
          AND er.EvaluationReportKey<>1
        GROUP BY sy.Description, er.ReportName
        ORDER BY sy.Description, er.ReportName"""))

# ══════════════════════════════════════════════════════════════════════════════
# AXIS 3 — Attendance
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/axis3/kpis")
def axis3_kpis():
    w = wf()
    s = query(f"""
        SELECT COUNT(DISTINCT f.StudentOid) AS students_tracked,
               ROUND(100.0*SUM(CASE WHEN f.IsPresent=1 THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),1) AS presence_rate,
               ROUND(100.0*SUM(CASE WHEN f.IsLate=1 THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),1) AS late_rate,
               COUNT(*) AS total_records
        FROM dbo.FactStudentAttendance f 
        WHERE f.StudentOid IS NOT NULL {w}""")
    t = query(f"""
        SELECT COUNT(DISTINCT f.TeacherKey) AS teachers_tracked,
               ROUND(AVG(CAST(f.NumberOfHours AS FLOAT)),2) AS avg_hours,
               ROUND(100.0*SUM(CASE WHEN f.IsLate=1 THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),1) AS teacher_late_rate
        FROM dbo.FactTeacherAttendance f WHERE f.TeacherKey<>1 {w}""")
    return jsonify({**(s[0] if s else {}), **(t[0] if t else {})})

@app.route("/api/axis3/absence-rate-by-month")
def axis3_absence_rate_by_month():
    return jsonify(query(f"""
        SELECT d.Year AS year, d.Month AS month, d.MonthName AS month_name,
               COUNT(*) AS total_records,
               SUM(CASE WHEN f.IsPresent=0 THEN 1 ELSE 0 END) AS absent_count,
               ROUND(100.0*SUM(CASE WHEN f.IsPresent=0 THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),1) AS absence_rate
        FROM dbo.FactStudentAttendance f
        JOIN dbo.DimDate d ON f.DateKey=d.DateKey
        WHERE f.StudentOid IS NOT NULL
          AND d.DateKey<>-1 
          {wf()}
        GROUP BY d.Year,d.Month,d.MonthName ORDER BY d.Year,d.Month"""))

@app.route("/api/axis3/teacher-hours-by-month")
def axis3_teacher_hours_by_month():
    return jsonify(query(f"""
        SELECT d.MonthName AS month_name, d.Month AS month, d.Year AS year,
               SUM(CAST(f.NumberOfHours AS FLOAT)) AS total_hours,
               COUNT(DISTINCT f.TeacherKey) AS active_teachers,
               ROUND(AVG(CAST(f.NumberOfHours AS FLOAT)),2) AS avg_hours_per_teacher
        FROM dbo.FactTeacherAttendance f
        JOIN dbo.DimDate d ON f.DateKey=d.DateKey
        WHERE f.TeacherKey<>1 AND d.DateKey<>-1 {wf()}
        GROUP BY d.MonthName,d.Month,d.Year ORDER BY d.Year,d.Month"""))

@app.route("/api/axis3/top-absent-students")
def axis3_top_absent_students():
    return jsonify(query(f"""
        SELECT TOP 10 s.FirstName+' '+s.LastName AS student_name,
               COUNT(*) AS total_days,
               SUM(CASE WHEN f.IsPresent=0 THEN 1 ELSE 0 END) AS absent_days,
               ROUND(100.0*SUM(CASE WHEN f.IsPresent=0 THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),1) AS absence_rate
        FROM dbo.FactStudentAttendance f
        JOIN dbo.DimStudent s ON f.StudentOid = s.StudentOid
        WHERE f.StudentOid IS NOT NULL
          AND s.StudentOid IS NOT NULL
          {wf()}
        GROUP BY s.FirstName,s.LastName HAVING COUNT(*)>=3
        ORDER BY absence_rate DESC"""))

@app.route("/api/axis3/attendance-vs-score")
def axis3_attendance_vs_score():
    w = wf("att")
    return jsonify(query(f"""
        SELECT s.FirstName+' '+s.LastName AS student_name,
               ROUND(100.0*SUM(CASE WHEN att.IsPresent=0 THEN 1 ELSE 0 END)/NULLIF(COUNT(att.FactKey),0),1) AS absence_rate,
               ROUND(AVG(CAST(ev.Average AS FLOAT)),2) AS avg_score
        FROM dbo.FactStudentAttendance att
        JOIN dbo.DimStudent s ON att.StudentOid = s.StudentOid
        JOIN dbo.FactStudentEvaluation ev ON att.StudentOid = ev.StudentOid
        JOIN dbo.DimEvaluationReport er ON ev.EvaluationReportKey = er.EvaluationReportKey
        WHERE att.StudentOid IS NOT NULL
          AND s.StudentOid IS NOT NULL
          AND ev.Average IS NOT NULL 
          {w}
        GROUP BY s.FirstName,s.LastName
        HAVING COUNT(att.FactKey)>=3 AND COUNT(ev.FactKey)>=3"""))

# ══════════════════════════════════════════════════════════════════════════════
# AXIS 4 — Weather Impact (Open-Meteo historical API)
# ══════════════════════════════════════════════════════════════════════════════

SOUSSE_LAT = 35.8288
SOUSSE_LON = 10.6400

def _fetch_weather(start_date: str, end_date: str) -> list:
    url = (
        f"https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={SOUSSE_LAT}&longitude={SOUSSE_LON}"
        f"&start_date={start_date}&end_date={end_date}"
        f"&daily=precipitation_sum&timezone=Africa%2FTunis"
    )
    try:
        with urllib.request.urlopen(url, timeout=8) as r:
            data = _json.loads(r.read())
        dates = data["daily"]["time"]
        precip = data["daily"]["precipitation_sum"]
        return [
            {"date": d, "precipitation_mm": p or 0.0, "is_rainy": (p or 0) > 1.0}
            for d, p in zip(dates, precip)
        ]
    except Exception as e:
        logger.warning(f"Open-Meteo unavailable: {e}")
        return []

@app.route("/api/axis4/weather-vs-absence")
def axis4_weather_vs_absence():
    school_year_key = request.args.get("school_year_key")
    wh = ""
    if school_year_key and school_year_key.isdigit():
        result = query(f"SELECT SchoolYearOid FROM dbo.DimSchoolYear WHERE SchoolYearKey = {int(school_year_key)}")
        if result and result[0].get("SchoolYearOid"):
            guid = result[0]["SchoolYearOid"]
            wh = f"AND att.SchoolYearPeriodOid = '{guid}'"
    elif school_year_key:
        wh = f"AND att.SchoolYearPeriodOid = '{school_year_key}'"
    
    attendance = query(f"""
        SELECT d.FullDate AS full_date,
               COUNT(*) AS total,
               SUM(CASE WHEN f.IsPresent=0 THEN 1 ELSE 0 END) AS absent_count,
               ROUND(100.0*SUM(CASE WHEN f.IsPresent=0 THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),1) AS absence_rate
        FROM dbo.FactStudentAttendance f
        JOIN dbo.DimDate d ON f.DateKey=d.DateKey
        WHERE f.StudentOid IS NOT NULL
          AND d.DateKey<>-1 
          AND d.FullDate IS NOT NULL 
          {wh}
        GROUP BY d.FullDate ORDER BY d.FullDate""")
    if not attendance:
        return jsonify([])
    dates = [r["full_date"] for r in attendance]
    start = str(min(dates))[:10]
    end = str(max(dates))[:10]
    weather = {w["date"]: w for w in _fetch_weather(start, end)}
    result = []
    for row in attendance:
        d_str = str(row["full_date"])[:10]
        w = weather.get(d_str, {"precipitation_mm": 0.0, "is_rainy": False})
        result.append({**row, "full_date": d_str,
                       "precipitation_mm": w["precipitation_mm"],
                       "is_rainy": w["is_rainy"]})
    return jsonify(result)

@app.route("/api/axis4/rainy-vs-dry-summary")
def axis4_rainy_vs_dry():
    school_year_key = request.args.get("school_year_key")
    wh = ""
    if school_year_key and school_year_key.isdigit():
        result = query(f"SELECT SchoolYearOid FROM dbo.DimSchoolYear WHERE SchoolYearKey = {int(school_year_key)}")
        if result and result[0].get("SchoolYearOid"):
            guid = result[0]["SchoolYearOid"]
            wh = f"AND att.SchoolYearPeriodOid = '{guid}'"
    elif school_year_key:
        wh = f"AND att.SchoolYearPeriodOid = '{school_year_key}'"
    
    attendance = query(f"""
        SELECT d.FullDate AS full_date,
               ROUND(100.0*SUM(CASE WHEN f.IsPresent=0 THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),1) AS absence_rate
        FROM dbo.FactStudentAttendance f
        JOIN dbo.DimDate d ON f.DateKey=d.DateKey
        WHERE f.StudentOid IS NOT NULL
          AND d.DateKey<>-1 
          AND d.FullDate IS NOT NULL 
          {wh}
        GROUP BY d.FullDate""")
    if not attendance:
        return jsonify({"rainy_avg": None, "dry_avg": None, "n_rainy": 0, "n_dry": 0})
    dates = [r["full_date"] for r in attendance]
    start = str(min(dates))[:10]
    end = str(max(dates))[:10]
    weather = {w["date"]: w for w in _fetch_weather(start, end)}
    rainy, dry = [], []
    for row in attendance:
        d_str = str(row["full_date"])[:10]
        w = weather.get(d_str, {"is_rainy": False})
        (rainy if w["is_rainy"] else dry).append(row["absence_rate"])
    def safe_avg(lst):
        return round(sum(lst)/len(lst), 1) if lst else None
    return jsonify({
        "rainy_avg": safe_avg(rainy), "n_rainy": len(rainy),
        "dry_avg": safe_avg(dry), "n_dry": len(dry),
    })

@app.route("/api/axis4/seasonal-patterns")
def axis4_seasonal():
    school_year_key = request.args.get("school_year_key")
    wh = ""
    if school_year_key and school_year_key.isdigit():
        result = query(f"SELECT SchoolYearOid FROM dbo.DimSchoolYear WHERE SchoolYearKey = {int(school_year_key)}")
        if result and result[0].get("SchoolYearOid"):
            guid = result[0]["SchoolYearOid"]
            wh = f"AND att.SchoolYearPeriodOid = '{guid}'"
    elif school_year_key:
        wh = f"AND att.SchoolYearPeriodOid = '{school_year_key}'"
    
    monthly_att = query(f"""
        SELECT d.Month AS month, d.MonthName AS month_name,
               ROUND(100.0*SUM(CASE WHEN f.IsPresent=0 THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),1) AS absence_rate,
               MIN(CAST(d.FullDate AS VARCHAR(10))) AS sample_start,
               MAX(CAST(d.FullDate AS VARCHAR(10))) AS sample_end
        FROM dbo.FactStudentAttendance f
        JOIN dbo.DimDate d ON f.DateKey=d.DateKey
        WHERE f.StudentOid IS NOT NULL
          AND d.DateKey<>-1 
          AND d.FullDate IS NOT NULL 
          {wh}
        GROUP BY d.Month,d.MonthName ORDER BY d.Month""")
    if not monthly_att:
        return jsonify([])
    all_dates = [r for r in monthly_att if r["sample_start"]]
    if all_dates:
        start = min(r["sample_start"] for r in all_dates)
        end = max(r["sample_end"] for r in all_dates)
        daily_w = _fetch_weather(start[:10], end[:10])
        month_precip = {}
        for w in daily_w:
            m = int(w["date"][5:7])
            month_precip.setdefault(m, []).append(w["precipitation_mm"])
        for row in monthly_att:
            vals = month_precip.get(row["month"], [])
            row["avg_precipitation"] = round(sum(vals)/len(vals), 1) if vals else 0.0
    return jsonify(monthly_att)

# ══════════════════════════════════════════════════════════════════════════════
# AXIS 5 — Pedagogical Session Analysis
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/axis5/outcomes-by-report")
def axis5_outcomes_by_report():
    subject = request.args.get("subject", "")
    wh = f"AND c.Description='{subject}'" if subject else ""
    school_year_key = request.args.get("school_year_key")
    wy = ""
    if school_year_key and school_year_key.isdigit():
        result = query(f"SELECT SchoolYearOid FROM dbo.DimSchoolYear WHERE SchoolYearKey = {int(school_year_key)}")
        if result and result[0].get("SchoolYearOid"):
            guid = result[0]["SchoolYearOid"]
            wy = f"AND er.SchoolYearPeriodOid = '{guid}'"
    elif school_year_key:
        wy = f"AND er.SchoolYearPeriodOid = '{school_year_key}'"
    
    return jsonify(query(f"""
        SELECT er.ReportName AS report_name,
               c.Description AS subject,
               sy.Description AS year_label,
               ROUND(AVG(CAST(f.Average AS FLOAT)),2) AS avg_score,
               ROUND(STDEV(CAST(f.Average AS FLOAT)),2) AS score_stddev,
               COUNT(DISTINCT er.StudentOid) AS student_count,
               ROUND(100.0*SUM(CASE WHEN f.Average>=10 THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),1) AS pass_rate
        FROM dbo.FactStudentEvaluation f
        JOIN dbo.DimEvaluationReport er ON f.EvaluationReportKey=er.EvaluationReportKey
        JOIN dbo.DimContent c ON f.ContentKey=c.ContentKey
        JOIN dbo.DimSchoolYear sy ON er.SchoolYearPeriodOid = sy.SchoolYearOid
        WHERE er.StudentOid IS NOT NULL
          AND f.Average IS NOT NULL
          AND er.EvaluationReportKey<>1 
          AND c.ContentKey<>1 
          AND sy.SchoolYearKey<>1
          {wh} {wy}
        GROUP BY er.ReportName,c.Description,sy.Description
        ORDER BY sy.Description,er.ReportName"""))

@app.route("/api/axis5/dispersion-by-report")
def axis5_dispersion():
    school_year_key = request.args.get("school_year_key")
    wy = ""
    if school_year_key and school_year_key.isdigit():
        result = query(f"SELECT SchoolYearOid FROM dbo.DimSchoolYear WHERE SchoolYearKey = {int(school_year_key)}")
        if result and result[0].get("SchoolYearOid"):
            guid = result[0]["SchoolYearOid"]
            wy = f"AND er.SchoolYearPeriodOid = '{guid}'"
    elif school_year_key:
        wy = f"AND er.SchoolYearPeriodOid = '{school_year_key}'"
    
    return jsonify(query(f"""
        SELECT er.ReportName AS report_name,
               ROUND(MIN(CAST(f.Average AS FLOAT)),2) AS score_min,
               ROUND(MAX(CAST(f.Average AS FLOAT)),2) AS score_max,
               ROUND(AVG(CAST(f.Average AS FLOAT)),2) AS score_avg,
               ROUND(STDEV(CAST(f.Average AS FLOAT)),2) AS score_stddev,
               COUNT(*) AS n
        FROM dbo.FactStudentEvaluation f
        JOIN dbo.DimEvaluationReport er ON f.EvaluationReportKey=er.EvaluationReportKey
        WHERE er.StudentOid IS NOT NULL
          AND f.Average IS NOT NULL
          AND er.EvaluationReportKey<>1 
          {wy}
        GROUP BY er.ReportName
        ORDER BY er.ReportName"""))

@app.route("/api/axis5/subject-list")
def axis5_subject_list():
    return jsonify(query("""
        SELECT DISTINCT c.Description AS subject
        FROM dbo.DimContent c
        WHERE c.ContentKey<>1
        ORDER BY c.Description"""))

@app.route("/api/axis5/presence-performance-by-report")
def axis5_presence_performance():
    school_year_key = request.args.get("school_year_key")
    wy = ""
    if school_year_key and school_year_key.isdigit():
        result = query(f"SELECT SchoolYearOid FROM dbo.DimSchoolYear WHERE SchoolYearKey = {int(school_year_key)}")
        if result and result[0].get("SchoolYearOid"):
            guid = result[0]["SchoolYearOid"]
            wy = f"AND ev.SchoolYearPeriodOid = '{guid}'"
    elif school_year_key:
        wy = f"AND ev.SchoolYearPeriodOid = '{school_year_key}'"
    
    return jsonify(query(f"""
        SELECT er.ReportName AS report_name,
               ROUND(AVG(CAST(ev.Average AS FLOAT)),2) AS avg_score,
               ROUND(100.0*SUM(CASE WHEN att.IsPresent=1 THEN 1 ELSE 0 END)/NULLIF(COUNT(att.FactKey),0),1) AS presence_rate
        FROM dbo.FactStudentEvaluation ev
        JOIN dbo.DimEvaluationReport er ON ev.EvaluationReportKey=er.EvaluationReportKey
        LEFT JOIN dbo.FactStudentAttendance att
               ON ev.StudentOid = att.StudentOid
        WHERE ev.StudentOid IS NOT NULL
          AND ev.Average IS NOT NULL
          AND er.EvaluationReportKey<>1 
          {wy}
        GROUP BY er.ReportName
        ORDER BY er.ReportName"""))

# ══════════════════════════════════════════════════════════════════════════════
# AXIS 6 — Academic Risk Detection
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/axis6/risk-scores")
def axis6_risk_scores():
    school_year_key = request.args.get("school_year_key")
    wy = ""
    if school_year_key and school_year_key.isdigit():
        result = query(f"SELECT SchoolYearOid FROM dbo.DimSchoolYear WHERE SchoolYearKey = {int(school_year_key)}")
        if result and result[0].get("SchoolYearOid"):
            guid = result[0]["SchoolYearOid"]
            wy = f"AND ev.SchoolYearPeriodOid = '{guid}'"
    elif school_year_key:
        wy = f"AND ev.SchoolYearPeriodOid = '{school_year_key}'"
    
    return jsonify(query(f"""
        WITH academic AS (
            SELECT ev.StudentOid,
                   ROUND(AVG(CAST(ev.Average AS FLOAT)),2) AS avg_score,
                   ROUND(100.0*SUM(CASE WHEN ev.Average<10 THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),1) AS fail_rate_pct
            FROM dbo.FactStudentEvaluation ev
            WHERE ev.StudentOid IS NOT NULL
              AND ev.Average IS NOT NULL 
              {wy}
            GROUP BY ev.StudentOid
        ),
        attendance AS (
            SELECT att.StudentOid,
                   ROUND(100.0*SUM(CASE WHEN att.IsPresent=0 THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),1) AS absence_rate
            FROM dbo.FactStudentAttendance att
            WHERE att.StudentOid IS NOT NULL
            GROUP BY att.StudentOid
        ),
        payment AS (
            SELECT p.StudentOid, COUNT(*) AS payment_count
            FROM dbo.FactStudentPayment p
            WHERE p.StudentOid IS NOT NULL
            GROUP BY p.StudentOid
        )
        SELECT TOP 30
            s.FirstName+' '+s.LastName AS student_name,
            ROUND(a.avg_score,2) AS avg_score,
            COALESCE(att.absence_rate,0) AS absence_rate,
            a.fail_rate_pct AS fail_rate_pct,
            COALESCE(pay.payment_count,0) AS payment_count,
            ROUND(LEAST(100,
                (CASE WHEN a.avg_score < 10 THEN (10 - a.avg_score) * 4.0 ELSE 0 END)
                + COALESCE(att.absence_rate,0) * 0.35
                + a.fail_rate_pct * 0.25
            ), 1) AS risk_score,
            CASE
                WHEN (CASE WHEN a.avg_score<10 THEN (10-a.avg_score)*4.0 ELSE 0 END)
                   + COALESCE(att.absence_rate,0)*0.35
                   + a.fail_rate_pct*0.25 >= 60 THEN 'High'
                WHEN (CASE WHEN a.avg_score<10 THEN (10-a.avg_score)*4.0 ELSE 0 END)
                   + COALESCE(att.absence_rate,0)*0.35
                   + a.fail_rate_pct*0.25 >= 35 THEN 'Medium'
                ELSE 'Low'
            END AS risk_level
        FROM academic a
        JOIN dbo.DimStudent s ON a.StudentOid = s.StudentOid
        LEFT JOIN attendance att ON a.StudentOid = att.StudentOid
        LEFT JOIN payment pay ON a.StudentOid = pay.StudentOid
        WHERE s.StudentOid IS NOT NULL
        ORDER BY risk_score DESC"""))

@app.route("/api/axis6/risk-distribution")
def axis6_risk_distribution():
    school_year_key = request.args.get("school_year_key")
    wy = ""
    if school_year_key and school_year_key.isdigit():
        result = query(f"SELECT SchoolYearOid FROM dbo.DimSchoolYear WHERE SchoolYearKey = {int(school_year_key)}")
        if result and result[0].get("SchoolYearOid"):
            guid = result[0]["SchoolYearOid"]
            wy = f"AND ev.SchoolYearPeriodOid = '{guid}'"
    elif school_year_key:
        wy = f"AND ev.SchoolYearPeriodOid = '{school_year_key}'"
    
    return jsonify(query(f"""
        WITH academic AS (
            SELECT ev.StudentOid,
                   AVG(CAST(ev.Average AS FLOAT)) AS avg_score,
                   100.0*SUM(CASE WHEN ev.Average<10 THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0) AS fail_rate_pct
            FROM dbo.FactStudentEvaluation ev
            WHERE ev.StudentOid IS NOT NULL
              AND ev.Average IS NOT NULL 
              {wy}
            GROUP BY ev.StudentOid
        ),
        attendance AS (
            SELECT att.StudentOid,
                   100.0*SUM(CASE WHEN att.IsPresent=0 THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0) AS absence_rate
            FROM dbo.FactStudentAttendance att
            WHERE att.StudentOid IS NOT NULL
            GROUP BY att.StudentOid
        ),
        scored AS (
            SELECT
                CASE
                    WHEN (CASE WHEN a.avg_score<10 THEN (10-a.avg_score)*4 ELSE 0 END)
                       + COALESCE(att.absence_rate,0)*0.35
                       + a.fail_rate_pct*0.25 >= 60 THEN 'High'
                    WHEN (CASE WHEN a.avg_score<10 THEN (10-a.avg_score)*4 ELSE 0 END)
                       + COALESCE(att.absence_rate,0)*0.35
                       + a.fail_rate_pct*0.25 >= 35 THEN 'Medium'
                    ELSE 'Low'
                END AS risk_level,
                a.avg_score, COALESCE(att.absence_rate,0) AS absence_rate
            FROM academic a
            LEFT JOIN attendance att ON a.StudentOid = att.StudentOid
        )
        SELECT risk_level,
               COUNT(*) AS student_count,
               ROUND(AVG(avg_score),2) AS avg_score,
               ROUND(AVG(absence_rate),1) AS avg_absence_rate
        FROM scored
        GROUP BY risk_level
        ORDER BY CASE risk_level WHEN 'High' THEN 1 WHEN 'Medium' THEN 2 ELSE 3 END"""))

@app.route("/api/axis6/early-warning-indicators")
def axis6_early_warning():
    school_year_key = request.args.get("school_year_key")
    wy = ""
    if school_year_key and school_year_key.isdigit():
        result = query(f"SELECT SchoolYearOid FROM dbo.DimSchoolYear WHERE SchoolYearKey = {int(school_year_key)}")
        if result and result[0].get("SchoolYearOid"):
            guid = result[0]["SchoolYearOid"]
            wy = f"AND ev.SchoolYearPeriodOid = '{guid}'"
    elif school_year_key:
        wy = f"AND ev.SchoolYearPeriodOid = '{school_year_key}'"
    
    return jsonify(query(f"""
        WITH academic AS (
            SELECT ev.StudentOid,
                   AVG(CAST(ev.Average AS FLOAT)) AS avg_score,
                   100.0*SUM(CASE WHEN ev.Average<10 THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0) AS fail_rate_pct
            FROM dbo.FactStudentEvaluation ev
            WHERE ev.StudentOid IS NOT NULL
              AND ev.Average IS NOT NULL 
              {wy}
            GROUP BY ev.StudentOid
        ),
        attendance AS (
            SELECT att.StudentOid,
                   100.0*SUM(CASE WHEN att.IsPresent=0 THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0) AS absence_rate
            FROM dbo.FactStudentAttendance att
            WHERE att.StudentOid IS NOT NULL
            GROUP BY att.StudentOid
        )
        SELECT TOP 20
            s.FirstName+' '+s.LastName AS student_name,
            ROUND(a.avg_score,2) AS avg_score,
            ROUND(COALESCE(att.absence_rate,0),1) AS absence_rate,
            ROUND(a.fail_rate_pct,1) AS fail_rate_pct,
            CASE WHEN a.avg_score < 10 THEN 1 ELSE 0 END AS flag_low_score,
            CASE WHEN COALESCE(att.absence_rate,0) > 20 THEN 1 ELSE 0 END AS flag_high_absence,
            CASE WHEN a.fail_rate_pct > 50 THEN 1 ELSE 0 END AS flag_high_fail_rate,
            (CASE WHEN a.avg_score < 10 THEN 1 ELSE 0 END
            +CASE WHEN COALESCE(att.absence_rate,0)>20 THEN 1 ELSE 0 END
            +CASE WHEN a.fail_rate_pct>50 THEN 1 ELSE 0 END) AS warning_count
        FROM academic a
        JOIN dbo.DimStudent s ON a.StudentOid = s.StudentOid
        LEFT JOIN attendance att ON a.StudentOid = att.StudentOid
        WHERE s.StudentOid IS NOT NULL
          AND (CASE WHEN a.avg_score<10 THEN 1 ELSE 0 END
              +CASE WHEN COALESCE(att.absence_rate,0)>20 THEN 1 ELSE 0 END
              +CASE WHEN a.fail_rate_pct>50 THEN 1 ELSE 0 END) >= 2
        ORDER BY warning_count DESC, a.avg_score ASC"""))

# ══════════════════════════════════════════════════════════════════════════════
# AXIS 7 — Geographic Distribution
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/axis7/students-by-zone")
def axis7_students_by_zone():
    school_year_key = request.args.get("school_year_key")
    wy = ""
    if school_year_key and school_year_key.isdigit():
        result = query(f"SELECT SchoolYearOid FROM dbo.DimSchoolYear WHERE SchoolYearKey = {int(school_year_key)}")
        if result and result[0].get("SchoolYearOid"):
            guid = result[0]["SchoolYearOid"]
            wy = f"AND att.SchoolYearPeriodOid = '{guid}'"
    elif school_year_key:
        wy = f"AND att.SchoolYearPeriodOid = '{school_year_key}'"
    
    return jsonify(query(f"""
        SELECT
            COALESCE(s.ZoneName, s.Governorate, 'Unknown') AS zone_name,
            COUNT(DISTINCT s.StudentOid) AS student_count,
            ROUND(100.0 * SUM(CASE WHEN att.IsPresent = 0 THEN 1 ELSE 0 END)
                / NULLIF(COUNT(att.FactKey), 0), 1) AS absence_rate,
            ROUND(100.0 * SUM(CASE WHEN att.IsLate = 1 THEN 1 ELSE 0 END)
                / NULLIF(COUNT(att.FactKey), 0), 1) AS late_rate,
            ROUND(AVG(CAST(ev.Average AS FLOAT)), 2) AS avg_score
        FROM dbo.DimStudent s
        LEFT JOIN dbo.FactStudentAttendance att ON s.StudentOid = att.StudentOid {wy}
        LEFT JOIN dbo.FactStudentEvaluation ev ON s.StudentOid = ev.StudentOid 
            AND ev.Average IS NOT NULL
        WHERE s.StudentOid IS NOT NULL
          AND COALESCE(s.ZoneName, s.Governorate) IS NOT NULL
          AND COALESCE(s.ZoneName, s.Governorate) <> ''
        GROUP BY COALESCE(s.ZoneName, s.Governorate)
        ORDER BY student_count DESC"""))

@app.route("/api/axis7/zone-list")
def axis7_zone_list():
    return jsonify(query("""
        SELECT DISTINCT
            COALESCE(ZoneName, Governorate, 'Unknown') AS zone_name,
            COUNT(*) AS student_count
        FROM dbo.DimStudent
        WHERE StudentOid IS NOT NULL
          AND COALESCE(ZoneName, Governorate) IS NOT NULL
          AND COALESCE(ZoneName, Governorate) <> ''
        GROUP BY COALESCE(ZoneName, Governorate)
        ORDER BY student_count DESC"""))

@app.route("/api/axis7/governorate-summary")
def axis7_governorate_summary():
    school_year_key = request.args.get("school_year_key")
    wy = ""
    if school_year_key and school_year_key.isdigit():
        result = query(f"SELECT SchoolYearOid FROM dbo.DimSchoolYear WHERE SchoolYearKey = {int(school_year_key)}")
        if result and result[0].get("SchoolYearOid"):
            guid = result[0]["SchoolYearOid"]
            wy = f"AND att.SchoolYearPeriodOid = '{guid}'"
    elif school_year_key:
        wy = f"AND att.SchoolYearPeriodOid = '{school_year_key}'"
    
    return jsonify(query(f"""
        SELECT
            COALESCE(s.Governorate, 'Unknown') AS governorate,
            COUNT(DISTINCT s.StudentOid) AS student_count,
            ROUND(100.0 * SUM(CASE WHEN att.IsPresent = 0 THEN 1 ELSE 0 END)
                / NULLIF(COUNT(att.FactKey), 0), 1) AS absence_rate,
            ROUND(100.0 * SUM(CASE WHEN att.IsLate = 1 THEN 1 ELSE 0 END)
                / NULLIF(COUNT(att.FactKey), 0), 1) AS late_rate
        FROM dbo.DimStudent s
        LEFT JOIN dbo.FactStudentAttendance att ON s.StudentOid = att.StudentOid {wy}
        WHERE s.StudentOid IS NOT NULL
        GROUP BY s.Governorate
        ORDER BY student_count DESC"""))

@app.route("/api/axis7/zone-detail/<zone_name>")
def axis7_zone_detail(zone_name):
    school_year_key = request.args.get("school_year_key")
    wy = ""
    if school_year_key and school_year_key.isdigit():
        result = query(f"SELECT SchoolYearOid FROM dbo.DimSchoolYear WHERE SchoolYearKey = {int(school_year_key)}")
        if result and result[0].get("SchoolYearOid"):
            guid = result[0]["SchoolYearOid"]
            wy = f"AND att.SchoolYearPeriodOid = '{guid}'"
    elif school_year_key:
        wy = f"AND att.SchoolYearPeriodOid = '{school_year_key}'"
    
    return jsonify(query(f"""
        SELECT TOP 15
            s.FirstName + ' ' + s.LastName AS student_name,
            COALESCE(s.ZoneName, s.Governorate, 'Unknown') AS zone_name,
            ROUND(100.0 * SUM(CASE WHEN att.IsPresent = 0 THEN 1 ELSE 0 END)
                / NULLIF(COUNT(att.FactKey), 0), 1) AS absence_rate,
            ROUND(100.0 * SUM(CASE WHEN att.IsLate = 1 THEN 1 ELSE 0 END)
                / NULLIF(COUNT(att.FactKey), 0), 1) AS late_rate,
            ROUND(AVG(CAST(ev.Average AS FLOAT)), 2) AS avg_score
        FROM dbo.DimStudent s
        LEFT JOIN dbo.FactStudentAttendance att ON s.StudentOid = att.StudentOid {wy}
        LEFT JOIN dbo.FactStudentEvaluation ev ON s.StudentOid = ev.StudentOid
        WHERE s.StudentOid IS NOT NULL
          AND COALESCE(s.ZoneName, s.Governorate) = '{zone_name}'
          AND ev.Average IS NOT NULL
        GROUP BY s.FirstName, s.LastName, s.ZoneName, s.Governorate
        HAVING COUNT(att.FactKey) >= 3
        ORDER BY absence_rate DESC"""))

# ══════════════════════════════════════════════════════════════════════════════
# AXIS 8 — Absence Through Holiday & Exam Periods
# ══════════════════════════════════════════════════════════════════════════════

ACADEMIC_CALENDAR = {
    "2024-2025": [
        {"label": "Pre-Mawlid", "type": "pre_holiday", "start": "2024-09-13", "end": "2024-09-14"},
        {"label": "Mawlid Break", "type": "holiday", "start": "2024-09-15", "end": "2024-09-15"},
        {"label": "Post-Mawlid", "type": "post_holiday", "start": "2024-09-16", "end": "2024-09-17"},
        {"label": "Pre-Autumn Break", "type": "pre_holiday", "start": "2024-10-19", "end": "2024-10-25"},
        {"label": "Autumn Break", "type": "holiday", "start": "2024-10-26", "end": "2024-11-03"},
        {"label": "Post-Autumn Break", "type": "post_holiday", "start": "2024-11-04", "end": "2024-11-10"},
        {"label": "Pre-T1 Exams", "type": "pre_exam", "start": "2024-11-18", "end": "2024-11-29"},
        {"label": "T1 Exam Session", "type": "exam", "start": "2024-11-30", "end": "2024-12-07"},
        {"label": "Pre-Winter Break", "type": "pre_holiday", "start": "2024-12-21", "end": "2024-12-27"},
        {"label": "Winter Break", "type": "holiday", "start": "2024-12-28", "end": "2025-01-05"},
        {"label": "Post-Winter Break", "type": "post_holiday", "start": "2025-01-06", "end": "2025-01-12"},
        {"label": "Pre-T2 Exams", "type": "pre_exam", "start": "2025-02-17", "end": "2025-02-28"},
        {"label": "T2 Exam Session", "type": "exam", "start": "2025-03-01", "end": "2025-03-08"},
        {"label": "Pre-Spring Break", "type": "pre_holiday", "start": "2025-03-08", "end": "2025-03-14"},
        {"label": "Spring Break", "type": "holiday", "start": "2025-03-15", "end": "2025-03-23"},
        {"label": "Post-Spring Break", "type": "post_holiday", "start": "2025-03-24", "end": "2025-03-30"},
        {"label": "Pre-Eid al-Fitr", "type": "pre_holiday", "start": "2025-03-28", "end": "2025-03-29"},
        {"label": "Eid al-Fitr Break", "type": "holiday", "start": "2025-03-30", "end": "2025-04-01"},
        {"label": "Post-Eid al-Fitr", "type": "post_holiday", "start": "2025-04-02", "end": "2025-04-04"},
        {"label": "Pre-Final Exams", "type": "pre_exam", "start": "2025-05-01", "end": "2025-05-14"},
        {"label": "Final Exam Session", "type": "exam", "start": "2025-05-15", "end": "2025-06-03"},
        {"label": "Pre-Eid al-Adha", "type": "pre_holiday", "start": "2025-06-04", "end": "2025-06-05"},
        {"label": "Eid al-Adha Break", "type": "holiday", "start": "2025-06-06", "end": "2025-06-08"},
        {"label": "Post-Eid al-Adha", "type": "post_holiday", "start": "2025-06-09", "end": "2025-06-10"},
    ],
    "2025-2026": [
        {"label": "Pre-Mawlid", "type": "pre_holiday", "start": "2025-09-02", "end": "2025-09-03"},
        {"label": "Mawlid Break", "type": "holiday", "start": "2025-09-04", "end": "2025-09-05"},
        {"label": "Post-Mawlid", "type": "post_holiday", "start": "2025-09-06", "end": "2025-09-08"},
        {"label": "Pre-Autumn Break", "type": "pre_holiday", "start": "2025-10-11", "end": "2025-10-17"},
        {"label": "Autumn Break", "type": "holiday", "start": "2025-10-18", "end": "2025-10-26"},
        {"label": "Post-Autumn Break", "type": "post_holiday", "start": "2025-10-27", "end": "2025-11-02"},
        {"label": "Pre-T1 Exams", "type": "pre_exam", "start": "2025-11-03", "end": "2025-11-14"},
        {"label": "T1 Exam Session", "type": "exam", "start": "2025-11-15", "end": "2025-11-22"},
        {"label": "Pre-Winter Break", "type": "pre_holiday", "start": "2025-12-20", "end": "2025-12-26"},
        {"label": "Winter Break", "type": "holiday", "start": "2025-12-27", "end": "2026-01-04"},
        {"label": "Post-Winter Break", "type": "post_holiday", "start": "2026-01-05", "end": "2026-01-11"},
        {"label": "Pre-T2 Exams", "type": "pre_exam", "start": "2026-02-02", "end": "2026-02-13"},
        {"label": "T2 Exam Session", "type": "exam", "start": "2026-02-14", "end": "2026-02-21"},
        {"label": "Pre-Spring Break", "type": "pre_holiday", "start": "2026-02-28", "end": "2026-03-06"},
        {"label": "Spring Break", "type": "holiday", "start": "2026-03-07", "end": "2026-03-15"},
        {"label": "Post-Spring Break", "type": "post_holiday", "start": "2026-03-16", "end": "2026-03-22"},
        {"label": "Pre-Eid al-Fitr", "type": "pre_holiday", "start": "2026-03-18", "end": "2026-03-19"},
        {"label": "Eid al-Fitr Break", "type": "holiday", "start": "2026-03-20", "end": "2026-03-22"},
        {"label": "Post-Eid al-Fitr", "type": "post_holiday", "start": "2026-03-23", "end": "2026-03-25"},
        {"label": "Pre-Final Exams", "type": "pre_exam", "start": "2026-04-20", "end": "2026-05-03"},
        {"label": "Final Exam Session", "type": "exam", "start": "2026-05-04", "end": "2026-05-24"},
        {"label": "Pre-Eid al-Adha", "type": "pre_holiday", "start": "2026-05-25", "end": "2026-05-26"},
        {"label": "Eid al-Adha Break", "type": "holiday", "start": "2026-05-27", "end": "2026-05-29"},
        {"label": "Post-Eid al-Adha", "type": "post_holiday", "start": "2026-05-30", "end": "2026-06-01"},
    ],
}

def _get_period_type(date_str: str, year_label: str) -> dict:
    try:
        d = _dt2.date.fromisoformat(date_str[:10])
    except Exception:
        return {"period_type": "normal", "period_label": "Normal Period"}
    cal = ACADEMIC_CALENDAR.get(year_label, [])
    if not cal:
        for y_label in ["2024-2025", "2025-2026"]:
            cal = ACADEMIC_CALENDAR.get(y_label, [])
            if cal:
                break
    for entry in cal:
        s = _dt2.date.fromisoformat(entry["start"])
        e = _dt2.date.fromisoformat(entry["end"])
        if s <= d <= e:
            return {"period_type": entry["type"], "period_label": entry["label"]}
    return {"period_type": "normal", "period_label": "Normal Period"}

def _get_year_label(school_year_key):
    year_label = "2024-2025"
    if school_year_key and school_year_key.isdigit():
        yr = query(f"SELECT Description FROM dbo.DimSchoolYear WHERE SchoolYearKey = {int(school_year_key)}")
        if yr:
            year_label = yr[0].get("Description", "2024-2025")
    return year_label

@app.route("/api/axis8/absence-by-period-type")
def axis8_by_period_type():
    school_year_key = request.args.get("school_year_key")
    wh = ""
    if school_year_key and school_year_key.isdigit():
        result = query(f"SELECT SchoolYearOid FROM dbo.DimSchoolYear WHERE SchoolYearKey = {int(school_year_key)}")
        if result and result[0].get("SchoolYearOid"):
            guid = result[0]["SchoolYearOid"]
            wh = f"AND att.SchoolYearPeriodOid = '{guid}'"
    elif school_year_key:
        wh = f"AND att.SchoolYearPeriodOid = '{school_year_key}'"
    
    year_label = _get_year_label(school_year_key)
    daily = query(f"""
        SELECT d.FullDate AS full_date,
               ROUND(100.0 * SUM(CASE WHEN f.IsPresent = 0 THEN 1 ELSE 0 END)
                     / NULLIF(COUNT(*), 0), 1) AS absence_rate
        FROM dbo.FactStudentAttendance f
        JOIN dbo.DimDate d ON f.DateKey = d.DateKey
        WHERE f.StudentOid IS NOT NULL 
          AND d.DateKey <> -1 
          AND d.FullDate IS NOT NULL 
          {wh}
        GROUP BY d.FullDate ORDER BY d.FullDate""")
    buckets = defaultdict(list)
    for row in daily:
        p = _get_period_type(str(row["full_date"])[:10], year_label)
        buckets[p["period_type"]].append(row["absence_rate"])
    order = ["pre_holiday", "post_holiday", "pre_exam", "exam", "normal"]
    labels = {"pre_holiday": "Before Holiday", "post_holiday": "After Holiday",
              "pre_exam": "Before Exams", "exam": "Exam Period", "normal": "Normal Days"}
    return jsonify([{
        "period_type": pt,
        "period_label": labels[pt],
        "avg_absence_rate": round(sum(buckets[pt]) / len(buckets[pt]), 1) if buckets[pt] else 0.0,
        "n_days": len(buckets[pt]),
    } for pt in order])

@app.route("/api/axis8/daily-absence-tagged")
def axis8_daily_tagged():
    school_year_key = request.args.get("school_year_key")
    wh = ""
    if school_year_key and school_year_key.isdigit():
        result = query(f"SELECT SchoolYearOid FROM dbo.DimSchoolYear WHERE SchoolYearKey = {int(school_year_key)}")
        if result and result[0].get("SchoolYearOid"):
            guid = result[0]["SchoolYearOid"]
            wh = f"AND att.SchoolYearPeriodOid = '{guid}'"
    elif school_year_key:
        wh = f"AND att.SchoolYearPeriodOid = '{school_year_key}'"
    
    year_label = _get_year_label(school_year_key)
    daily = query(f"""
        SELECT d.FullDate AS full_date,
               ROUND(100.0 * SUM(CASE WHEN f.IsPresent = 0 THEN 1 ELSE 0 END)
                     / NULLIF(COUNT(*), 0), 1) AS absence_rate,
               COUNT(*) AS total_records
        FROM dbo.FactStudentAttendance f
        JOIN dbo.DimDate d ON f.DateKey = d.DateKey
        WHERE f.StudentOid IS NOT NULL
          AND d.DateKey <> -1 
          AND d.FullDate IS NOT NULL 
          {wh}
        GROUP BY d.FullDate ORDER BY d.FullDate""")
    result = []
    for row in daily:
        d_str = str(row["full_date"])[:10]
        p = _get_period_type(d_str, year_label)
        result.append({**row, "full_date": d_str, **p})
    return jsonify(result)

@app.route("/api/axis8/calendar-periods")
def axis8_calendar():
    year_label = request.args.get("year_label", "2024-2025")
    return jsonify(ACADEMIC_CALENDAR.get(year_label, []))

@app.route("/api/axis8/holiday-impact-summary")
def axis8_holiday_impact():
    school_year_key = request.args.get("school_year_key")
    wh = ""
    if school_year_key and school_year_key.isdigit():
        result = query(f"SELECT SchoolYearOid FROM dbo.DimSchoolYear WHERE SchoolYearKey = {int(school_year_key)}")
        if result and result[0].get("SchoolYearOid"):
            guid = result[0]["SchoolYearOid"]
            wh = f"AND att.SchoolYearPeriodOid = '{guid}'"
    elif school_year_key:
        wh = f"AND att.SchoolYearPeriodOid = '{school_year_key}'"
    
    year_label = _get_year_label(school_year_key)
    daily_map = {}
    for r in query(f"""
        SELECT CAST(d.FullDate AS VARCHAR(10)) AS full_date,
               ROUND(100.0 * SUM(CASE WHEN f.IsPresent = 0 THEN 1 ELSE 0 END)
                     / NULLIF(COUNT(*), 0), 1) AS absence_rate
        FROM dbo.FactStudentAttendance f
        JOIN dbo.DimDate d ON f.DateKey = d.DateKey
        WHERE f.StudentOid IS NOT NULL
          AND d.DateKey <> -1 
          AND d.FullDate IS NOT NULL 
          {wh}
        GROUP BY CAST(d.FullDate AS VARCHAR(10))"""):
        daily_map[r["full_date"][:10]] = r["absence_rate"]
    result = []
    for h in [e for e in ACADEMIC_CALENDAR.get(year_label, []) if e["type"] == "holiday"]:
        hs = _dt2.date.fromisoformat(h["start"])
        he = _dt2.date.fromisoformat(h["end"])
        pre_vals, post_vals = [], []
        for i in range(1, 6):
            pre_d = str(hs - _dt2.timedelta(days=i))
            post_d = str(he + _dt2.timedelta(days=i))
            if pre_d in daily_map:
                pre_vals.append(daily_map[pre_d])
            if post_d in daily_map:
                post_vals.append(daily_map[post_d])
        result.append({
            "holiday_label": h["label"],
            "start": h["start"],
            "end": h["end"],
            "pre_avg_absence": round(sum(pre_vals) / len(pre_vals), 1) if pre_vals else None,
            "post_avg_absence": round(sum(post_vals) / len(post_vals), 1) if post_vals else None,
        })
    return jsonify(result)

# ══════════════════════════════════════════════════════════════════════════════
# AXIS 9 — Previous School (Prep Origin) vs Performance
# ══════════════════════════════════════════════════════════════════════════════

SCHOOL_GROUPS = {
    "SLS Leaders School": ["sls", "leaders school", "leader school", "sousse leaders"],
    "Allama": ["allama", "علامة", "alema", "alalma", "allema"],
    "Avicenne / Ibn Sina": ["avicenne", "avecenne", "ibn sina", "ابن سينا", "ibn rochd"],
    "Amad": ["amad"],
    "Select School": ["select school", "select skills", "النخبة والمهارات"],
    "Les Élites / AKC": ["élites", "elites", "les elites", "akc", "أبو القاسم", "a.k.c"],
    "Marie Curie": ["marie curie"],
    "Esseddik": ["esseddik", "الصديق"],
    "Pilote": ["pilote", "النموذجية"],
    "Le Petit Prince / LPP": ["petit prince", "الأمير الصغير", "lpp", "lpl"],
    "Les Lauréats": ["lauréat", "laureats", "المتفوقون", "laureat"],
    "Sahloul 4 / HS": ["sahloul", "hs1", "hs2", "hs ", "h.s", "hammam sousse"],
    "Ennour / Annour": ["ennour", "annour", "النور", "nour academy"],
    "Perseverance": ["persevér", "persever", "المثابرة"],
    "Kalaa Soghra": ["kalaa", "قلعة"],
    "Khezama": ["khezam", "خزامة"],
    "Cité Erriadh": ["erriadh", "riadh", "الرياض", "cite riadh"],
    "Monastir International": ["monastir", "international monastir", "المنستير"],
    "Other / Unknown": [],
}

def _normalize_school(name: str) -> str:
    if not name:
        return "Other / Unknown"
    nl = name.lower().strip()
    for canonical, patterns in SCHOOL_GROUPS.items():
        if canonical == "Other / Unknown":
            continue
        for p in patterns:
            if p.lower() in nl:
                return canonical
    return "Other / Unknown"

@app.route("/api/axis9/performance-by-school")
def axis9_performance_by_school():
    school_year_key = request.args.get("school_year_key")
    wy = ""
    if school_year_key and school_year_key.isdigit():
        result = query(f"SELECT SchoolYearOid FROM dbo.DimSchoolYear WHERE SchoolYearKey = {int(school_year_key)}")
        if result and result[0].get("SchoolYearOid"):
            guid = result[0]["SchoolYearOid"]
            wy = f"AND ev.SchoolYearPeriodOid = '{guid}'"
    elif school_year_key:
        wy = f"AND ev.SchoolYearPeriodOid = '{school_year_key}'"
    
    rows = query(f"""
        SELECT s.LastSchool AS raw_school,
               COUNT(DISTINCT s.StudentOid) AS student_count,
               ROUND(AVG(CAST(ev.Average AS FLOAT)), 2) AS avg_score,
               ROUND(100.0 * SUM(CASE WHEN ev.Average >= 10 THEN 1 ELSE 0 END)
                   / NULLIF(COUNT(ev.FactKey), 0), 1) AS pass_rate,
               ROUND(100.0 * SUM(CASE WHEN att.IsPresent = 0 THEN 1 ELSE 0 END)
                   / NULLIF(COUNT(att.FactKey), 0), 1) AS absence_rate
        FROM dbo.DimStudent s
        LEFT JOIN dbo.FactStudentEvaluation ev ON s.StudentOid = ev.StudentOid AND ev.Average IS NOT NULL {wy}
        LEFT JOIN dbo.FactStudentAttendance att ON s.StudentOid = att.StudentOid
        WHERE s.StudentOid IS NOT NULL 
          AND s.LastSchool IS NOT NULL 
          AND s.LastSchool <> ''
        GROUP BY s.LastSchool""")
    agg = defaultdict(lambda: {"student_count": 0, "score_sum": 0.0, "score_n": 0,
                                "pass_sum": 0.0, "pass_n": 0, "absence_sum": 0.0, "absence_n": 0})
    for r in rows:
        c = _normalize_school(r["raw_school"])
        n = r["student_count"] or 0
        agg[c]["student_count"] += n
        if r["avg_score"] is not None:
            agg[c]["score_sum"] += (r["avg_score"] or 0) * n
            agg[c]["score_n"] += n
        if r["pass_rate"] is not None:
            agg[c]["pass_sum"] += (r["pass_rate"] or 0) * n
            agg[c]["pass_n"] += n
        if r["absence_rate"] is not None:
            agg[c]["absence_sum"] += (r["absence_rate"] or 0) * n
            agg[c]["absence_n"] += n
    result = [{"school_name": k, "student_count": a["student_count"],
               "avg_score": round(a["score_sum"] / a["score_n"], 2) if a["score_n"] else None,
               "pass_rate": round(a["pass_sum"] / a["pass_n"], 1) if a["pass_n"] else None,
               "absence_rate": round(a["absence_sum"] / a["absence_n"], 1) if a["absence_n"] else None}
              for k, a in agg.items()]
    result.sort(key=lambda x: x["student_count"], reverse=True)
    return jsonify(result)

@app.route("/api/axis9/school-ranking")
def axis9_school_ranking():
    school_year_key = request.args.get("school_year_key")
    wy = ""
    if school_year_key and school_year_key.isdigit():
        result = query(f"SELECT SchoolYearOid FROM dbo.DimSchoolYear WHERE SchoolYearKey = {int(school_year_key)}")
        if result and result[0].get("SchoolYearOid"):
            guid = result[0]["SchoolYearOid"]
            wy = f"AND ev.SchoolYearPeriodOid = '{guid}'"
    elif school_year_key:
        wy = f"AND ev.SchoolYearPeriodOid = '{school_year_key}'"
    
    rows = query(f"""
        SELECT s.LastSchool AS raw_school,
               COUNT(DISTINCT s.StudentOid) AS student_count,
               ROUND(AVG(CAST(ev.Average AS FLOAT)), 2) AS avg_score,
               ROUND(100.0 * SUM(CASE WHEN ev.Average >= 10 THEN 1 ELSE 0 END)
                   / NULLIF(COUNT(ev.FactKey), 0), 1) AS pass_rate
        FROM dbo.DimStudent s
        LEFT JOIN dbo.FactStudentEvaluation ev ON s.StudentOid = ev.StudentOid AND ev.Average IS NOT NULL {wy}
        WHERE s.StudentOid IS NOT NULL 
          AND s.LastSchool IS NOT NULL 
          AND s.LastSchool <> ''
        GROUP BY s.LastSchool 
        HAVING COUNT(DISTINCT s.StudentOid) >= 3""")
    agg = defaultdict(lambda: {"sc": 0, "ss": 0.0, "sn": 0, "ps": 0.0, "pn": 0})
    for r in rows:
        c = _normalize_school(r["raw_school"])
        n = r["student_count"] or 0
        agg[c]["sc"] += n
        if r["avg_score"] is not None:
            agg[c]["ss"] += (r["avg_score"] or 0) * n
            agg[c]["sn"] += n
        if r["pass_rate"] is not None:
            agg[c]["ps"] += (r["pass_rate"] or 0) * n
            agg[c]["pn"] += n
    result = [{"school_name": k, "student_count": v["sc"],
               "avg_score": round(v["ss"] / v["sn"], 2) if v["sn"] else None,
               "pass_rate": round(v["ps"] / v["pn"], 1) if v["pn"] else None}
              for k, v in agg.items() if v["sc"] >= 5]
    result.sort(key=lambda x: (x["avg_score"] or 0), reverse=True)
    return jsonify(result)

@app.route("/api/axis9/school-list")
def axis9_school_list():
    rows = query("SELECT DISTINCT LastSchool FROM dbo.DimStudent WHERE StudentOid IS NOT NULL AND LastSchool IS NOT NULL AND LastSchool <> ''")
    seen, result = set(), []
    for r in rows:
        c = _normalize_school(r["LastSchool"])
        if c not in seen:
            seen.add(c)
            result.append({"canonical": c, "sample_raw": r["LastSchool"]})
    return jsonify(result)

@app.route("/api/axis9/students-from-school/<school_name>")
def axis9_students_from_school(school_name):
    school_year_key = request.args.get("school_year_key")
    wy = ""
    if school_year_key and school_year_key.isdigit():
        result = query(f"SELECT SchoolYearOid FROM dbo.DimSchoolYear WHERE SchoolYearKey = {int(school_year_key)}")
        if result and result[0].get("SchoolYearOid"):
            guid = result[0]["SchoolYearOid"]
            wy = f"AND ev.SchoolYearPeriodOid = '{guid}'"
    elif school_year_key:
        wy = f"AND ev.SchoolYearPeriodOid = '{school_year_key}'"
    
    rows = query(f"""
        SELECT s.FirstName + ' ' + s.LastName AS student_name,
               s.LastSchool AS prev_school,
               ROUND(AVG(CAST(ev.Average AS FLOAT)), 2) AS avg_score,
               ROUND(100.0 * SUM(CASE WHEN att.IsPresent = 0 THEN 1 ELSE 0 END)
                   / NULLIF(COUNT(att.FactKey), 0), 1) AS absence_rate,
               ROUND(100.0 * SUM(CASE WHEN ev.Average >= 10 THEN 1 ELSE 0 END)
                   / NULLIF(COUNT(ev.FactKey), 0), 1) AS pass_rate
        FROM dbo.DimStudent s
        LEFT JOIN dbo.FactStudentEvaluation ev ON s.StudentOid = ev.StudentOid AND ev.Average IS NOT NULL {wy}
        LEFT JOIN dbo.FactStudentAttendance att ON s.StudentOid = att.StudentOid
        WHERE s.StudentOid IS NOT NULL 
          AND s.LastSchool IS NOT NULL 
          AND s.LastSchool <> ''
        GROUP BY s.FirstName, s.LastName, s.LastSchool
        HAVING COUNT(ev.FactKey) >= 2
        ORDER BY avg_score DESC""")
    return jsonify([r for r in rows if _normalize_school(r["prev_school"]) == school_name])

# ══════════════════════════════════════════════════════════════════════════════
# ERROR HANDLER
# ══════════════════════════════════════════════════════════════════════════════

from werkzeug.exceptions import HTTPException

@app.errorhandler(HTTPException)
def handle_http_error(e):
    return jsonify({"error": e.name, "message": e.description}), e.code

@app.errorhandler(Exception)
def handle_server_error(e):
    logger.error(f"Unhandled exception: {e}")
    return jsonify({"error": "Internal server error", "message": str(e)}), 500
# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    logger.info("Educated BI API → http://localhost:5000")
    app.run(debug=True, port=5000, use_reloader=False)