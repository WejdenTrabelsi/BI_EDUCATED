"""
dashboard/app.py — reuses project db/connection.py directly.
Run from inside education_bi/ :  python dashboard/app.py
"""
import sys, os
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

def wf(alias="f"):
    k = request.args.get("school_year_key")
    return f"AND {alias}.SchoolYearKey = {int(k)}" if k else ""

@app.before_request
def _once():
    app.before_request_funcs[None].remove(_once)
    test_connection()

@app.route("/api/school-years")
def school_years():
    return jsonify(query("SELECT SchoolYearKey,YearLabel,IsCurrent FROM dbo.DimSchoolYear WHERE SchoolYearKey<>1 ORDER BY SchoolYearKey"))

@app.route("/api/axis1/kpis")
def axis1_kpis():
    rows = query(f"""
        SELECT COUNT(DISTINCT f.StudentKey) AS total_students,
               ROUND(AVG(CAST(f.Score AS FLOAT)),2) AS avg_score,
               ROUND(100.0*SUM(CASE WHEN f.Score>=10 THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),1) AS pass_rate,
               ROUND(100.0*SUM(CASE WHEN f.Score< 10 THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),1) AS fail_rate,
               MAX(f.Score) AS max_score, MIN(f.Score) AS min_score
        FROM dbo.FactStudentEvaluation f
        WHERE f.StudentKey<>1 AND f.Score IS NOT NULL {wf()}""")
    return jsonify(rows[0] if rows else {})

@app.route("/api/axis1/avg-by-subject")
def axis1_avg_by_subject():
    return jsonify(query(f"""
        SELECT c.ContentName AS subject,
               ROUND(AVG(CAST(f.Score AS FLOAT)),2) AS avg_score,
               COUNT(*) AS n_evaluations,
               ROUND(100.0*SUM(CASE WHEN f.Score>=10 THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),1) AS pass_rate
        FROM dbo.FactStudentEvaluation f
        JOIN dbo.DimContent c ON f.ContentKey=c.ContentKey
        WHERE f.StudentKey<>1 AND f.Score IS NOT NULL AND c.ContentKey<>1 {wf()}
        GROUP BY c.ContentName ORDER BY avg_score DESC"""))

@app.route("/api/axis1/score-distribution")
def axis1_score_distribution():
    return jsonify(query(f"""
        SELECT CASE WHEN Score<2 THEN '0-2' WHEN Score<4 THEN '2-4' WHEN Score<6 THEN '4-6'
                    WHEN Score<8 THEN '6-8' WHEN Score<10 THEN '8-10' WHEN Score<12 THEN '10-12'
                    WHEN Score<14 THEN '12-14' WHEN Score<16 THEN '14-16' WHEN Score<18 THEN '16-18'
                    ELSE '18-20' END AS bin, COUNT(*) AS count
        FROM dbo.FactStudentEvaluation f
        WHERE f.StudentKey<>1 AND f.Score IS NOT NULL {wf()}
        GROUP BY CASE WHEN Score<2 THEN '0-2' WHEN Score<4 THEN '2-4' WHEN Score<6 THEN '4-6'
                      WHEN Score<8 THEN '6-8' WHEN Score<10 THEN '8-10' WHEN Score<12 THEN '10-12'
                      WHEN Score<14 THEN '12-14' WHEN Score<16 THEN '14-16' WHEN Score<18 THEN '16-18'
                      ELSE '18-20' END ORDER BY MIN(Score)"""))

@app.route("/api/axis1/avg-by-school-year")
def axis1_avg_by_school_year():
    return jsonify(query("""
        SELECT sy.YearLabel AS year_label, sy.SchoolYearKey AS school_year_key,
               ROUND(AVG(CAST(f.Score AS FLOAT)),2) AS avg_score,
               ROUND(100.0*SUM(CASE WHEN f.Score>=10 THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),1) AS pass_rate,
               COUNT(DISTINCT f.StudentKey) AS student_count
        FROM dbo.FactStudentEvaluation f
        JOIN dbo.DimSchoolYear sy ON f.SchoolYearKey=sy.SchoolYearKey
        WHERE f.StudentKey<>1 AND f.Score IS NOT NULL AND sy.SchoolYearKey<>1
        GROUP BY sy.YearLabel,sy.SchoolYearKey ORDER BY sy.SchoolYearKey"""))

@app.route("/api/axis1/pass-fail-by-subject")
def axis1_pass_fail_by_subject():
    return jsonify(query(f"""
        SELECT TOP 12 c.ContentName AS subject,
               SUM(CASE WHEN f.Score>=10 THEN 1 ELSE 0 END) AS pass_count,
               SUM(CASE WHEN f.Score< 10 THEN 1 ELSE 0 END) AS fail_count,
               COUNT(*) AS total
        FROM dbo.FactStudentEvaluation f
        JOIN dbo.DimContent c ON f.ContentKey=c.ContentKey
        WHERE f.StudentKey<>1 AND f.Score IS NOT NULL AND c.ContentKey<>1 {wf()}
        GROUP BY c.ContentName ORDER BY total DESC"""))

@app.route("/api/axis3/kpis")
def axis3_kpis():
    w = wf()
    s = query(f"""
        SELECT COUNT(DISTINCT f.StudentKey) AS students_tracked,
               ROUND(100.0*SUM(CASE WHEN f.IsPresent=1 THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),1) AS presence_rate,
               ROUND(100.0*SUM(CASE WHEN f.IsLate=1    THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),1) AS late_rate,
               COUNT(*) AS total_records
        FROM dbo.FactStudentAttendance f WHERE f.StudentKey<>1 {w}""")
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
        WHERE f.StudentKey<>1 AND d.DateKey<>-1 {wf()}
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
        JOIN dbo.DimStudent s ON f.StudentKey=s.StudentKey
        WHERE f.StudentKey<>1 AND s.StudentKey<>1 {wf()}
        GROUP BY s.FirstName,s.LastName HAVING COUNT(*)>=3
        ORDER BY absence_rate DESC"""))

@app.route("/api/axis3/attendance-vs-score")
def axis3_attendance_vs_score():
    w = wf("att")
    return jsonify(query(f"""
        SELECT s.FirstName+' '+s.LastName AS student_name,
               ROUND(100.0*SUM(CASE WHEN att.IsPresent=0 THEN 1 ELSE 0 END)/NULLIF(COUNT(att.FactKey),0),1) AS absence_rate,
               ROUND(AVG(CAST(ev.Score AS FLOAT)),2) AS avg_score
        FROM dbo.FactStudentAttendance att
        JOIN dbo.DimStudent s ON att.StudentKey=s.StudentKey
        JOIN dbo.FactStudentEvaluation ev
             ON att.StudentKey=ev.StudentKey AND att.SchoolYearKey=ev.SchoolYearKey
        WHERE att.StudentKey<>1 AND s.StudentKey<>1 AND ev.Score IS NOT NULL {w}
        GROUP BY s.FirstName,s.LastName
        HAVING COUNT(att.FactKey)>=3 AND COUNT(ev.FactKey)>=3"""))

@app.errorhandler(Exception)
def handle_error(e):
    logger.error(f"API error: {e}")
    return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    logger.info("Educated BI API → http://localhost:5000")
    app.run(debug=True, port=5000, use_reloader=False)

# ══════════════════════════════════════════════════════════════════════════════
# AXIS 2 — Temporal Progression & Trends
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/axis2/progression-by-student")
def axis2_progression_by_student():
    """Score per school year per student — for slope/trend lines."""
    student_key = request.args.get("student_key")
    w = f"AND f.StudentKey = {int(student_key)}" if student_key else ""
    return jsonify(query(f"""
        SELECT sy.YearLabel AS year_label, sy.SchoolYearKey,
               s.FirstName+' '+s.LastName AS student_name,
               ROUND(AVG(CAST(f.Score AS FLOAT)),2) AS avg_score,
               COUNT(*) AS n_evals
        FROM dbo.FactStudentEvaluation f
        JOIN dbo.DimSchoolYear sy ON f.SchoolYearKey=sy.SchoolYearKey
        JOIN dbo.DimStudent s    ON f.StudentKey=s.StudentKey
        WHERE f.StudentKey<>1 AND s.StudentKey<>1
          AND f.Score IS NOT NULL AND sy.SchoolYearKey<>1 {w}
        GROUP BY sy.YearLabel,sy.SchoolYearKey,s.FirstName,s.LastName
        ORDER BY sy.SchoolYearKey"""))


@app.route("/api/axis2/year-over-year-by-subject")
def axis2_yoy_by_subject():
    """Average score per subject per school year — heatmap/multi-line data."""
    return jsonify(query("""
        SELECT sy.YearLabel AS year_label, sy.SchoolYearKey,
               c.ContentName AS subject,
               ROUND(AVG(CAST(f.Score AS FLOAT)),2) AS avg_score
        FROM dbo.FactStudentEvaluation f
        JOIN dbo.DimSchoolYear sy ON f.SchoolYearKey=sy.SchoolYearKey
        JOIN dbo.DimContent    c  ON f.ContentKey=c.ContentKey
        WHERE f.StudentKey<>1 AND f.Score IS NOT NULL
          AND sy.SchoolYearKey<>1 AND c.ContentKey<>1
        GROUP BY sy.YearLabel,sy.SchoolYearKey,c.ContentName
        ORDER BY sy.SchoolYearKey,c.ContentName"""))


@app.route("/api/axis2/stability-index")
def axis2_stability_index():
    """
    Performance stability per student across years:
    low stddev = stable, high stddev = volatile.
    Approximated as MAX-MIN range (SQL Server has no STDEV on grouped sets easily).
    """
    wf2 = wf()
    return jsonify(query(f"""
        SELECT TOP 20
            s.FirstName+' '+s.LastName AS student_name,
            COUNT(DISTINCT f.SchoolYearKey) AS years_active,
            ROUND(AVG(CAST(f.Score AS FLOAT)),2)  AS avg_score,
            ROUND(MAX(CAST(f.Score AS FLOAT)) - MIN(CAST(f.Score AS FLOAT)),2) AS score_range,
            ROUND(STDEV(CAST(f.Score AS FLOAT)),2) AS score_stddev
        FROM dbo.FactStudentEvaluation f
        JOIN dbo.DimStudent s ON f.StudentKey=s.StudentKey
        WHERE f.StudentKey<>1 AND s.StudentKey<>1 AND f.Score IS NOT NULL
        GROUP BY s.FirstName,s.LastName
        HAVING COUNT(DISTINCT f.SchoolYearKey)>=2 AND COUNT(*)>=5
        ORDER BY score_stddev ASC"""))


@app.route("/api/axis2/regression-detection")
def axis2_regression():
    """
    Students whose avg score in the latest year is significantly lower
    than their historical average (regression flag).
    """
    return jsonify(query("""
        WITH history AS (
            SELECT f.StudentKey,
                   AVG(CAST(f.Score AS FLOAT)) AS hist_avg,
                   MAX(sy.SchoolYearKey)        AS latest_year_key
            FROM dbo.FactStudentEvaluation f
            JOIN dbo.DimSchoolYear sy ON f.SchoolYearKey=sy.SchoolYearKey
            WHERE f.StudentKey<>1 AND f.Score IS NOT NULL AND sy.SchoolYearKey<>1
            GROUP BY f.StudentKey
        ),
        latest AS (
            SELECT f.StudentKey,
                   AVG(CAST(f.Score AS FLOAT)) AS latest_avg
            FROM dbo.FactStudentEvaluation f
            JOIN history h ON f.StudentKey=h.StudentKey
                          AND f.SchoolYearKey=h.latest_year_key
            WHERE f.Score IS NOT NULL
            GROUP BY f.StudentKey
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
        JOIN latest  l ON h.StudentKey=l.StudentKey
        JOIN dbo.DimStudent s ON h.StudentKey=s.StudentKey
        WHERE s.StudentKey<>1
        ORDER BY delta ASC"""))


@app.route("/api/axis2/semester-comparison")
def axis2_semester():
    """Avg score per evaluation report (semester proxy) per school year."""
    return jsonify(query("""
        SELECT sy.YearLabel AS year_label,
               er.ReportName AS report_name,
               ROUND(AVG(CAST(f.Score AS FLOAT)),2) AS avg_score,
               COUNT(DISTINCT f.StudentKey) AS student_count
        FROM dbo.FactStudentEvaluation f
        JOIN dbo.DimSchoolYear        sy ON f.SchoolYearKey=sy.SchoolYearKey
        JOIN dbo.DimEvaluationReport  er ON f.EvaluationReportKey=er.EvaluationReportKey
        WHERE f.StudentKey<>1 AND f.Score IS NOT NULL
          AND sy.SchoolYearKey<>1 AND er.EvaluationReportKey<>1
        GROUP BY sy.YearLabel,er.ReportName
        ORDER BY sy.YearLabel,er.ReportName"""))


# ══════════════════════════════════════════════════════════════════════════════
# AXIS 4 — Weather Impact  (Open-Meteo historical API)
# ══════════════════════════════════════════════════════════════════════════════
import urllib.request, json as _json, datetime as _dt

SOUSSE_LAT  = 35.8288
SOUSSE_LON  = 10.6400

def _fetch_weather(start_date: str, end_date: str) -> list[dict]:
    """
    Pull daily precipitation_sum from Open-Meteo historical archive.
    Returns list of {date, precipitation_mm, is_rainy}.
    Free, no API key required.
    """
    url = (
        f"https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={SOUSSE_LAT}&longitude={SOUSSE_LON}"
        f"&start_date={start_date}&end_date={end_date}"
        f"&daily=precipitation_sum&timezone=Africa%2FTunis"
    )
    try:
        with urllib.request.urlopen(url, timeout=8) as r:
            data = _json.loads(r.read())
        dates  = data["daily"]["time"]
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
    """
    Join student attendance (by date) with Open-Meteo precipitation.
    Returns daily: date, absence_rate, precipitation_mm, is_rainy.
    """
    school_year_key = request.args.get("school_year_key")
    wh = f"AND f.SchoolYearKey={int(school_year_key)}" if school_year_key else ""

    attendance = query(f"""
        SELECT d.FullDate AS full_date,
               COUNT(*) AS total,
               SUM(CASE WHEN f.IsPresent=0 THEN 1 ELSE 0 END) AS absent_count,
               ROUND(100.0*SUM(CASE WHEN f.IsPresent=0 THEN 1 ELSE 0 END)
                     /NULLIF(COUNT(*),0),1) AS absence_rate
        FROM dbo.FactStudentAttendance f
        JOIN dbo.DimDate d ON f.DateKey=d.DateKey
        WHERE f.StudentKey<>1 AND d.DateKey<>-1 AND d.FullDate IS NOT NULL {wh}
        GROUP BY d.FullDate ORDER BY d.FullDate""")

    if not attendance:
        return jsonify([])

    dates = [r["full_date"] for r in attendance]
    start = str(min(dates))[:10]
    end   = str(max(dates))[:10]
    weather = {w["date"]: w for w in _fetch_weather(start, end)}

    result = []
    for row in attendance:
        d_str = str(row["full_date"])[:10]
        w     = weather.get(d_str, {"precipitation_mm": 0.0, "is_rainy": False})
        result.append({**row, "full_date": d_str,
                        "precipitation_mm": w["precipitation_mm"],
                        "is_rainy": w["is_rainy"]})
    return jsonify(result)


@app.route("/api/axis4/rainy-vs-dry-summary")
def axis4_rainy_vs_dry():
    """
    Aggregate absence rate split by rainy vs dry days.
    Calls weather-vs-absence internally and aggregates.
    """
    school_year_key = request.args.get("school_year_key")
    wh = f"AND f.SchoolYearKey={int(school_year_key)}" if school_year_key else ""

    attendance = query(f"""
        SELECT d.FullDate AS full_date,
               ROUND(100.0*SUM(CASE WHEN f.IsPresent=0 THEN 1 ELSE 0 END)
                     /NULLIF(COUNT(*),0),1) AS absence_rate
        FROM dbo.FactStudentAttendance f
        JOIN dbo.DimDate d ON f.DateKey=d.DateKey
        WHERE f.StudentKey<>1 AND d.DateKey<>-1 AND d.FullDate IS NOT NULL {wh}
        GROUP BY d.FullDate""")

    if not attendance:
        return jsonify({"rainy_avg": None, "dry_avg": None, "n_rainy": 0, "n_dry": 0})

    dates   = [r["full_date"] for r in attendance]
    start   = str(min(dates))[:10]
    end     = str(max(dates))[:10]
    weather = {w["date"]: w for w in _fetch_weather(start, end)}

    rainy, dry = [], []
    for row in attendance:
        d_str = str(row["full_date"])[:10]
        w     = weather.get(d_str, {"is_rainy": False})
        (rainy if w["is_rainy"] else dry).append(row["absence_rate"])

    def safe_avg(lst):
        return round(sum(lst)/len(lst), 1) if lst else None

    return jsonify({
        "rainy_avg": safe_avg(rainy), "n_rainy": len(rainy),
        "dry_avg":   safe_avg(dry),   "n_dry":   len(dry),
    })


@app.route("/api/axis4/seasonal-patterns")
def axis4_seasonal():
    """Monthly average absence rate + average precipitation."""
    school_year_key = request.args.get("school_year_key")
    wh = f"AND f.SchoolYearKey={int(school_year_key)}" if school_year_key else ""

    monthly_att = query(f"""
        SELECT d.Month AS month, d.MonthName AS month_name,
               ROUND(100.0*SUM(CASE WHEN f.IsPresent=0 THEN 1 ELSE 0 END)
                     /NULLIF(COUNT(*),0),1) AS absence_rate,
               MIN(CAST(d.FullDate AS VARCHAR(10))) AS sample_start,
               MAX(CAST(d.FullDate AS VARCHAR(10))) AS sample_end
        FROM dbo.FactStudentAttendance f
        JOIN dbo.DimDate d ON f.DateKey=d.DateKey
        WHERE f.StudentKey<>1 AND d.DateKey<>-1 AND d.FullDate IS NOT NULL {wh}
        GROUP BY d.Month,d.MonthName ORDER BY d.Month""")

    if not monthly_att:
        return jsonify([])

    all_dates = [r for r in monthly_att if r["sample_start"]]
    if all_dates:
        start = min(r["sample_start"] for r in all_dates)
        end   = max(r["sample_end"]   for r in all_dates)
        daily_w = _fetch_weather(start[:10], end[:10])
        month_precip: dict[int, list] = {}
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
    """Student outcomes per evaluation report for a given subject."""
    subject = request.args.get("subject", "")
    wh = f"AND c.ContentName='{subject}'" if subject else ""
    school_year_key = request.args.get("school_year_key")
    wy = f"AND f.SchoolYearKey={int(school_year_key)}" if school_year_key else ""
    return jsonify(query(f"""
        SELECT er.ReportName AS report_name,
               c.ContentName AS subject,
               sy.YearLabel  AS year_label,
               ROUND(AVG(CAST(f.Score AS FLOAT)),2)  AS avg_score,
               ROUND(STDEV(CAST(f.Score AS FLOAT)),2) AS score_stddev,
               COUNT(DISTINCT f.StudentKey) AS student_count,
               ROUND(100.0*SUM(CASE WHEN f.Score>=10 THEN 1 ELSE 0 END)
                     /NULLIF(COUNT(*),0),1) AS pass_rate
        FROM dbo.FactStudentEvaluation f
        JOIN dbo.DimEvaluationReport er ON f.EvaluationReportKey=er.EvaluationReportKey
        JOIN dbo.DimContent          c  ON f.ContentKey=c.ContentKey
        JOIN dbo.DimSchoolYear       sy ON f.SchoolYearKey=sy.SchoolYearKey
        WHERE f.StudentKey<>1 AND f.Score IS NOT NULL
          AND er.EvaluationReportKey<>1 AND c.ContentKey<>1 AND sy.SchoolYearKey<>1
          {wh} {wy}
        GROUP BY er.ReportName,c.ContentName,sy.YearLabel
        ORDER BY sy.YearLabel,er.ReportName"""))


@app.route("/api/axis5/dispersion-by-report")
def axis5_dispersion():
    """Score dispersion (stddev, min, max, Q1, Q3) per report — box plot data."""
    school_year_key = request.args.get("school_year_key")
    wy = f"AND f.SchoolYearKey={int(school_year_key)}" if school_year_key else ""
    return jsonify(query(f"""
        SELECT er.ReportName AS report_name,
               ROUND(MIN(CAST(f.Score AS FLOAT)),2)  AS score_min,
               ROUND(MAX(CAST(f.Score AS FLOAT)),2)  AS score_max,
               ROUND(AVG(CAST(f.Score AS FLOAT)),2)  AS score_avg,
               ROUND(STDEV(CAST(f.Score AS FLOAT)),2) AS score_stddev,
               COUNT(*) AS n
        FROM dbo.FactStudentEvaluation f
        JOIN dbo.DimEvaluationReport er ON f.EvaluationReportKey=er.EvaluationReportKey
        WHERE f.StudentKey<>1 AND f.Score IS NOT NULL
          AND er.EvaluationReportKey<>1 {wy}
        GROUP BY er.ReportName
        ORDER BY er.ReportName"""))


@app.route("/api/axis5/subject-list")
def axis5_subject_list():
    return jsonify(query("""
        SELECT DISTINCT c.ContentName AS subject
        FROM dbo.DimContent c
        WHERE c.ContentKey<>1
        ORDER BY c.ContentName"""))


@app.route("/api/axis5/presence-performance-by-report")
def axis5_presence_performance():
    """Per evaluation report: attendance rate + avg score — correlation view."""
    school_year_key = request.args.get("school_year_key")
    wy  = f"AND ev.SchoolYearKey={int(school_year_key)}" if school_year_key else ""
    wya = f"AND att.SchoolYearKey={int(school_year_key)}" if school_year_key else ""
    return jsonify(query(f"""
        SELECT er.ReportName AS report_name,
               ROUND(AVG(CAST(ev.Score AS FLOAT)),2) AS avg_score,
               ROUND(100.0*SUM(CASE WHEN att.IsPresent=1 THEN 1 ELSE 0 END)
                     /NULLIF(COUNT(att.FactKey),0),1) AS presence_rate
        FROM dbo.FactStudentEvaluation ev
        JOIN dbo.DimEvaluationReport er ON ev.EvaluationReportKey=er.EvaluationReportKey
        LEFT JOIN dbo.FactStudentAttendance att
               ON ev.StudentKey=att.StudentKey AND ev.SchoolYearKey=att.SchoolYearKey
        WHERE ev.StudentKey<>1 AND ev.Score IS NOT NULL
          AND er.EvaluationReportKey<>1 {wy} {wya}
        GROUP BY er.ReportName
        ORDER BY er.ReportName"""))


# ══════════════════════════════════════════════════════════════════════════════
# AXIS 6 — Academic Risk Detection
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/axis6/risk-scores")
def axis6_risk_scores():
    """
    Composite risk score per student (0–100, higher = more at risk).
    Formula weights (tunable):
      - Academic score below average  → up to 40 pts
      - Absence rate                  → up to 35 pts
      - Payment issues (fail rate)    → up to 25 pts
    """
    school_year_key = request.args.get("school_year_key")
    wy  = f"AND ev.SchoolYearKey={int(school_year_key)}" if school_year_key else ""
    wya = f"AND att.SchoolYearKey={int(school_year_key)}" if school_year_key else ""
    wyp = f"AND p.SchoolYearKey={int(school_year_key)}"   if school_year_key else ""

    return jsonify(query(f"""
        WITH academic AS (
            SELECT ev.StudentKey,
                   ROUND(AVG(CAST(ev.Score AS FLOAT)),2) AS avg_score,
                   ROUND(100.0*SUM(CASE WHEN ev.Score<10 THEN 1 ELSE 0 END)
                         /NULLIF(COUNT(*),0),1) AS fail_rate_pct
            FROM dbo.FactStudentEvaluation ev
            WHERE ev.StudentKey<>1 AND ev.Score IS NOT NULL {wy}
            GROUP BY ev.StudentKey
        ),
        attendance AS (
            SELECT att.StudentKey,
                   ROUND(100.0*SUM(CASE WHEN att.IsPresent=0 THEN 1 ELSE 0 END)
                         /NULLIF(COUNT(*),0),1) AS absence_rate
            FROM dbo.FactStudentAttendance att
            WHERE att.StudentKey<>1 {wya}
            GROUP BY att.StudentKey
        ),
        payment AS (
            SELECT p.StudentKey,
                   COUNT(*) AS payment_count
            FROM dbo.FactStudentPayment p
            WHERE p.StudentKey<>1 {wyp}
            GROUP BY p.StudentKey
        )
        SELECT TOP 30
            s.FirstName+' '+s.LastName AS student_name,
            ROUND(a.avg_score,2)       AS avg_score,
            COALESCE(att.absence_rate,0) AS absence_rate,
            a.fail_rate_pct            AS fail_rate_pct,
            COALESCE(pay.payment_count,0) AS payment_count,
            -- Risk score: higher = more at risk
            ROUND(
                LEAST(100,
                    -- Academic risk: max 40 pts when score=0
                    (CASE WHEN a.avg_score < 10
                          THEN (10 - a.avg_score) * 4.0
                          ELSE 0 END)
                    -- Absence risk: max 35 pts at 100% absent
                    + COALESCE(att.absence_rate,0) * 0.35
                    -- Fail rate risk: max 25 pts at 100% fail
                    + a.fail_rate_pct * 0.25
                ), 1
            ) AS risk_score,
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
        JOIN dbo.DimStudent s   ON a.StudentKey=s.StudentKey
        LEFT JOIN attendance att ON a.StudentKey=att.StudentKey
        LEFT JOIN payment    pay ON a.StudentKey=pay.StudentKey
        WHERE s.StudentKey<>1
        ORDER BY risk_score DESC"""))


@app.route("/api/axis6/risk-distribution")
def axis6_risk_distribution():
    """Count of students per risk level + avg metrics per level."""
    school_year_key = request.args.get("school_year_key")
    wy  = f"AND ev.SchoolYearKey={int(school_year_key)}" if school_year_key else ""
    wya = f"AND att.SchoolYearKey={int(school_year_key)}" if school_year_key else ""
    return jsonify(query(f"""
        WITH academic AS (
            SELECT ev.StudentKey,
                   AVG(CAST(ev.Score AS FLOAT)) AS avg_score,
                   100.0*SUM(CASE WHEN ev.Score<10 THEN 1 ELSE 0 END)
                        /NULLIF(COUNT(*),0) AS fail_rate_pct
            FROM dbo.FactStudentEvaluation ev
            WHERE ev.StudentKey<>1 AND ev.Score IS NOT NULL {wy}
            GROUP BY ev.StudentKey
        ),
        attendance AS (
            SELECT att.StudentKey,
                   100.0*SUM(CASE WHEN att.IsPresent=0 THEN 1 ELSE 0 END)
                        /NULLIF(COUNT(*),0) AS absence_rate
            FROM dbo.FactStudentAttendance att
            WHERE att.StudentKey<>1 {wya}
            GROUP BY att.StudentKey
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
            LEFT JOIN attendance att ON a.StudentKey=att.StudentKey
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
    """Students with at least 2 of 3 warning signals active."""
    school_year_key = request.args.get("school_year_key")
    wy  = f"AND ev.SchoolYearKey={int(school_year_key)}" if school_year_key else ""
    wya = f"AND att.SchoolYearKey={int(school_year_key)}" if school_year_key else ""
    return jsonify(query(f"""
        WITH academic AS (
            SELECT ev.StudentKey,
                   AVG(CAST(ev.Score AS FLOAT)) AS avg_score,
                   100.0*SUM(CASE WHEN ev.Score<10 THEN 1 ELSE 0 END)
                        /NULLIF(COUNT(*),0) AS fail_rate_pct
            FROM dbo.FactStudentEvaluation ev
            WHERE ev.StudentKey<>1 AND ev.Score IS NOT NULL {wy}
            GROUP BY ev.StudentKey
        ),
        attendance AS (
            SELECT att.StudentKey,
                   100.0*SUM(CASE WHEN att.IsPresent=0 THEN 1 ELSE 0 END)
                        /NULLIF(COUNT(*),0) AS absence_rate
            FROM dbo.FactStudentAttendance att
            WHERE att.StudentKey<>1 {wya}
            GROUP BY att.StudentKey
        )
        SELECT TOP 20
            s.FirstName+' '+s.LastName AS student_name,
            ROUND(a.avg_score,2)       AS avg_score,
            ROUND(COALESCE(att.absence_rate,0),1) AS absence_rate,
            ROUND(a.fail_rate_pct,1)   AS fail_rate_pct,
            -- Individual warning flags
            CASE WHEN a.avg_score < 10 THEN 1 ELSE 0 END AS flag_low_score,
            CASE WHEN COALESCE(att.absence_rate,0) > 20 THEN 1 ELSE 0 END AS flag_high_absence,
            CASE WHEN a.fail_rate_pct > 50 THEN 1 ELSE 0 END AS flag_high_fail_rate,
            -- Total warnings
            (CASE WHEN a.avg_score < 10 THEN 1 ELSE 0 END
            +CASE WHEN COALESCE(att.absence_rate,0)>20 THEN 1 ELSE 0 END
            +CASE WHEN a.fail_rate_pct>50 THEN 1 ELSE 0 END) AS warning_count
        FROM academic a
        JOIN dbo.DimStudent s ON a.StudentKey=s.StudentKey
        LEFT JOIN attendance att ON a.StudentKey=att.StudentKey
        WHERE s.StudentKey<>1
          AND (CASE WHEN a.avg_score<10 THEN 1 ELSE 0 END
              +CASE WHEN COALESCE(att.absence_rate,0)>20 THEN 1 ELSE 0 END
              +CASE WHEN a.fail_rate_pct>50 THEN 1 ELSE 0 END) >= 2
        ORDER BY warning_count DESC, a.avg_score ASC"""))
