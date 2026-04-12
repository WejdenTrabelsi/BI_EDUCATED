import { useState, useEffect } from "react";

import Axis1 from "./axes/Axis1";
import Axis2 from "./axes/Axis2";
import Axis3 from "./axes/Axis3";
import Axis4 from "./axes/Axis4";
import Axis5 from "./axes/Axis5";
import Axis6 from "./axes/Axis6";
import Axis7 from "./axes/Axis7";
import Axis8 from "./axes/Axis8";
import Axis9 from "./axes/Axis9";

const API = "http://localhost:5000/api";

const C = {
  teal: "#2DD4BF", amber: "#F59E0B", rose: "#FB7185", indigo: "#818CF8",
  violet: "#A78BFA", emerald: "#34D399", sky: "#38BDF8", orange: "#FB923C",
  bg: "#0F172A", card: "#1E293B", cardHover: "#243044",
  border: "#334155", text: "#F1F5F9", textMuted: "#94A3B8",
};

const AXES = [
  { id: "axis1", label: "A1 — Academic", color: C.teal },
  { id: "axis2", label: "A2 — Trends", color: C.violet },
  { id: "axis3", label: "A3 — Attendance", color: C.rose },
  { id: "axis4", label: "A4 — Weather", color: C.sky },
  { id: "axis5", label: "A5 — Sessions", color: C.emerald },
  { id: "axis6", label: "A6 — Risk", color: C.amber },
  { id: "axis7", label: "A7 — Geography", color: C.indigo },
  { id: "axis8", label: "A8 — Holiday Impact", color: C.orange },
  { id: "axis9", label: "A9 — Prep Schools", color: C.emerald },
];

export default function Dashboard() {
  const [axis, setAxis] = useState("axis1");
  const [yearKey, setYearKey] = useState("");
  const [years, setYears] = useState([]);
  const [yearErr, setYearErr] = useState(null);

  useEffect(() => {
    fetch(`${API}/school-years`)
      .then(r => r.ok ? r.json() : Promise.reject("Failed to load years"))
      .then(y => {
        setYears(y);
        const current = y.find(x => x.IsCurrent || x.isCurrent);
        if (current) setYearKey(String(current.SchoolYearKey || current.id));
      })
      .catch(e => {
        console.error(e);
        setYearErr(e.message || "Year API error");
      });
  }, []);

  const qs = yearKey ? `?school_year_key=${yearKey}` : "";
  const currentColor = AXES.find(a => a.id === axis)?.color || C.teal;

  return (
    <div style={{ minHeight: "100vh", background: C.bg, color: C.text, fontFamily: "'DM Sans', sans-serif", paddingBottom: 48 }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=Space+Mono:wght@400;700&display=swap');
        * { box-sizing: border-box; }
        ::-webkit-scrollbar { width: 5px; background: ${C.bg}; }
        ::-webkit-scrollbar-thumb { background: ${C.border}; border-radius: 3px; }
      `}</style>

      {/* Header */}
      <div style={{ background: `${C.card}F2`, backdropFilter: "blur(16px)", borderBottom: `1px solid ${C.border}`, position: "sticky", top: 0, zIndex: 100 }}>
        <div style={{ maxWidth: 1300, margin: "0 auto", display: "flex", alignItems: "center", justifyContent: "space-between", height: 62, padding: "0 36px", gap: 12, flexWrap: "wrap" }}>
          <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <div style={{ width: 34, height: 34, borderRadius: 9, background: `linear-gradient(135deg,${C.teal},${C.indigo})`, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 13, fontWeight: 700, color: "#0F172A" }}>E</div>
            <div>
              <div style={{ fontSize: 15, fontWeight: 700, letterSpacing: "-0.3px" }}>Educated BI</div>
              <div style={{ fontSize: 10, color: C.textMuted }}>Intelligent Analytics Platform</div>
            </div>
          </div>

          {/* Axis Navigation */}
          <div style={{ display: "flex", gap: 6, flexWrap: "wrap", justifyContent: "center" }}>
            {AXES.map(({ id, label, color }) => (
              <button
                key={id}
                onClick={() => setAxis(id)}
                style={{
                  background: axis === id ? `${color}18` : "transparent",
                  border: `1px solid ${axis === id ? color : C.border}`,
                  borderRadius: 8,
                  padding: "6px 12px",
                  cursor: "pointer",
                  color: axis === id ? color : C.textMuted,
                  fontSize: 11,
                  fontWeight: axis === id ? 600 : 400,
                }}
              >
                {label}
              </button>
            ))}
          </div>

          {/* Year Selector */}
          <div style={{ display: "flex", alignItems: "center", gap: 8, flexShrink: 0 }}>
            <span style={{ fontSize: 11, color: yearErr ? C.rose : C.textMuted }}>
              {yearErr ? "Year API error" : "Year"}
            </span>
            <select
              value={yearKey}
              onChange={e => setYearKey(e.target.value)}
              style={{
                background: C.card,
                border: `1px solid ${yearErr ? C.rose : C.border}`,
                borderRadius: 7,
                color: C.text,
                padding: "5px 10px",
                fontSize: 12,
                outline: "none",
                cursor: "pointer"
              }}
            >
              <option value="">All years</option>
              {years.map(y => (
                <option key={y.SchoolYearKey || y.id} value={String(y.SchoolYearKey || y.id)}>
                  {y.YearLabel || y.name || y.Description}{y.IsCurrent || y.isCurrent ? " ★" : ""}
                </option>
              ))}
            </select>
          </div>
        </div>
      </div>

      <div style={{ height: 2, background: `linear-gradient(90deg,${currentColor},${currentColor}40,transparent)` }} />

      <div style={{ maxWidth: 1300, margin: "0 auto", padding: "28px 36px" }}>
        {axis === "axis1" && <Axis1 qs={qs} />}
        {axis === "axis2" && <Axis2 qs={qs} />}
        {axis === "axis3" && <Axis3 qs={qs} />}
        {axis === "axis4" && <Axis4 qs={qs} />}
        {axis === "axis5" && <Axis5 qs={qs} />}
        {axis === "axis6" && <Axis6 qs={qs} />}
        {axis === "axis7" && <Axis7 qs={qs} />}
        {axis === "axis8" && <Axis8 qs={qs} />}
        {axis === "axis9" && <Axis9 qs={qs} />}
      </div>
    </div>
  );
}