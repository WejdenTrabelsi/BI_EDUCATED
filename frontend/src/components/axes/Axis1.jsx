import { useState, useEffect, useRef } from "react";
import { Chart, registerables } from "chart.js";
Chart.register(...registerables);

// Shared constants (you can move these to a separate file later)
const C = {
  teal: "#2DD4BF", amber: "#F59E0B", rose: "#FB7185", indigo: "#818CF8",
  violet: "#A78BFA", emerald: "#34D399", sky: "#38BDF8", orange: "#FB923C",
  bg: "#0F172A", card: "#1E293B", cardHover: "#243044",
  border: "#334155", text: "#F1F5F9", textMuted: "#94A3B8",
};

const CD = {
  responsive: true,
  maintainAspectRatio: false,
  plugins: {
    legend: { labels: { color: C.textMuted, font: { family: "'DM Sans', sans-serif", size: 11 }, boxWidth: 10, padding: 12 } },
    tooltip: {
      backgroundColor: "#1E293B",
      borderColor: C.border,
      borderWidth: 1,
      titleColor: C.text,
      bodyColor: C.textMuted,
      padding: 10,
      cornerRadius: 8,
    },
  },
  scales: {
    x: { ticks: { color: C.textMuted, font: { family: "'DM Sans', sans-serif", size: 10 } }, grid: { color: "rgba(51,65,85,0.4)" } },
    y: { ticks: { color: C.textMuted, font: { family: "'DM Sans', sans-serif", size: 10 } }, grid: { color: "rgba(51,65,85,0.4)" } },
  },
};

// You need to import these from where they are defined
// For now, I'll assume they are in the same folder or you will import them
// import { apiFetch, useChart } from "../../utils/chartUtils";
// import { KpiCard, CC, SH, IS, ErrBanner } from "../shared";

export default function Axis1({ qs }) {
  const [kpi, setKpi] = useState(null);
  const [subj, setSubj] = useState([]);
  const [dist, setDist] = useState([]);
  const [byY, setByY] = useState([]);
  const [err, setErr] = useState(null);

  const r1 = useRef();
  const r2 = useRef();
  const r3 = useRef();
  const r4 = useRef();

  useEffect(() => {
    setErr(null);
    Promise.all([
      apiFetch(`/axis1/kpis${qs}`),
      apiFetch(`/axis1/avg-by-subject${qs}`),
      apiFetch(`/axis1/score-distribution${qs}`),
      apiFetch(`/axis1/avg-by-school-year`),
      apiFetch(`/axis1/pass-fail-by-subject${qs}`),
    ])
      .then(([k, s, d, y]) => {
        setKpi(k);
        setSubj(s);
        setDist(d);
        setByY(y);
      })
      .catch((e) => {
        console.error("Axis1 fetch failed:", e);
        setErr(e.message);
      });
  }, [qs]);

  const tot = dist.reduce((s, d) => s + (d.count || 0), 0);

  // Charts
  useChart(r1, {
    type: "bar",
    data: {
      labels: subj.map((d) => (d.subject || "").slice(0, 9) + "…"),
      datasets: [{
        label: "Avg",
        data: subj.map((d) => d.avg_score),
        backgroundColor: subj.map((d) =>
          d.avg_score >= 12 ? `${C.teal}CC` : d.avg_score >= 10 ? `${C.amber}CC` : `${C.rose}CC`
        ),
        borderWidth: 0,
        borderRadius: 5,
      }],
    },
    options: {
      ...CD,
      plugins: { ...CD.plugins, legend: { display: false } },
      scales: { x: { ...CD.scales.x }, y: { ...CD.scales.y, min: 0, max: 20 } },
    },
  });

  useChart(r2, {
    type: "bar",
    data: {
      labels: dist.map((d) => d.bin),
      datasets: [{
        label: "Students",
        data: dist.map((d) => d.count),
        backgroundColor: dist.map((d) => {
          const m = (parseFloat(d.bin.split("-")[0]) + parseFloat(d.bin.split("-")[1])) / 2;
          return m < 10 ? `${C.rose}BB` : m < 14 ? `${C.amber}BB` : `${C.teal}BB`;
        }),
        borderWidth: 0,
        borderRadius: 4,
      }],
    },
    options: {
      ...CD,
      plugins: {
        ...CD.plugins,
        legend: { display: false },
        tooltip: {
          ...CD.plugins.tooltip,
          callbacks: { label: (ctx) => ` ${ctx.parsed.y} (${((ctx.parsed.y / tot) * 100).toFixed(1)}%)` },
        },
      },
      scales: { x: { ...CD.scales.x }, y: { ...CD.scales.y } },
    },
  });

  useChart(r3, {
    type: "line",
    data: {
      labels: byY.map((d) => d.year_label),
      datasets: [
        {
          label: "Avg score",
          data: byY.map((d) => d.avg_score),
          borderColor: C.teal,
          backgroundColor: `${C.teal}18`,
          tension: 0.35,
          fill: true,
          pointBackgroundColor: C.teal,
          pointRadius: 4,
          yAxisID: "y",
        },
        {
          label: "Pass %",
          data: byY.map((d) => d.pass_rate),
          borderColor: C.amber,
          backgroundColor: "transparent",
          tension: 0.35,
          borderDash: [5, 3],
          pointBackgroundColor: C.amber,
          pointRadius: 3,
          yAxisID: "y2",
        },
      ],
    },
    options: {
      ...CD,
      scales: {
        x: { ...CD.scales.x },
        y: { ...CD.scales.y, min: 0, max: 20, position: "left" },
        y2: { ...CD.scales.y, min: 0, max: 100, position: "right", grid: { drawOnChartArea: false } },
      },
    },
  });

  useChart(r4, {
    type: "bar",
    data: {
      labels: subj.slice(0, 8).map((d) => (d.subject || "").slice(0, 10)),
      datasets: [
        {
          label: "Pass",
          data: subj.slice(0, 8).map((d) => d.pass_count || Math.round((d.n_evaluations || 500) * (d.pass_rate || 0) / 100)),
          backgroundColor: `${C.teal}CC`,
          borderRadius: 4,
          stack: "s",
        },
        {
          label: "Fail",
          data: subj.slice(0, 8).map((d) => d.fail_count || Math.round((d.n_evaluations || 500) * (1 - (d.pass_rate || 0) / 100))),
          backgroundColor: `${C.rose}CC`,
          borderRadius: 4,
          stack: "s",
        },
      ],
    },
    options: {
      ...CD,
      scales: { x: { ...CD.scales.x, stacked: true }, y: { ...CD.scales.y, stacked: true } },
    },
  });

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 22 }}>
      <SH axis="A1" title="Academic Performance Analysis" desc="Average grades · score distribution · pass/fail rates · year-over-year comparison" color={C.teal} />
      <ErrBanner axis="Axis 1" err={err} />

      {kpi && (
        <>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(4,1fr)", gap: 14 }}>
            <KpiCard label="Total students" value={kpi.total_students} color={C.teal} />
            <KpiCard label="Average score" value={kpi.avg_score} unit="/20" color={C.indigo} />
            <KpiCard label="Pass rate" value={kpi.pass_rate} unit="%" sub="Score ≥ 10/20" color={C.amber} />
            <KpiCard label="Fail rate" value={kpi.fail_rate} unit="%" sub="Score < 10/20" color={C.rose} />
          </div>

          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
            <CC title="Average score by subject" subtitle="Teal ≥ 12 · amber 10–12 · rose < 10" height={240}>
              <canvas ref={r1} />
            </CC>
            <CC title="Score distribution" subtitle="Students per score interval" height={240}>
              <canvas ref={r2} />
            </CC>
          </div>

          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
            <CC title="Performance across school years" subtitle="Avg score (left) vs pass rate % (right)" height={240}>
              <canvas ref={r3} />
            </CC>
            <CC title="Pass vs fail by subject" subtitle="Stacked count per subject" height={240}>
              <canvas ref={r4} />
            </CC>
          </div>

          <IS
            color={C.teal}
            items={[
              { label: "Best subject", value: subj[0]?.subject },
              { label: "Weakest subject", value: subj[subj.length - 1]?.subject },
              { label: "Score range", value: `${kpi.min_score} – ${kpi.max_score} / 20` },
              { label: "Pass/fail split", value: `${kpi.pass_rate}% / ${kpi.fail_rate}%` },
            ]}
          />
        </>
      )}
    </div>
  );
}