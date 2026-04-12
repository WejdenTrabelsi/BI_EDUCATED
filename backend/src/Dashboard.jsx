import { useState, useEffect, useRef } from "react";
import { Chart, registerables } from "chart.js";
Chart.register(...registerables);

const API = "http://localhost:5000/api";
const C = {
  teal:"#2DD4BF",amber:"#F59E0B",rose:"#FB7185",indigo:"#818CF8",
  violet:"#A78BFA",emerald:"#34D399",sky:"#38BDF8",orange:"#FB923C",
  bg:"#0F172A",card:"#1E293B",cardHover:"#243044",
  border:"#334155",text:"#F1F5F9",textMuted:"#94A3B8",
};
const CD={responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:C.textMuted,font:{family:"'DM Sans',sans-serif",size:11},boxWidth:10,padding:12}},tooltip:{backgroundColor:"#1E293B",borderColor:C.border,borderWidth:1,titleColor:C.text,bodyColor:C.textMuted,padding:10,cornerRadius:8}},scales:{x:{ticks:{color:C.textMuted,font:{family:"'DM Sans',sans-serif",size:10}},grid:{color:"rgba(51,65,85,0.4)"}},y:{ticks:{color:C.textMuted,font:{family:"'DM Sans',sans-serif",size:10}},grid:{color:"rgba(51,65,85,0.4)"}}}};

async function apiFetch(path) {
  const r = await fetch(`${API}${path}`);
  if (!r.ok) {
    const msg = await r.text().catch(() => r.statusText);
    throw new Error(`[${r.status}] ${path} — ${msg}`);
  }
  const data = await r.json();
  if (path.startsWith("/school-years") && Array.isArray(data)) {
    return data.map(y => ({
      SchoolYearKey: y.SchoolYearKey ?? y.id ?? y.school_year_key,
      YearLabel: y.YearLabel ?? y.name ?? y.Description ?? y.year_label,
      IsCurrent: y.IsCurrent ?? y.isCurrent ?? 0,
    }));
  }
  return data;
}

function useChart(ref, cfg) {
  useEffect(() => {
    if (!ref.current) return;
    const c = new Chart(ref.current.getContext("2d"), cfg);
    return () => c.destroy();
  }, [JSON.stringify(cfg)]);
}

// ── Shared UI primitives ─────────────────────────────────────────────────────
function ErrBanner({ axis, err }) {
  if (!err) return null;
  return (
    <div style={{background:"#3B0A14",border:`1px solid ${C.rose}40`,borderRadius:10,padding:"12px 16px",display:"flex",gap:10,alignItems:"flex-start"}}>
      <span style={{color:C.rose,fontSize:16,flexShrink:0}}>⚠</span>
      <div>
        <div style={{fontSize:12,fontWeight:600,color:C.rose}}>{axis} — Failed to load data from API</div>
        <div style={{fontSize:11,color:C.textMuted,marginTop:3,fontFamily:"'Space Mono',monospace"}}>{err}</div>
        <div style={{fontSize:11,color:C.textMuted,marginTop:4}}>Make sure Flask is running at <code style={{color:C.amber}}>localhost:5000</code> and your database is connected.</div>
      </div>
    </div>
  );
}

function KpiCard({label,value,unit="",sub,color=C.teal}){
  return(
    <div style={{background:C.card,border:`1px solid ${C.border}`,borderRadius:14,padding:"18px 20px",borderTop:`3px solid ${color}`}}>
      <div style={{fontSize:11,color:C.textMuted,textTransform:"uppercase",letterSpacing:"0.08em"}}>{label}</div>
      <div style={{display:"flex",alignItems:"baseline",gap:4,marginTop:6}}>
        <span style={{fontSize:28,fontWeight:700,color,fontFamily:"'Space Mono',monospace",letterSpacing:"-1px"}}>
          {typeof value==="number"?value.toLocaleString("fr-TN",{maximumFractionDigits:1}):(value??"—")}
        </span>
        {unit&&<span style={{fontSize:13,color:C.textMuted}}>{unit}</span>}
      </div>
      {sub&&<div style={{fontSize:11,color:C.textMuted,marginTop:2}}>{sub}</div>}
    </div>
  );
}

function CC({title,subtitle,children,height=250}){
  return(
    <div style={{background:C.card,border:`1px solid ${C.border}`,borderRadius:14,padding:"18px 20px",display:"flex",flexDirection:"column",gap:12}}>
      <div>
        <div style={{fontSize:13,fontWeight:600,color:C.text}}>{title}</div>
        {subtitle&&<div style={{fontSize:11,color:C.textMuted,marginTop:2}}>{subtitle}</div>}
      </div>
      <div style={{height,position:"relative"}}>{children}</div>
    </div>
  );
}

function SH({axis,title,desc,color=C.teal}){
  return(
    <div style={{display:"flex",alignItems:"center",gap:14,marginBottom:20,paddingBottom:14,borderBottom:`1px solid ${C.border}`}}>
      <div style={{width:42,height:42,borderRadius:10,background:`${color}18`,border:`1px solid ${color}40`,display:"flex",alignItems:"center",justifyContent:"center",fontFamily:"'Space Mono',monospace",fontSize:11,fontWeight:700,color,flexShrink:0}}>{axis}</div>
      <div>
        <div style={{fontSize:18,fontWeight:700,color:C.text}}>{title}</div>
        <div style={{fontSize:12,color:C.textMuted,marginTop:2}}>{desc}</div>
      </div>
    </div>
  );
}

function IS({items,color}){
  return(
    <div style={{background:`${color}0F`,border:`1px solid ${color}30`,borderRadius:12,padding:"14px 18px",display:"flex",gap:28,flexWrap:"wrap"}}>
      {items.map(({label,value})=>(
        <div key={label}>
          <div style={{fontSize:10,color:C.textMuted,textTransform:"uppercase",letterSpacing:"0.07em"}}>{label}</div>
          <div style={{fontSize:13,fontWeight:600,color,marginTop:2,fontFamily:"'Space Mono',monospace"}}>{value??"—"}</div>
        </div>
      ))}
    </div>
  );
}

function RB({level}){
  const m={High:[C.rose,"#3B0A14"],Medium:[C.amber,"#2D1A00"],Low:[C.teal,"#003330"]};
  const[fg,bg]=m[level]||[C.textMuted,C.card];
  return <span style={{background:bg,color:fg,border:`1px solid ${fg}40`,borderRadius:20,padding:"2px 10px",fontSize:11,fontWeight:600}}>{level}</span>;
}

// Safe name helper to prevent .split() crash
const safeName = (name) => {
  if (!name) return "Unknown";
  return String(name).trim().split(" ")[0];
};

// ── Axis 1 — Academic Performance ────────────────────────────────────────────
function Axis1({qs}){
  const[kpi,setKpi]=useState(null);const[subj,setSubj]=useState([]);const[dist,setDist]=useState([]);const[byY,setByY]=useState([]);const[err,setErr]=useState(null);
  useEffect(()=>{
    setErr(null);
    Promise.all([
      apiFetch(`/axis1/kpis${qs}`),
      apiFetch(`/axis1/avg-by-subject${qs}`),
      apiFetch(`/axis1/score-distribution${qs}`),
      apiFetch(`/axis1/avg-by-school-year`),
      apiFetch(`/axis1/pass-fail-by-subject${qs}`),
    ]).then(([k,s,d,y])=>{setKpi(k);setSubj(s);setDist(d);setByY(y);})
      .catch(e=>{console.error("Axis1 fetch failed:",e);setErr(e.message);});
  },[qs]);
  const r1=useRef(),r2=useRef(),r3=useRef(),r4=useRef();
  const tot=dist.reduce((s,d)=>s+d.count,0);
  useChart(r1,{type:"bar",data:{labels:subj.map(d=>(d.subject||"").slice(0,9)+"…"),datasets:[{label:"Avg",data:subj.map(d=>d.avg_score),backgroundColor:subj.map(d=>d.avg_score>=12?`${C.teal}CC`:d.avg_score>=10?`${C.amber}CC`:`${C.rose}CC`),borderWidth:0,borderRadius:5}]},options:{...CD,plugins:{...CD.plugins,legend:{display:false}},scales:{x:{...CD.scales.x},y:{...CD.scales.y,min:0,max:20}}}});
  useChart(r2,{type:"bait commit -m "r",data:{labels:dist.map(d=>d.bin),datasets:[{label:"Students",data:dist.map(d=>d.count),backgroundColor:dist.map(d=>{const m=(parseFloat(d.bin.split("-")[0])+parseFloat(d.bin.split("-")[1]))/2;return m<10?`${C.rose}BB`:m<14?`${C.amber}BB`:`${C.teal}BB`;}),borderWidth:0,borderRadius:4}]},options:{...CD,plugins:{...CD.plugins,legend:{display:false},tooltip:{...CD.plugins.tooltip,callbacks:{label:ctx=>` ${ctx.parsed.y} (${((ctx.parsed.y/tot)*100).toFixed(1)}%)`}}},scales:{x:{...CD.scales.x},y:{...CD.scales.y}}}});
  useChart(r3,{type:"line",data:{labels:byY.map(d=>d.year_label),datasets:[{label:"Avg score",data:byY.map(d=>d.avg_score),borderColor:C.teal,backgroundColor:`${C.teal}18`,tension:0.35,fill:true,pointBackgroundColor:C.teal,pointRadius:4,yAxisID:"y"},{label:"Pass %",data:byY.map(d=>d.pass_rate),borderColor:C.amber,backgroundColor:"transparent",tension:0.35,borderDash:[5,3],pointBackgroundColor:C.amber,pointRadius:3,yAxisID:"y2"}]},options:{...CD,scales:{x:{...CD.scales.x},y:{...CD.scales.y,min:0,max:20,position:"left"},y2:{...CD.scales.y,min:0,max:100,position:"right",grid:{drawOnChartArea:false}}}}});
  useChart(r4,{type:"bar",data:{labels:subj.slice(0,8).map(d=>(d.subject||"").slice(0,10)),datasets:[{label:"Pass",data:subj.slice(0,8).map(d=>d.pass_count||Math.round((d.n_evaluations||500)*d.pass_rate/100)),backgroundColor:`${C.teal}CC`,borderRadius:4,stack:"s"},{label:"Fail",data:subj.slice(0,8).map(d=>d.fail_count||Math.round((d.n_evaluations||500)*(1-d.pass_rate/100))),backgroundColor:`${C.rose}CC`,borderRadius:4,stack:"s"}]},options:{...CD,scales:{x:{...CD.scales.x,stacked:true},y:{...CD.scales.y,stacked:true}}}});
  return(<div style={{display:"flex",flexDirection:"column",gap:22}}><SH axis="A1" title="Academic Performance Analysis" desc="Average grades · score distribution · pass/fail rates · year-over-year comparison" color={C.teal}/><ErrBanner axis="Axis 1" err={err}/>{kpi&&<><div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:14}}><KpiCard label="Total students" value={kpi.total_students} color={C.teal}/><KpiCard label="Average score" value={kpi.avg_score} unit="/20" color={C.indigo}/><KpiCard label="Pass rate" value={kpi.pass_rate} unit="%" sub="Score ≥ 10/20" color={C.amber}/><KpiCard label="Fail rate" value={kpi.fail_rate} unit="%" sub="Score < 10/20" color={C.rose}/></div><div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:16}}><CC title="Average score by subject" subtitle="Teal ≥ 12 · amber 10–12 · rose < 10" height={240}><canvas ref={r1}/></CC><CC title="Score distribution" subtitle="Students per score interval" height={240}><canvas ref={r2}/></CC></div><div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:16}}><CC title="Performance across school years" subtitle="Avg score (left) vs pass rate % (right)" height={240}><canvas ref={r3}/></CC><CC title="Pass vs fail by subject" subtitle="Stacked count per subject" height={240}><canvas ref={r4}/></CC></div><IS color={C.teal} items={[{label:"Best subject",value:subj[0]?.subject},{label:"Weakest subject",value:subj[subj.length-1]?.subject},{label:"Score range",value:`${kpi.min_score} – ${kpi.max_score} / 20`},{label:"Pass/fail split",value:`${kpi.pass_rate}% / ${kpi.fail_rate}%`}]}/></>}</div>);
}

// ── Axis 2 remains unchanged (for now) ───────────────────────────────────────
function Axis2({qs}){
  const[prog,setProg]=useState([]);const[stab,setStab]=useState([]);const[regr,setRegr]=useState([]);const[sem,setSem]=useState([]);const[err,setErr]=useState(null);
  useEffect(()=>{
    setErr(null);
    Promise.all([
      apiFetch(`/axis2/progression-by-student${qs}`),
      apiFetch(`/axis2/stability-index`),
      apiFetch(`/axis2/regression-detection`),
      apiFetch(`/axis2/semester-comparison`),
    ]).then(([p,s,r,sm])=>{setProg(p);setStab(s);setRegr(r);setSem(sm);})
      .catch(e=>{console.error("Axis2 fetch failed:",e);setErr(e.message);});
  },[qs]);
  const r1=useRef(),r2=useRef(),r3=useRef();
  const byY=Object.values(prog.reduce((a,r)=>{const k=r.year_label;if(!a[k])a[k]={year_label:k,total:0,count:0};a[k].total+=r.avg_score;a[k].count++;return a;},{})).sort((a,b)=>a.year_label.localeCompare(b.year_label));
  useChart(r1,{type:"line",data:{labels:byY.map(d=>d.year_label),datasets:[{label:"Avg score",data:byY.map(d=>+(d.total/d.count).toFixed(2)),borderColor:C.violet,backgroundColor:`${C.violet}18`,tension:0.4,fill:true,pointBackgroundColor:C.violet,pointRadius:5}]},options:{...CD,scales:{x:{...CD.scales.x},y:{...CD.scales.y,min:0,max:20}}}});
  useChart(r2,{type:"bar",data:{labels:stab.map(d=>safeName(d.student_name)),datasets:[{label:"Avg score",data:stab.map(d=>d.avg_score),backgroundColor:`${C.sky}BB`,borderRadius:4,yAxisID:"y"},{label:"Std dev",data:stab.map(d=>d.score_stddev),backgroundColor:`${C.orange}BB`,borderRadius:4,yAxisID:"y2"}]},options:{...CD,scales:{x:{...CD.scales.x},y:{...CD.scales.y,position:"left",max:20},y2:{...CD.scales.y,position:"right",grid:{drawOnChartArea:false},title:{display:true,text:"Std dev",color:C.textMuted}}}}});
  const sorted=[...regr].sort((a,b)=>a.delta-b.delta);
  useChart(r3,{type:"bar",data:{labels:sorted.map(d=>safeName(d.student_name)),datasets:[{label:"Score delta",data:sorted.map(d=>d.delta),backgroundColor:sorted.map(d=>d.delta<0?`${C.rose}CC`:`${C.emerald}CC`),borderWidth:0,borderRadius:4}]},options:{indexAxis:"y",...CD,plugins:{...CD.plugins,legend:{display:false}},scales:{x:{...CD.scales.x,title:{display:true,text:"Score delta",color:C.textMuted}},y:{...CD.scales.y}}}});
  return(<div style={{display:"flex",flexDirection:"column",gap:22}}><SH axis="A2" title="Temporal Progression & Trends" desc="Score progression · stability index · regression detection · semester comparison" color={C.violet}/><ErrBanner axis="Axis 2" err={err}/><div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:14}}><KpiCard label="Years tracked" value={byY.length} color={C.violet}/><KpiCard label="Improving" value={regr.filter(r=>r.trend_label==="Improvement").length} color={C.emerald}/><KpiCard label="Regressing" value={regr.filter(r=>r.trend_label==="Regression").length} color={C.rose}/><KpiCard label="Stable profiles" value={stab.filter(s=>s.score_stddev<1.5).length} color={C.sky} sub="Std dev < 1.5"/></div><div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:16}}><CC title="Average score progression over years" subtitle="Aggregated across all students" height={240}><canvas ref={r1}/></CC><CC title="Performance stability index" subtitle="Blue: avg score · Orange: std deviation" height={240}><canvas ref={r2}/></CC></div><div style={{display:"grid",gridTemplateColumns:"2fr 1fr",gap:16}}><CC title="Regression & improvement detection" subtitle="Delta = latest − historical avg · rose=regression · green=improvement" height={280}><canvas ref={r3}/></CC><CC title="Semester breakdown" subtitle="Avg score per period" height={280}><div style={{overflowY:"auto",height:"100%"}}>{sem.map((s,i)=>(<div key={i} style={{display:"flex",justifyContent:"space-between",alignItems:"center",padding:"10px 0",borderBottom:`1px solid ${C.border}`}}><div><div style={{fontSize:12,fontWeight:600,color:C.text}}>{s.report_name}</div><div style={{fontSize:11,color:C.textMuted}}>{s.year_label}</div></div><span style={{fontFamily:"'Space Mono',monospace",fontSize:14,fontWeight:700,color:s.avg_score>=12?C.teal:s.avg_score>=10?C.amber:C.rose}}>{s.avg_score?.toFixed(1)}<span style={{fontSize:10,color:C.textMuted}}>/20</span></span></div>))}</div></CC></div><IS color={C.violet} items={[{label:"Most stable",value:stab[0]?.student_name},{label:"Biggest regression",value:regr.find(r=>r.trend_label==="Regression")?.student_name},{label:"Biggest improvement",value:regr.find(r=>r.trend_label==="Improvement")?.student_name},{label:"Avg std dev",value:stab.length?(stab.reduce((s,r)=>s+r.score_stddev,0)/stab.length).toFixed(2)+" pts":"—"}]}/></div>);
}

// ── Axis 3 — Attendance (FIXED) ──────────────────────────────────────────────
function Axis3({qs}){
  const[kpi,setKpi]=useState(null);
  const[absMon,setAbsMon]=useState([]);
  const[tchHrs,setTchHrs]=useState([]);
  const[topAbs,setTopAbs]=useState([]);
  const[sc,setSc]=useState([]);
  const[err,setErr]=useState(null);

  useEffect(()=>{
    setErr(null);
    Promise.all([
      apiFetch(`/axis3/kpis${qs}`),
      apiFetch(`/axis3/absence-rate-by-month${qs}`),
      apiFetch(`/axis3/teacher-hours-by-month${qs}`),
      apiFetch(`/axis3/top-absent-students${qs}`),
      apiFetch(`/axis3/attendance-vs-score${qs}`),
    ]).then(([k,a,t,ta,s])=>{
      setKpi(k);
      setAbsMon(a || []);
      setTchHrs(t || []);
      setTopAbs(ta || []);
      setSc(s || []);
    })
    .catch(e=>{console.error("Axis3 fetch failed:",e);setErr(e.message);});
  },[qs]);

  const r1=useRef(),r2=useRef(),r3=useRef(),r4=useRef();

  useChart(r1,{type:"line",data:{labels:absMon.map(d=>d.month_name),datasets:[{label:"Absence %",data:absMon.map(d=>d.absence_rate||0),borderColor:C.rose,backgroundColor:`${C.rose}18`,tension:0.4,fill:true,pointBackgroundColor:C.rose,pointRadius:4}]},options:{...CD,scales:{x:{...CD.scales.x},y:{...CD.scales.y,min:0,ticks:{...CD.scales.y.ticks,callback:v=>`${v}%`}}}}});

  useChart(r2,{type:"bar",data:{labels:tchHrs.map(d=>d.month_name),datasets:[{label:"Total sessions",data:tchHrs.map(d=>d.total_sessions||0),backgroundColor:`${C.indigo}BB`,borderRadius:5}]},options:{...CD,scales:{x:{...CD.scales.x},y:{...CD.scales.y,position:"left"}}}});

  useChart(r3,{type:"bar",data:{labels:topAbs.map(d=>safeName(d.student_name)),datasets:[{label:"Absence %",data:topAbs.map(d=>d.absence_rate||0),backgroundColor:topAbs.map(d=>{const rate=d.absence_rate||0;return rate>=35?`${C.rose}CC`:rate>=20?`${C.amber}CC`:`${C.teal}CC`;}),borderWidth:0,borderRadius:5}]},options:{indexAxis:"y",...CD,plugins:{...CD.plugins,legend:{display:false}},scales:{x:{...CD.scales.x,min:0,max:100,ticks:{...CD.scales.x.ticks,callback:v=>`${v}%`}},y:{...CD.scales.y}}}});

  useChart(r4,{type:"scatter",data:{datasets:[{data:sc.map(d=>({x:d.absence_rate||0,y:d.avg_score||0,name:d.student_name|| "Unknown"})),backgroundColor:sc.map(d=>{const abs=d.absence_rate||0;const score=d.avg_score||0;return (score<10&&abs>20)?`${C.rose}CC`:score>=14?`${C.teal}CC`:`${C.amber}88`;}),pointRadius:5}]},options:{...CD,plugins:{...CD.plugins,legend:{display:false},tooltip:{...CD.plugins.tooltip,callbacks:{label:ctx=>[ctx.raw.name,`Absence: ${ctx.raw.x?.toFixed(1)}%`,`Score: ${ctx.raw.y?.toFixed(2)}/20`]}}},scales:{x:{...CD.scales.x,title:{display:true,text:"Absence rate (%)",color:C.textMuted}},y:{...CD.scales.y,min:0,max:20,title:{display:true,text:"Avg score /20",color:C.textMuted}}}}});

  return(
    <div style={{display:"flex",flexDirection:"column",gap:22}}>
      <SH axis="A3" title="Attendance & Engagement" desc="Absence rates · teacher hours · chronic absenteeism · attendance–performance correlation" color={C.rose}/>
      <ErrBanner axis="Axis 3" err={err}/>

      {kpi && <>
        <div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:14}}>
          <KpiCard label="Presence rate" value={kpi.presence_rate} unit="%" sub={`${kpi.students_tracked||0} students`} color={C.teal}/>
          <KpiCard label="Absence rate" value={kpi.absence_rate ?? (100 - (kpi.presence_rate||0)).toFixed(1)} unit="%" color={C.rose}/>
          <KpiCard label="Student late rate" value={kpi.late_rate} unit="%" color={C.amber}/>
          <KpiCard label="Teacher late rate" value={kpi.teacher_late_rate ?? 0} unit="%" color={C.amber}/>
        </div>

        <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:16}}>
          <CC title="Monthly absence rate" subtitle="Percentage per month" height={230}><canvas ref={r1}/></CC>
          <CC title="Teacher sessions by month" subtitle="Total sessions" height={230}><canvas ref={r2}/></CC>
        </div>

        <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:16}}>
          <CC title="Top most absent students" subtitle="Rose ≥ 35% · Amber 20–35% · Teal < 20%" height={280}><canvas ref={r3}/></CC>
          <CC title="Absence vs score scatter" subtitle="Rose = at-risk zone (high absence + low score)" height={280}><canvas ref={r4}/></CC>
        </div>

        <IS color={C.rose} items={[
          {label:"Most absent",value:topAbs[0]?.student_name || "—"},
          {label:"Peak month",value:absMon.length ? absMon.reduce((a,b)=>a.absence_rate>b.absence_rate?a:b).month_name : "—"},
          {label:"Teacher late rate",value:`${kpi.teacher_late_rate ?? 0}%`},
          {label:"At-risk definition",value:"Absence > 20% + Score < 10"}
        ]}/>
      </>}
    </div>
  );
}

// ── Axis 4 — Weather Impact ──────────────────────────────────────────────────
function Axis4({qs}){
  const[wData,setWData]=useState([]);const[rvd,setRvd]=useState(null);const[seas,setSeas]=useState([]);const[err,setErr]=useState(null);
  useEffect(()=>{
    setErr(null);
    Promise.all([
      apiFetch(`/axis4/weather-vs-absence${qs}`),
      apiFetch(`/axis4/rainy-vs-dry-summary${qs}`),
      apiFetch(`/axis4/seasonal-patterns${qs}`),
    ]).then(([w,r,s])=>{setWData(w);setRvd(r);setSeas(s);})
      .catch(e=>{console.error("Axis4 fetch failed:",e);setErr(e.message);});
  },[qs]);
  const r1=useRef(),r2=useRef(),r3=useRef();
  useChart(r1,{type:"scatter",data:{datasets:[{label:"Dry",data:wData.filter(d=>!d.is_rainy).map(d=>({x:d.precipitation_mm,y:d.absence_rate,date:d.full_date})),backgroundColor:`${C.sky}CC`,pointRadius:6},{label:"Rainy",data:wData.filter(d=>d.is_rainy).map(d=>({x:d.precipitation_mm,y:d.absence_rate,date:d.full_date})),backgroundColor:`${C.rose}CC`,pointRadius:6}]},options:{...CD,plugins:{...CD.plugins,tooltip:{...CD.plugins.tooltip,callbacks:{label:ctx=>[ctx.dataset.label,`Precip: ${ctx.raw.x}mm`,`Absence: ${ctx.raw.y}%`]}}},scales:{x:{...CD.scales.x,title:{display:true,text:"Precipitation (mm)",color:C.textMuted}},y:{...CD.scales.y,title:{display:true,text:"Absence rate (%)",color:C.textMuted}}}}});
  useChart(r2,{type:"bar",data:{labels:seas.map(d=>d.month_name),datasets:[{label:"Absence %",data:seas.map(d=>d.absence_rate),backgroundColor:`${C.rose}CC`,borderRadius:4,yAxisID:"y"},{label:"Precip mm",data:seas.map(d=>d.avg_precipitation||0),backgroundColor:`${C.sky}88`,borderRadius:4,yAxisID:"y2"}]},options:{...CD,scales:{x:{...CD.scales.x},y:{...CD.scales.y,position:"left"},y2:{...CD.scales.y,position:"right",grid:{drawOnChartArea:false}}}}});
  useChart(r3,{type:"bar",data:{labels:["Rainy days","Dry days"],datasets:[{label:"Avg absence %",data:rvd?[rvd.rainy_avg,rvd.dry_avg]:[0,0],backgroundColor:[`${C.rose}CC`,`${C.sky}CC`],borderWidth:0,borderRadius:8}]},options:{...CD,plugins:{...CD.plugins,legend:{display:false}},scales:{x:{...CD.scales.x},y:{...CD.scales.y,min:0,ticks:{...CD.scales.y.ticks,callback:v=>`${v}%`}}}}});
  const sens=rvd&&rvd.rainy_avg&&rvd.dry_avg?(rvd.rainy_avg-rvd.dry_avg).toFixed(1):"—";
  return(<div style={{display:"flex",flexDirection:"column",gap:22}}><SH axis="A4" title="Contextual Analysis — Weather Impact" desc="Absence on rainy vs dry days · seasonal patterns · weather sensitivity · Open-Meteo data" color={C.sky}/><ErrBanner axis="Axis 4" err={err}/><div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:14}}><KpiCard label="Rainy-day absence avg" value={rvd?.rainy_avg} unit="%" color={C.rose}/><KpiCard label="Dry-day absence avg" value={rvd?.dry_avg} unit="%" color={C.sky}/><KpiCard label="Weather sensitivity" value={sens} unit="pts" sub="Rainy − dry delta" color={C.amber}/><KpiCard label="Rainy days tracked" value={rvd?.n_rainy} sub={`${rvd?.n_dry??0} dry days`} color={C.indigo}/></div><div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:16}}><CC title="Precipitation vs absence — scatter" subtitle="Each dot = one school day · rose=rainy · blue=dry" height={260}><canvas ref={r1}/></CC><CC title="Rainy vs dry — direct comparison" subtitle="Mean absence rate by weather condition" height={260}><canvas ref={r3}/></CC></div><CC title="Seasonal patterns — absence & precipitation by month" subtitle="Rose bars = absence % · Blue bars = avg precipitation mm" height={260}><canvas ref={r2}/></CC><IS color={C.sky} items={[{label:"Weather sensitivity",value:`+${sens} pts on rainy days`},{label:"Rainiest month",value:seas.length?seas.reduce((a,b)=>(a.avg_precipitation||0)>(b.avg_precipitation||0)?a:b).month_name:"—"},{label:"Worst absence month",value:seas.length?seas.reduce((a,b)=>a.absence_rate>b.absence_rate?a:b).month_name:"—"},{label:"Data source",value:"Open-Meteo archive · Sousse"}]}/></div>);
}

// ── Axis 5 — Pedagogical Sessions ────────────────────────────────────────────
function Axis5({qs}){
  const[subjects,setSubjects]=useState([]);const[selSubj,setSelSubj]=useState("");const[outcomes,setOutcomes]=useState([]);const[disp,setDisp]=useState([]);const[pp,setPp]=useState([]);const[err,setErr]=useState(null);
  useEffect(()=>{
    apiFetch("/axis5/subject-list")
      .then(s=>{setSubjects(s);if(s[0])setSelSubj(s[0].subject||s[0]);})
      .catch(e=>{console.error("Axis5 subject-list fetch failed:",e);setErr(e.message);});
  },[]);
  useEffect(()=>{
    if(!selSubj&&subjects.length===0) return;
    setErr(null);
    const sq=selSubj?`${qs?qs+"&":"?"}subject=${encodeURIComponent(selSubj)}`:qs;
    Promise.all([
      apiFetch(`/axis5/outcomes-by-report${sq}`),
      apiFetch(`/axis5/dispersion-by-report${qs}`),
      apiFetch(`/axis5/presence-performance-by-report${qs}`),
    ]).then(([o,d,p])=>{setOutcomes(o);setDisp(d);setPp(p);})
      .catch(e=>{console.error("Axis5 fetch failed:",e);setErr(e.message);});
  },[qs,selSubj]);
  const r1=useRef(),r2=useRef(),r3=useRef();
  useChart(r1,{type:"bar",data:{labels:outcomes.map(d=>`${d.report_name}`),datasets:[{label:"Avg score",data:outcomes.map(d=>d.avg_score),backgroundColor:`${C.emerald}CC`,borderRadius:5,yAxisID:"y"},{label:"Pass %",data:outcomes.map(d=>d.pass_rate),backgroundColor:`${C.amber}88`,borderRadius:5,yAxisID:"y2"}]},options:{...CD,scales:{x:{...CD.scales.x},y:{...CD.scales.y,max:20,position:"left"},y2:{...CD.scales.y,min:0,max:100,position:"right",grid:{drawOnChartArea:false}}}}});
  useChart(r2,{type:"bar",data:{labels:disp.map(d=>d.report_name),datasets:[{label:"Score range ± σ",data:disp.map(d=>[Math.max(0,d.score_avg-(d.score_stddev||0)),Math.min(20,d.score_avg+(d.score_stddev||0))]),backgroundColor:`${C.sky}66`,borderColor:C.sky,borderWidth:1,borderRadius:4}]},options:{...CD,scales:{x:{...CD.scales.x},y:{...CD.scales.y,min:0,max:20}}}});
  useChart(r3,{type:"scatter",data:{datasets:[{data:pp.map(d=>({x:d.presence_rate,y:d.avg_score,name:d.report_name})),backgroundColor:`${C.emerald}CC`,pointRadius:8}]},options:{...CD,plugins:{...CD.plugins,legend:{display:false},tooltip:{...CD.plugins.tooltip,callbacks:{label:ctx=>[ctx.raw.name,`Presence: ${ctx.raw.x?.toFixed(1)}%`,`Score: ${ctx.raw.y?.toFixed(2)}/20`]}}},scales:{x:{...CD.scales.x,min:70,max:100,title:{display:true,text:"Presence rate (%)",color:C.textMuted}},y:{...CD.scales.y,min:0,max:20,title:{display:true,text:"Avg score /20",color:C.textMuted}}}}});
  return(<div style={{display:"flex",flexDirection:"column",gap:22}}><SH axis="A5" title="Pedagogical Session Analysis" desc="Outcomes per evaluation report · score dispersion · presence–performance correlation" color={C.emerald}/><ErrBanner axis="Axis 5" err={err}/><div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:14}}><KpiCard label="Evaluation reports" value={disp.length} color={C.emerald}/><KpiCard label="Best report avg" value={disp.length?Math.max(...disp.map(d=>d.score_avg)).toFixed(1):"—"} unit="/20" color={C.teal}/><KpiCard label="Highest dispersion" value={disp.length?Math.max(...disp.map(d=>d.score_stddev||0)).toFixed(2):"—"} unit=" σ" color={C.amber}/><KpiCard label="Subjects" value={subjects.length} color={C.indigo}/></div><div style={{display:"flex",alignItems:"center",gap:10}}><span style={{fontSize:12,color:C.textMuted}}>Subject:</span><select value={selSubj} onChange={e=>setSelSubj(e.target.value)} style={{background:C.card,border:`1px solid ${C.border}`,borderRadius:8,color:C.text,padding:"6px 12px",fontSize:12,outline:"none",cursor:"pointer"}}>{subjects.map((s,i)=><option key={i} value={s.subject||s}>{s.subject||s}</option>)}</select></div><div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:16}}><CC title={`Outcomes by report — ${selSubj||"all"}`} subtitle="Avg score (green) vs pass rate % (amber)" height={240}><canvas ref={r1}/></CC><CC title="Score dispersion per report" subtitle="Bar = avg ± 1σ range" height={240}><canvas ref={r2}/></CC></div><CC title="Presence rate vs score per evaluation period" subtitle="Each dot = one report — does higher presence predict higher score?" height={260}><canvas ref={r3}/></CC><IS color={C.emerald} items={[{label:"Best period",value:disp.length?disp.reduce((a,b)=>a.score_avg>b.score_avg?a:b).report_name:"—"},{label:"Most volatile",value:disp.length?disp.reduce((a,b)=>(a.score_stddev||0)>(b.score_stddev||0)?a:b).report_name:"—"},{label:"Total evaluations",value:disp.reduce((s,d)=>s+(d.n||0),0).toLocaleString()},{label:"Subject in focus",value:selSubj||"All"}]}/></div>);
}

// ── Axis 6 — Academic Risk (Fully Fixed) ─────────────────────────────────────
function Axis6({qs}){
  const[scores,setScores]=useState([]); 
  const[dist,setDist]=useState([]); 
  const[warn,setWarn]=useState([]); 
  const[err,setErr]=useState(null);

  useEffect(()=>{
    setErr(null);
    Promise.all([
      apiFetch(`/axis6/risk-scores${qs}`),
      apiFetch(`/axis6/risk-distribution${qs}`),
      apiFetch(`/axis6/early-warning-indicators${qs}`),
    ]).then(([s,d,w])=>{
      setScores(Array.isArray(s) ? s : []);
      setDist(Array.isArray(d) ? d : []);
      setWarn(Array.isArray(w) ? w : []);
    })
    .catch(e=>{
      console.error("Axis6 fetch failed:", e);
      setErr(e.message || "Unknown error loading risk data");
    });
  },[qs]);

  const r1=useRef(), r2=useRef();
  const top15 = scores.slice(0,15);

  // Safe number formatter
  const num = (value, decimals = 1) => {
    const n = parseFloat(value);
    return isNaN(n) ? 0 : n.toFixed(decimals);
  };

  useChart(r1,{
    type:"bar",
    data:{
      labels:top15.map(d => safeName(d.student_name)),
      datasets:[{
        label:"Risk score",
        data:top15.map(d => parseFloat(d.risk_score) || 0),
        backgroundColor:top15.map(d => 
          d.risk_level==="High" ? `${C.rose}CC` : 
          d.risk_level==="Medium" ? `${C.amber}CC` : `${C.teal}CC`
        ),
        borderWidth:0,
        borderRadius:5
      }]
    },
    options:{
      indexAxis:"y",
      ...CD,
      plugins:{...CD.plugins, legend:{display:false}},
      scales:{ x:{...CD.scales.x, min:0, max:100}, y:{...CD.scales.y} }
    }
  });

  useChart(r2,{
    type:"doughnut",
    data:{
      labels:dist.map(d => d.risk_level || "Unknown"),
      datasets:[{
        data:dist.map(d => parseFloat(d.student_count) || 0),
        backgroundColor:[`${C.rose}CC`, `${C.amber}CC`, `${C.teal}CC`],
        borderWidth:2,
        borderColor:C.bg,
        hoverOffset:8
      }]
    },
    options:{ ...CD, cutout:"65%" }
  });

  const highN = dist.find(d => d.risk_level === "High")?.student_count || 0;
  const medN = dist.find(d => d.risk_level === "Medium")?.student_count || 0;

  return(
    <div style={{display:"flex",flexDirection:"column",gap:22}}>
      <SH axis="A6" title="Academic Risk Detection" 
          desc="Composite risk score · early warning indicators · at-risk profiles" 
          color={C.rose}/>

      <ErrBanner axis="Axis 6" err={err}/>

      <div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:14}}>
        <KpiCard label="High-risk students" value={highN} color={C.rose} sub="Risk ≥ 60"/>
        <KpiCard label="Medium-risk" value={medN} color={C.amber} sub="Risk 35–60"/>
        <KpiCard label="Students analysed" value={dist.reduce((s,d)=>s+(parseFloat(d.student_count)||0),0)} color={C.teal}/>
        <KpiCard label="3-flag warnings" value={warn.filter(w=>(parseFloat(w.warning_count)||0)>=3).length} color={C.violet} sub="Need immediate action"/>
      </div>

      <div style={{display:"grid",gridTemplateColumns:"2fr 1fr",gap:16}}>
        <CC title="Risk score — top 15 at-risk students" subtitle="Rose=high · amber=medium · teal=low" height={340}>
          <canvas ref={r1}/>
        </CC>
        <CC title="Risk level distribution" subtitle="All students with sufficient data" height={340}>
          <canvas ref={r2}/>
          <div style={{textAlign:"center",marginTop:-160,pointerEvents:"none"}}>
            <div style={{fontSize:22,fontWeight:700,color:C.rose,fontFamily:"'Space Mono',monospace"}}>{highN}</div>
            <div style={{fontSize:11,color:C.textMuted}}>high risk</div>
          </div>
        </CC>
      </div>

      {/* Early Warning Table - Fully Safe */}
      <div style={{background:C.card,border:`1px solid ${C.border}`,borderRadius:14,overflow:"hidden"}}>
        <div style={{padding:"16px 20px",borderBottom:`1px solid ${C.border}`,display:"flex",justifyContent:"space-between",alignItems:"center"}}>
          <div>
            <div style={{fontSize:13,fontWeight:600,color:C.text}}>Early warning indicators</div>
            <div style={{fontSize:11,color:C.textMuted,marginTop:2}}>Students with ≥ 2 active risk flags</div>
          </div>
          <span style={{background:`${C.rose}18`,color:C.rose,border:`1px solid ${C.rose}40`,borderRadius:20,padding:"3px 12px",fontSize:11,fontWeight:600}}>
            {warn.length} students
          </span>
        </div>

        <div style={{overflowX:"auto"}}>
          <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
            <thead>
              <tr style={{background:`${C.border}40`}}>
                {["Student","Avg score","Absence %","Fail rate","Low score","High absence","High fail","Flags","Risk"].map(h =>
                  <th key={h} style={{padding:"10px 16px",textAlign:"left",color:C.textMuted,fontWeight:500,whiteSpace:"nowrap"}}>{h}</th>
                )}
              </tr>
            </thead>
            <tbody>
              {warn.map((w,i) => (
                <tr key={i} style={{borderTop:`1px solid ${C.border}`,background:i%2===0?"transparent":`${C.card}80`}}>
                  <td style={{padding:"10px 16px",color:C.text,fontWeight:500}}>{w.student_name || "—"}</td>
                  <td style={{padding:"10px 16px",color:(parseFloat(w.avg_score)||0)<10?C.rose:C.amber,fontFamily:"'Space Mono',monospace"}}>
                    {num(w.avg_score)}
                  </td>
                  <td style={{padding:"10px 16px",color:(parseFloat(w.absence_rate)||0)>20?C.rose:C.textMuted,fontFamily:"'Space Mono',monospace"}}>
                    {num(w.absence_rate)}%
                  </td>
                  <td style={{padding:"10px 16px",color:(parseFloat(w.fail_rate_pct)||0)>50?C.rose:C.textMuted,fontFamily:"'Space Mono',monospace"}}>
                    {num(w.fail_rate_pct)}%
                  </td>
                  <td style={{padding:"10px 16px",textAlign:"center"}}>
                    {(w.flag_low_score === 1 || w.flag_low_score === true) ? <span style={{color:C.rose}}>●</span> : <span style={{color:C.border}}>○</span>}
                  </td>
                  <td style={{padding:"10px 16px",textAlign:"center"}}>
                    {(w.flag_high_absence === 1 || w.flag_high_absence === true) ? <span style={{color:C.rose}}>●</span> : <span style={{color:C.border}}>○</span>}
                  </td>
                  <td style={{padding:"10px 16px",textAlign:"center"}}>
                    {(w.flag_high_fail_rate === 1 || w.flag_high_fail_rate === true) ? <span style={{color:C.rose}}>●</span> : <span style={{color:C.border}}>○</span>}
                  </td>
                  <td style={{padding:"10px 16px",textAlign:"center"}}>
                    <span style={{fontFamily:"'Space Mono',monospace",fontWeight:700,color:(parseFloat(w.warning_count)||0)>=3?C.rose:C.amber}}>
                      {parseFloat(w.warning_count)||0}/3
                    </span>
                  </td>
                  <td style={{padding:"10px 16px"}}>
                    <RB level={(parseFloat(w.warning_count)||0)>=3 ? "High" : "Medium"}/>
                  </td>
                </tr>
              ))}
              {warn.length === 0 && (
                <tr>
                  <td colSpan={9} style={{padding:30,textAlign:"center",color:C.textMuted,fontStyle:"italic"}}>
                    No students with multiple risk flags detected in the current period.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      <IS color={C.rose} items={[
        {label:"Highest risk",value:scores[0]?.student_name || "—"},
        {label:"Formula",value:"Acad 40% + Absence 35% + Fail 25%"},
        {label:"High threshold",value:"Risk score ≥ 60"},
        {label:"3-flag students",value:`${warn.filter(w=>(parseFloat(w.warning_count)||0)>=3).length} need immediate action`}
      ]}/>
    </div>
  );
}

// Helper function (add this near safeName if not already there)
const num = (value, decimals = 1) => {
  const n = parseFloat(value);
  return isNaN(n) ? "0" : n.toFixed(decimals);
};

// ── Axis 7 — Geographic Distribution ────────────────────────────────────────
function Axis7({qs}){
  const[zones,setZones]=useState([]);const[err,setErr]=useState(null);
  useEffect(()=>{
    setErr(null);
    apiFetch(`/axis7/students-by-zone${qs}`)
      .then(setZones)
      .catch(e=>{console.error("Axis7 fetch failed:",e);setErr(e.message);});
  },[qs]);
  const r1=useRef();
  useChart(r1,{type:"bar",data:{labels:zones.map(z=>z.zone_name),datasets:[{label:"Student count",data:zones.map(z=>z.student_count),backgroundColor:`${C.indigo}CC`,borderRadius:5,yAxisID:"y"},{label:"Absence %",data:zones.map(z=>z.absence_rate),backgroundColor:`${C.rose}CC`,borderRadius:5,yAxisID:"y2"}]},options:{...CD,scales:{x:{...CD.scales.x},y:{...CD.scales.y,position:"left"},y2:{...CD.scales.y,position:"right",grid:{drawOnChartArea:false},ticks:{...CD.scales.y.ticks,callback:v=>`${v}%`}}}}});
  return(<div style={{display:"flex",flexDirection:"column",gap:22}}><SH axis="A7" title="Geographic Distribution" desc="Student distribution, absence & late rates by zone" color={C.indigo}/><ErrBanner axis="Axis 7" err={err}/><div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:14}}><KpiCard label="Total zones" value={zones.length} color={C.indigo}/><KpiCard label="Total students" value={zones.reduce((s,z)=>s+z.student_count,0)} color={C.teal}/><KpiCard label="Avg absence rate" value={zones.length?(zones.reduce((s,z)=>s+z.absence_rate,0)/zones.length).toFixed(1):0} unit="%" color={C.rose}/><KpiCard label="Top zone" value={zones.length?zones.reduce((a,b)=>a.student_count>b.student_count?a:b).zone_name:"—"} color={C.amber}/></div><CC title="Students per zone" subtitle="Blue bars: student count · Red bars: absence rate" height={300}><canvas ref={r1}/></CC><div style={{overflowX:"auto"}}><table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}><thead><tr style={{background:`${C.border}40`}}>{["Zone","Students","Absence %","Late %","Avg Score"].map(h=><th key={h} style={{padding:"10px 16px",textAlign:"left",color:C.textMuted}}>{h}</th>)}</tr></thead><tbody>{zones.map((z,i)=>(<tr key={i} style={{borderTop:`1px solid ${C.border}`}}><td style={{padding:"10px 16px",color:C.text}}>{z.zone_name}</td><td style={{padding:"10px 16px",fontFamily:"'Space Mono',monospace"}}>{z.student_count}</td><td style={{padding:"10px 16px",color:z.absence_rate>10?C.rose:C.teal}}>{z.absence_rate?.toFixed(1)}%</td><td style={{padding:"10px 16px",color:z.late_rate>5?C.amber:C.textMuted}}>{z.late_rate?.toFixed(1)}%</td><td style={{padding:"10px 16px",color:z.avg_score>=12?C.teal:C.amber}}>{z.avg_score?.toFixed(1)}/20</td></tr>))}</tbody></table></div></div>);
}

// ── Axis 8 — Holiday & Exam Period Impact ────────────────────────────────────
function Axis8({qs}){
  const[periods,setPeriods]=useState([]);const[err,setErr]=useState(null);
  useEffect(()=>{
    setErr(null);
    apiFetch(`/axis8/absence-by-period-type${qs}`)
      .then(setPeriods)
      .catch(e=>{console.error("Axis8 fetch failed:",e);setErr(e.message);});
  },[qs]);
  const r1=useRef();
  useChart(r1,{type:"bar",data:{labels:periods.map(p=>p.period_label),datasets:[{label:"Avg absence rate",data:periods.map(p=>p.avg_absence_rate),backgroundColor:periods.map(p=>p.period_type==="exam"?`${C.teal}CC`:p.period_type==="pre_exam"?`${C.amber}CC`:`${C.rose}CC`),borderRadius:5}]},options:{...CD,scales:{x:{...CD.scales.x},y:{...CD.scales.y,min:0,ticks:{...CD.scales.y.ticks,callback:v=>`${v}%`}}}}});
  const maxPeriod=periods.length?periods.reduce((a,b)=>a.avg_absence_rate>b.avg_absence_rate?a:b):null;
  const minPeriod=periods.length?periods.reduce((a,b)=>a.avg_absence_rate<b.avg_absence_rate?a:b):null;
  return(<div style={{display:"flex",flexDirection:"column",gap:22}}><SH axis="A8" title="Holiday & Exam Period Analysis" desc="Absence rates before/after holidays and during exam periods" color={C.orange}/><ErrBanner axis="Axis 8" err={err}/><div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:14}}><KpiCard label="Period types" value={periods.length} color={C.orange}/><KpiCard label="Highest absence" value={maxPeriod?.avg_absence_rate??0} unit="%" sub={maxPeriod?.period_label} color={C.rose}/><KpiCard label="Lowest absence" value={minPeriod?.avg_absence_rate??0} unit="%" sub={minPeriod?.period_label} color={C.teal}/><KpiCard label="Days analyzed" value={periods.reduce((s,p)=>s+(p.n_days||0),0)} color={C.indigo}/></div><CC title="Absence rate by period type" subtitle="Teal = exam periods · amber = pre-exam · rose = holiday-adjacent" height={320}><canvas ref={r1}/></CC><div style={{display:"grid",gridTemplateColumns:"repeat(5,1fr)",gap:12}}>{periods.map((p,i)=>(<div key={i} style={{background:C.card,border:`1px solid ${C.border}`,borderRadius:12,padding:"14px 16px",borderTop:`3px solid ${p.period_type==="exam"?C.teal:p.period_type==="pre_exam"?C.amber:C.rose}`}}><div style={{fontSize:10,color:C.textMuted,textTransform:"uppercase",letterSpacing:"0.06em",marginBottom:6}}>{p.period_label}</div><div style={{fontSize:22,fontWeight:700,fontFamily:"'Space Mono',monospace",color:p.period_type==="exam"?C.teal:p.period_type==="pre_exam"?C.amber:C.rose}}>{p.avg_absence_rate}%</div><div style={{fontSize:11,color:C.textMuted,marginTop:4}}>{p.n_days} days</div></div>))}</div></div>);
}

// ── Axis 9 — Prep School Origin ──────────────────────────────────────────────
function Axis9({qs}){
  const[schools,setSchools]=useState([]);const[err,setErr]=useState(null);
  useEffect(()=>{
    setErr(null);
    apiFetch(`/axis9/performance-by-school${qs}`)
      .then(setSchools)
      .catch(e=>{console.error("Axis9 fetch failed:",e);setErr(e.message);});
  },[qs]);
  const r1=useRef();
  const top10=schools.slice(0,10);
  useChart(r1,{type:"bar",data:{labels:top10.map(s=>s.school_name.length>15?s.school_name.slice(0,13)+"…":s.school_name),datasets:[{label:"Avg score",data:top10.map(s=>s.avg_score),backgroundColor:`${C.emerald}CC`,borderRadius:5,yAxisID:"y"},{label:"Pass %",data:top10.map(s=>s.pass_rate),backgroundColor:`${C.amber}88`,borderRadius:5,yAxisID:"y2"}]},options:{...CD,scales:{x:{...CD.scales.x},y:{...CD.scales.y,min:0,max:20,position:"left"},y2:{...CD.scales.y,min:0,max:100,position:"right",grid:{drawOnChartArea:false},ticks:{...CD.scales.y.ticks,callback:v=>`${v}%`}}}}});
  const best=schools.length?schools.reduce((a,b)=>(a.avg_score||0)>(b.avg_score||0)?a:b):null;
  return(<div style={{display:"flex",flexDirection:"column",gap:22}}><SH axis="A9" title="Previous School Performance" desc="How students from different prep schools perform in our institution" color={C.emerald}/><ErrBanner axis="Axis 9" err={err}/><div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:14}}><KpiCard label="Schools tracked" value={schools.length} color={C.emerald}/><KpiCard label="Best school avg" value={best?.avg_score??0} unit="/20" sub={best?.school_name} color={C.teal}/><KpiCard label="Top pass rate" value={schools.length?Math.max(...schools.map(s=>s.pass_rate||0)).toFixed(1):0} unit="%" color={C.amber}/><KpiCard label="Total students" value={schools.reduce((s,sc)=>s+(sc.student_count||0),0)} color={C.indigo}/></div><CC title="Top schools by average score" subtitle="Green: avg score /20 · Amber: pass rate %" height={300}><canvas ref={r1}/></CC><div style={{overflowX:"auto"}}><table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}><thead><tr style={{background:`${C.border}40`}}>{["Previous School","Students","Avg Score","Pass Rate","Absence Rate"].map(h=><th key={h} style={{padding:"10px 16px",textAlign:"left",color:C.textMuted}}>{h}</th>)}</tr></thead><tbody>{schools.map((s,i)=>(<tr key={i} style={{borderTop:`1px solid ${C.border}`}}><td style={{padding:"10px 16px",color:C.text}}>{s.school_name}</td><td style={{padding:"10px 16px",fontFamily:"'Space Mono',monospace"}}>{s.student_count}</td><td style={{padding:"10px 16px",color:(s.avg_score||0)>=12?C.teal:C.amber,fontFamily:"'Space Mono',monospace"}}>{s.avg_score?.toFixed(1)??'—'}/20</td><td style={{padding:"10px 16px",color:(s.pass_rate||0)>=70?C.teal:C.amber,fontFamily:"'Space Mono',monospace"}}>{s.pass_rate?.toFixed(1)??'—'}%</td><td style={{padding:"10px 16px",color:(s.absence_rate||0)>10?C.rose:C.textMuted,fontFamily:"'Space Mono',monospace"}}>{s.absence_rate?.toFixed(1)??'—'}%</td></tr>))}</tbody></table></div></div>);
}
const AXES=[
  {id:"axis1",label:"A1 — Academic",color:C.teal},
  {id:"axis2",label:"A2 — Trends",color:C.violet},
  {id:"axis3",label:"A3 — Attendance",color:C.rose},
  {id:"axis4",label:"A4 — Weather",color:C.sky},
  {id:"axis5",label:"A5 — Sessions",color:C.emerald},
  {id:"axis6",label:"A6 — Risk",color:C.amber},
  {id:"axis7",label:"A7 — Geography",color:C.indigo},
  {id:"axis8",label:"A8 — Holiday Impact",color:C.orange},
  {id:"axis9",label:"A9 — Prep Schools",color:C.emerald},
];

export default function Dashboard(){
  const[axis,setAxis]=useState("axis1");
  const[yearKey,setYearKey]=useState("");
  const[years,setYears]=useState([]);
  const[yearErr,setYearErr]=useState(null);

  useEffect(()=>{
    apiFetch("/school-years")
      .then(y=>{
        setYears(y);
        const c=y.find(x=>x.IsCurrent);
        if(c) setYearKey(String(c.SchoolYearKey));
      })
      .catch(e=>{console.error("school-years fetch failed:",e);setYearErr(e.message);});
  },[]);

  const qs=yearKey?`?school_year_key=${yearKey}`:"";
  const ac=AXES.find(a=>a.id===axis)?.color||C.teal;

  return(
    <div style={{minHeight:"100vh",background:C.bg,color:C.text,fontFamily:"'DM Sans',sans-serif",paddingBottom:48}}>
      <style>{`@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=Space+Mono:wght@400;700&display=swap');*{box-sizing:border-box}::-webkit-scrollbar{width:5px;background:${C.bg}}::-webkit-scrollbar-thumb{background:${C.border};border-radius:3px}select option{background:${C.card}}table tr:hover{background:${C.cardHover}!important}`}</style>

      {/* Header */}
      <div style={{background:`${C.card}F2`,backdropFilter:"blur(16px)",borderBottom:`1px solid ${C.border}`,padding:"0 36px",position:"sticky",top:0,zIndex:100}}>
        <div style={{maxWidth:1300,margin:"0 auto",display:"flex",alignItems:"center",justifyContent:"space-between",height:62,gap:12,flexWrap:"wrap"}}>
          <div style={{display:"flex",alignItems:"center",gap:10,flexShrink:0}}>
            <div style={{width:34,height:34,borderRadius:9,background:`linear-gradient(135deg,${C.teal},${C.indigo})`,display:"flex",alignItems:"center",justifyContent:"center",fontSize:13,fontWeight:700,color:"#0F172A",fontFamily:"'Space Mono',monospace"}}>E</div>
            <div>
              <div style={{fontSize:15,fontWeight:700,letterSpacing:"-0.3px"}}>Educated BI</div>
              <div style={{fontSize:10,color:C.textMuted,marginTop:-1}}>Intelligent Analytics Platform</div>
            </div>
          </div>

          <div style={{display:"flex",gap:6,flexWrap:"wrap",justifyContent:"center"}}>
            {AXES.map(({id,label,color})=>(
              <button key={id} onClick={()=>setAxis(id)} style={{background:axis===id?`${color}18`:"transparent",border:`1px solid ${axis===id?color:C.border}`,borderRadius:8,padding:"6px 12px",cursor:"pointer",color:axis===id?color:C.textMuted,fontSize:11,fontWeight:axis===id?600:400,transition:"all .15s"}}>{label}</button>
            ))}
          </div>

          <div style={{display:"flex",alignItems:"center",gap:8,flexShrink:0}}>
            <span style={{fontSize:11,color:yearErr?C.rose:C.textMuted}}>{yearErr?"Year API error":"Year"}</span>
            <select value={yearKey} onChange={e=>setYearKey(e.target.value)} style={{background:C.card,border:`1px solid ${yearErr?C.rose:C.border}`,borderRadius:7,color:C.text,padding:"5px 10px",fontSize:12,outline:"none",cursor:"pointer"}}>
              <option value="">All years</option>
              {years.map(y=>(
                <option key={y.SchoolYearKey} value={String(y.SchoolYearKey)}>
                  {y.YearLabel}{y.IsCurrent?" ★":""}
                </option>
              ))}
            </select>
          </div>
        </div>
      </div>

      <div style={{height:2,background:`linear-gradient(90deg,${ac},${ac}40,transparent)`,transition:"background 0.3s"}}/>

      <div style={{maxWidth:1300,margin:"0 auto",padding:"28px 36px"}}>
        {axis==="axis1"&&<Axis1 qs={qs}/>}
        {axis==="axis2"&&<Axis2 qs={qs}/>}
        {axis==="axis3"&&<Axis3 qs={qs}/>}
        {axis==="axis4"&&<Axis4 qs={qs}/>}
        {axis==="axis5"&&<Axis5 qs={qs}/>}
        {axis==="axis6"&&<Axis6 qs={qs}/>}
        {axis==="axis7"&&<Axis7 qs={qs}/>}
        {axis==="axis8"&&<Axis8 qs={qs}/>}
        {axis==="axis9"&&<Axis9 qs={qs}/>}
      </div>
    </div>
  );
}