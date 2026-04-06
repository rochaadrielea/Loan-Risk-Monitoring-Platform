# data_quality/reporter.py
# CLI Reporter + PDF Reporter + Multi-Page HTML Reporter
# Graian Capital Management — brand palette: Navy #1C2B4A | Gold #B89650
# Author: Adriele Rocha Weisz

from __future__ import annotations
from datetime import datetime
from pathlib import Path
from .checks import Issue

NAVY  = "#1C2B4A"
GOLD  = "#B89650"
LIGHT = "#F5F6F8"
WHITE = "#FFFFFF"
RED   = "#C0392B"
AMBER = "#D4820A"
GREEN = "#1A7A4A"
GREY  = "#6B7280"
BLUE  = "#2563EB"


# ─────────────────────────────────────────────────────────────────────────────
# CLI REPORTER
# ─────────────────────────────────────────────────────────────────────────────

class CLIReporter:
    BOLD = "\033[1m"; RESET = "\033[0m"
    C = {"navy":"\033[38;2;28;43;74m","gold":"\033[38;2;184;150;80m",
         "red":"\033[38;2;192;57;43m","amber":"\033[38;2;212;130;10m",
         "green":"\033[38;2;26;122;74m","grey":"\033[38;2;107;114;128m",
         "white":"\033[97m","blue":"\033[38;2;37;99;235m"}

    def _c(self, color, text): return f"{self.C.get(color,'')}{text}{self.RESET}"
    def _b(self, text): return f"{self.BOLD}{text}{self.RESET}"
    def _badge(self, sev): return {
        "HIGH":self._c("red","🔴 HIGH  "),
        "MEDIUM":self._c("amber","🟡 MEDIUM"),
        "LOW":self._c("green","🟢 LOW   ")}.get(sev, sev)

    def print_header(self, config, run_time):
        meta=config.get("project",{}); w=60
        print()
        print(self._c("gold","╔"+"═"*w+"╗"))
        print(self._c("gold","║")+self._b(self._c("white",
            f"  {'GRAIAN CAPITAL MANAGEMENT':^{w-2}}  "))+self._c("gold","║"))
        print(self._c("gold","║")+self._c("grey",
            f"  {'Data Quality Pipeline  ·  '+run_time:^{w-2}}  ")+self._c("gold","║"))
        print(self._c("gold","╚"+"═"*w+"╝")); print()

    def print_ingestion(self, tables):
        print(self._b(self._c("gold","  📥  DATA INGESTION")))
        print(self._c("grey","  "+"─"*55))
        for name,df in tables.items():
            print(f"  {self._c('green','✅')} {self._b(name):<28} "
                  f"{self._c('grey',f'{len(df)} rows × {len(df.columns)} cols')}")
        print()

    def print_drift(self, events):
        if not events: return
        print(self._b(self._c("red","  ⚠️   SCHEMA DRIFT DETECTED")))
        print(self._c("grey","  "+"─"*55))
        icons={"COLUMN_REMOVED":"🔴","TABLE_REMOVED":"🔴",
               "COLUMN_ADDED":"🟡","TABLE_ADDED":"🟡","ROW_COUNT_CHANGE":"🟢"}
        for e in events:
            print(f"  {icons.get(e['type'],'⚪')}  {e['detail']}")
        print()

    def print_issues(self, issues):
        actionable=[i for i in issues if not i.is_observation]
        print(self._b(self._c("gold","  🔍  DATA QUALITY FINDINGS")))
        print(self._c("grey","  "+"─"*55))
        if not actionable:
            print(f"  {self._c('green','✅')} No actionable issues.")
        else:
            for sev in ["HIGH","MEDIUM","LOW"]:
                for i in [x for x in actionable if x.severity==sev]:
                    cnt=f"({i.affected} records)" if i.affected else ""
                    print(f"  {self._badge(i.severity)}  "
                          f"{self._c('grey','['+i.category+']'):<20} "
                          f"{i.description} {self._c('grey',cnt)}")
        counts={s:sum(1 for i in actionable if i.severity==s)
                for s in ["HIGH","MEDIUM","LOW"]}
        print(); print(
            f"  Summary: {self._c('red',str(counts['HIGH'])+' HIGH')}  |  "
            f"{self._c('amber',str(counts['MEDIUM'])+' MEDIUM')}  |  "
            f"{self._c('green',str(counts['LOW'])+' LOW')}"); print()

    def print_observations(self, issues):
        obs=[i for i in issues if i.is_observation]
        if not obs: return
        print(self._b(self._c("blue","  ℹ️   OBSERVATIONS")))
        print(self._c("grey","  "+"─"*55))
        for o in obs:
            print(f"  {self._c('blue','◉')}  {o.description}")
        print()

    def print_remediation(self, issues):
        print(self._b(self._c("gold","  🔧  REMEDIATION APPLIED")))
        print(self._c("grey","  "+"─"*55))
        rem=[i for i in issues if not i.is_observation and i.severity in ("HIGH","MEDIUM")]
        if not rem: print(f"  {self._c('green','✅')} No remediation required.")
        else:
            for i in rem:
                short=i.suggestion[:80]+"..." if len(i.suggestion)>80 else i.suggestion
                print(f"  {self._c('gold','🔧')} {short}")
        print()

    def print_outputs(self, paths):
        print(self._b(self._c("gold","  📤  OUTPUTS")))
        print(self._c("grey","  "+"─"*55))
        for p in paths:
            icon=self._c("green","✅") if Path(p).exists() else self._c("red","❌")
            print(f"  {icon} {p}")
        print()

    def print_suggestions(self, suggestions):
        print(self._b(self._c("gold","  💡  POWER QUERY GUIDE")))
        print(self._c("grey","  "+"─"*55))
        for s in suggestions:
            print(f"\n  {self._badge(s['severity'])}  {self._b(s['id'])} — {s['table']}")
            print(f"  {self._c('grey',s['description'])}")
            for line in s["powerquery_hint"].split("\n"):
                print(f"    {self._c('navy',line)}")
        print()


# ─────────────────────────────────────────────────────────────────────────────
# MULTI-PAGE HTML REPORTER
# Generates one main dashboard + one page per severity/category
# ─────────────────────────────────────────────────────────────────────────────

# Registry of every check the tool can run — for the Checks Inventory page
CHECKS_REGISTRY = [
    {"id":"CHK-01","name":"Null Detection","cat":"COMPLETENESS",
     "method":"Counts missing values in every column. Reports count and percentage.",
     "why":"Null values in key columns break Power BI joins. Nulls in numeric columns silently distort averages."},
    {"id":"CHK-02","name":"IQR Outlier Detection","cat":"OUTLIER",
     "method":"Computes Q1, Q3, and IQR for every numeric column. Flags values outside Q1−3×IQR or Q3+3×IQR.",
     "why":"3×IQR catches values that are extreme relative to the spread of the column, regardless of absolute scale."},
    {"id":"CHK-03","name":"Z-Score Outlier Detection","cat":"OUTLIER",
     "method":"Computes mean and standard deviation. Flags values where |z-score| > 3.",
     "why":"Z-score catches values far from the mean even when the IQR spread is small — complementary to IQR."},
    {"id":"CHK-04","name":"Benford's Law Analysis","cat":"STATISTICAL",
     "method":"For positive numeric columns with ≥30 rows, computes first-digit distribution and compares to log10(1+1/d).",
     "why":"Natural financial amounts follow Benford's Law. Deviation indicates fabrication, rounding patterns, or entry errors."},
    {"id":"CHK-05","name":"Coefficient of Variation","cat":"STATISTICAL",
     "method":"Computes CV = std/mean for every numeric column. High CV on stable columns (FX rates, ratios) is flagged.",
     "why":"Measures relative dispersion. A column expected to be stable (e.g. daily FX) with high CV signals volatility or errors."},
    {"id":"CHK-06","name":"Negative Value Detection","cat":"INTEGRITY",
     "method":"For numeric columns whose name suggests positivity (price, value, ratio, rate), flags negative values.",
     "why":"Prices and ratios should be non-negative. Negatives may indicate sign convention errors at source."},
    {"id":"CHK-07","name":"Date Parse Validation","cat":"COMPLETENESS",
     "method":"Attempts to parse every date column. Counts values that cannot be converted.",
     "why":"Unparseable dates become text in Power BI and are excluded from all date-based calculations and slicers."},
    {"id":"CHK-08","name":"Future Date Detection","cat":"INTEGRITY",
     "method":"Flags date values after today's date in date columns.",
     "why":"Future dates in transaction data typically indicate entry errors and cause incorrect time series charts."},
    {"id":"CHK-09","name":"Business-Day Gap Detection","cat":"TEMPORAL",
     "method":"Uses numpy.busday_count to count missing business days in date sequences. Weekends excluded.",
     "why":"Missing weekdays in a transaction feed may mean data was not loaded for those days."},
    {"id":"CHK-10","name":"Duplicate Row Detection","cat":"INTEGRITY",
     "method":"Uses pandas.DataFrame.duplicated() to find fully identical rows.",
     "why":"Duplicate rows inflate totals — e.g. a duplicated transaction doubles Loan Exposure for that row."},
    {"id":"CHK-11","name":"Primary Key Duplicate Detection","cat":"INTEGRITY",
     "method":"Infers likely PK column (100% unique, no nulls, name contains 'code'/'id'). Flags duplicate values.",
     "why":"Duplicate PKs break Many→One relationships in Power BI and cause incorrect aggregations."},
    {"id":"CHK-12","name":"Referential Integrity","cat":"REFERENTIAL",
     "method":"Auto-detects FK→PK relationships by name similarity and value overlap. Flags orphaned values.",
     "why":"Rows with orphaned FK values are silently excluded from all joined Power BI calculations."},
    {"id":"CHK-13","name":"Temporal Alignment","cat":"TEMPORAL",
     "method":"Compares date ranges across all tables. Flags start/end mismatches as observations.",
     "why":"Date range mismatches between tables (e.g. prices vs transactions) need to be confirmed as intentional."},
    {"id":"CHK-14","name":"Disconnected Table Detection","cat":"ARCHITECTURE",
     "method":"Finds tables with no key-column overlap with any other table. Notes date columns as DAX candidates.",
     "why":"Disconnected tables cannot be joined in Power BI's relationship panel and require DAX date logic instead."},
    {"id":"CHK-15","name":"Schema Drift Detection","cat":"GOVERNANCE",
     "method":"Compares current column list and row counts to saved baseline (schema_baseline.json).",
     "why":"Column removals break DAX measures immediately. This catches changes before Power BI is refreshed."},
    {"id":"CHK-16","name":"Value Set Drift","cat":"GOVERNANCE",
     "method":"Compares categorical column value sets to baseline. Flags new values not seen before.",
     "why":"New category values (e.g. new portfolio codes) may need mapping in Power Query before loading."},
    {"id":"CHK-17","name":"Numeric Range Drift","cat":"GOVERNANCE",
     "method":"Compares min/max of numeric columns to baseline. Flags changes > 50%.",
     "why":"A column whose max suddenly increases 10× (like loan-property-value-ratio: 2.15→33.33) signals a data problem."},
    {"id":"CHK-18","name":"Business Rules Validation","cat":"CONFIG",
     "method":"Runs config.json-defined rules: allowed_values, value_range, column_exists.",
     "why":"Domain-specific rules that statistics cannot detect — e.g. only LN, EQ, FI are valid product types."},
    {"id":"CHK-19","name":"Mixed Type Detection","cat":"STRUCTURE",
     "method":"For columns inferred as numeric, counts values that cannot be parsed as numbers.",
     "why":"Mixed types cause Power BI to convert the column to text, breaking all numeric calculations."},
    {"id":"CHK-20","name":"Encoding Issue Detection","cat":"STRUCTURE",
     "method":"Scans string columns for control characters and non-UTF-8 bytes.",
     "why":"Encoding issues cause import errors or invisible characters that break exact-match joins."},
]


class HTMLReporter:

    def __init__(self, config, issues, suggestions, tables, run_time,
                 drift_events=None, baseline_date=None):
        self.config        = config
        self.issues        = issues
        self.suggestions   = suggestions
        self.tables        = tables
        self.run_time      = run_time
        self.drift_events  = drift_events or []
        self.baseline_date = baseline_date or "No previous run found"

    # ── public entry point ────────────────────────────────────────────────────

    def _safe_name(self) -> str:
        """Sanitised project/dataset name for use in filenames."""
        import re
        proj = self.config.get("project", {})
        name = proj.get("name", "").strip()
        if not name:
            tables = list(self.tables.keys())
            name = "_".join(tables[:2]) if tables else "dataset"
        return re.sub(r"[^a-zA-Z0-9_-]", "_", name).strip("_").lower()

    def _pdf_name(self) -> str:
        """Master PDF filename."""
        date_part = self.run_time[:10].replace("-", "")
        return f"quality_report_{self._safe_name()}_{date_part}.pdf"

    def _section_pdf_name(self, section: str) -> str:
        """Section-specific PDF filename — e.g. observations_business_case_20260405.pdf"""
        labels = {
            "high":         "high_findings",
            "medium":       "medium_findings",
            "low":          "low_findings",
            "observations": "observations",
            "drift":        "drift_report",
            "checks":       "checks_inventory",
        }
        prefix = labels.get(section, section)
        date_part = self.run_time[:10].replace("-", "")
        return f"{prefix}_{self._safe_name()}_{date_part}.pdf"

    def generate(self, output_path: str):
        """Generate all HTML pages into the same directory as output_path."""
        base_dir  = Path(output_path).parent
        meta      = self.config.get("project", {})
        actionable   = [i for i in self.issues if not i.is_observation]
        observations = [i for i in self.issues if i.is_observation]
        counts = {s: sum(1 for i in actionable if i.severity == s)
                  for s in ["HIGH", "MEDIUM", "LOW"]}

        # Source attribution: table name → "filename → sheet: name"
        src_cfg = self.config.get("sources", {})
        def source_label(table_name):
            src = src_cfg.get(table_name, {})
            fname = Path(src.get("file","")).name if src.get("file") else table_name
            sheet = src.get("sheet","")
            if sheet:
                return f"{fname} → sheet: {sheet}"
            return fname

        pages = {
            "high":        [i for i in actionable if i.severity == "HIGH"],
            "medium":      [i for i in actionable if i.severity == "MEDIUM"],
            "low":         [i for i in actionable if i.severity == "LOW"],
            "observations":observations,
        }

        # Generate sub-pages
        for sev, issues_list in pages.items():
            self._write_findings_page(
                base_dir / f"report_{sev}.html",
                sev, issues_list, source_label, meta)

        self._write_drift_page(base_dir / "report_drift.html", meta)
        self._write_checks_page(base_dir / "report_checks.html", actionable,
                                observations, meta)
        self._write_templates_page(base_dir / "report_templates.html", meta)

        # ── Generate section-specific PDFs so each sub-page has its own download ──
        section_titles = {
            "high":         "High Severity Findings",
            "medium":       "Medium Severity Findings",
            "low":          "Low Severity Findings",
            "observations": "Observations",
        }
        _pdf_r = PDFReporter(self.config, self.issues, [], self.tables, self.run_time)
        for sev, items in pages.items():
            s_pdf = base_dir / self._section_pdf_name(sev)
            try:
                _pdf_r.generate_section(str(s_pdf), section_titles[sev], items,
                                        is_obs=(sev == "observations"))
            except Exception as e:
                print(f"  ⚠️  Section PDF skipped ({sev}): {e}")

        # Drift PDF — full comparison table
        try:
            _pdf_r.generate_drift_pdf(
                str(base_dir / self._section_pdf_name("drift")),
                self.drift_events, self.baseline_date)
        except Exception as e:
            print(f"  ⚠️  Drift PDF skipped: {e}")

        # Checks Inventory PDF — full 20-check table
        try:
            _pdf_r.generate_checks_pdf(
                str(base_dir / self._section_pdf_name("checks")),
                actionable, observations)
        except Exception as e:
            print(f"  ⚠️  Checks PDF skipped: {e}")

        # Generate main dashboard last
        self._write_main(Path(output_path), counts, len(observations),
                         len(self.drift_events), meta, source_label)

    # ── SHARED CSS ────────────────────────────────────────────────────────────

    def _css(self):
        return """
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{
  --navy:#1C2B4A; --navy-mid:#2A3F61; --gold:#B89650;
  --white:#FFFFFF; --bg:#F7F8FA; --surface:#FFFFFF;
  --border:#DDE1E7; --border-light:#EAEDF2;
  --text:#1A202C; --text-primary:#1A202C;
  --text-mid:#4A5568; --text-secondary:#4A5568;
  --text-light:#718096; --text-muted:#718096;
  /* Severity — muted, professional */
  --sev-high-text:#7B1E1E; --sev-high-bg:#FDF2F2; --sev-high-border:#C8A8A8;
  --sev-med-text:#7A4D0A;  --sev-med-bg:#FEF9F0;  --sev-med-border:#C8B888;
  --sev-low-text:#144D2E;  --sev-low-bg:#F2FAF5;  --sev-low-border:#A8C8B8;
  --sev-obs-text:#1E3A5F;  --sev-obs-bg:#F2F5FA;  --sev-obs-border:#A8B8C8;
  /* Aliases kept for backward compat */
  --critical:#7B1E1E; --critical-bg:#FDF2F2; --critical-border:#C8A8A8;
  --warning:#7A4D0A;  --warning-bg:#FEF9F0;  --warning-border:#C8B888;
  --ok:#144D2E;       --ok-bg:#F2FAF5;       --ok-border:#A8C8B8;
  --info:#1E3A5F;     --info-bg:#F2F5FA;     --info-border:#A8B8C8;
}
body{font-family:'Segoe UI','Calibri',system-ui,sans-serif;
  background:var(--bg);color:var(--text);min-height:100vh;font-size:13px}
header{background:var(--navy);position:sticky;top:0;z-index:100;
  border-bottom:3px solid var(--gold)}
.hinner{display:flex;align-items:center;justify-content:space-between;padding:10px 32px}
.hbrand{display:flex;align-items:center;gap:10px}
.hlogo{width:30px;height:30px;background:var(--gold);border-radius:3px;display:flex;
  align-items:center;justify-content:center;font-size:13px;font-weight:800;
  color:var(--navy);letter-spacing:-0.5px}
.hname{color:var(--white);font-size:13px;font-weight:600;letter-spacing:.2px}
.hsub{color:rgba(255,255,255,.4);font-size:10px;letter-spacing:.3px;text-transform:uppercase}
.hmeta{display:flex;align-items:center;gap:8px}
.run-time{color:rgba(255,255,255,.35);font-size:10px;margin-right:4px}
.back-btn{display:inline-flex;align-items:center;gap:5px;padding:5px 12px;border-radius:3px;
  background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.15);
  color:rgba(255,255,255,.65);font-size:11px;font-weight:500;text-decoration:none;
  transition:all .12s;letter-spacing:.2px}
.back-btn:hover{background:rgba(255,255,255,.12);color:white;border-color:rgba(255,255,255,.25)}
.gold-btn{background:rgba(184,150,80,.12);border-color:rgba(184,150,80,.4);
  color:rgba(184,150,80,.9)}
.gold-btn:hover{background:rgba(184,150,80,.22);color:var(--gold)}
.main{padding:20px 32px;display:flex;flex-direction:column;gap:16px;
  max-width:1440px;margin:0 auto}
.page-title{font-size:18px;font-weight:700;color:var(--navy);margin-bottom:2px;
  letter-spacing:-.2px}
.page-sub{font-size:12px;color:var(--text-light);line-height:1.5}
.section{background:var(--surface);border:1px solid var(--border);
  border-radius:4px;overflow:hidden}
.sh{padding:10px 18px;display:flex;align-items:center;gap:10px;
  background:var(--navy);border-bottom:1px solid rgba(255,255,255,.08)}
.sh h2{color:var(--white);font-size:11px;font-weight:600;
  text-transform:uppercase;letter-spacing:.8px}
.sh-badge{background:var(--gold);color:var(--navy);font-size:10px;font-weight:700;
  padding:1px 7px;border-radius:2px;letter-spacing:.3px}
.empty{padding:32px;text-align:center;color:var(--text-light);font-size:13px}
footer{background:var(--navy);padding:12px 32px;display:flex;
  justify-content:space-between;margin-top:12px;
  border-top:2px solid var(--gold)}
footer p{color:rgba(255,255,255,.3);font-size:10px;letter-spacing:.2px}
footer strong{color:var(--gold)}
"""

    def _head(self, title, extra_css=""):
        return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Graian — {title}</title>
<style>{self._css()}{extra_css}</style>
</head>
<body>"""

    def _header(self, subtitle, show_back=True, pdf_filename=None):
        back = '<a href="quality_report.html" class="back-btn">← Dashboard</a>' if show_back else ""
        if show_back:
            dl_name = pdf_filename or self._pdf_name()
            action_btns = f"""
<a href="/" class="back-btn" title="Run a new analysis">+ New Analysis</a>
<a href="#" class="back-btn gold-btn" id="pdf-dl-btn"
   onclick="var s=window.location.pathname.split('/');
            var sid=s.length>2?s[2]:'.';
            this.href='/download/'+sid+'/{dl_name}';
            return true;"
   download="{dl_name}">⬇ PDF</a>"""
        else:
            action_btns = ""
        return f"""
<header>
  <div class="gold-bar"></div>
  <div class="hinner">
    <div class="hbrand">
      <div class="hlogo">G</div>
      <div><div class="hname">Graian Capital Management</div>
      <div class="hsub">{subtitle}</div></div>
    </div>
    <div class="hmeta">
      <span class="run-time">{self.run_time}</span>
      {action_btns}
      {back}
    </div>
  </div>
</header>"""

    def _footer(self):
        meta = self.config.get("project", {})
        return f"""
<footer>
  <p>Generated by <strong>Graian Data Quality Pipeline</strong> · {self.run_time}</p>
  <p>v{meta.get('version','1.0.0')} · Confidential</p>
</footer>
</body></html>"""

    # ── MAIN DASHBOARD ────────────────────────────────────────────────────────

    # Professional SVG icons for nav cards — no emoji
    _SVG = {
        "high":   '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#8B1A1A" stroke-width="1.75" style="display:block;flex-shrink:0"><path d="M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>',
        "medium": '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#7A4F00" stroke-width="1.75" style="display:block;flex-shrink:0"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>',
        "low":    '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#1A4731" stroke-width="1.75" style="display:block;flex-shrink:0"><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg>',
        "obs":    '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#1A3A5C" stroke-width="1.75" style="display:block;flex-shrink:0"><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg>',
        "drift":  '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#2A3F61" stroke-width="1.75" style="display:block;flex-shrink:0"><polyline points="16 3 21 3 21 8"/><line x1="4" y1="20" x2="21" y2="3"/><polyline points="21 16 21 21 16 21"/></svg>',
        "checks": '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#2A3F61" stroke-width="1.75" style="display:block;flex-shrink:0"><polyline points="9 11 12 14 22 4"/><path d="M21 12v7a2 2 0 01-2 2H5a2 2 0 01-2-2V5a2 2 0 012-2h11"/></svg>',
    }

    def _write_main(self, path, counts, n_obs, n_drift, meta, source_label):
        css = """
/* ── KPI cards ── */
.kpi-row{display:grid;grid-template-columns:repeat(4,1fr);gap:0;
  border:1px solid var(--border);margin:20px 32px 0;background:var(--border)}
.kpi{background:var(--surface);padding:18px 22px;border-right:1px solid var(--border)}
.kpi:last-child{border-right:none}
.kpi.high{border-top:3px solid #8B1A1A}
.kpi.medium{border-top:3px solid #7A4D0A}
.kpi.low{border-top:3px solid #144D2E}
.kpi.obs{border-top:3px solid #1E3A5F}
.kpi-val{font-size:34px;font-weight:700;line-height:1;letter-spacing:-1px;
  font-variant-numeric:tabular-nums;color:var(--navy)}
.kpi-label{font-size:10px;color:var(--text-muted);font-weight:500;
  text-transform:uppercase;letter-spacing:.7px;margin-top:6px;
  display:flex;align-items:center;gap:5px}
.kpi-dot{width:6px;height:6px;border-radius:50%;display:inline-block;flex-shrink:0}
.dot-critical{background:#8B1A1A}.dot-warning{background:#7A4D0A}
.dot-ok{background:#144D2E}.dot-info{background:#1E3A5F}
/* ── nav cards ── */
.nav-cards{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;padding:0 32px}
.nav-card{background:var(--surface);border:1px solid var(--border);padding:16px 18px;
  cursor:pointer;transition:background .12s;text-decoration:none;
  display:flex;align-items:flex-start;gap:14px;border-left:3px solid var(--border)}
.nav-card:hover{background:#F0F3F9;border-color:var(--navy-mid)}
.nav-card.nc-high{border-left-color:var(--sev-high-border)}
.nav-card.nc-medium{border-left-color:var(--sev-med-border)}
.nav-card.nc-low{border-left-color:var(--sev-low-border)}
.nav-card.nc-obs{border-left-color:var(--sev-obs-border)}
.nav-card.nc-drift,.nav-card.nc-checks{border-left-color:var(--navy-mid)}
.nc-icon-wrap{flex-shrink:0;width:36px;height:36px;border:1px solid var(--border);
  display:flex;align-items:center;justify-content:center;background:var(--bg)}
.nc-icon-wrap svg{width:18px;height:18px}
.nc-icon-wrap.iwh{background:var(--sev-high-bg);border-color:var(--sev-high-border)}
.nc-icon-wrap.iwm{background:var(--sev-med-bg);border-color:var(--sev-med-border)}
.nc-icon-wrap.iwl{background:var(--sev-low-bg);border-color:var(--sev-low-border)}
.nc-icon-wrap.iwo{background:var(--sev-obs-bg);border-color:var(--sev-obs-border)}
.nc-icon-wrap.iwd,.nc-icon-wrap.iwc{background:var(--bg);border-color:var(--border)}
.nc-body{flex:1;min-width:0}
.nc-title{font-size:12px;font-weight:600;color:var(--text-primary);margin-bottom:2px}
.nc-sub{font-size:11px;color:var(--text-muted);line-height:1.4;margin-bottom:6px}
.nc-badge{display:inline-block;padding:2px 8px;border:1px solid;border-radius:2px;
  font-size:10px;font-weight:600;letter-spacing:.2px}
.nc-badge.high{background:var(--sev-high-bg);color:var(--sev-high-text);
  border-color:var(--sev-high-border)}
.nc-badge.medium{background:var(--sev-med-bg);color:var(--sev-med-text);
  border-color:var(--sev-med-border)}
.nc-badge.low{background:var(--sev-low-bg);color:var(--sev-low-text);
  border-color:var(--sev-low-border)}
.nc-badge.obs{background:var(--sev-obs-bg);color:var(--sev-obs-text);
  border-color:var(--sev-obs-border)}
.nc-badge.clean{background:var(--sev-low-bg);color:var(--sev-low-text);
  border-color:var(--sev-low-border)}
.nc-badge.alert{background:var(--sev-high-bg);color:var(--sev-high-text);
  border-color:var(--sev-high-border)}
.nc-badge.navy{background:var(--bg);color:var(--text-secondary);
  border-color:var(--border)}
/* ── Overview table — Excel-like ── */
.ov-table table{width:100%;border-collapse:collapse;font-size:12px}
.ov-table thead th{padding:8px 12px;text-align:left;background:var(--bg);
  color:var(--text-secondary);font-size:10px;font-weight:600;
  text-transform:uppercase;letter-spacing:.5px;
  border-bottom:2px solid var(--border);border-right:1px solid var(--border-light);
  white-space:nowrap;cursor:pointer;user-select:none}
.ov-table thead th:last-child{border-right:none}
.ov-table thead th:hover{background:#E8ECF4}
.ov-table td{padding:8px 12px;border-bottom:1px solid var(--border-light);
  border-right:1px solid var(--border-light);vertical-align:top}
.ov-table td:last-child{border-right:none}
.ov-table tr:nth-child(even) td{background:#FAFBFD}
.ov-table tr:hover td{background:#EBF0F9}
.ov-table tr:last-child td{border-bottom:none}
/* ── Source table ── */
.src-table table{width:100%;border-collapse:collapse;font-size:12px}
.src-table thead th{padding:8px 12px;text-align:left;background:var(--bg);
  color:var(--text-secondary);font-size:10px;font-weight:600;
  text-transform:uppercase;letter-spacing:.5px;border-bottom:2px solid var(--border)}
.src-table td{padding:8px 12px;border-bottom:1px solid var(--border-light)}
.src-table tr:nth-child(even) td{background:#FAFBFD}
.src-table tr:hover td{background:#EBF0F9}
.src-table tr:last-child td{border-bottom:none}
.src-name{font-weight:600;color:var(--navy)}
/* ── Unified nav+KPI cards ── */
.unified-grid{display:grid;grid-template-columns:repeat(6,1fr);gap:6px}
.ucard{background:var(--surface);border:1px solid var(--border);
  border-top:3px solid var(--border);padding:10px 14px 8px;
  text-decoration:none;display:flex;flex-direction:column;gap:0;
  transition:background .12s;cursor:pointer}
.ucard:hover{background:#F0F3F9}
.ucard:hover .uc-arrow{color:var(--navy)}
.uc-high{border-top-color:#8B1A1A}
.uc-med{border-top-color:#7A4D0A}
.uc-low{border-top-color:#144D2E}
.uc-obs{border-top-color:#1E3A5F}
.uc-drift,.uc-checks{border-top-color:#2A3F61}
.uc-top{display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:1px}
.uc-icon{opacity:.35;flex-shrink:0;margin-top:1px}
.uc-num{font-size:24px;font-weight:700;line-height:1;letter-spacing:-.5px;
  font-variant-numeric:tabular-nums}
.uc-high .uc-num{color:#7B1E1E}
.uc-med  .uc-num{color:#7A4D0A}
.uc-low  .uc-num{color:#144D2E}
.uc-obs  .uc-num{color:#1E3A5F}
.uc-drift .uc-num,.uc-checks .uc-num{color:var(--navy)}
.uc-label{font-size:11px;font-weight:700;color:var(--navy);letter-spacing:.1px;
  margin-top:4px}
.uc-sub{font-size:10px;color:var(--text-muted);line-height:1.3;margin-top:1px}
.uc-foot{display:flex;align-items:center;justify-content:space-between;
  margin-top:5px;padding-top:4px;border-top:1px solid var(--border-light)}
.uc-arrow{font-size:13px;color:var(--border);font-weight:300;transition:color .12s}
/* ── Filter bar ── */
.ov-fbtn{padding:3px 10px;border:1px solid var(--border);background:var(--surface);
  font-size:11px;font-weight:500;cursor:pointer;transition:all .1s;
  color:var(--text-secondary);font-family:inherit;border-radius:2px}
.ov-fbtn:hover{border-color:var(--navy);color:var(--navy);background:#F0F3F9}
.ov-fbtn.active{background:var(--navy);border-color:var(--navy);color:var(--white);font-weight:600}
.ov-fbtn.fh.active{background:var(--sev-high-text);border-color:var(--sev-high-text);color:white}
.ov-fbtn.fm.active{background:var(--sev-med-text);border-color:var(--sev-med-text);color:white}
.ov-fbtn.fl.active{background:var(--sev-low-text);border-color:var(--sev-low-text);color:white}
.ov-fbtn.fo.active{background:var(--sev-obs-text);border-color:var(--sev-obs-text);color:white}
/* ── Nav list — used by the main dashboard navigation ── */
.nav-list{display:flex;flex-direction:column;border:1px solid var(--border);overflow:hidden}
.nav-item{background:var(--surface);display:flex;align-items:center;gap:12px;
  padding:12px 16px;text-decoration:none;border-bottom:1px solid var(--border-light);
  transition:background .1s}
.nav-item:last-child{border-bottom:none}
.nav-item:hover{background:#F0F3F9}
.nav-item:hover .nav-arrow{color:var(--navy)}
.nav-icon-box{flex-shrink:0;width:32px;height:32px;border:1px solid var(--border);
  display:flex;align-items:center;justify-content:center;background:var(--bg);
  overflow:hidden}
.nav-icon-box svg{width:18px !important;height:18px !important;
  max-width:18px !important;max-height:18px !important;display:block !important}
.ni-critical{background:#FDF2F2;border-color:#C8A8A8}
.ni-warning{background:#FEF9F0;border-color:#C8B888}
.ni-ok{background:#F2FAF5;border-color:#A8C8B8}
.ni-info,.ni-navy{background:#F2F5FA;border-color:#A8B8C8}
.nav-body{flex:1;min-width:0}
.nav-title{font-size:12px;font-weight:600;color:var(--navy);margin-bottom:1px}
.nav-sub{font-size:11px;color:var(--text-muted);white-space:nowrap;
  overflow:hidden;text-overflow:ellipsis}
.nav-right{display:flex;align-items:center;gap:8px;flex-shrink:0}
.nav-status{font-size:10px;font-weight:600;padding:2px 7px;border:1px solid;
  white-space:nowrap}
.ns-critical{background:#FDF2F2;color:#7B1E1E;border-color:#C8A8A8}
.ns-warning{background:#FEF9F0;color:#7A4D0A;border-color:#C8B888}
.ns-ok{background:#F2FAF5;color:#144D2E;border-color:#A8C8B8}
.ns-info{background:#F2F5FA;color:#1E3A5F;border-color:#A8B8C8}
.ns-neutral{background:var(--bg);color:var(--text-muted);border-color:var(--border)}
.nav-arrow{color:var(--border);font-size:16px;font-weight:300;transition:color .1s}
/* ── Sev pills ── */
.sev-pill{display:inline-flex;align-items:center;gap:5px;font-size:10px;
  font-weight:600;letter-spacing:.3px;text-transform:uppercase}
.sev-dot{width:6px;height:6px;border-radius:50%;flex-shrink:0;display:inline-block}
.sHIGH{color:#7B1E1E}.sHIGH .sev-dot{background:#7B1E1E}
.sMEDIUM{color:#7A4D0A}.sMEDIUM .sev-dot{background:#7A4D0A}
.sLOW{color:#144D2E}.sLOW .sev-dot{background:#144D2E}
.sOBS{color:#1E3A5F}.sOBS .sev-dot{background:#1E3A5F}
.cat-pill{font-size:10px;font-weight:500;text-transform:uppercase;
  letter-spacing:.4px;color:var(--text-muted)}
.loc-cell{font-family:Consolas,monospace;font-size:11px;color:var(--navy);font-weight:500}
/* ── Search bar ── */
.ov-search{display:flex;align-items:center;gap:8px;padding:8px 12px;
  border-bottom:1px solid var(--border-light);background:var(--surface)}
.ov-search input{flex:1;border:1px solid var(--border);padding:4px 8px;
  font-size:11px;font-family:inherit;color:var(--text);outline:none}
.ov-search input:focus{border-color:var(--navy)}
"""
        all_actionable = [i for i in self.issues if not i.is_observation]
        all_obs        = [i for i in self.issues if i.is_observation]

        drift_badge = (f'<span class="nc-badge alert">{n_drift} change(s) detected</span>'
                       if n_drift > 0 else
                       '<span class="nc-badge clean">No changes detected</span>')
        high_badge = (f'<span class="nc-badge high">{counts["HIGH"]} issue(s)</span>'
                      if counts["HIGH"] > 0 else '<span class="nc-badge clean">None found</span>')
        med_badge  = (f'<span class="nc-badge medium">{counts["MEDIUM"]} issue(s)</span>'
                      if counts["MEDIUM"] > 0 else '<span class="nc-badge clean">None found</span>')
        low_badge  = (f'<span class="nc-badge low">{counts["LOW"]} issue(s)</span>'
                      if counts["LOW"] > 0 else '<span class="nc-badge clean">None found</span>')
        obs_badge  = f'<span class="nc-badge obs">{n_obs} note(s)</span>'

        # Overview table rows — all findings + observations
        ov_rows = ""
        for i in all_actionable + all_obs:
            sev_cls = "sOBS" if i.is_observation else f"s{i.severity}"
            sev_lbl = "OBS" if i.is_observation else i.severity
            col_part = f".{i.column}" if i.column else ""
            ov_rows += (
                f'<tr data-sev="{sev_lbl}">' 
                f'<td><span class="sev-pill {sev_cls}"><span class="sev-dot"></span>{sev_lbl}</span></td>'
                f'<td><span class="cat-pill">{i.category}</span></td>'
                f'<td class="loc-cell">{i.table}{col_part}</td>'
                f'<td style="font-size:12px;color:var(--text)">{i.description}</td>'
                f'<td style="font-size:11px;color:var(--grey)">{i.suggestion[:80]}{"…" if len(i.suggestion)>80 else ""}</td>'
                f'</tr>'
            )

        # Source inventory rows
        src_rows = ""
        for name, df in self.tables.items():
            src = self.config.get("sources", {}).get(name, {})
            fname = Path(src.get("file", "")).name if src.get("file") else name
            sheet = src.get("sheet", "")
            loc = f"{fname} → {sheet}" if sheet else fname
            cols_prev = ", ".join(str(c) for c in list(df.columns)[:5])
            if len(df.columns) > 5: cols_prev += " …"
            src_rows += (f"<tr><td class='src-name'>{name}</td>"
                         f"<td>{loc}</td><td>{len(df):,}</td>"
                         f"<td>{len(df.columns)}</td><td>{cols_prev}</td></tr>")

        svgs = self._SVG
        _pdf_fn = self._pdf_name()
        _main_hdr = f"""
<header>
  <div class="gold-bar"></div>
  <div class="hinner">
    <div class="hbrand">
      <div class="hlogo">G</div>
      <div><div class="hname">Graian Capital Management</div>
      <div class="hsub">Data Quality Dashboard</div></div>
    </div>
    <div class="hmeta">
      <span class="run-time">{self.run_time}</span>
      <a href="/" class="back-btn">+ New Analysis</a>
      <a href="#" class="back-btn gold-btn"
         onclick="var s=window.location.pathname.split('/');
                  var sid=s.length>2?s[2]:'.';
                  this.href='/download/'+sid+'/{_pdf_fn}';
                  return true;"
         download="{_pdf_fn}">⬇ PDF</a>
    </div>
  </div>
</header>"""
        # Status badges — muted, no loud colors
        def ns(cls, txt): return f'<span class="nav-status {cls}">{txt}</span>'
        def issue_label(n, cls_found, cls_clear):
            if n == 0: return ns(cls_clear, "Clear")
            word = "issue" if n == 1 else "issues"
            return ns(cls_found, f"{n} {word}")
        h_st = issue_label(counts["HIGH"],   "ns-critical", "ns-ok")
        m_st = issue_label(counts["MEDIUM"], "ns-warning",  "ns-ok")
        l_st = issue_label(counts["LOW"],    "ns-ok",       "ns-ok")
        o_st = ns("ns-info", f"{n_obs} note" if n_obs == 1 else f"{n_obs} notes")
        d_st = ns("ns-critical", f"{n_drift} change" if n_drift == 1 else f"{n_drift} changes") if n_drift else ns("ns-ok", "No changes")

        # Dot indicator for KPIs
        def dot(cls): return f'<span class="kpi-dot {cls}" style="width:7px;height:7px;border-radius:50%;display:inline-block;flex-shrink:0"></span>'

        html = (self._head("Quality Dashboard", css)
                + _main_hdr
                + f"""
<div class="main">
<div class="unified-grid">
  <a href="report_high.html" target="_blank" class="ucard uc-high">
    <div class="uc-top">
      <div class="uc-icon">{svgs['high']}</div>
      <div class="uc-num">{counts['HIGH']}</div>
    </div>
    <div class="uc-label">High Severity</div>
    <div class="uc-sub">Breaks calculations — fix before loading</div>
    <div class="uc-foot">{h_st}<span class="uc-arrow">›</span></div>
  </a>
  <a href="report_medium.html" target="_blank" class="ucard uc-med">
    <div class="uc-top">
      <div class="uc-icon">{svgs['medium']}</div>
      <div class="uc-num">{counts['MEDIUM']}</div>
    </div>
    <div class="uc-label">Medium Severity</div>
    <div class="uc-sub">Distorts metrics — fix recommended</div>
    <div class="uc-foot">{m_st}<span class="uc-arrow">›</span></div>
  </a>
  <a href="report_low.html" target="_blank" class="ucard uc-low">
    <div class="uc-top">
      <div class="uc-icon">{svgs['low']}</div>
      <div class="uc-num">{counts['LOW']}</div>
    </div>
    <div class="uc-label">Low Severity</div>
    <div class="uc-sub">Investigate — flag and monitor</div>
    <div class="uc-foot">{l_st}<span class="uc-arrow">›</span></div>
  </a>
  <a href="report_observations.html" target="_blank" class="ucard uc-obs">
    <div class="uc-top">
      <div class="uc-icon">{svgs['obs']}</div>
      <div class="uc-num">{n_obs}</div>
    </div>
    <div class="uc-label">Observations</div>
    <div class="uc-sub">Expected behaviour — documented for traceability</div>
    <div class="uc-foot">{o_st}<span class="uc-arrow">›</span></div>
  </a>
  <a href="report_drift.html" target="_blank" class="ucard uc-drift">
    <div class="uc-top">
      <div class="uc-icon">{svgs['drift']}</div>
      <div class="uc-num">{n_drift}</div>
    </div>
    <div class="uc-label">Schema Drift Report</div>
    <div class="uc-sub">{"No changes since last run" if n_drift == 0 else str(n_drift)+" change(s) detected"}</div>
    <div class="uc-foot">{d_st}<span class="uc-arrow">›</span></div>
  </a>
  <a href="report_checks.html" target="_blank" class="ucard uc-checks">
    <div class="uc-top">
      <div class="uc-icon">{svgs['checks']}</div>
      <div class="uc-num">20</div>
    </div>
    <div class="uc-label">Checks Inventory</div>
    <div class="uc-sub">All 20 checks run · {self.run_time}</div>
    <div class="uc-foot"><span class="nav-status ns-neutral">View report</span>
      <span class="uc-arrow">›</span></div>
  </a>
</div>

<div class="ov-table">
  <div class="sh"><h2>All Findings — Overview</h2>
  <span class="sh-badge">{len(all_actionable)+len(all_obs)}</span></div>
  <div class="ov-search">
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="#718096"
      stroke-width="2"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>
    <input type="text" id="ov-search-input" placeholder="Search findings, locations, categories…"
      oninput="ovSearch(this.value)">
    <span style="font-size:11px;font-weight:600;color:var(--text-light);margin:0 4px 0 8px">Filter:</span>
    <button onclick="ovFilter('ALL',this)" class="ov-fbtn active">All ({len(all_actionable)+len(all_obs)})</button>
    <button onclick="ovFilter('HIGH',this)" class="ov-fbtn fh">High ({counts["HIGH"]})</button>
    <button onclick="ovFilter('MEDIUM',this)" class="ov-fbtn fm">Medium ({counts["MEDIUM"]})</button>
    <button onclick="ovFilter('LOW',this)" class="ov-fbtn fl">Low ({counts["LOW"]})</button>
    <button onclick="ovFilter('OBS',this)" class="ov-fbtn fo">Obs ({n_obs})</button>
  </div>
  {"<table><thead><tr><th>Severity</th><th>Category</th><th>Location</th><th>Finding</th><th>Suggested Action</th></tr></thead><tbody>"+ov_rows+"</tbody></table>"
   if ov_rows else '<div class="empty">No findings — data is clean.</div>'}
</div>

<div class="section src-table">
  <div class="sh"><h2>Data Source Inventory</h2>
  <span class="sh-badge">{len(self.tables)}</span></div>
  <table><thead><tr><th>Table</th><th>Source</th><th>Rows</th><th>Cols</th><th>Columns</th></tr></thead>
  <tbody>{src_rows}</tbody></table>
</div>
</div>"""
                + """
<script>
// ── Overview filter & search ─────────────────────────────────────────────
var _SEV = 'ALL';
var _Q   = '';

function ovFilter(sev, btn) {
  _SEV = sev;
  document.querySelectorAll('.ov-fbtn').forEach(function(b) {
    b.classList.remove('active');
  });
  btn.classList.add('active');
  _run();
}

function ovSearch(val) {
  _Q = val ? val.toLowerCase().trim() : '';
  _run();
}

function _run() {
  // Use the most specific selector possible
  var ovTable = document.querySelector('.ov-table');
  if (!ovTable) return;
  var tbody = ovTable.querySelector('tbody');
  if (!tbody) return;
  var rows = tbody.querySelectorAll('tr');
  rows.forEach(function(r) {
    // Read attribute directly — most reliable cross-browser
    var sev = r.getAttribute('data-sev') || '';
    var sevOk = (_SEV === 'ALL' || sev === _SEV);
    var txt = r.textContent ? r.textContent.toLowerCase() : '';
    var qOk = (_Q === '' || txt.indexOf(_Q) !== -1);
    r.style.display = (sevOk && qOk) ? '' : 'none';
  });
}

// ── Column sorting ────────────────────────────────────────────────────────
function sortTable(th, colIdx) {
  var table = th.closest('table');
  var tbody = table.querySelector('tbody');
  var rows  = Array.from(tbody.querySelectorAll('tr'));
  var asc   = th.classList.contains('sort-asc');
  // Reset all headers
  table.querySelectorAll('thead th').forEach(h => {
    h.classList.remove('sort-asc','sort-desc');
  });
  th.classList.add(asc ? 'sort-desc' : 'sort-asc');
  rows.sort(function(a, b) {
    var aT = a.cells[colIdx] ? a.cells[colIdx].textContent.trim() : '';
    var bT = b.cells[colIdx] ? b.cells[colIdx].textContent.trim() : '';
    var aNum = parseFloat(aT.replace(/[^0-9.-]/g,''));
    var bNum = parseFloat(bT.replace(/[^0-9.-]/g,''));
    if (!isNaN(aNum) && !isNaN(bNum)) {
      return asc ? bNum - aNum : aNum - bNum;
    }
    return asc ? bT.localeCompare(aT) : aT.localeCompare(bT);
  });
  rows.forEach(r => tbody.appendChild(r));
}

// Add click handlers to all sortable headers after DOM load
document.addEventListener('DOMContentLoaded', function() {
  document.querySelectorAll('thead th').forEach(function(th, idx) {
    th.addEventListener('click', function() { sortTable(th, idx); });
  });
});
</script>"""
                + self._footer())

        path.write_text(html, encoding="utf-8")

    # ── FINDINGS PAGE (HIGH / MEDIUM / LOW / OBSERVATIONS) ───────────────────

    def _write_findings_page(self, path, sev, issues, source_label, meta):
        labels = {
            "high":         ("🔴 High Severity Findings",        "var(--red)",   "#FEF2F2", "HIGH"),
            "medium":       ("🟡 Medium Severity Findings",       "var(--amber)", "#FFFBEB", "MEDIUM"),
            "low":          ("🟢 Low Severity Findings",          "var(--green)", "#F0FDF4", "LOW"),
            "observations": ("ℹ️ Observations",                  "var(--blue)",  "#EFF6FF", "OBS"),
        }
        title, color, bg, label = labels[sev]

        # Map color to muted CSS vars
        _cmap = {
            "var(--red)":   "var(--critical)",
            "var(--amber)": "var(--warning)",
            "var(--green)": "var(--ok)",
            "var(--blue)":  "var(--info)",
        }
        _bgmap = {
            "#FEF2F2": "var(--critical-bg)",
            "#FFFBEB": "var(--warning-bg)",
            "#F0FDF4": "var(--ok-bg)",
            "#EFF6FF": "var(--info-bg)",
        }
        mc = _cmap.get(color, color)
        mb = _bgmap.get(bg, bg)
        css = f"""
/* ── Accordion list ── */
.findings-list{{border:1px solid var(--border);background:var(--surface)}}
.finding-item{{border-bottom:1px solid var(--border-light)}}
.finding-item:last-child{{border-bottom:none}}
.finding-item summary{{list-style:none;cursor:pointer;display:flex;
  align-items:flex-start;gap:12px;padding:12px 16px;
  transition:background .1s;user-select:none}}
.finding-item summary::-webkit-details-marker{{display:none}}
.finding-item summary::marker{{display:none}}
.finding-item summary:hover{{background:#F5F7FB}}
.finding-item[open]>summary{{background:var(--bg);border-bottom:1px solid var(--border-light)}}
.fi-toggle{{flex-shrink:0;width:16px;display:flex;align-items:center;
  justify-content:center;color:var(--text-muted);font-size:11px;margin-top:3px}}
.fi-toggle::before{{content:'▶';display:block}}
.finding-item[open] .fi-toggle::before{{content:'▼'}}
.fi-cat{{font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:.5px;
  padding:2px 8px;border:1px solid;white-space:nowrap;flex-shrink:0;margin-top:2px;
  color:{mc};background:{mb};border-color:{mc}40}}
.fi-body{{flex:1;min-width:0}}
.fi-headline{{font-size:12px;font-weight:600;color:var(--navy);line-height:1.5}}
.fi-src{{font-size:11px;color:var(--text-light);font-family:'Consolas',monospace;
  margin-top:2px}}
/* ── Expanded detail ── */
.finding-detail{{padding:16px 20px 18px 20px;
  display:flex;flex-direction:column;gap:14px;background:var(--surface)}}
.fc-explain{{font-size:12px;color:var(--text-mid);line-height:1.7;
  background:{mb};padding:12px 14px;border-left:2px solid {mc}}}
.fc-action{{font-size:12px;color:var(--grey);background:var(--bg);
  padding:10px 14px;border-left:3px solid var(--border)}}
.snippet-wrap{{overflow-x:auto;border:1px solid var(--border)}}
.snippet-wrap table{{width:100%;border-collapse:collapse;font-size:11px;min-width:400px}}
.snippet-wrap thead{{background:var(--navy)}}
.snippet-wrap th{{padding:7px 12px;text-align:left;color:rgba(255,255,255,.8);
  font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:.5px;
  white-space:nowrap}}
.snippet-wrap td{{padding:7px 12px;border-bottom:1px solid var(--border-light);
  font-family:'Consolas',monospace;font-size:11px;color:var(--text)}}
.snippet-wrap tr:nth-child(even) td{{background:#FAFBFC}}
.snippet-wrap tr:last-child td{{border-bottom:none}}
.snippet-wrap td.hl{{background:var(--critical-bg);color:var(--critical);font-weight:600}}
.snippet-wrap td.flag{{font-size:10px;font-weight:600;text-transform:uppercase;
  letter-spacing:.4px;color:{mc}}}
.tech-toggle{{font-size:11px;color:var(--navy);cursor:pointer;user-select:none;
  display:flex;align-items:center;gap:5px;font-weight:500;border:none;
  background:none;padding:0;opacity:.7}}
.tech-toggle:hover{{opacity:1}}
.tech-body{{display:none;background:var(--bg);border:1px solid var(--border);
  padding:10px 14px;margin-top:8px}}
.tech-body.open{{display:block}}
.tech-row{{display:flex;justify-content:space-between;padding:4px 0;
  border-bottom:1px solid var(--border-light);font-size:11px}}
.tech-row:last-child{{border-bottom:none}}
.tech-key{{color:var(--text-light);font-weight:500}}
.tech-val{{font-family:'Consolas',monospace;color:var(--navy);font-size:11px}}
.empty-page{{padding:60px 40px;text-align:center}}
.empty-icon{{font-size:36px;margin-bottom:12px;opacity:.4}}
.empty-title{{font-size:16px;font-weight:600;color:var(--navy);margin-bottom:6px}}
.empty-sub{{font-size:12px;color:var(--text-light)}}
"""
        is_obs = (sev == "observations")
        header_bg = "background:#1E3A5F;" if is_obs else ""
        header_col = 'style="color:#93C5FD;"' if is_obs else ""

        cards_html = ""
        for idx, issue in enumerate(issues):
            # Headline — use the description as a base, simplified
            headline = issue.description

            # Source attribution
            src_lbl = source_label(issue.table)
            col_part = f" → column: {issue.column}" if issue.column else ""
            full_src = src_lbl + col_part

            # Snippet table
            snippet_html = ""
            if issue.snippet_rows:
                all_keys = [k for k in issue.snippet_rows[0].keys()
                            if not k.startswith("__")]
                highlight_col = issue.snippet_rows[0].get("__highlight__", "")
                flag_label = issue.snippet_rows[0].get("__flag__", "")
                th = "".join(f"<th>{k}</th>" for k in all_keys) + "<th>FLAG</th>"
                rows_html = ""
                for row in issue.snippet_rows:
                    tds = ""
                    for k in all_keys:
                        v = row.get(k, "")
                        v_str = str(v) if v is not None else "NULL"
                        cls = ' class="hl"' if k == highlight_col else ""
                        tds += f"<td{cls}>{v_str}</td>"
                    tds += f'<td class="flag">{flag_label}</td>'
                    rows_html += f"<tr>{tds}</tr>"
                snippet_html = f"""
<div>
  <div style="font-size:11px;color:var(--grey);font-weight:600;
    text-transform:uppercase;letter-spacing:.5px;margin-bottom:8px">
    Sample rows where this issue was found (max 5 shown)
  </div>
  <div class="snippet-wrap">
    <table><thead><tr>{th}</tr></thead><tbody>{rows_html}</tbody></table>
  </div>
</div>"""

            # Technical details
            tech_html = ""
            if issue.stats:
                rows_t = "".join(
                    f'<div class="tech-row"><span class="tech-key">{k}</span>'
                    f'<span class="tech-val">{v}</span></div>'
                    for k, v in issue.stats.items())
                tech_html = f"""
<div>
  <button class="tech-toggle" onclick="toggleTech(this)">
    ▶ Technical detail
  </button>
  <div class="tech-body"><div>{rows_t}</div></div>
</div>"""

            cards_html += f"""
<details class="finding-item">
  <summary>
    <span class="fi-toggle"></span>
    <span class="fi-cat">{issue.category}</span>
    <span class="fi-body">
      <div class="fi-headline">{headline}</div>
      <div class="fi-src">📁 {full_src}</div>
    </span>
  </summary>
  <div class="finding-detail">
    {f'<div class="fc-explain">{issue.explanation}</div>' if issue.explanation else ""}
    {snippet_html}
    <div class="fc-action">
      <strong style="color:var(--navy)">Suggested action:</strong> {issue.suggestion}
    </div>
    {tech_html}
  </div>
</details>"""

        if not cards_html:
            cards_html = f"""
<div class="empty-page">
  <div class="empty-icon">✅</div>
  <div class="empty-title">No {sev.replace('_',' ')} findings</div>
  <div class="empty-sub">The pipeline checked all tables and found nothing in this category.</div>
</div>"""

        subtitle = f"Data Quality Report — {title}"
        html = (self._head(title, css)
                + self._header(subtitle, pdf_filename=self._section_pdf_name(sev))
                + f"""
<div class="main">
  <div>
    <div class="page-title">{title}</div>
    <div class="page-sub">Run at {self.run_time} · {len(issues)} finding(s) — click any row to expand</div>
  </div>
  <div class="findings-list">
    {cards_html}
  </div>
</div>"""
                + f"""
<script>
function toggleTech(btn){{
  var body=btn.nextElementSibling;
  body.classList.toggle('open');
  btn.textContent=(body.classList.contains('open')?'▼':'▶')+' Technical detail';
}}
</script>"""
                + self._footer())

        path.write_text(html, encoding="utf-8")

    # ── DRIFT PAGE ────────────────────────────────────────────────────────────

    def _write_drift_page(self, path, meta):
        css = """
.drift-banner{display:flex;align-items:center;gap:14px;padding:16px 20px;
  border:1px solid var(--border);background:var(--surface)}
.drift-banner.ok{border-left:4px solid #16A34A;background:#F0FDF4}
.drift-banner.alert{border-left:4px solid #B45309;background:#FFFBEB}
.db-icon{font-size:22px;flex-shrink:0}
.db-title{font-size:14px;font-weight:700;color:var(--navy)}
.db-sub{font-size:12px;color:var(--text-muted);margin-top:1px}
.bl-strip{display:flex;gap:0;border:1px solid var(--border);border-top:none;background:var(--bg)}
.bl-cell{padding:10px 18px;flex:1;border-right:1px solid var(--border)}
.bl-cell:last-child{border-right:none}
.bl-label{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.6px;
  color:var(--text-muted);margin-bottom:3px}
.bl-val{font-size:12px;font-weight:600;color:var(--navy)}
.bl-val.warn{color:#B45309}
.drift-table{width:100%;border-collapse:collapse;font-size:12px}
.drift-table thead tr{background:var(--navy)}
.drift-table th{padding:9px 14px;text-align:left;color:rgba(255,255,255,.75);
  font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:.6px;
  white-space:nowrap;border-right:1px solid rgba(255,255,255,.08)}
.drift-table th:last-child{border-right:none}
.drift-table td{padding:10px 14px;border-bottom:1px solid var(--border-light);vertical-align:top}
.drift-table tr:last-child td{border-bottom:none}
.drift-table tr.risk-HIGH td{background:#FEF8F8}
.drift-table tr.risk-MEDIUM td{background:#FFFDF0}
.drift-table tr.risk-LOW td{background:#F5FDF8}
.drift-table tr:hover td{filter:brightness(.97)}
.risk-badge{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.4px;
  padding:2px 8px;border:1px solid;white-space:nowrap;display:inline-block}
.rb-HIGH{color:#7B1E1E;background:#FDF2F2;border-color:#C8A8A8}
.rb-MEDIUM{color:#7A4D0A;background:#FEF9F0;border-color:#C8B888}
.rb-LOW{color:#144D2E;background:#F2FAF5;border-color:#A8C8B8}
.drift-table td.tname{font-family:'Consolas',monospace;font-size:11px;color:var(--navy);
  font-weight:600;white-space:nowrap}
.drift-table td.detail{font-size:12px;color:var(--text-mid);line-height:1.6;max-width:520px}
"""
        type_icons = {
            "TABLE_REMOVED":       ("🔴", "Table Removed"),
            "TABLE_ADDED":         ("🟡", "Table Added"),
            "COLUMN_REMOVED":      ("🔴", "Column Removed"),
            "COLUMN_ADDED":        ("🟡", "Column Added"),
            "ROW_COUNT_CHANGE":    ("🟢", "Row Count Change"),
            "VALUE_SET_DRIFT":     ("🟡", "New Category Values"),
            "NUMERIC_RANGE_DRIFT": ("🟡", "Numeric Range Change"),
        }
        bl = self.baseline_date
        is_first_run = "No previous run" in bl or "first run" in bl.lower()
        if "—" in bl:
            bl_files_part, bl_rest = bl.split("—", 1)
            bl_files_part = bl_files_part.strip(); bl_rest = bl_rest.strip()
        else:
            bl_files_part = "—"; bl_rest = "First run — no baseline existed yet"

        n = len(self.drift_events)
        if not self.drift_events:
            banner_cls, banner_icon = "ok", "✅"
            banner_title = "No changes detected since the last run"
            banner_sub = "Dataset structure matches the saved baseline. Power BI can be refreshed safely."
        else:
            banner_cls, banner_icon = "alert", "⚠️"
            banner_title = f"{n} change{'s' if n!=1 else ''} detected since the last run"
            high_n = sum(1 for e in self.drift_events if e.get("risk")=="HIGH")
            banner_sub = (f"{high_n} high-risk change{'s' if high_n!=1 else ''} require attention before refreshing Power BI."
                          if high_n else "Review the changes below before refreshing Power BI.")

        if self.drift_events:
            rows_html = ""
            for e in self.drift_events:
                risk = e.get("risk","LOW")
                etype = e.get("type","")
                icon, label = type_icons.get(etype, ("⚪", etype.replace("_"," ").title()))
                rows_html += (f'<tr class="risk-{risk}">'
                    f'<td><span class="risk-badge rb-{risk}">{risk}</span></td>'
                    f'<td style="font-size:12px;font-weight:600;color:var(--navy);white-space:nowrap">{icon} {label}</td>'
                    f'<td class="tname">{e.get("table","—")}</td>'
                    f'<td class="detail">{e.get("detail","")}</td></tr>')
            content_html = f"""
<div class="section" style="overflow-x:auto">
  <table class="drift-table">
    <thead><tr>
      <th style="width:80px">Risk</th><th style="width:160px">Change Type</th>
      <th style="width:150px">Table / Column</th><th>What Changed &amp; Why It Matters</th>
    </tr></thead>
    <tbody>{rows_html}</tbody>
  </table>
</div>"""
        else:
            content_html = """
<div style="padding:48px;text-align:center;background:var(--surface);border:1px solid var(--border)">
  <div style="font-size:36px;margin-bottom:12px">✅</div>
  <div style="font-size:15px;font-weight:700;color:var(--navy);margin-bottom:6px">Dataset structure is unchanged</div>
  <div style="font-size:12px;color:var(--text-muted);max-width:480px;margin:0 auto;line-height:1.6">
    All tables, columns, value sets, and numeric ranges match the saved baseline exactly.</div>
</div>"""

        html = (self._head("Schema Drift Report", css)
                + self._header("Schema Drift Report", pdf_filename=self._section_pdf_name("drift"))
                + f"""
<div class="main">
  <div><div class="page-title">Schema Drift Report</div>
  <div class="page-sub">Compares the current dataset structure against the saved baseline.</div></div>
  <div class="drift-banner {banner_cls}">
    <span class="db-icon">{banner_icon}</span>
    <div><div class="db-title">{banner_title}</div>
    <div class="db-sub">{banner_sub}</div></div>
  </div>
  <div class="bl-strip">
    <div class="bl-cell"><div class="bl-label">Baseline source files</div>
      <div class="bl-val {'warn' if is_first_run else ''}">{bl_files_part}</div></div>
    <div class="bl-cell"><div class="bl-label">Baseline saved</div>
      <div class="bl-val">{bl_rest if bl_rest else "First run — no baseline existed yet"}</div></div>
    <div class="bl-cell"><div class="bl-label">Current run</div>
      <div class="bl-val">{self.run_time}</div></div>
  </div>
  {content_html}
</div>"""
                + self._footer())
        path.write_text(html, encoding="utf-8")

    # ── CHECKS INVENTORY PAGE ─────────────────────────────────────────────────

    def _write_checks_page(self, path, actionable, observations, meta):
        css = """
/* ── Run summary bar ── */
.run-bar{display:flex;gap:0;border:1px solid var(--border);
  background:var(--border);margin-bottom:0}
.run-bar-item{background:var(--surface);padding:14px 24px;border-right:1px solid var(--border);
  flex:1}
.run-bar-item:last-child{border-right:none}
.rb-label{font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:.6px;
  color:var(--text-muted);margin-bottom:4px}
.rb-val{font-size:20px;font-weight:700;color:var(--navy);letter-spacing:-.5px;
  font-variant-numeric:tabular-nums}
.rb-val.small{font-size:13px;font-weight:600;letter-spacing:0}
/* ── Checks table ── */
.checks-table{width:100%;border-collapse:collapse;font-size:12px}
.checks-table thead tr{background:var(--navy)}
.checks-table th{padding:9px 14px;text-align:left;color:rgba(255,255,255,.75);
  font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:.6px;
  white-space:nowrap;border-right:1px solid rgba(255,255,255,.08)}
.checks-table th:last-child{border-right:none}
.checks-table td{padding:10px 14px;border-bottom:1px solid var(--border-light);
  border-right:1px solid var(--border-light);vertical-align:top}
.checks-table td:last-child{border-right:none}
.checks-table tr:nth-child(even) td{background:#FAFBFD}
.checks-table tr:hover td{background:#EBF0F9}
.checks-table tr:last-child td{border-bottom:none}
/* ── Filter toolbar — Excel-style ── */
.chk-toolbar{display:flex;align-items:center;gap:8px;padding:10px 14px;
  border-bottom:2px solid var(--border);background:var(--bg)}
.chk-toolbar label{font-size:10px;font-weight:700;text-transform:uppercase;
  letter-spacing:.5px;color:var(--navy);white-space:nowrap}
.chk-search{flex:1;min-width:160px;border:1px solid var(--border);
  padding:5px 8px;font-size:12px;font-family:inherit;color:var(--text);
  outline:none;background:var(--surface)}
.chk-search:focus{border-color:var(--navy)}
.chk-select{border:1px solid var(--border);padding:5px 8px;font-size:11px;
  font-family:inherit;color:var(--text);outline:none;background:var(--surface);
  cursor:pointer;min-width:130px}
.chk-select:focus{border-color:var(--navy)}
.chk-reset{padding:5px 12px;border:1px solid var(--border);background:var(--surface);
  font-size:11px;font-family:inherit;color:var(--text-muted);cursor:pointer}
.chk-reset:hover{border-color:var(--navy);color:var(--navy);background:#F0F3F9}
.chk-count{font-size:11px;color:var(--text-muted);margin-left:auto;white-space:nowrap}
/* ── Filter toolbar — Excel-style ── */
.chk-toolbar{display:flex;align-items:center;gap:8px;padding:10px 14px;
  border-bottom:2px solid var(--border);background:var(--bg)}
.chk-toolbar label{font-size:10px;font-weight:700;text-transform:uppercase;
  letter-spacing:.5px;color:var(--navy);white-space:nowrap}
.chk-search{flex:1;min-width:160px;border:1px solid var(--border);
  padding:5px 8px;font-size:12px;font-family:inherit;color:var(--text);
  outline:none;background:var(--surface)}
.chk-search:focus{border-color:var(--navy)}
.chk-select{border:1px solid var(--border);padding:5px 8px;font-size:11px;
  font-family:inherit;color:var(--text);outline:none;background:var(--surface);
  cursor:pointer;min-width:130px}
.chk-select:focus{border-color:var(--navy)}
.chk-reset{padding:5px 12px;border:1px solid var(--border);background:var(--surface);
  font-size:11px;font-family:inherit;color:var(--text-muted);cursor:pointer}
.chk-reset:hover{border-color:var(--navy);color:var(--navy);background:#F0F3F9}
.chk-count{font-size:11px;color:var(--text-muted);margin-left:auto;white-space:nowrap}
/* Status cell */
.st-dot{display:inline-block;width:8px;height:8px;border-radius:50%;
  vertical-align:middle;margin-right:6px;flex-shrink:0}
.st-high .st-dot{background:var(--sev-high-text)}
.st-med  .st-dot{background:var(--sev-med-text)}
.st-low  .st-dot{background:var(--sev-low-text)}
.st-obs  .st-dot{background:var(--sev-obs-text)}
.st-ok   .st-dot{background:var(--sev-low-text)}
.st-label{font-size:11px;font-weight:600;vertical-align:middle}
.st-high .st-label{color:var(--sev-high-text)}
.st-med  .st-label{color:var(--sev-med-text)}
.st-low  .st-label{color:var(--sev-low-text)}
.st-obs  .st-label{color:var(--sev-obs-text)}
.st-ok   .st-label{color:var(--sev-low-text)}
.chk-id{font-size:10px;font-weight:600;color:var(--navy);letter-spacing:.3px;white-space:nowrap}
.chk-name{font-size:12px;font-weight:600;color:var(--navy);margin-top:2px}
.chk-method{font-size:11px;color:var(--text-muted);line-height:1.5;margin-top:3px}
.cat-tag{font-size:10px;font-weight:500;color:var(--text-muted);
  text-transform:uppercase;letter-spacing:.4px}
"""
        # Build lookup: which categories found issues
        found_cats = {}
        for i in actionable + observations:
            key = i.category
            if key not in found_cats:
                found_cats[key] = {"count":0,"severity":i.severity,"obs":i.is_observation}
            found_cats[key]["count"] += 1

        if self.drift_events:
            n_d = len(self.drift_events)
            if "GOVERNANCE" not in found_cats:
                found_cats["GOVERNANCE"] = {"count": n_d, "severity": "HIGH", "obs": False}
            else:
                found_cats["GOVERNANCE"]["count"] += n_d

        def status_cell(found):
            if not found:
                return '<span class="st-ok"><span class="st-dot"></span><span class="st-label">Clean</span></span>'
            c = found["count"]
            word = "issue" if c == 1 else "issues"
            if found["obs"]:
                return f'<span class="st-obs"><span class="st-dot"></span><span class="st-label">{c} observation{"s" if c!=1 else ""}</span></span>'
            sev = found["severity"]
            cls = {"HIGH":"st-high","MEDIUM":"st-med","LOW":"st-low"}.get(sev,"st-low")
            return f'<span class="{cls}"><span class="st-dot"></span><span class="st-label">{c} {sev.lower()} {word}</span></span>'

        # Collect unique categories for the filter dropdown
        all_cats = sorted(set(c["cat"] for c in CHECKS_REGISTRY))

        table_rows = ""
        for chk in CHECKS_REGISTRY:
            found = found_cats.get(chk["cat"])
            result_type = "clean" if not found else ("obs" if found.get("obs") else found.get("severity","LOW").lower())
            table_rows += f"""<tr data-cat="{chk["cat"]}" data-result="{result_type}">
  <td><div class="chk-id">{chk["id"]}</div></td>
  <td><div class="chk-name">{chk["name"]}</div>
      <div class="chk-method">{chk["method"]}</div></td>
  <td><span class="cat-tag">{chk["cat"]}</span></td>
  <td>{status_cell(found)}</td>
  <td style="font-size:11px;color:var(--text-muted);max-width:280px">{chk["why"]}</td>
</tr>"""

        total = len(self.tables)
        total_rows_count = sum(len(df) for df in self.tables.values())
        total_cols = sum(len(df.columns) for df in self.tables.values())

        html = (self._head("Checks Inventory", css)
                + self._header("Checks Inventory", pdf_filename=self._section_pdf_name("checks"))
                + f"""
<div class="main">
  <div>
    <div class="page-title">Checks Inventory</div>
    <div class="page-sub">Every check run this session — what was looked for and what was found</div>
  </div>
  <div class="run-bar">
    <div class="run-bar-item">
      <div class="rb-label">Run at</div>
      <div class="rb-val small">{self.run_time}</div>
    </div>
    <div class="run-bar-item">
      <div class="rb-label">Tables checked</div>
      <div class="rb-val">{total}</div>
    </div>
    <div class="run-bar-item">
      <div class="rb-label">Total rows</div>
      <div class="rb-val">{total_rows_count:,}</div>
    </div>
    <div class="run-bar-item">
      <div class="rb-label">Total columns</div>
      <div class="rb-val">{total_cols}</div>
    </div>
    <div class="run-bar-item">
      <div class="rb-label">Checks run</div>
      <div class="rb-val">{len(CHECKS_REGISTRY)}</div>
    </div>
  </div>
  <div class="section">
    <div class="chk-toolbar">
      <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="#718096" stroke-width="2" style="flex-shrink:0"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>
      <input class="chk-search" id="chk-q" placeholder="Search checks…" oninput="chkRun()">
      <label>Category</label>
      <select class="chk-select" id="chk-cat" onchange="chkRun()">
        <option value="">All categories</option>
        {"".join(f'<option value="{c}">{c}</option>' for c in all_cats)}
      </select>
      <label>Result</label>
      <select class="chk-select" id="chk-res" onchange="chkRun()">
        <option value="">All results</option>
        <option value="clean">Clean only</option>
        <option value="high">High issues</option>
        <option value="medium">Medium issues</option>
        <option value="low">Low issues</option>
        <option value="obs">Observations</option>
      </select>
      <button class="chk-reset" onclick="chkReset()">Reset</button>
      <span class="chk-count" id="chk-count">{len(CHECKS_REGISTRY)} checks</span>
    </div>
    <table class="checks-table" id="chk-table">
      <thead>
        <tr>
          <th style="width:72px;white-space:nowrap">ID</th>
          <th>Check Name &amp; Method</th>
          <th style="width:100px">Category</th>
          <th style="width:140px">Result</th>
          <th>Why It Matters</th>
        </tr>
      </thead>
      <tbody>{table_rows}</tbody>
    </table>
  </div>
</div>"""
                + """
<script>
function chkRun() {
  var q   = (document.getElementById('chk-q').value   || '').toLowerCase().trim();
  var cat = (document.getElementById('chk-cat').value || '').toLowerCase();
  var res = (document.getElementById('chk-res').value || '').toLowerCase();
  var tbody = document.querySelector('#chk-table tbody');
  if (!tbody) return;
  var rows = tbody.querySelectorAll('tr');
  var visible = 0;
  rows.forEach(function(r) {
    var rCat = (r.getAttribute('data-cat') || '').toLowerCase();
    var rRes = (r.getAttribute('data-result') || '').toLowerCase();
    var txt  = r.textContent.toLowerCase();
    var catOk = (!cat || rCat === cat);
    var resOk = (!res || rRes === res);
    var qOk   = (!q   || txt.indexOf(q) !== -1);
    var show  = catOk && resOk && qOk;
    r.style.display = show ? '' : 'none';
    if (show) visible++;
  });
  var cnt = document.getElementById('chk-count');
  if (cnt) cnt.textContent = visible + ' of """ + str(len(CHECKS_REGISTRY)) + """ checks';
}
function chkReset() {
  document.getElementById('chk-q').value   = '';
  document.getElementById('chk-cat').value = '';
  document.getElementById('chk-res').value = '';
  chkRun();
}
</script>"""
                + self._footer())
        path.write_text(html, encoding="utf-8")

    # ── TEMPLATES PAGE ────────────────────────────────────────────────────────

    def _write_templates_page(self, path, meta):
        css = """
.tmpl-card{background:var(--white);border-radius:10px;padding:22px 24px;
  box-shadow:0 1px 4px rgba(0,0,0,.06);border-left:5px solid var(--gold)}
.tmpl-id{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:1px;
  color:var(--grey);margin-bottom:4px}
.tmpl-name{font-size:15px;font-weight:700;color:var(--navy);margin-bottom:10px}
.tmpl-block{margin-bottom:10px}
.tmpl-label{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;
  color:var(--grey);margin-bottom:4px}
.tmpl-text{font-size:12px;color:var(--text);line-height:1.6;
  background:var(--light);padding:10px 14px;border-radius:6px}
.tmpl-grid{display:grid;grid-template-columns:repeat(2,1fr);gap:14px}
"""
        cards_html = ""
        for chk in CHECKS_REGISTRY:
            cards_html += f"""
<div class="tmpl-card">
  <div class="tmpl-id">{chk['id']} · {chk['cat']}</div>
  <div class="tmpl-name">{chk['name']}</div>
  <div class="tmpl-block">
    <div class="tmpl-label">How it works</div>
    <div class="tmpl-text">{chk['method']}</div>
  </div>
  <div class="tmpl-block">
    <div class="tmpl-label">Why it matters</div>
    <div class="tmpl-text">{chk['why']}</div>
  </div>
</div>"""

        html = (self._head("Alert Templates", css)
                + self._header("Alert Templates & Methodology")
                + f"""
<div class="main">
  <div>
    <div class="page-title">📖 Alert Templates & Methodology</div>
    <div class="page-sub">
      Every finding in this report is generated by one of these {len(CHECKS_REGISTRY)} checks.
      No AI is involved — all text is deterministic, auditable, and generated from real statistics.
      This page documents the methodology for every check.
    </div>
  </div>
  <div class="tmpl-grid">{cards_html}</div>
</div>"""
                + self._footer())
        path.write_text(html, encoding="utf-8")


# ─────────────────────────────────────────────────────────────────────────────
# PDF REPORTER (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

class PDFReporter:
    def __init__(self, config, issues, suggestions, tables, run_time):
        self.config=config; self.issues=issues; self.suggestions=suggestions
        self.tables=tables; self.run_time=run_time

    def generate(self, output_path: str):
        try:
            from reportlab.lib.pagesizes import A4
            from reportlab.lib import colors
            from reportlab.lib.units import cm
            from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer,
                Table, TableStyle, HRFlowable, PageBreak, KeepTogether)
            from reportlab.lib.styles import ParagraphStyle
            from reportlab.lib.enums import TA_RIGHT
        except ImportError:
            print("  ⚠️  reportlab not installed. Skipping PDF."); return

        navy=colors.HexColor(NAVY); gold=colors.HexColor(GOLD)
        light=colors.HexColor(LIGHT); white=colors.HexColor(WHITE)
        red_c=colors.HexColor(RED); amb_c=colors.HexColor(AMBER)
        grn_c=colors.HexColor(GREEN); gry_c=colors.HexColor(GREY)
        blu_c=colors.HexColor(BLUE)

        doc=SimpleDocTemplate(output_path,pagesize=A4,
            leftMargin=2.2*cm,rightMargin=2.2*cm,
            topMargin=2.5*cm,bottomMargin=2.5*cm)

        H1=ParagraphStyle("H1",fontSize=22,textColor=navy,fontName="Helvetica-Bold",spaceAfter=4)
        H2=ParagraphStyle("H2",fontSize=13,textColor=gold,fontName="Helvetica-Bold",spaceBefore=18,spaceAfter=6)
        H2B=ParagraphStyle("H2B",fontSize=13,textColor=blu_c,fontName="Helvetica-Bold",spaceBefore=18,spaceAfter=6)
        H3=ParagraphStyle("H3",fontSize=10,textColor=navy,fontName="Helvetica-Bold",spaceBefore=10,spaceAfter=4)
        BODY=ParagraphStyle("BODY",fontSize=9,textColor=colors.HexColor("#2D3748"),leading=14,spaceAfter=4)
        SMALL=ParagraphStyle("SMALL",fontSize=8,textColor=gry_c,leading=12)
        CODE=ParagraphStyle("CODE",fontSize=8,fontName="Courier",textColor=navy,
                             leading=12,backColor=light,borderPadding=4)
        META=ParagraphStyle("META",fontSize=8,textColor=gry_c,alignment=TA_RIGHT)

        story=[]; W=16.6*cm
        actionable=[i for i in self.issues if not i.is_observation]
        observations=[i for i in self.issues if i.is_observation]
        meta=self.config.get("project",{})
        dataset_name=meta.get("name","").strip()

        # Cover — subtitle identifies the dataset
        subtitle_text=(f"{dataset_name} — Data Quality Report"
                       if dataset_name else "Data Quality Pipeline Report")
        story+=[Spacer(1,7*cm),HRFlowable(width=W,thickness=3,color=gold,spaceAfter=40),
                Paragraph("Graian Capital Management",H1),Spacer(1,0.3*cm),
                Paragraph(subtitle_text,
                    ParagraphStyle("SUB",fontSize=14,textColor=gry_c,fontName="Helvetica")),
                Spacer(1,0.5*cm),
                HRFlowable(width=W,thickness=1,color=colors.HexColor("#E2E8F0"),spaceAfter=12)]
        ct=Table([["Author",meta.get("author","—")],["Generated",self.run_time],
                   ["Version",meta.get("version","1.0.0")],
                   ["Tables",", ".join(self.tables.keys())],
                   ["Records",f"{sum(len(df) for df in self.tables.values()):,} rows total"]],
                  colWidths=[3.5*cm,W-3.5*cm])
        ct.setStyle(TableStyle([("FONTNAME",(0,0),(-1,-1),"Helvetica"),
            ("FONTSIZE",(0,0),(-1,-1),9),("FONTNAME",(0,0),(0,-1),"Helvetica-Bold"),
            ("TEXTCOLOR",(0,0),(0,-1),navy),("TEXTCOLOR",(1,0),(1,-1),colors.HexColor("#2D3748")),
            ("ROWBACKGROUNDS",(0,0),(-1,-1),[white,light]),
            ("TOPPADDING",(0,0),(-1,-1),6),("BOTTOMPADDING",(0,0),(-1,-1),6),
            ("GRID",(0,0),(-1,-1),0.25,colors.HexColor("#E2E8F0"))]))
        story+=[ct,Spacer(1,0.5*cm),HRFlowable(width=W,thickness=3,color=gold),PageBreak()]

        # Summary
        counts={s:sum(1 for i in actionable if i.severity==s) for s in ["HIGH","MEDIUM","LOW"]}
        story.append(Paragraph("1. Issue Summary",H2))
        story.append(HRFlowable(width=W,thickness=0.5,color=colors.HexColor("#E2E8F0"),spaceAfter=8))
        sd=[["Severity","Count","Impact"],
            ["🔴 HIGH",str(counts["HIGH"]),"Breaks calculations — must fix before loading"],
            ["🟡 MEDIUM",str(counts["MEDIUM"]),"Distorts metrics — fix recommended"],
            ["🟢 LOW",str(counts["LOW"]),"Investigate — flag and monitor"],
            ["TOTAL",str(len(actionable)),""],
            ["ℹ️ Observations",str(len(observations)),"Expected behaviour — documented for traceability"]]
        st=Table(sd,colWidths=[3.5*cm,2.5*cm,W-6*cm])
        st.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,0),navy),("TEXTCOLOR",(0,0),(-1,0),white),
            ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),("FONTNAME",(0,1),(-1,-1),"Helvetica"),
            ("FONTSIZE",(0,0),(-1,-1),9),("ROWBACKGROUNDS",(0,1),(-1,-2),[white,light]),
            ("BACKGROUND",(0,-1),(-1,-1),colors.HexColor("#EFF6FF")),
            ("TEXTCOLOR",(0,-1),(-1,-1),blu_c),
            ("BACKGROUND",(0,-2),(-1,-2),colors.HexColor("#EBF4FF")),
            ("FONTNAME",(0,-2),(-1,-2),"Helvetica-Bold"),
            ("GRID",(0,0),(-1,-1),0.25,colors.HexColor("#E2E8F0")),
            ("TOPPADDING",(0,0),(-1,-1),7),("BOTTOMPADDING",(0,0),(-1,-1),7)]))
        story+=[st,Spacer(1,0.4*cm)]

        # ── Helper: build one finding header+body table pair ─────────────────
        sev_color={"HIGH":red_c,"MEDIUM":amb_c,"LOW":grn_c}
        sev_bg={"HIGH":colors.HexColor("#FEF2F2"),"MEDIUM":colors.HexColor("#FFFBEB"),
                "LOW":colors.HexColor("#F0FDF4")}

        def _finding_pair(issue):
            c=sev_color.get(issue.severity,gry_c); bg=sev_bg.get(issue.severity,light)
            t1=Table([[
                Paragraph(f"<b>{issue.severity}</b>",ParagraphStyle("S",fontSize=8,textColor=c,fontName="Helvetica-Bold")),
                Paragraph(f"<b>{issue.category}</b> — {issue.table}"
                    +(f".{issue.column}" if issue.column else ""),
                    ParagraphStyle("C",fontSize=8,textColor=navy,fontName="Helvetica-Bold")),
                Paragraph(str(issue.affected)+" records",SMALL)]],
                colWidths=[2*cm,10*cm,4.6*cm])
            t1.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,-1),bg),
                ("TOPPADDING",(0,0),(-1,-1),5),("BOTTOMPADDING",(0,0),(-1,-1),5),
                ("LEFTPADDING",(0,0),(-1,-1),6),
                ("GRID",(0,0),(-1,-1),0.25,colors.HexColor("#E2E8F0"))]))
            t2=Table([[Paragraph(issue.explanation or issue.suggestion,SMALL)]],colWidths=[W])
            t2.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,-1),light),
                ("LEFTPADDING",(0,0),(-1,-1),8),("TOPPADDING",(0,0),(-1,-1),4),
                ("BOTTOMPADDING",(0,0),(-1,-1),6),
                ("GRID",(0,0),(-1,-1),0.25,colors.HexColor("#E2E8F0"))]))
            return t1, t2

        # ── 2. Actionable Findings — new page ────────────────────────────────
        story.append(PageBreak())
        findings_hdr=[
            Paragraph("2. Actionable Findings",H2),
            HRFlowable(width=W,thickness=0.5,color=colors.HexColor("#E2E8F0"),spaceAfter=8)]
        if not actionable:
            story+=findings_hdr+[Paragraph("No actionable issues found.",BODY)]
        else:
            t1,t2=_finding_pair(actionable[0])
            story.append(KeepTogether(findings_hdr+[t1,t2]))
            story.append(Spacer(1,0.2*cm))
            for issue in actionable[1:]:
                t1,t2=_finding_pair(issue)
                story+=[KeepTogether([t1,t2]),Spacer(1,0.2*cm)]

        # ── Helper: build one observation header+body table pair ──────────────
        obs_bg=colors.HexColor("#EFF6FF")

        def _obs_pair(obs):
            t1=Table([[
                Paragraph(f"<b>{obs.category}</b>",ParagraphStyle("OC",fontSize=8,textColor=blu_c,fontName="Helvetica-Bold")),
                Paragraph(f"<b>{obs.table}</b>"+(f".{obs.column}" if obs.column else ""),
                    ParagraphStyle("OT",fontSize=8,textColor=navy,fontName="Helvetica-Bold")),
                Paragraph("Observation",SMALL)]],colWidths=[3*cm,9*cm,4.6*cm])
            t1.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,-1),obs_bg),
                ("TOPPADDING",(0,0),(-1,-1),5),("BOTTOMPADDING",(0,0),(-1,-1),5),
                ("LEFTPADDING",(0,0),(-1,-1),6),
                ("GRID",(0,0),(-1,-1),0.25,colors.HexColor("#E2E8F0"))]))
            t2=Table([[Paragraph((obs.explanation or obs.description)+" — "+obs.suggestion,SMALL)]],colWidths=[W])
            t2.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,-1),light),
                ("LEFTPADDING",(0,0),(-1,-1),8),("TOPPADDING",(0,0),(-1,-1),4),
                ("BOTTOMPADDING",(0,0),(-1,-1),6),
                ("GRID",(0,0),(-1,-1),0.25,colors.HexColor("#E2E8F0"))]))
            return t1, t2

        # ── 3. Observations — new page ────────────────────────────────────────
        story.append(PageBreak())
        obs_hdr=[
            Paragraph("3. Observations",H2B),
            HRFlowable(width=W,thickness=0.5,color=colors.HexColor("#E2E8F0"),spaceAfter=8),
            Paragraph("Expected behaviour — documented for traceability.",BODY),
            Spacer(1,0.2*cm)]
        if not observations:
            story+=obs_hdr+[Paragraph("No observations.",BODY)]
        else:
            t1,t2=_obs_pair(observations[0])
            story.append(KeepTogether(obs_hdr+[t1,t2]))
            story.append(Spacer(1,0.15*cm))
            for obs in observations[1:]:
                t1,t2=_obs_pair(obs)
                story+=[KeepTogether([t1,t2]),Spacer(1,0.15*cm)]

        story.append(PageBreak())
        story.append(Paragraph("4. Data Source Inventory",H2))
        src_cfg=self.config.get("sources",{})
        inv=[["Table","Rows","Columns","Source"]]
        for name,df in self.tables.items():
            src=src_cfg.get(name,{})
            inv.append([name,str(len(df)),str(len(df.columns)),
                        Path(src.get("file","—")).name])
        it=Table(inv,colWidths=[4*cm,2*cm,2.5*cm,W-8.5*cm])
        it.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,0),navy),("TEXTCOLOR",(0,0),(-1,0),white),
            ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),("FONTNAME",(0,1),(-1,-1),"Helvetica"),
            ("FONTSIZE",(0,0),(-1,-1),8),("ROWBACKGROUNDS",(0,1),(-1,-1),[white,light]),
            ("GRID",(0,0),(-1,-1),0.25,colors.HexColor("#E2E8F0")),
            ("TOPPADDING",(0,0),(-1,-1),6),("BOTTOMPADDING",(0,0),(-1,-1),6)]))
        story+=[it,Spacer(1,1*cm),HRFlowable(width=W,thickness=0.5,color=gold),
                Spacer(1,0.2*cm),Paragraph(
                    f"Graian Data Quality Pipeline v{meta.get('version','1.0.0')} "
                    f"· {self.run_time} · {meta.get('author','—')}",META)]
        doc.build(story)
        print(f"  ✅ PDF generated: {output_path}")

    def generate_section(self, output_path: str, section_title: str,
                         items: list, is_obs: bool = False):
        """Generate a standalone PDF for one section (findings or observations)."""
        try:
            from reportlab.lib.pagesizes import A4
            from reportlab.lib import colors
            from reportlab.lib.units import cm
            from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer,
                Table, TableStyle, HRFlowable, KeepTogether)
            from reportlab.lib.styles import ParagraphStyle
            from reportlab.lib.enums import TA_RIGHT
        except ImportError:
            print("  ⚠️  reportlab not installed. Skipping section PDF."); return

        navy=colors.HexColor(NAVY); gold=colors.HexColor(GOLD)
        light=colors.HexColor(LIGHT); white=colors.HexColor(WHITE)
        red_c=colors.HexColor(RED); amb_c=colors.HexColor(AMBER)
        grn_c=colors.HexColor(GREEN); gry_c=colors.HexColor(GREY)
        blu_c=colors.HexColor(BLUE)

        doc=SimpleDocTemplate(output_path,pagesize=A4,
            leftMargin=2.2*cm,rightMargin=2.2*cm,
            topMargin=2.5*cm,bottomMargin=2.5*cm)
        W=16.6*cm
        meta=self.config.get("project",{})
        dataset_name=meta.get("name","").strip()

        BODY=ParagraphStyle("BODY",fontSize=9,textColor=colors.HexColor("#2D3748"),leading=14,spaceAfter=4)
        SMALL=ParagraphStyle("SMALL",fontSize=8,textColor=gry_c,leading=12)
        META=ParagraphStyle("META",fontSize=8,textColor=gry_c,alignment=TA_RIGHT)
        TITLE=ParagraphStyle("TITLE",fontSize=18,textColor=navy,fontName="Helvetica-Bold",spaceAfter=4)
        SUBTITLE=ParagraphStyle("SUB",fontSize=11,textColor=gry_c,fontName="Helvetica",spaceAfter=4)
        INFO=ParagraphStyle("INFO",fontSize=9,textColor=colors.HexColor("#4A5568"),leading=16,spaceAfter=4)

        story=[
            HRFlowable(width=W,thickness=3,color=gold,spaceAfter=16),
            Paragraph("Graian Capital Management",TITLE),
            Spacer(1,0.1*cm),
            Paragraph(section_title,SUBTITLE),
            Spacer(1,0.25*cm),
        ]
        if dataset_name:
            story.append(Paragraph(f"Dataset: {dataset_name}",INFO))
        story.append(Paragraph(f"Generated: {self.run_time}",INFO))
        if not is_obs:
            story.append(Paragraph(f"Findings in this section: {len(items)}",INFO))
        story+=[
            Spacer(1,0.25*cm),
            HRFlowable(width=W,thickness=1,color=colors.HexColor("#E2E8F0"),spaceAfter=14),
        ]

        if not items:
            story.append(Paragraph(f"No {section_title.lower()} found in this dataset.",BODY))
        elif is_obs:
            obs_bg=colors.HexColor("#EFF6FF")
            for obs in items:
                t1=Table([[
                    Paragraph(f"<b>{obs.category}</b>",ParagraphStyle("OC",fontSize=8,textColor=blu_c,fontName="Helvetica-Bold")),
                    Paragraph(f"<b>{obs.table}</b>"+(f".{obs.column}" if obs.column else ""),
                        ParagraphStyle("OT",fontSize=8,textColor=navy,fontName="Helvetica-Bold")),
                    Paragraph("Observation",SMALL)]],colWidths=[3*cm,9*cm,4.6*cm])
                t1.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,-1),obs_bg),
                    ("TOPPADDING",(0,0),(-1,-1),5),("BOTTOMPADDING",(0,0),(-1,-1),5),
                    ("LEFTPADDING",(0,0),(-1,-1),6),
                    ("GRID",(0,0),(-1,-1),0.25,colors.HexColor("#E2E8F0"))]))
                t2=Table([[Paragraph((obs.explanation or obs.description)+" — "+obs.suggestion,SMALL)]],colWidths=[W])
                t2.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,-1),light),
                    ("LEFTPADDING",(0,0),(-1,-1),8),("TOPPADDING",(0,0),(-1,-1),4),
                    ("BOTTOMPADDING",(0,0),(-1,-1),6),
                    ("GRID",(0,0),(-1,-1),0.25,colors.HexColor("#E2E8F0"))]))
                story+=[KeepTogether([t1,t2]),Spacer(1,0.15*cm)]
        else:
            sev_color={"HIGH":red_c,"MEDIUM":amb_c,"LOW":grn_c}
            sev_bg={"HIGH":colors.HexColor("#FEF2F2"),"MEDIUM":colors.HexColor("#FFFBEB"),"LOW":colors.HexColor("#F0FDF4")}
            EXPL=ParagraphStyle("EXPL",fontSize=8,textColor=colors.HexColor("#2D3748"),leading=13,spaceAfter=0)
            ACT_LABEL=ParagraphStyle("ACT_L",fontSize=8,textColor=navy,fontName="Helvetica-Bold",spaceAfter=0)
            ACT_BODY=ParagraphStyle("ACT_B",fontSize=8,textColor=colors.HexColor("#2D3748"),leading=13,spaceAfter=0)
            SNIP_HDR=ParagraphStyle("SH",fontSize=7.5,textColor=white,fontName="Helvetica-Bold")
            SNIP_CEL=ParagraphStyle("SC",fontSize=7.5,textColor=colors.HexColor("#2D3748"),leading=11)
            SNIP_FLAG=ParagraphStyle("SF",fontSize=7.5,textColor=red_c,fontName="Helvetica-Bold",leading=11)

            for issue in items:
                c=sev_color.get(issue.severity,gry_c); bg=sev_bg.get(issue.severity,light)

                # ── 1. Header row ─────────────────────────────────────────────
                t1=Table([[
                    Paragraph(f"<b>{issue.severity}</b>",ParagraphStyle("S",fontSize=8,textColor=c,fontName="Helvetica-Bold")),
                    Paragraph(f"<b>{issue.category}</b> — {issue.table}"+(f".{issue.column}" if issue.column else ""),
                        ParagraphStyle("C",fontSize=8,textColor=navy,fontName="Helvetica-Bold")),
                    Paragraph(str(issue.affected)+" records",SMALL)]],colWidths=[2*cm,10*cm,4.6*cm])
                t1.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,-1),bg),
                    ("TOPPADDING",(0,0),(-1,-1),5),("BOTTOMPADDING",(0,0),(-1,-1),5),
                    ("LEFTPADDING",(0,0),(-1,-1),6),
                    ("GRID",(0,0),(-1,-1),0.25,colors.HexColor("#E2E8F0"))]))

                # ── 2. Explanation paragraph ──────────────────────────────────
                expl_text = issue.explanation or issue.description
                t2=Table([[Paragraph(expl_text, EXPL)]],colWidths=[W])
                t2.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,-1),light),
                    ("LEFTPADDING",(0,0),(-1,-1),10),("RIGHTPADDING",(0,0),(-1,-1),10),
                    ("TOPPADDING",(0,0),(-1,-1),7),("BOTTOMPADDING",(0,0),(-1,-1),7),
                    ("GRID",(0,0),(-1,-1),0.25,colors.HexColor("#E2E8F0"))]))

                block=[KeepTogether([t1,t2])]

                # ── 3. Sample rows table ──────────────────────────────────────
                rows=getattr(issue,"snippet_rows",[])
                if rows:
                    highlight_col=rows[0].get("__highlight__","")
                    # Build column list: skip internal keys, put highlighted col first
                    all_cols=[k for k in rows[0].keys()
                               if not k.startswith("__")]
                    display_cols=([highlight_col]+[c for c in all_cols if c!=highlight_col]
                                   if highlight_col in all_cols else all_cols)
                    # Limit to 6 cols max to fit the page
                    display_cols=display_cols[:6]
                    col_w=W/len(display_cols)

                    hdr_row=[Paragraph(col.upper(),SNIP_HDR) for col in display_cols]
                    tbl_rows=[hdr_row]
                    for r in rows:
                        def _fmt(v):
                            if v is None: return "—"
                            if isinstance(v,float): return f"{v:,.6f}".rstrip("0").rstrip(".")
                            return str(v)
                        data_row=[]
                        for col in display_cols:
                            val=_fmt(r.get(col))
                            style=SNIP_FLAG if col==highlight_col else SNIP_CEL
                            data_row.append(Paragraph(val,style))
                        tbl_rows.append(data_row)

                    snip_tbl=Table(tbl_rows,colWidths=[col_w]*len(display_cols),repeatRows=1)
                    ts=[("BACKGROUND",(0,0),(-1,0),colors.HexColor("#2D3748")),
                        ("FONTNAME",(0,1),(-1,-1),"Helvetica"),
                        ("FONTSIZE",(0,0),(-1,-1),7.5),
                        ("ROWBACKGROUNDS",(0,1),(-1,-1),[white,colors.HexColor("#FFF8F8")]),
                        ("GRID",(0,0),(-1,-1),0.25,colors.HexColor("#E2E8F0")),
                        ("TOPPADDING",(0,0),(-1,-1),4),("BOTTOMPADDING",(0,0),(-1,-1),4),
                        ("LEFTPADDING",(0,0),(-1,-1),5),("VALIGN",(0,0),(-1,-1),"MIDDLE")]
                    snip_tbl.setStyle(TableStyle(ts))
                    lbl=Table([[Paragraph(
                        f"SAMPLE ROWS WHERE THIS ISSUE WAS FOUND (MAX {len(rows)} SHOWN)",
                        ParagraphStyle("LBL",fontSize=7,textColor=gry_c,fontName="Helvetica-Bold")
                    )]],colWidths=[W])
                    lbl.setStyle(TableStyle([
                        ("TOPPADDING",(0,0),(-1,-1),6),("BOTTOMPADDING",(0,0),(-1,-1),2),
                        ("LEFTPADDING",(0,0),(-1,-1),0)]))
                    block+=[lbl,snip_tbl]

                # ── 4. Suggested action ───────────────────────────────────────
                if issue.suggestion:
                    act=Table([[
                        Paragraph("Suggested action:", ACT_LABEL),
                        Paragraph(issue.suggestion, ACT_BODY)
                    ]],colWidths=[3.2*cm,W-3.2*cm])
                    act.setStyle(TableStyle([
                        ("BACKGROUND",(0,0),(-1,-1),colors.HexColor("#FFFDF5")),
                        ("TOPPADDING",(0,0),(-1,-1),6),("BOTTOMPADDING",(0,0),(-1,-1),6),
                        ("LEFTPADDING",(0,0),(-1,-1),8),
                        ("GRID",(0,0),(-1,-1),0.25,colors.HexColor("#E8DDB8")),
                        ("VALIGN",(0,0),(-1,-1),"TOP")]))
                    block.append(act)

                story+=block+[Spacer(1,0.35*cm)]

        story+=[Spacer(1,1*cm),HRFlowable(width=W,thickness=0.5,color=gold),Spacer(1,0.2*cm),
                Paragraph(f"Graian Data Quality Pipeline v{meta.get('version','1.0.0')} "
                          f"· {self.run_time} · {meta.get('author','—')}",META)]
        doc.build(story)
        print(f"  ✅ Section PDF: {output_path}")

    def generate_checks_pdf(self, output_path: str, actionable: list, observations: list):
        """Generate a full Checks Inventory PDF with all 20 checks and their results."""
        try:
            from reportlab.lib.pagesizes import A4
            from reportlab.lib import colors
            from reportlab.lib.units import cm
            from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer,
                Table, TableStyle, HRFlowable)
            from reportlab.lib.styles import ParagraphStyle
            from reportlab.lib.enums import TA_RIGHT, TA_CENTER
        except ImportError:
            print("  ⚠️  reportlab not installed. Skipping checks PDF."); return

        navy=colors.HexColor(NAVY); gold=colors.HexColor(GOLD)
        light=colors.HexColor(LIGHT); white=colors.HexColor(WHITE)
        red_c=colors.HexColor("#7B1E1E"); amb_c=colors.HexColor("#7A4D0A")
        grn_c=colors.HexColor("#144D2E"); blu_c=colors.HexColor("#1E3A5F")
        gry_c=colors.HexColor(GREY)

        from reportlab.lib.pagesizes import landscape
        doc=SimpleDocTemplate(output_path,pagesize=landscape(A4),
            leftMargin=1.8*cm,rightMargin=1.8*cm,topMargin=1.8*cm,bottomMargin=1.8*cm)
        W=25.3*cm   # A4 landscape 297mm − 2×18mm margins
        meta=self.config.get("project",{})
        dataset_name=meta.get("name","").strip()

        H1=ParagraphStyle("H1",fontSize=18,textColor=navy,fontName="Helvetica-Bold",spaceAfter=4)
        SUB=ParagraphStyle("SUB",fontSize=11,textColor=gry_c,fontName="Helvetica",spaceAfter=4)
        INFO=ParagraphStyle("INFO",fontSize=9,textColor=colors.HexColor("#4A5568"),leading=16,spaceAfter=4)
        SMALL=ParagraphStyle("SMALL",fontSize=7.5,textColor=gry_c,leading=11)
        META=ParagraphStyle("META",fontSize=8,textColor=gry_c,alignment=TA_RIGHT)
        CAT=ParagraphStyle("CAT",fontSize=8,textColor=gry_c,fontName="Helvetica-Bold")

        # Build found_cats lookup
        found_cats = {}
        for i in actionable + observations:
            key = i.category
            if key not in found_cats:
                found_cats[key] = {"count":0,"severity":i.severity,"obs":i.is_observation}
            found_cats[key]["count"] += 1

        def status_cell_text(found):
            if not found: return ("Clean", grn_c)
            c = found["count"]
            word = "issue" if c == 1 else "issues"
            if found["obs"]:
                return (f"{c} observation{'s' if c!=1 else ''}", blu_c)
            sev = found["severity"]
            col = {"HIGH":red_c,"MEDIUM":amb_c,"LOW":grn_c}.get(sev, grn_c)
            return (f"{c} {sev.lower()} {word}", col)

        story=[
            HRFlowable(width=W,thickness=3,color=gold,spaceAfter=16),
            Paragraph("Graian Capital Management",H1),
            Spacer(1,0.1*cm),
            Paragraph("Checks Inventory",SUB),
            Spacer(1,0.25*cm),
        ]
        if dataset_name:
            story.append(Paragraph(f"Dataset: {dataset_name}",INFO))
        story+=[
            Paragraph(f"Generated: {self.run_time}",INFO),
            Paragraph(f"20 checks run across {len(self.tables)} tables · "
                      f"{sum(len(df) for df in self.tables.values()):,} total rows",INFO),
            Spacer(1,0.25*cm),
            HRFlowable(width=W,thickness=1,color=colors.HexColor("#E2E8F0"),spaceAfter=14),
        ]

        # Table — landscape gives 25.3cm, plenty of room for every category word
        hdr_style = ParagraphStyle("H",fontSize=8,textColor=white,fontName="Helvetica-Bold")
        def hdr(t): return Paragraph(t, hdr_style)
        def bdy(t, color=None):
            st = ParagraphStyle("B",fontSize=8,textColor=color or colors.HexColor("#2D3748"),leading=11)
            return Paragraph(t, st)
        def cat(t):
            # Wide column + no word split = guaranteed single-word-per-line display
            st = ParagraphStyle("CT",fontSize=8,textColor=gry_c,leading=11,fontName="Helvetica-Bold")
            return Paragraph(t, st)

        # ID=1.7  Name=6.5  Category=3.8  Result=3.2  Why=10.1  → total 25.3cm
        col_widths = [1.7*cm, 6.5*cm, 3.8*cm, 3.2*cm, 10.1*cm]
        rows = [[hdr("ID"), hdr("Check Name & Method"), hdr("Category"),
                 hdr("Result"), hdr("Why It Matters")]]

        for chk in CHECKS_REGISTRY:
            found = found_cats.get(chk["cat"])
            status_text, status_color = status_cell_text(found)
            rows.append([
                bdy(chk["id"], colors.HexColor(NAVY)),
                Paragraph(f"<b>{chk['name']}</b><br/><font size='7' color='#718096'>{chk['method']}</font>",
                    ParagraphStyle("NM",fontSize=8,leading=11,textColor=colors.HexColor("#1A202C"))),
                cat(chk["cat"]),
                bdy(status_text, status_color),
                bdy(chk["why"]),
            ])

        tbl = Table(rows, colWidths=col_widths, repeatRows=1)
        tbl.setStyle(TableStyle([
            ("BACKGROUND",(0,0),(-1,0),navy),
            ("FONTNAME",(0,1),(-1,-1),"Helvetica"),
            ("FONTSIZE",(0,0),(-1,-1),8),
            ("ROWBACKGROUNDS",(0,1),(-1,-1),[white,colors.HexColor(LIGHT)]),
            ("GRID",(0,0),(-1,-1),0.25,colors.HexColor("#E2E8F0")),
            ("TOPPADDING",(0,0),(-1,-1),5),("BOTTOMPADDING",(0,0),(-1,-1),5),
            ("LEFTPADDING",(0,0),(-1,-1),6),("VALIGN",(0,0),(-1,-1),"TOP"),
        ]))
        story.append(tbl)
        story+=[Spacer(1,0.8*cm),HRFlowable(width=W,thickness=0.5,color=gold),Spacer(1,0.2*cm),
                Paragraph(f"Graian Data Quality Pipeline v{meta.get('version','1.0.0')} "
                          f"· {self.run_time} · {meta.get('author','—')}",META)]
        doc.build(story)
        print(f"  ✅ Checks PDF: {output_path}")

    def generate_drift_pdf(self, output_path: str, drift_events: list, baseline_date: str):
        """Generate a full Schema Drift PDF with the comparison table."""
        try:
            from reportlab.lib.pagesizes import A4
            from reportlab.lib import colors
            from reportlab.lib.units import cm
            from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer,
                Table, TableStyle, HRFlowable)
            from reportlab.lib.styles import ParagraphStyle
            from reportlab.lib.enums import TA_RIGHT
        except ImportError:
            print("  ⚠️  reportlab not installed. Skipping drift PDF."); return

        navy=colors.HexColor(NAVY); gold=colors.HexColor(GOLD)
        light=colors.HexColor(LIGHT); white=colors.HexColor(WHITE)
        red_c=colors.HexColor("#7B1E1E"); amb_c=colors.HexColor("#7A4D0A")
        grn_c=colors.HexColor("#144D2E"); gry_c=colors.HexColor(GREY)

        doc=SimpleDocTemplate(output_path,pagesize=A4,
            leftMargin=1.8*cm,rightMargin=1.8*cm,topMargin=2*cm,bottomMargin=2*cm)
        W=17.4*cm
        meta=self.config.get("project",{})
        dataset_name=meta.get("name","").strip()

        H1=ParagraphStyle("H1",fontSize=18,textColor=navy,fontName="Helvetica-Bold",spaceAfter=4)
        SUB=ParagraphStyle("SUB",fontSize=11,textColor=gry_c,fontName="Helvetica",spaceAfter=4)
        INFO=ParagraphStyle("INFO",fontSize=9,textColor=colors.HexColor("#4A5568"),leading=16,spaceAfter=4)
        BODY=ParagraphStyle("BODY",fontSize=9,textColor=colors.HexColor("#2D3748"),leading=14,spaceAfter=4)
        META=ParagraphStyle("META",fontSize=8,textColor=gry_c,alignment=TA_RIGHT)

        # Parse baseline string
        bl = baseline_date or ""
        if "—" in bl:
            bl_files, bl_rest = bl.split("—",1)
            bl_files = bl_files.strip(); bl_rest = bl_rest.strip()
        else:
            bl_files = "—"; bl_rest = "First run — no baseline existed yet"

        story=[
            HRFlowable(width=W,thickness=3,color=gold,spaceAfter=16),
            Paragraph("Graian Capital Management",H1),
            Spacer(1,0.1*cm),
            Paragraph("Schema Drift Report",SUB),
            Spacer(1,0.25*cm),
        ]
        if dataset_name:
            story.append(Paragraph(f"Dataset: {dataset_name}",INFO))
        story+=[
            Paragraph(f"Generated: {self.run_time}",INFO),
            Spacer(1,0.25*cm),
            HRFlowable(width=W,thickness=1,color=colors.HexColor("#E2E8F0"),spaceAfter=14),
        ]

        # Baseline info strip
        n = len(drift_events)
        status_text = (f"{n} change{'s' if n!=1 else ''} detected since the last run"
                       if drift_events else "No changes detected since the last run")
        status_color = colors.HexColor("#7A4D0A") if drift_events else colors.HexColor("#144D2E")
        status_bg = colors.HexColor("#FFFBEB") if drift_events else colors.HexColor("#F0FDF4")
        banner = Table([[Paragraph(
            ("⚠️  " if drift_events else "✅  ") + status_text,
            ParagraphStyle("BN",fontSize=10,textColor=status_color,fontName="Helvetica-Bold",leading=14)
        )]],colWidths=[W])
        banner.setStyle(TableStyle([
            ("BACKGROUND",(0,0),(-1,-1),status_bg),
            ("TOPPADDING",(0,0),(-1,-1),8),("BOTTOMPADDING",(0,0),(-1,-1),8),
            ("LEFTPADDING",(0,0),(-1,-1),10),
            ("GRID",(0,0),(-1,-1),0.25,colors.HexColor("#E2E8F0")),
        ]))
        story+=[banner, Spacer(1,0.15*cm)]

        # Baseline info table
        info_rows = [
            ["Baseline source files", bl_files],
            ["Baseline saved", bl_rest if bl_rest else "First run — no baseline existed yet"],
            ["Current run", self.run_time],
        ]
        info_tbl = Table(info_rows, colWidths=[4.5*cm, W-4.5*cm])
        info_tbl.setStyle(TableStyle([
            ("FONTNAME",(0,0),(0,-1),"Helvetica-Bold"),("FONTNAME",(1,0),(1,-1),"Helvetica"),
            ("FONTSIZE",(0,0),(-1,-1),8),("TEXTCOLOR",(0,0),(0,-1),navy),
            ("ROWBACKGROUNDS",(0,0),(-1,-1),[light,white]),
            ("TOPPADDING",(0,0),(-1,-1),5),("BOTTOMPADDING",(0,0),(-1,-1),5),
            ("LEFTPADDING",(0,0),(-1,-1),8),
            ("GRID",(0,0),(-1,-1),0.25,colors.HexColor("#E2E8F0")),
        ]))
        story+=[info_tbl, Spacer(1,0.3*cm)]

        if not drift_events:
            no_drift = Table([[Paragraph(
                "All tables, columns, value sets, and numeric ranges match the saved baseline exactly. "
                "Power BI can be refreshed with confidence.",
                ParagraphStyle("ND",fontSize=9,textColor=colors.HexColor("#14532D"),leading=14)
            )]],colWidths=[W])
            no_drift.setStyle(TableStyle([
                ("BACKGROUND",(0,0),(-1,-1),colors.HexColor("#F0FDF4")),
                ("TOPPADDING",(0,0),(-1,-1),10),("BOTTOMPADDING",(0,0),(-1,-1),10),
                ("LEFTPADDING",(0,0),(-1,-1),12),
                ("GRID",(0,0),(-1,-1),0.25,colors.HexColor("#A8C8B8")),
            ]))
            story.append(no_drift)
        else:
            type_labels = {
                "TABLE_REMOVED":       "Table Removed",
                "TABLE_ADDED":         "Table Added",
                "COLUMN_REMOVED":      "Column Removed",
                "COLUMN_ADDED":        "Column Added",
                "ROW_COUNT_CHANGE":    "Row Count Change",
                "VALUE_SET_DRIFT":     "New Category Values",
                "NUMERIC_RANGE_DRIFT": "Numeric Range Change",
            }
            risk_colors = {"HIGH":red_c,"MEDIUM":amb_c,"LOW":grn_c}
            hdr_style=ParagraphStyle("H",fontSize=8,textColor=white,fontName="Helvetica-Bold")
            def hdr(t): return Paragraph(t,hdr_style)
            def bdy(t,c=None):
                return Paragraph(t,ParagraphStyle("B",fontSize=8,textColor=c or colors.HexColor("#2D3748"),leading=11))

            col_widths=[1.8*cm,3.2*cm,3.2*cm,W-8.2*cm]
            rows=[[hdr("Risk"),hdr("Change Type"),hdr("Table / Column"),hdr("What Changed & Why It Matters")]]
            row_bgs=[]
            for e in drift_events:
                risk=e.get("risk","LOW")
                rc=risk_colors.get(risk,grn_c)
                bg={"HIGH":colors.HexColor("#FEF8F8"),"MEDIUM":colors.HexColor("#FFFDF0"),
                    "LOW":colors.HexColor("#F5FDF8")}.get(risk,white)
                row_bgs.append(bg)
                rows.append([
                    bdy(risk,rc),
                    bdy(type_labels.get(e.get("type",""),e.get("type",""))),
                    bdy(e.get("table","—"),navy),
                    bdy(e.get("detail","")),
                ])
            tbl=Table(rows,colWidths=col_widths,repeatRows=1)
            ts=[("BACKGROUND",(0,0),(-1,0),navy),
                ("FONTNAME",(0,1),(-1,-1),"Helvetica"),
                ("FONTSIZE",(0,0),(-1,-1),8),
                ("GRID",(0,0),(-1,-1),0.25,colors.HexColor("#E2E8F0")),
                ("TOPPADDING",(0,0),(-1,-1),5),("BOTTOMPADDING",(0,0),(-1,-1),5),
                ("LEFTPADDING",(0,0),(-1,-1),6),("VALIGN",(0,0),(-1,-1),"TOP")]
            for i,bg in enumerate(row_bgs,1):
                ts.append(("BACKGROUND",(0,i),(-1,i),bg))
            tbl.setStyle(TableStyle(ts))
            story.append(tbl)

        story+=[Spacer(1,0.8*cm),HRFlowable(width=W,thickness=0.5,color=gold),Spacer(1,0.2*cm),
                Paragraph(f"Graian Data Quality Pipeline v{meta.get('version','1.0.0')} "
                          f"· {self.run_time} · {meta.get('author','—')}",META)]
        doc.build(story)
        print(f"  ✅ Drift PDF: {output_path}")


# ─────────────────────────────────────────────────────────────────────────────
# COMBINED REPORTER
# ─────────────────────────────────────────────────────────────────────────────

class Reporter:
    def __init__(self, config, issues, observations, suggestions, tables):
        self.run_time     = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.cli          = CLIReporter()
        self.all_issues   = issues + observations
        self.pdf          = PDFReporter(config, issues+observations,
                                        suggestions, tables, self.run_time)
        self.config       = config
        self.issues       = issues
        self.observations = observations
        self.suggestions  = suggestions
        self.tables       = tables

    def print_header(self):       self.cli.print_header(self.config, self.run_time)
    def print_ingestion(self):    self.cli.print_ingestion(self.tables)
    def print_drift(self, ev):    self.cli.print_drift(ev)
    def print_issues(self):       self.cli.print_issues(self.all_issues)
    def print_observations(self): self.cli.print_observations(self.all_issues)
    def print_remediation(self):  self.cli.print_remediation(self.all_issues)
    def print_outputs(self, p):   self.cli.print_outputs(p)
    def print_suggestions(self):  self.cli.print_suggestions(self.suggestions)
    def generate_pdf(self, path): self.pdf.generate(path)

    def generate_html(self, path: str, drift_events=None, baseline_date=None):
        bl = baseline_date or "No previous run found"
        rep = HTMLReporter(
            self.config, self.all_issues, self.suggestions,
            self.tables, self.run_time,
            drift_events=drift_events or [],
            baseline_date=bl)
        rep.generate(path)
        self._pdf_filename = rep._pdf_name()
        pages = ["quality_report.html","report_high.html","report_medium.html",
                 "report_low.html","report_observations.html","report_drift.html",
                 "report_checks.html","report_templates.html"]
        print(f"  ✅ HTML report: {path} ({len(pages)} pages generated)")