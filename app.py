"""
GRAIAN CAPITAL MANAGEMENT — Data Quality Pipeline Web Interface
Run: python app.py   →   http://localhost:5000
"""

import json
import uuid
import warnings
from datetime import datetime
from pathlib import Path

import pandas as pd

warnings.filterwarnings("ignore")

from flask import Flask, render_template, request, redirect, url_for, send_file

# ── Pipeline imports — all from data_quality package ─────────────────────────
from data_quality import (
    SchemaProfiler, DataQualityChecker, Reporter, SuggestionEngine
)
from data_quality.governance import SchemaDriftDetector, DataDictionaryExporter

app = Flask(__name__)
app.secret_key = "graian-dq-2024"

SESSIONS_DIR = Path("output") / "sessions"
SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
ALLOWED_EXT      = {".xlsx", ".xls", ".csv"}
SKIP_SHEET_WORDS = {"requirement", "readme", "metadata", "info", "about", "legend"}


# ─────────────────────────────────────────────────────────────────────────────
# AUTO CONFIG — build config from uploaded files, no user input needed
# ─────────────────────────────────────────────────────────────────────────────

def auto_config(file_paths: dict) -> dict:
    sources = {}
    for filename, filepath in file_paths.items():
        ext  = Path(filename).suffix.lower()
        base = Path(filename).stem.lower().replace(" ", "_").replace("-", "_")

        if ext in (".xlsx", ".xls"):
            try:
                import openpyxl
                wb = openpyxl.load_workbook(filepath, read_only=True)
                for sheet in wb.sheetnames:
                    if any(w in sheet.lower() for w in SKIP_SHEET_WORDS):
                        continue
                    try:
                        df_t = pd.read_excel(filepath, sheet_name=sheet, nrows=3)
                        if len(df_t) < 1 or len(df_t.columns) < 2:
                            continue
                    except Exception:
                        continue
                    sname = sheet.lower().replace(" ", "_").replace("-", "_")
                    sources[sname] = {
                        "type": "excel", "file": str(filepath), "sheet": sheet
                    }
            except Exception as e:
                print(f"Warning: {filename}: {e}")

        elif ext == ".csv":
            try:
                with open(filepath, "r", encoding="utf-8-sig") as f:
                    line = f.readline()
                delim = ";" if line.count(";") > line.count(",") else ","
                sources[base] = {
                    "type": "csv", "file": str(filepath), "delimiter": delim
                }
            except Exception as e:
                print(f"Warning: {filename}: {e}")

    all_stems = [Path(fn).stem for fn in file_paths.keys()]
    project_name = all_stems[0].replace("_"," ").replace("-"," ").title() if all_stems else "Dataset"
    return {
        "project": {"name": project_name, "author": "Graian Capital Management", "version": "1.0.0"},
        "sources": sources, "relationships": [], "business_rules": [],
    }


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE RUNNER
# ─────────────────────────────────────────────────────────────────────────────

def run_pipeline(config: dict, session_dir: Path) -> dict:
    # Load tables
    tables = {}
    for name, src in config.get("sources", {}).items():
        fp = Path(src["file"])
        if not fp.exists():
            continue
        try:
            if src["type"] == "excel":
                df = pd.read_excel(fp, sheet_name=src.get("sheet", 0))
            else:
                df = pd.read_csv(fp, sep=src.get("delimiter", ","))
                df.columns = df.columns.str.strip().str.lstrip("\ufeff")
            tables[name] = df
        except Exception as e:
            print(f"Error loading {name}: {e}")

    if not tables:
        return {"error": "No tables could be loaded from the uploaded files."}

    # Profile + check
    profiler             = SchemaProfiler(tables)
    profiles, discovered = profiler.run()
    checker              = DataQualityChecker(tables, config, profiles, discovered)
    all_findings         = checker.run()
    issues       = [f for f in all_findings if not f.is_observation]
    observations = [f for f in all_findings if f.is_observation]
    suggestions  = SuggestionEngine(issues, config).generate()

    # Drift detection — reset baseline per session so stale state never contaminates a fresh upload
    _APP_DIR = Path(__file__).parent.resolve()
    baseline_path = _APP_DIR / "output" / "schema_baseline.json"
    if baseline_path.exists():
        baseline_path.unlink()
    drift_detector = SchemaDriftDetector(baseline_path)
    drift_events   = drift_detector.check(tables, config)
    drift_detector.save(tables, config)

    # Generate outputs via Reporter
    reporter = Reporter(config, issues, observations, suggestions, tables)
    reporter.generate_html(str(session_dir / "quality_report.html"),
                           drift_events=drift_events)
    pdf_name = getattr(reporter, "_pdf_filename", "quality_report.pdf")
    reporter.generate_pdf(str(session_dir / pdf_name))
    DataDictionaryExporter(profiles).export(session_dir / "data_dictionary.xlsx")

    def to_dict(i):
        return {
            "severity": i.severity, "category": i.category,
            "table": i.table, "column": i.column or "—",
            "description": i.description, "affected": i.affected,
            "suggestion": i.suggestion,
        }

    run_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return {
        "run_time":     run_time,
        "tables":       {n: {"rows": len(df), "cols": len(df.columns)}
                         for n, df in tables.items()},
        "issues":       [to_dict(i) for i in issues],
        "observations": [to_dict(o) for o in observations],
        "drift_events": drift_events,
        "counts": {
            "HIGH":         sum(1 for i in issues if i.severity == "HIGH"),
            "MEDIUM":       sum(1 for i in issues if i.severity == "MEDIUM"),
            "LOW":          sum(1 for i in issues if i.severity == "LOW"),
            "observations": len(observations),
            "drift":        len(drift_events),
        },
        "downloads": {
            "html":       "quality_report.html",
            "pdf":        pdf_name,
            "dictionary": "data_dictionary.xlsx",
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/analyze", methods=["POST"])
def analyze():
    uploaded = request.files.getlist("files")
    if not uploaded or all(f.filename == "" for f in uploaded):
        return redirect(url_for("index"))

    sid         = str(uuid.uuid4())[:8]
    session_dir = SESSIONS_DIR / sid
    session_dir.mkdir(parents=True, exist_ok=True)

    file_paths = {}
    for f in uploaded:
        if not f.filename or Path(f.filename).suffix.lower() not in ALLOWED_EXT:
            continue
        dest = session_dir / f.filename
        f.save(str(dest))
        file_paths[f.filename] = dest

    if not file_paths:
        return redirect(url_for("index"))

    config  = auto_config(file_paths)
    results = run_pipeline(config, session_dir)

    if "error" in results:
        return render_template("index.html", error=results["error"])

    (session_dir / "results.json").write_text(
        json.dumps(results, indent=2, default=str), encoding="utf-8")

    # Fix internal links in all generated HTML pages to use Flask routes
    _fix_html_links(session_dir, sid)

    return redirect(url_for("results", sid=sid))


def _fix_html_links(session_dir: Path, sid: str):
    """Replace relative HTML links with Flask-routed equivalents."""
    pages = ["quality_report.html","report_high.html","report_medium.html",
             "report_low.html","report_observations.html","report_drift.html",
             "report_checks.html","report_templates.html"]
    replacements = {f"{p}": f"/results/{sid}/{p}" for p in pages}
    # main dashboard links to sub-pages
    replacements["quality_report.html"] = f"/results/{sid}"
    for page in pages:
        fp = session_dir / page
        if not fp.exists():
            continue
        html = fp.read_text(encoding="utf-8")
        for old_link, new_link in replacements.items():
            html = html.replace(f'href="{old_link}"', f'href="{new_link}"')
        # Fix back button link on sub-pages
        html = html.replace(
            f'href="/results/{sid}/quality_report.html"',
            f'href="/results/{sid}"'
        )
        fp.write_text(html, encoding="utf-8")


@app.route("/results/<sid>")
def results(sid):
    # Serve the pipeline-generated multi-page HTML directly
    html_file = SESSIONS_DIR / sid / "quality_report.html"
    if not html_file.exists():
        return redirect(url_for("index"))
    return send_file(str(html_file), mimetype="text/html")


@app.route("/results/<sid>/<filename>")
def results_page(sid, filename):
    # Serve any of the sub-pages generated by the pipeline
    allowed = {
        "quality_report.html", "report_high.html", "report_medium.html",
        "report_low.html", "report_observations.html", "report_drift.html",
        "report_checks.html", "report_templates.html",
    }
    if filename not in allowed:
        return "Not found", 404
    fp = SESSIONS_DIR / sid / filename
    if not fp.exists():
        return "Not available", 404
    return send_file(str(fp), mimetype="text/html")


@app.route("/download/<sid>/<filename>")
def download(sid, filename):
    # Security: no path traversal
    if "/" in filename or "\\" in filename or ".." in filename:
        return "Not found", 404
    # Static allowlist for non-PDF files
    if filename in {"quality_report.html", "data_dictionary.xlsx"}:
        fp = SESSIONS_DIR / sid / filename
        if not fp.exists():
            return "Not available", 404
        return send_file(str(fp), as_attachment=True, download_name=filename)
    # Any PDF from this session — master or section-specific
    if filename.endswith(".pdf"):
        fp = SESSIONS_DIR / sid / filename
        if not fp.exists():
            # Fallback: serve the master PDF if exact file not found
            pdfs = sorted((SESSIONS_DIR / sid).glob("quality_report*.pdf"))
            if not pdfs:
                return "Not available", 404
            fp = pdfs[0]; filename = fp.name
        return send_file(str(fp), as_attachment=True, download_name=filename)
    return "Not found", 404


if __name__ == "__main__":
    print("\n  Graian Data Quality Pipeline — Web UI")
    print("  → http://localhost:5000\n")
    app.run(debug=True, port=5000)