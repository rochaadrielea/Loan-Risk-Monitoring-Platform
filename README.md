# Loan Risk Monitoring Platform

A schema-agnostic data quality pipeline and web interface for financial datasets. Profiles any tabular dataset, runs 20 deterministic quality checks, separates actionable issues from expected observations, and generates a full stakeholder report in under 5 seconds.

Built with Python, Flask, pandas, and ReportLab. No ML. No LLM. Every finding is traceable to a specific statistical calculation.

---

## What It Does

- Accepts Excel and CSV files via drag-and-drop or CLI
- Auto-discovers schema: column types, primary keys, foreign keys, relationships, and outliers — no configuration required
- Runs 20 quality checks across completeness, integrity, outliers, referential integrity, temporal alignment, and structure
- Separates issues that require action from observations that document expected behaviour
- Validates uploads against a hardcoded expected schema — flags missing tables or columns before any downstream tool loads the data
- Generates 5 outputs automatically: HTML report, per-severity PDFs, Excel checklist with Power Query steps, data dictionary, and cleaned files

---

## Architecture

```
run_quality_check.py          CLI entry point
app.py                        Flask web interface (localhost:5000)
data_quality/
    profiler.py               Schema auto-discovery engine
    checks.py                 20 quality checks + Issue dataclass
    suggestions.py            Maps issues to Power Query remediation steps
    reporter.py               HTML report, PDFs, Excel checklist, data dictionary
    governance.py             Schema drift detection against hardcoded expected schema
```

### Execution Sequence

```python
tables      = load_tables(config)
drift       = SchemaDriftDetector(EXPECTED_SCHEMA).check(tables)
profiles    = SchemaProfiler(tables).run()
findings    = DataQualityChecker(tables, config, profiles).run()
issues      = [f for f in findings if not f.is_observation]
obs         = [f for f in findings if f.is_observation]
suggestions = SuggestionEngine(issues, config).generate()
Reporter(config, issues, obs, suggestions, tables).generate_html(...)
Reporter(config, issues, obs, suggestions, tables).generate_pdf(...)
```

---

## Quality Checks

| ID | Check | Category | Severity | Type |
|---|---|---|---|---|
| CHK-01 | Null values in primary key column | COMPLETENESS | HIGH | Issue |
| CHK-02 | Null values in foreign key column | COMPLETENESS | MEDIUM | Issue |
| CHK-03 | Null values in other columns | COMPLETENESS | LOW | Issue |
| CHK-04 | Fully empty rows | COMPLETENESS | HIGH | Issue |
| CHK-05 | Fully duplicate rows | INTEGRITY | MEDIUM | Issue |
| CHK-06 | Duplicate primary key values | INTEGRITY | HIGH | Issue |
| CHK-07 | High cardinality in categorical column | INTEGRITY | LOW | Issue |
| CHK-08 | Values outside allowed set (business rule) | INTEGRITY | LOW | Issue |
| CHK-09 | Required column missing (business rule) | INTEGRITY | HIGH | Issue |
| CHK-10 | Mixed types in column | STRUCTURE | HIGH | Issue |
| CHK-11 | IQR outliers in ratio or value column | OUTLIER | LOW/MEDIUM | Issue |
| CHK-12 | Negative values in non-trading column | OUTLIER | LOW | Issue |
| CHK-13 | Orphaned FK values — HIGH confidence relationship | REFERENTIAL | HIGH | Issue |
| CHK-14 | Orphaned FK values — MEDIUM confidence relationship | REFERENTIAL | MEDIUM | Issue |
| CHK-15 | Business-day gaps in date sequence | TEMPORAL | LOW | Issue |
| CHK-16 | IQR outliers in trading flow column (quantity, volume) | OUTLIER | LOW | Observation |
| CHK-17 | Date coverage start gap vs reference table | TEMPORAL | LOW | Observation |
| CHK-18 | Date coverage end gap vs reference table | TEMPORAL | LOW | Observation |
| CHK-19 | Table has no key relationship to other tables | ARCHITECTURE | LOW | Observation |
| CHK-20 | Required table or column missing from upload | DRIFT | HIGH | Drift event |

### Outlier Method

3xIQR fence: `Q1 - 3 * IQR` and `Q3 + 3 * IQR`. Trading flow columns (quantity, volume) are flagged as observations — large paired buy/sell values are routine in a loan portfolio and should not be excluded from sums. Ratio and value columns are flagged as actionable issues because outliers distort averages.

### Issues vs Observations

Issues require action before loading into any downstream tool. Observations document expected behaviour — temporal misalignment between tables with different update frequencies, or tables that connect via date logic rather than key relationships. Separating them prevents alert fatigue and keeps the checklist focused.

### Schema Drift

The pipeline validates every upload against a hardcoded expected schema rather than comparing against the previous upload. This means uploading dataset v1, then v2, then v1 again produces no false drift. Only genuinely structural changes — missing tables or missing columns — are flagged.

---

## Outputs

| File | Format | Description |
|---|---|---|
| `quality_report.html` | 8-page HTML | Full report with accordion findings and expandable data snippets |
| `quality_report_[severity].pdf` | PDF per severity | Branded report — one PDF per severity level, each downloadable independently |
| `quality_checklist.xlsx` | Excel | Issue list with dropdown status (Open / Done), Power Query step, and remediation hint |
| `data_dictionary.xlsx` | Excel | Auto-generated schema: inferred types, null counts, unique %, min/max, sample values |
| `cleaned_[filename]` | Excel / CSV | Cleaned data with nulls forward-filled and outlier flag columns added. Original structure preserved. |

---

## Installation

```bash
pip install -r requirements.txt
```

Requirements:

```
flask>=2.3.0
pandas>=2.0.0
numpy>=1.24.0
openpyxl>=3.1.0
reportlab>=4.0.0
```

---

## Running the Pipeline

### Option A — Web interface (non-technical)

Double-click `launch_pipeline.bat`. The server starts and the browser opens automatically.

Or manually:

```bash
python app.py
```

Open `http://localhost:5000`, drag and drop your files, and click Run Quality Analysis. The full report renders in-browser in approximately 5 seconds.

### Option B — CLI

```bash
python run_quality_check.py
```

Reads sources from `config.json`. Outputs all files to the `output/` directory.

### Resetting the drift baseline

Delete `output/schema_baseline.json`. The next upload creates a fresh baseline automatically.

---

## Configuration

`config.json` defines source file paths and optional business rules. The pipeline does not require a schema definition — it discovers structure automatically.

```json
{
  "sources": {
    "transactions": { "file": "data/business_case.xlsx", "type": "excel", "sheet": "transactions" },
    "fx":           { "file": "data/fx.csv",             "type": "csv",   "delimiter": "," }
  },
  "business_rules": [
    {
      "id": "BR-001",
      "applies_to": "products",
      "validator": "allowed_values",
      "column": "product-type",
      "values": ["LN", "EQ", "FI", "FX", "CASH"]
    }
  ]
}
```

---

## Repository Structure

```
.
├── data_quality/
│   ├── __init__.py
│   ├── profiler.py
│   ├── checks.py
│   ├── suggestions.py
│   ├── reporter.py
│   └── governance.py
├── templates/
├── run_quality_check.py
├── app.py
├── config.json
├── launch_pipeline.bat
├── requirements.txt
└── README.md
```

---

## Design Decisions

**Schema-agnostic.** An earlier version required a config file defining every column. The profiler in v2 infers column types, primary keys, foreign keys, relationships, and outlier fences directly from the data. The pipeline runs on any dataset without configuration changes.

**Deterministic only.** 80 to 180 rows is too small for ML training signal. Every check is a statistical rule. Every finding is traceable to a specific calculation. No unexplained flags.

**Issues separate from observations.** A temporal gap between a transaction table and a monthly price snapshot is expected behaviour, not a data error. Flagging it as an issue would generate noise on every correct dataset. Observations are documented for traceability, not actioned.

**Forward-fill in the pipeline, not in the downstream tool.** When a time series column has missing values, the pipeline fills them before output using the most recent available value. The cleaned file is what gets loaded downstream. The original raw file is never modified.

---

## Author

Adriele Rocha Weisz
Zurich, Switzerland
[GitHub](https://github.com/rochaadrielea) · [Portfolio](https://www.risingleaders.com.br/adrielerocha)