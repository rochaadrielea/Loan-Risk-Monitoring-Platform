# Graian Capital Management — Data Operations Business Case

> **Role:** Data Operations Engineer/Analyst · Business Case Submission · April 2026  
> **Author:** Adriele Rocha Weisz · Zurich, Switzerland  
> **Evaluator:** Eric Della-Negra, COO — Graian Capital Management SA, Lugano  
> **Regulated:** FINMA-licensed asset manager · ~20 people

---

## 📋 What Was Asked

> *"Build a tool in Power BI that allows:*
> 1. *To calculate and report loan-exposure (type LN) by portfolio at a given date*
> 2. *To calculate and report loan-exposure (type LN) by issuer at a given date*
> 3. *To calculate and report loan-to-value by portfolio at a given date*
> 4. *To calculate and report loan-to-value by issuer at a given date*
> 5. *Cases 1 to 4 in %*
> 6. *Cases 1 to 4 over time (history with dynamic slicing window)*
>
> Data quality may not be bullet-proof. Please highlight any concerns and how you handle it."*
>
> — Business case brief, Graian Capital Management SA

---

## 🚀 What Was Delivered

| Deliverable | Description | Status |
|---|---|---|
| **Power BI Dashboard** | 6-page interactive report — all 6 requirements | ✅ Complete |
| **Python Data Quality Pipeline** | Schema-agnostic pipeline — 20 checks, 8 outputs | ✅ Complete |
| **Web Interface (Flask)** | Drag-and-drop data quality UI — localhost:5000 | ✅ Complete |
| **DAX Architecture Doc** | Full measure documentation with reasoning | ✅ Complete |
| **Data Dictionary** | Auto-generated column types, ranges, relationships | ✅ Complete |
| **Quality Checklist** | Excel checklist with Power Query steps and DAX hints | ✅ Complete |

---

## 📊 Power BI Dashboard — 6 Pages

### Data Model — 5 Tables

```
business_case.xlsx
├── transactions    PK: transaction-code   FK: portfolio-code, product-code
│                  quantity × price = loan exposure
├── products        PK: product-code       product-type = LN filter applied
│                  outlier-flag calculated column (3×IQR method)
└── portfolios      PK: portfolio-code     2 rows: P1-EUR, P2-USD

fx.csv             price-date, EURUSD      NO hard relationship → DAX LASTNONBLANK
prices.csv         price-date, P1-EUR, P2-USD  month-end only → DAX LASTNONBLANK
```

**Hard relationships:**
- `transactions → products` on `product-code` (Many-to-One)
- `transactions → portfolios` on `portfolio-code` (Many-to-One)

**Why no hard join for fx/prices:**  
Date ranges don't align across dataset swaps. `LASTNONBLANK` resolves the most recent available value at `MAX(transaction-date)` — adapts to any date range automatically.

---

### DAX Measures — 5 Measures (home table: transactions)

#### Design Principles Applied to Every Measure

| Principle | Implementation | Why |
|---|---|---|
| **LN filter in DAX** | `products[product-type] = "LN"` inside `CALCULATE` | Holds regardless of new product types in future datasets |
| **DIVIDE() not /** | `DIVIDE([Loan Exposure], [Portfolio Value])` | Zero-safe — never breaks charts when denominator is zero |
| **LASTNONBLANK for FX/prices** | `LASTNONBLANK(fx[EURUSD], 1)` | Adapts to any date range — no hardcoded rates |

---

#### Measure 1 — Loan Exposure

```dax
-- VAR pattern: calculate first, apply business logic after
-- IF net position = 0, return BLANK so Power BI hides automatically
-- Cascades to Loan Exposure % via DIVIDE

Loan Exposure =
VAR Result =
    ROUND(
        CALCULATE(
            SUMX(
                transactions,
                transactions[transaction-quantity] *
                transactions[transaction-price]
            ),
            products[product-type] = "LN"
        ),
        2
    )
RETURN
    IF(Result = 0, BLANK(), Result)
```

**Why VAR + BLANK():**  
Floating point arithmetic in DAX produces values like `5.8E-11` when buys and sells cancel out exactly. This is not zero — it is a binary rounding artifact. A filter like `> 0.001` would risk hiding real future data. `BLANK()` is Power BI's native signal to hide — no filters needed anywhere. Cascades automatically to all dependent measures.

---

#### Measure 2 — Loan Exposure %

```dax
-- DIVIDE returns BLANK when [Loan Exposure] is BLANK
-- No additional IF() needed — cascades automatically

Loan Exposure % =
DIVIDE(
    [Loan Exposure],
    [Portfolio Value]
)
```

---

#### Measure 3 — Avg LTV

```dax
-- Dynamic outlier exclusion via 3×IQR fence
-- Matches exact same method used in Python pipeline (CHK-02)

Avg LTV =
AVERAGEX(
    FILTER(
        products,
        products[product-type] = "LN" &&
        products[outlier-flag] = "OK"
    ),
    DIVIDE(1, products[loan-property-value-ratio])
)
```

**Why outlier exclusion:**  
`PR-P4N4N` has `loan-property-value-ratio = 33.33` — LTV of 0.03. Including it would pull the average to a meaningless number. The `outlier-flag` calculated column excludes values outside `Q1 − 3×IQR` and `Q3 + 3×IQR` dynamically — same method as the Python pipeline.

---

#### Measure 4 — Avg LTV %

```dax
Avg LTV % = [Avg LTV] * 100
```

---

#### Measure 5 — Portfolio Value

```dax
-- LASTNONBLANK adapts to any date range automatically
-- No hard relationship needed between prices/fx and transactions

Portfolio Value =
VAR MaxDate = MAX(transactions[transaction-date])
VAR FXRate =
    CALCULATE(
        LASTNONBLANK(fx[EURUSD], 1),
        fx[price-date] <= MaxDate
    )
VAR P1Value =
    CALCULATE(
        LASTNONBLANK(prices[P1-EUR], 1),
        prices[price-date] <= MaxDate
    )
VAR P2Value =
    CALCULATE(
        LASTNONBLANK(prices[P2-USD], 1),
        prices[price-date] <= MaxDate
    )
RETURN
    (P1Value * FXRate) + P2Value
```

---

### Dashboard Pages

| Page | Requirements | Key Design Decisions |
|---|---|---|
| **P1 Overview** | Summary — no date filter | First screen answers "IS EVERYTHING OK?" — KPI cards + flagged products |
| **P2 Loan Exposure** | Req 1 & 2 | Two charts answer both requirements simultaneously. Cross-filtering active. Drill through to Transaction Detail |
| **P3 LTV** | Req 3 & 4 | Fixed gradient thresholds — 0.75 (FINMA), 0.80 (visual center), 1.0 (hard limit). Risk story always honest |
| **P4 Metrics %** | Req 5 | Same as P3 but in % — thresholds 75/80/100. FINMA-grounded |
| **P5 Time Series** | Req 6 | Dynamic slicing window. Zero reference line. Negative months = sells exceed buys — correct behaviour |
| **P6 Transaction Detail** | Drillthrough | Right-click any bar → Drill through. Bookmark button clears filter to show all transactions |

---

### LTV Risk Thresholds — Regulatory Basis

| Threshold | Value | Source |
|---|---|---|
| **Green (safe zone)** | < 0.75 (< 75%) | FINMA maximum LTV for investment properties — SBA self-regulation, August 2019 |
| **Amber (monitor)** | 0.75 – 1.0 (75–100%) | Above FINMA threshold — higher capital requirements apply |
| **Red (danger)** | = 1.0 (= 100%) | Brief definition: "loan-to-value = value of loan divided by value of property" — collateral no longer covers the loan |

> Source 1: Strike Advisory — *Loan-to-value (LTV) and mortgages in Switzerland* — strike-advisory.ch  
> Source 2: FINMA / Mondaq — *Swiss Regulator Further Restricts Mortgage Loan Requirements* — mondaq.com, December 2019  
> Source 3: Brief definition — business_case.xlsx, Requirements sheet

---

### Parameterization

3 Power Query parameters: `SourceFile`, `FXFile`, `PricesFile`

**To update with new data:**
```
Home → Transform Data → Edit Parameters → update 3 paths → OK → Apply Changes
```

**Shortcut:** If filenames are identical, just overwrite files and click Refresh Data — no path update needed. Tested across 5 dataset versions. Zero model changes required.

---

## 🐍 Python Data Quality Pipeline

### Architecture

```
run_quality_check.py          ← CLI entry point
app.py                        ← Flask web UI (localhost:5000)
data_quality/
├── profiler.py               ← Schema auto-discovery (types, PKs, relationships, outliers)
├── checks.py                 ← 20 quality checks — issues vs observations separated
├── suggestions.py            ← Maps issues to Power Query / DAX hints
├── reporter.py               ← CLI output, 8-page HTML, PDF, Excel checklist
└── governance.py             ← Schema drift detection + data dictionary export
```

**Design principle:** Schema-agnostic. No configuration needed — discovers structure from any dataset. Tested on 5 dataset versions. Zero pipeline changes required between runs.

---

### 20 Quality Checks

| ID | Check | Category | Method |
|---|---|---|---|
| CHK-01 | Null Detection | COMPLETENESS | Counts missing values per column |
| CHK-02 | IQR Outlier Detection | OUTLIER | Q1−3×IQR, Q3+3×IQR fence |
| CHK-03 | Z-Score Outlier Detection | OUTLIER | \|z-score\| > 3 |
| CHK-04 | Benford's Law Analysis | STATISTICAL | First-digit distribution vs log10(1+1/d) |
| CHK-05 | Coefficient of Variation | STATISTICAL | CV = std/mean for stable columns |
| CHK-06 | Negative Value Detection | INTEGRITY | Flags negatives in price/value/ratio columns |
| CHK-07 | Date Parse Validation | COMPLETENESS | Counts unparseable date values |
| CHK-08 | Future Date Detection | INTEGRITY | Flags dates after today |
| CHK-09 | Business-Day Gap Detection | TEMPORAL | numpy.busday_count — weekends excluded |
| CHK-10 | Duplicate Row Detection | INTEGRITY | pandas.DataFrame.duplicated() |
| CHK-11 | Primary Key Duplicate Detection | INTEGRITY | Inferred PK — 100% unique, no nulls |
| CHK-12 | Referential Integrity | REFERENTIAL | Auto-detected FK→PK by name similarity + value overlap |
| CHK-13 | Temporal Alignment | TEMPORAL | Date range comparison across tables |
| CHK-14 | Disconnected Table Detection | ARCHITECTURE | No key-column overlap — DAX candidates |
| CHK-15 | Schema Drift Detection | GOVERNANCE | Column list + row count vs saved baseline |
| CHK-16 | Value Set Drift | GOVERNANCE | Categorical value sets vs baseline |
| CHK-17 | Numeric Range Drift | GOVERNANCE | Min/max changes > 50% vs baseline |
| CHK-18 | Business Rules Validation | CONFIG | allowed_values, value_range, column_exists |
| CHK-19 | Mixed Type Detection | STRUCTURE | Non-numeric values in numeric columns |
| CHK-20 | Encoding Issue Detection | STRUCTURE | Control characters + non-UTF-8 bytes |

---

### Quality Report Results — Original Dataset

| Severity | Count | Finding |
|---|---|---|
| HIGH | 0 | None |
| MEDIUM | 1 | 16 statistical outliers in transaction-quantity (3×IQR) |
| LOW | 4 | 4 outlier loan-property-value-ratios · 52 business-day gaps in transaction-date · 10 gaps in fx.price-date · 5 nulls in fx.EURUSD |
| OBSERVATIONS | 5 | Temporal misalignment between tables (expected) · fx and prices recommended for DAX LASTNONBLANK (not hard join) |

---

### Pipeline Outputs

```
output/
├── cleaned_business case.xlsx     ← Original sheet names preserved + outlier flag columns
├── cleaned_fx.csv                 ← Nulls forward-filled
├── cleaned_prices.csv             ← Nulls forward-filled
├── quality_checklist.xlsx         ← 10 columns: severity, category, Power Query steps, DAX hints, ☐/☑ dropdown
├── quality_report.pdf             ← Named quality_report_[dataset]_[date].pdf
├── quality_report.html            ← 8-page multi-section HTML dashboard
├── data_dictionary.xlsx           ← Auto-generated column types, ranges, unique values
└── schema_baseline.json           ← Saved after every run for drift detection
```

---

### Power Query — Fill Down Applied

5 null EURUSD values exist in the source file. Fixed in Power Query — not in DAX — following the principle:

> **Power Query = data preparation** (fix what's broken at source)  
> **DAX = business logic** (calculate what the business needs)

```
fx table → select EURUSD column → Transform → Fill → Fill Down
prices table → select P1-EUR → Fill Down → select P2-USD → Fill Down
```

---

## 🛠️ Installation & Usage

### Requirements

```
flask>=2.3.0
pandas>=2.0.0
numpy>=1.24.0
openpyxl>=3.1.0
reportlab>=4.0.0
```

### Install

```bash
pip install -r requirements.txt
```

### Run — CLI

```bash
python run_quality_check.py
```

### Run — Web UI

```bash
python app.py
# Open http://localhost:5000
```

### Update Power BI with new data

```
Home → Transform Data → Edit Parameters → update 3 paths → OK → Apply Changes
```

---

## 📂 Repository Structure

```
graian-data-operations/
├── data_quality/
│   ├── __init__.py
│   ├── profiler.py           ← Schema auto-discovery engine
│   ├── checks.py             ← 20 quality checks + Issue dataclass
│   ├── suggestions.py        ← Power Query / DAX transformation guide generator
│   ├── reporter.py           ← CLI + 8-page HTML + PDF + Excel reporter
│   └── governance.py         ← Schema drift detector + data dictionary exporter
├── templates/                ← Flask HTML templates
├── run_quality_check.py      ← CLI entry point
├── app.py                    ← Flask web UI
├── config.json               ← Source definitions + business rules
├── requirements.txt
└── README.md
```

---

## 📖 Key Design Decisions

### Why schema-discovery instead of config-driven

An earlier version required a `config.json` defining every column. The schema-discovery approach (v2) profiles any dataset automatically — column types, primary keys, foreign keys, relationships, outliers, date gaps. The tool works on any dataset with the same structure without configuration changes.

### Why the pipeline and the dashboard use the same outlier method

Both use 3×IQR: `Q1 − 3×IQR` and `Q3 + 3×IQR`. The pipeline flags outliers in the quality report. The Power BI `outlier-flag` calculated column uses the same fence. When Eric loads a new dataset, both tools flag the same records — no inconsistency between the quality report and the dashboard.

### Why issues and observations are separated

Not every finding requires action. Temporal misalignment between fx and transactions is expected — different update frequencies. Disconnected tables (fx, prices) are by design — they use DAX LASTNONBLANK, not Power BI relationships. Separating issues from observations prevents alert fatigue and keeps the checklist focused on what actually needs fixing.

---

## 🔒 Data Quality — The Full Story

```
Pipeline finds it → documents it in quality report
Power BI handles it → Fill Down in Power Query + VAR BLANK() in DAX
Dashboard shows clean data → Eric sees accurate numbers, not artifacts
```

The brief says: *"Data quality may not be bullet-proof. Please highlight any concerns and how you handle it."*

Concerns are highlighted in the pipeline report (PDF, HTML, Excel checklist). The dashboard is the clean end product. The two tools work as a system.

---

## 🧭 April 13 Demo Script

### Before the meeting
- Have `business_case.xlsx`, `fx.csv`, `prices.csv` ready (new dataset Eric will provide)
- Open Power BI Desktop with .pbix loaded
- Have `python app.py` running in terminal (localhost:5000 open in browser)

### Live demo — Python pipeline first
1. Upload ORIGINAL files → sets the drift baseline
2. Upload NEW files → pipeline runs in ~5 seconds
3. Show drift report first — tool caught schema changes BEFORE Power BI touched anything
4. Show accordion UI — click to expand any finding
5. Key message: *"Pipeline finds it, documents it, tells you exactly what to fix in Power BI"*

### Live demo — Power BI
1. `Home → Transform Data → Edit Parameters` → update 3 paths → Apply Changes
2. OR: overwrite files with same names → click Refresh Data (under 1 minute)
3. Show all 6 pages
4. Demo drillthrough: right-click bar on P2 → Drill through → Transaction Detail

### Key answers if Eric asks
- **Why no hard join for fx/prices:** LASTNONBLANK adapts to any date range — hard join breaks when dates don't align between dataset swaps
- **Why fixed gradient:** A relative scale would make 50% LTV look amber even when safe — fixed thresholds tell the truth
- **Why 0.75 threshold:** FINMA maximum LTV for investment properties — SBA self-regulation recognised by FINMA, August 2019
- **Why Avg LTV excludes outliers:** PR-P4N4N ratio 33.33 would pull average to meaningless number — 3×IQR is same method as pipeline
- **Why VAR + BLANK():** Floating point produces 5.8E-11 for closed positions — BLANK() hides natively, no filters needed, works on any dataset

---

## 👤 Author

**Adriele Rocha Weisz**  
📍 Zurich, Switzerland  
🔗 [GitHub](https://github.com/rochaadrielea) | 🌍 [Portfolio](https://www.risingleaders.com.br/adrielerocha)

---

*Graian Capital Management SA · Data Operations Business Case · April 2026 · Confidential*
