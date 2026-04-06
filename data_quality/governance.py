# data_quality/governance.py
# Governance Layer — schema drift detection + data dictionary export
# Author: Adriele Rocha Weisz

from __future__ import annotations
import json
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import numpy as np


# ─────────────────────────────────────────────────────────────────────────────
# SCHEMA DRIFT DETECTOR
#
# Validates every upload against a HARDCODED expected schema.
# No baseline file is used for drift detection — each analysis is independent.
#
# What counts as drift (things that break Power BI or DAX):
#   HIGH  — required table missing entirely
#   HIGH  — required column missing from a table
#   LOW   — unexpected new column appeared (informational only)
#
# What is NOT flagged (not structural, does not break anything):
#   - Row count changes (just different data volume or time period)
#   - Numeric value range changes (expected when datasets swap)
#   - Categorical value changes (new/removed codes in the data)
#
# save() still writes a snapshot to disk — used only for the report's
# "baseline info" display, not for drift comparison.
# ─────────────────────────────────────────────────────────────────────────────

# Exact columns that Power BI relationships and DAX measures depend on.
# Changing this dict is the ONLY place that needs updating if the schema changes.
EXPECTED_SCHEMA: dict[str, list[str]] = {
    "transactions": [
        "transaction-code",
        "portfolio-code",
        "product-code",
        "transaction-quantity",
        "transaction-price",
        "transaction-date",
    ],
    "products": [
        "product-code",
        "product-type",
        "product-currency",
        "product-issuer-code",
        "loan-property-value-ratio",
    ],
    "portfolios": [
        "portfolio-code",
        "portfolio-currency",
    ],
    "fx": [
        "price-date",
        "EURUSD",
    ],
    "prices": [
        "price-date",
        "P1-EUR",
        "P2-USD",
    ],
}


class SchemaDriftDetector:

    def __init__(self, baseline_path: Path):
        self.baseline_path = Path(baseline_path)

    # ── public API ────────────────────────────────────────────────────────────

    def check(self, tables: dict, config: dict = None) -> list[dict]:
        """
        Validate uploaded tables against the hardcoded EXPECTED_SCHEMA.
        Returns list of drift events. Empty list = schema is intact.
        Each analysis is independent — no baseline file comparison.
        """
        # Normalize uploaded table names (strip version suffixes: fx_v2 -> fx)
        uploaded = {self._normalize_name(k): v for k, v in tables.items()}

        events = []

        for table, required_cols in EXPECTED_SCHEMA.items():
            if table not in uploaded:
                events.append({
                    "type":   "TABLE_MISSING",
                    "table":  table,
                    "risk":   "HIGH",
                    "detail": (
                        f"Table [{table}] was not found in the uploaded files. "
                        f"Every DAX measure and Power Query step that references "
                        f"[{table}] will fail immediately when Power BI refreshes. "
                        f"Ensure {table} is included in the upload."
                    ),
                })
                continue

            df = uploaded[table]
            actual_cols = set(df.columns)

            for col in required_cols:
                if col not in actual_cols:
                    events.append({
                        "type":   "COLUMN_MISSING",
                        "table":  table,
                        "risk":   "HIGH",
                        "detail": (
                            f"[{table}.{col}] is required but missing from the "
                            f"uploaded file. Any DAX measure or relationship that "
                            f"references [{col}] will break when Power BI refreshes. "
                            f"Check the source file and confirm the column name is exact."
                        ),
                    })

            # New columns are informational only — they don't break anything
            extra_cols = actual_cols - set(required_cols)
            for col in sorted(extra_cols):
                events.append({
                    "type":   "COLUMN_ADDED",
                    "table":  table,
                    "risk":   "LOW",
                    "detail": (
                        f"[{table}.{col}] is a new column not in the expected schema. "
                        f"No existing DAX measure uses it, so nothing will break. "
                        f"Verify it is mapped correctly in Power Query if needed."
                    ),
                })

        # Sort: HIGH first, then LOW
        risk_order = {"HIGH": 0, "LOW": 1}
        events.sort(key=lambda e: risk_order.get(e.get("risk", "LOW"), 1))
        return events

    def save(self, tables: dict, config: dict = None):
        """
        Save a snapshot of the current run to disk.
        Used only for the report's 'last run' info display —
        NOT used for drift comparison (that uses EXPECTED_SCHEMA).
        """
        sources = (config or {}).get("sources", {})
        snap = {"_saved_at": datetime.now().isoformat()}
        for name, df in tables.items():
            src   = sources.get(name, {})
            fname = Path(src.get("file", "")).name if src.get("file") else name
            snap[self._normalize_name(name)] = {
                "row_count": len(df),
                "columns":   list(df.columns),
                "files":     [fname] if fname else [],
            }
        self.baseline_path.parent.mkdir(parents=True, exist_ok=True)
        self.baseline_path.write_text(
            json.dumps(snap, indent=2, default=str), encoding="utf-8")

    def get_baseline_info(self) -> dict:
        """
        Returns metadata about the last run for display in the report.
        """
        if not self.baseline_path.exists():
            return {}
        try:
            data     = json.loads(self.baseline_path.read_text(encoding="utf-8"))
            saved_at = data.get("_saved_at", "")[:19].replace("T", " ")
            files, table_names, total_rows = set(), [], 0
            for key, val in data.items():
                if key.startswith("_"):
                    continue
                table_names.append(key)
                total_rows += val.get("row_count", 0)
                for f in val.get("files", []):
                    files.add(f)
            return {
                "saved_at":   saved_at,
                "files":      sorted(files),
                "tables":     table_names,
                "total_rows": total_rows,
            }
        except Exception:
            return {}

    # ── helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _normalize_name(name: str) -> str:
        """Strip trailing version suffixes: fx_v2 -> fx, prices_v3 -> prices."""
        import re
        return re.sub(r'_v\d+$', '', name, flags=re.IGNORECASE)



# ─────────────────────────────────────────────────────────────────────────────
# DATA DICTIONARY EXPORTER (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

class DataDictionaryExporter:

    def __init__(self, profiles: dict):
        self.profiles = profiles

    def export(self, output_path: Path) -> Path:
        try:
            from openpyxl import Workbook
            from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
            from openpyxl.utils import get_column_letter
        except ImportError:
            print("  ⚠️  openpyxl not available — skipping data dictionary.")
            return output_path

        wb = Workbook()
        ws = wb.active
        ws.title = "Data Dictionary"

        NAVY = "1C2B4A"; GOLD = "B89650"; LIGHT = "F5F6F8"
        thin   = Side(style="thin", color="D0D0D0")
        border = Border(left=thin, right=thin, top=thin, bottom=thin)

        def cell(row, col, value, bold=False, bg=None,
                 align="left", wrap=False, color=None):
            c = ws.cell(row=row, column=col, value=value)
            c.font = Font(name="Arial", size=9, bold=bold,
                          color=color or "1A1A1A")
            c.alignment = Alignment(horizontal=align, vertical="top",
                                    wrap_text=wrap)
            c.border = border
            if bg:
                c.fill = PatternFill("solid", start_color=bg)

        headers = ["Table","Column","Inferred Type","Nullable","Null %",
                   "Unique Values","Unique %","Min / Date Min","Max / Date Max",
                   "Sample Values","Notes"]
        for ci, h in enumerate(headers, 1):
            c = ws.cell(row=1, column=ci, value=h)
            c.font = Font(name="Arial", size=9, bold=True, color="FFFFFF")
            c.fill = PatternFill("solid", start_color=NAVY)
            c.alignment = Alignment(horizontal="center", vertical="center")
            c.border = border
        ws.row_dimensions[1].height = 22

        row_idx = 2
        for tname, profile in self.profiles.items():
            for cname, cp in profile.columns.items():
                bg = LIGHT if row_idx % 2 == 0 else "FFFFFF"
                notes = []
                if cp.is_likely_pk:     notes.append("inferred PK")
                if cp.is_likely_fk:     notes.append("inferred FK")
                if cp.outlier_count > 0: notes.append(f"{cp.outlier_count} outlier(s)")
                if cp.cardinality_flag == "HIGH": notes.append("high cardinality")
                if cp.has_negatives and cp.inferred_type == "numeric":
                    notes.append("contains negatives")

                if cp.inferred_type == "numeric":
                    vmin = f"{cp.num_min:,.4f}" if cp.num_min is not None else "—"
                    vmax = f"{cp.num_max:,.4f}" if cp.num_max is not None else "—"
                elif cp.inferred_type == "date":
                    vmin = str(cp.date_min.date()) if cp.date_min else "—"
                    vmax = str(cp.date_max.date()) if cp.date_max else "—"
                else:
                    vmin = vmax = "—"

                sample = ", ".join(str(v) for v in cp.sample_values[:3])
                cell(row_idx, 1,  tname,                    bg=bg)
                cell(row_idx, 2,  cname,                    bg=bg, bold=True)
                cell(row_idx, 3,  cp.inferred_type,         bg=bg, align="center")
                cell(row_idx, 4,  "Yes" if cp.null_count > 0 else "No",
                                                             bg=bg, align="center")
                cell(row_idx, 5,  f"{cp.null_pct:.1f}%",   bg=bg, align="center")
                cell(row_idx, 6,  cp.unique_count,          bg=bg, align="center")
                cell(row_idx, 7,  f"{cp.unique_pct:.1f}%", bg=bg, align="center")
                cell(row_idx, 8,  vmin,                     bg=bg)
                cell(row_idx, 9,  vmax,                     bg=bg)
                cell(row_idx, 10, sample, wrap=True,        bg=bg)
                cell(row_idx, 11, "; ".join(notes) if notes else "",
                     wrap=True, bg=bg, color=GOLD if notes else None)
                ws.row_dimensions[row_idx].height = 18
                row_idx += 1

        widths = [16, 22, 14, 9, 8, 13, 10, 16, 16, 30, 30]
        for ci, w in enumerate(widths, 1):
            ws.column_dimensions[get_column_letter(ci)].width = w
        ws.auto_filter.ref = f"A1:{get_column_letter(len(headers))}1"
        ws.freeze_panes    = "A2"

        ws_meta = wb.create_sheet("Metadata")
        ws_meta["A1"] = "Generated"
        ws_meta["B1"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ws_meta["A2"] = "Tables"
        ws_meta["B2"] = len(self.profiles)
        ws_meta["A3"] = "Total Columns"
        ws_meta["B3"] = sum(len(p.columns) for p in self.profiles.values())
        ws_meta["A4"] = "Tool"
        ws_meta["B4"] = "Graian Capital Management — Data Quality Pipeline"

        wb.save(output_path)
        return output_path