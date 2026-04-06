# data_quality/checks.py
# Data Quality Checker — Schema-Discovery Mode
# Author: Adriele Rocha Weisz

import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from typing import Optional
from .profiler import TableProfile, ColumnProfile, RelationshipHint

MIN_ROWS_FOR_STRUCTURAL_CHECKS = 10
MAX_SNIPPET_ROWS = 5   # max rows to capture for display in HTML report


@dataclass
class Issue:
    severity:       str
    category:       str
    table:          str
    column:         Optional[str]
    description:    str
    affected:       int
    examples:       list  = field(default_factory=list)
    suggestion:     str   = ""
    is_observation: bool  = False
    # Human-readable WHY explanation — filled with actual statistics
    # "16 values exceed the fence of 3×IQR. Typical range is X–Y."
    explanation:    str   = ""
    # Actual problem rows for display in the HTML snippet table
    # List of dicts: [{col1: val1, col2: val2, "__flag__": "OUTLIER"}, ...]
    snippet_rows:   list  = field(default_factory=list)
    # Statistics dict for the collapsible technical section
    # {"IQR fence lo": -3144532.97, "IQR fence hi": 3205698.74, ...}
    stats:          dict  = field(default_factory=dict)


def _capture_snippet(df: pd.DataFrame, mask: pd.Series,
                     highlight_col: str, flag_label: str,
                     key_cols: list = None) -> list:
    """
    Returns up to MAX_SNIPPET_ROWS rows where mask is True.
    Each row is a dict. The flagged column is tagged with __flag__.
    __highlight__ tells the renderer which column to colour.
    """
    rows_df = df[mask].head(MAX_SNIPPET_ROWS)
    result = []
    for _, row in rows_df.iterrows():
        d = row.to_dict()
        # Convert non-serialisable types
        for k, v in d.items():
            if pd.isna(v) if not isinstance(v, (list, dict)) else False:
                d[k] = None
            elif hasattr(v, "isoformat"):
                d[k] = str(v)[:10]
            elif isinstance(v, (np.integer,)):
                d[k] = int(v)
            elif isinstance(v, (np.floating,)):
                d[k] = float(v)
        d["__flag__"]      = flag_label
        d["__highlight__"] = highlight_col
        result.append(d)
    return result


class DataQualityChecker:

    def __init__(self, tables, config, profiles, relationships):
        self.tables        = tables
        self.config        = config
        self.profiles      = profiles
        self.relationships = relationships
        self.issues: list[Issue] = []

    def run(self) -> list[Issue]:
        self.issues = []
        for name, profile in self.profiles.items():
            self._check_table_level(name, profile)
            for col, cp in profile.columns.items():
                self._check_column(name, col, cp, profile)
        self._check_referential_integrity()
        self._check_temporal_alignment()
        self._check_disconnected_tables()
        self._check_business_rules()

        def sort_key(i):
            return (1 if i.is_observation else 0,
                    {"HIGH":0,"MEDIUM":1,"LOW":2}.get(i.severity,3))
        self.issues.sort(key=sort_key)
        return self.issues

    # ── table-level checks ────────────────────────────────────────────────────

    def _check_table_level(self, name, profile):
        df = self.tables.get(name, pd.DataFrame())

        if profile.fully_empty_rows > 0:
            mask = df.isna().all(axis=1)
            self.issues.append(Issue(
                severity="HIGH", category="COMPLETENESS",
                table=name, column=None,
                description=f"{profile.fully_empty_rows} fully empty row(s) found",
                affected=profile.fully_empty_rows,
                suggestion="Remove empty rows in Power Query: Home → Remove Rows → Remove Blank Rows.",
                explanation=(
                    f"The table [{name}] contains {profile.fully_empty_rows} row(s) "
                    f"where every single column is blank. These rows carry no information "
                    f"and will cause errors if they flow into Power BI calculations."
                ),
                snippet_rows=_capture_snippet(df, mask, "", "EMPTY ROW"),
                stats={"Empty rows": profile.fully_empty_rows,
                       "Total rows": profile.row_count}
            ))

        if profile.duplicate_rows > 0:
            mask = df.duplicated(keep=False)
            self.issues.append(Issue(
                severity="MEDIUM", category="INTEGRITY",
                table=name, column=None,
                description=f"{profile.duplicate_rows} fully duplicate row(s) detected",
                affected=profile.duplicate_rows,
                suggestion="In Power Query: Home → Remove Rows → Remove Duplicates.",
                explanation=(
                    f"{profile.duplicate_rows} rows in [{name}] are exact copies of another row — "
                    f"every column has the same value. Duplicate rows inflate totals "
                    f"(e.g. Loan Exposure would be double-counted for those transactions)."
                ),
                snippet_rows=_capture_snippet(df, mask, "", "DUPLICATE"),
                stats={"Duplicate rows": profile.duplicate_rows,
                       "Total rows": profile.row_count}
            ))

        if (profile.likely_pk
                and profile.duplicate_pk_rows > 0
                and profile.row_count >= MIN_ROWS_FOR_STRUCTURAL_CHECKS):
            pk = profile.likely_pk
            mask = df.duplicated(subset=[pk], keep=False)
            self.issues.append(Issue(
                severity="HIGH", category="INTEGRITY",
                table=name, column=pk,
                description=(
                    f"{profile.duplicate_pk_rows} rows share a duplicate "
                    f"inferred primary key [{pk}]"
                ),
                affected=profile.duplicate_pk_rows,
                examples=df[mask][pk].unique()[:5].tolist(),
                suggestion=(
                    f"Investigate duplicate [{pk}] values. "
                    "Suffix variants (-01, -02) may indicate rollovers — verify with source."
                ),
                explanation=(
                    f"The column [{pk}] appears to be the unique identifier for [{name}] "
                    f"(it has 100% unique values normally). {profile.duplicate_pk_rows} rows "
                    f"share a key that should be unique. In Power BI this breaks Many→One "
                    f"relationships and causes incorrect aggregations."
                ),
                snippet_rows=_capture_snippet(df, mask, pk, "DUPLICATE KEY"),
                stats={"Duplicate key rows": profile.duplicate_pk_rows,
                       "Key column": pk}
            ))

    # ── column-level checks ───────────────────────────────────────────────────

    def _check_column(self, table, col, cp, profile):
        df = self.tables.get(table, pd.DataFrame())

        # ── NULL check ────────────────────────────────────────────────────────
        if cp.null_count > 0:
            severity = ("HIGH" if cp.is_likely_pk
                        else "MEDIUM" if cp.is_likely_fk else "LOW")
            null_mask = df[col].isna() if col in df.columns else pd.Series([], dtype=bool)
            self.issues.append(Issue(
                severity=severity, category="COMPLETENESS",
                table=table, column=col,
                description=f"{cp.null_count} null(s) in [{col}] ({cp.null_pct:.1f}% of rows)",
                affected=cp.null_count,
                suggestion=self._null_suggestion(cp),
                explanation=(
                    f"Out of {profile.row_count} rows in [{table}], {cp.null_count} "
                    f"({cp.null_pct:.1f}%) have no value in [{col}]. "
                    + ("This column appears to be a unique identifier — nulls here mean "
                       "those rows cannot be linked to other tables in Power BI."
                       if cp.is_likely_pk else
                       "This column appears to be a link to another table — rows with nulls "
                       "here will be excluded from all joined calculations."
                       if cp.is_likely_fk else
                       "Missing values in this column may appear as blanks or zeros in "
                       "Power BI visuals, which can distort averages and totals.")
                ),
                snippet_rows=_capture_snippet(df, null_mask, col, "NULL")
                             if col in df.columns else [],
                stats={"Null count": cp.null_count,
                       "Null %": f"{cp.null_pct:.1f}%",
                       "Total rows": profile.row_count}
            ))

        # ── IQR OUTLIER check ─────────────────────────────────────────────────
        if cp.inferred_type == "numeric" and cp.outlier_count > 0:
            severity = ("MEDIUM"
                        if cp.outlier_count / max(profile.row_count, 1) > 0.05
                        else "LOW")
            if col in df.columns and cp.outlier_fence_lo is not None:
                nums = pd.to_numeric(df[col], errors="coerce")
                mask = (nums < cp.outlier_fence_lo) | (nums > cp.outlier_fence_hi)
                snippet = _capture_snippet(df, mask, col, "OUTLIER")
            else:
                snippet = []

            self.issues.append(Issue(
                severity=severity, category="OUTLIER",
                table=table, column=col,
                description=(
                    f"{cp.outlier_count} statistical outlier(s) in [{col}] "
                    f"(3×IQR fence: [{cp.outlier_fence_lo:,.4f} → {cp.outlier_fence_hi:,.4f}])"
                ),
                affected=cp.outlier_count,
                suggestion=(
                    f"Add flag column [is-outlier-{col}] in Power Query. "
                    "Preserve raw data — exclude flagged rows from averages in DAX."
                ),
                explanation=(
                    f"The middle 50% of values in [{col}] falls between "
                    f"{cp.num_q1:,.2f} and {cp.num_q3:,.2f} "
                    f"(this range is called the interquartile range, or IQR = {cp.num_iqr:,.2f}). "
                    f"Anything more than 3× the IQR below the lower end "
                    f"or above the upper end is considered statistically extreme. "
                    f"That gives a normal range of {cp.outlier_fence_lo:,.2f} to {cp.outlier_fence_hi:,.2f}. "
                    f"{cp.outlier_count} value(s) fall outside this range. "
                    f"This does not mean the values are wrong — large sells are legitimate — "
                    f"but they warrant review before being included in averages."
                ),
                snippet_rows=snippet,
                stats={
                    "Q1 (25th percentile)": f"{cp.num_q1:,.4f}",
                    "Q3 (75th percentile)": f"{cp.num_q3:,.4f}",
                    "IQR": f"{cp.num_iqr:,.4f}",
                    "Lower fence (Q1 − 3×IQR)": f"{cp.outlier_fence_lo:,.4f}",
                    "Upper fence (Q3 + 3×IQR)": f"{cp.outlier_fence_hi:,.4f}",
                    "Outlier count": cp.outlier_count,
                    "Min value": f"{cp.num_min:,.4f}",
                    "Max value": f"{cp.num_max:,.4f}",
                }
            ))

        # ── NEGATIVE VALUES check ─────────────────────────────────────────────
        if cp.inferred_type == "numeric" and cp.has_negatives:
            positive_hints = ["price","value","ratio","rate","amount","volume","pct"]
            if any(h in col.lower() for h in positive_hints):
                if col in df.columns:
                    nums = pd.to_numeric(df[col], errors="coerce")
                    mask = nums < 0
                    neg_count = int(mask.sum())
                    snippet = _capture_snippet(df, mask, col, "NEGATIVE")
                else:
                    neg_count = 0; snippet = []
                self.issues.append(Issue(
                    severity="MEDIUM", category="INTEGRITY",
                    table=table, column=col,
                    description=f"{neg_count} negative value(s) in [{col}] — unexpected for this field",
                    affected=neg_count,
                    suggestion=f"Verify sign convention for [{col}]. If sells are negative, confirm and document.",
                    explanation=(
                        f"The column [{col}] has a name suggesting it should always be positive "
                        f"(prices, values and ratios are typically ≥ 0). "
                        f"{neg_count} row(s) contain negative values. "
                        f"This may be intentional — for example, sells are sometimes recorded as "
                        f"negative quantities — or it may indicate a sign error at source. "
                        f"Either way, the convention needs to be confirmed and documented."
                    ),
                    snippet_rows=snippet,
                    stats={"Negative count": neg_count, "Column min": f"{cp.num_min:,.4f}"}
                ))

        # ── DATE checks ───────────────────────────────────────────────────────
        if cp.inferred_type == "date":
            if cp.date_parse_fail > 0:
                self.issues.append(Issue(
                    severity="HIGH", category="COMPLETENESS",
                    table=table, column=col,
                    description=f"{cp.date_parse_fail} value(s) in [{col}] could not be parsed as dates",
                    affected=cp.date_parse_fail,
                    suggestion=f"Standardize [{col}] to ISO 8601 (YYYY-MM-DD) at source.",
                    explanation=(
                        f"{cp.date_parse_fail} value(s) in [{col}] could not be read as dates. "
                        f"This usually means mixed date formats in the same column "
                        f"(e.g. some rows use DD/MM/YYYY and others use MM-DD-YYYY). "
                        f"Power BI will treat these as text and exclude them from all "
                        f"date-based calculations and slicers."
                    ),
                    stats={"Parse failures": cp.date_parse_fail}
                ))
            if cp.date_future > 0:
                if col in df.columns:
                    parsed = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
                    mask = parsed > pd.Timestamp.now()
                    snippet = _capture_snippet(df, mask, col, "FUTURE DATE")
                else:
                    snippet = []
                self.issues.append(Issue(
                    severity="MEDIUM", category="INTEGRITY",
                    table=table, column=col,
                    description=f"{cp.date_future} future date(s) in [{col}]",
                    affected=cp.date_future,
                    suggestion=f"Verify [{col}] — future dates may indicate entry errors.",
                    explanation=(
                        f"{cp.date_future} row(s) in [{col}] have dates in the future "
                        f"(after today). In a transaction dataset, future dates typically "
                        f"mean the date was entered incorrectly — for example, 2026 instead "
                        f"of 2022. These rows may appear as future data points in time "
                        f"series charts."
                    ),
                    snippet_rows=snippet,
                    stats={"Future date count": cp.date_future,
                           "Column max date": str(cp.date_max)[:10] if cp.date_max else "—"}
                ))
            if cp.date_gaps > 3:
                self.issues.append(Issue(
                    severity="LOW", category="TEMPORAL",
                    table=table, column=col,
                    description=f"{cp.date_gaps} business-day gap(s) in date sequence of [{col}]",
                    affected=cp.date_gaps,
                    suggestion=f"Review gaps in [{col}] — may be missing data or intentional (e.g. month-end prices).",
                    explanation=(
                        f"The dates in [{col}] are expected to be consecutive business days "
                        f"(weekends and public holidays are already excluded from this count). "
                        f"There are {cp.date_gaps} place(s) where one or more business days "
                        f"are skipped entirely. This could mean transactions genuinely did not "
                        f"occur on those days, or that data for those days is missing from the source."
                    ),
                    stats={"Business-day gaps": cp.date_gaps,
                           "Date range": f"{str(cp.date_min)[:10]} → {str(cp.date_max)[:10]}"
                           if cp.date_min else "—"}
                ))

        # ── MIXED TYPES ───────────────────────────────────────────────────────
        if cp.has_mixed_types:
            self.issues.append(Issue(
                severity="HIGH", category="STRUCTURE",
                table=table, column=col,
                description=f"[{col}] contains mixed types — some values cannot be parsed as {cp.inferred_type}",
                affected=0,
                suggestion=f"Set [{col}] type explicitly in Power Query. Non-conforming values become errors.",
                explanation=(
                    f"The column [{col}] was detected as type '{cp.inferred_type}' "
                    f"but some values could not be converted. This means the column "
                    f"contains a mix of types — for example, mostly numbers but some text "
                    f"like 'N/A' or 'unknown'. Power BI will either reject those values "
                    f"or convert the whole column to text, breaking all calculations."
                ),
                stats={"Inferred type": cp.inferred_type}
            ))

        # ── HIGH CARDINALITY ──────────────────────────────────────────────────
        if cp.cardinality_flag == "HIGH" and cp.inferred_type == "categorical":
            self.issues.append(Issue(
                severity="LOW", category="INTEGRITY",
                table=table, column=col,
                description=f"[{col}] has high cardinality ({cp.unique_count} unique values) for a categorical field",
                affected=0,
                suggestion=f"Consider normalising [{col}] into a lookup table if values repeat across records.",
                explanation=(
                    f"A categorical column is expected to have a limited set of values "
                    f"(like product types or portfolio codes). [{col}] has {cp.unique_count} "
                    f"unique values out of {profile.row_count} rows ({cp.unique_pct:.1f}% unique). "
                    f"This may indicate the column is actually a free-text field or an identifier "
                    f"that was misclassified. High cardinality categorical columns perform poorly "
                    f"in Power BI slicers and visual filters."
                ),
                stats={"Unique values": cp.unique_count,
                       "Unique %": f"{cp.unique_pct:.1f}%",
                       "Total rows": profile.row_count}
            ))

    # ── referential integrity ─────────────────────────────────────────────────

    def _check_referential_integrity(self):
        for rel in self.relationships:
            if rel.confidence == "LOW":
                continue
            if rel.from_table not in self.tables or rel.to_table not in self.tables:
                continue
            from_df = self.tables[rel.from_table]
            to_df   = self.tables[rel.to_table]
            if rel.from_column not in from_df.columns or rel.to_column not in to_df.columns:
                continue
            from_vals = set(from_df[rel.from_column].dropna().unique())
            to_vals   = set(to_df[rel.to_column].dropna().unique())
            orphaned  = from_vals - to_vals
            if orphaned:
                count = int(from_df[rel.from_column].isin(orphaned).sum())
                mask  = from_df[rel.from_column].isin(orphaned)
                self.issues.append(Issue(
                    severity="HIGH" if rel.confidence == "HIGH" else "MEDIUM",
                    category="REFERENTIAL",
                    table=rel.from_table, column=rel.from_column,
                    description=(
                        f"{count} value(s) in [{rel.from_table}.{rel.from_column}] "
                        f"not found in [{rel.to_table}.{rel.to_column}] "
                        f"— {len(orphaned)} orphaned codes"
                    ),
                    affected=count,
                    examples=sorted(str(v) for v in list(orphaned)[:5]),
                    suggestion=(
                        f"Orphaned codes must be added to [{rel.to_table}] "
                        f"or excluded — never dropped silently."
                    ),
                    explanation=(
                        f"The column [{rel.from_column}] in [{rel.from_table}] is a link "
                        f"to [{rel.to_table}.{rel.to_column}]. "
                        f"This link was auto-detected because {rel.match_pct:.0f}% of values overlap. "
                        f"{len(orphaned)} value(s) in [{rel.from_table}] have no matching entry "
                        f"in [{rel.to_table}]. In Power BI, those rows will be excluded from "
                        f"all joined calculations — they become invisible in your reports."
                    ),
                    snippet_rows=_capture_snippet(from_df, mask,
                                                  rel.from_column, "ORPHANED"),
                    stats={"Orphaned values": len(orphaned),
                           "Affected rows": count,
                           "Relationship confidence": rel.confidence,
                           "Detection reason": rel.reason}
                ))

    # ── temporal alignment ────────────────────────────────────────────────────

    def _check_temporal_alignment(self):
        date_ranges = {}
        for name, profile in self.profiles.items():
            for col, cp in profile.columns.items():
                if cp.inferred_type == "date" and cp.date_min and cp.date_max:
                    date_ranges[(name, col)] = (cp.date_min, cp.date_max)
        if len(date_ranges) < 2:
            return
        ref_key = max(date_ranges,
                      key=lambda k: (date_ranges[k][1]-date_ranges[k][0]).days)
        ref_min, ref_max = date_ranges[ref_key]
        ref_table, ref_col = ref_key

        for (name, col), (d_min, d_max) in date_ranges.items():
            if (name, col) == ref_key:
                continue
            if d_min > ref_min:
                gap = (d_min - ref_min).days
                self.issues.append(Issue(
                    severity="LOW", category="TEMPORAL",
                    table=name, column=col,
                    is_observation=True,
                    description=(
                        f"[{name}.{col}] starts {d_min.date()} — "
                        f"[{ref_table}.{ref_col}] goes back to {ref_min.date()} "
                        f"({gap}-day gap)"
                    ),
                    affected=0,
                    suggestion=(
                        f"Verify whether [{name}] coverage is expected to start "
                        f"at {d_min.date()}. If intentional, no action required. "
                        f"DAX LASTNONBLANK handles missing early dates gracefully."
                    ),
                    explanation=(
                        f"The table [{ref_table}] has data going back to {ref_min.date()}. "
                        f"The table [{name}] only starts from {d_min.date()} — "
                        f"{gap} days later. This is not an error. "
                        f"It is normal for reference tables (like monthly prices) to cover "
                        f"a different date range than transaction tables. "
                        f"The DAX measure LASTNONBLANK automatically uses the most recent "
                        f"available value, so no data is lost."
                    ),
                    stats={"Gap in days": gap,
                           f"{name} starts": str(d_min.date()),
                           f"{ref_table} starts": str(ref_min.date())}
                ))
            if d_max < ref_max:
                gap = (ref_max - d_max).days
                self.issues.append(Issue(
                    severity="LOW", category="TEMPORAL",
                    table=name, column=col,
                    is_observation=True,
                    description=(
                        f"[{name}.{col}] ends {d_max.date()} — "
                        f"[{ref_table}.{ref_col}] goes up to {ref_max.date()} "
                        f"({gap}-day gap)"
                    ),
                    affected=0,
                    suggestion=(
                        f"Confirm whether [{name}] is expected to end at {d_max.date()}. "
                        f"If intentional (e.g. month-end snapshot), no action required."
                    ),
                    explanation=(
                        f"The table [{ref_table}] has data up to {ref_max.date()}. "
                        f"The table [{name}] ends at {d_max.date()} — "
                        f"{gap} days earlier. This is expected for month-end snapshot tables "
                        f"(like portfolio prices) that are updated less frequently than "
                        f"daily transaction feeds. No action is needed."
                    ),
                    stats={"Gap in days": gap,
                           f"{name} ends": str(d_max.date()),
                           f"{ref_table} ends": str(ref_max.date())}
                ))

    # ── disconnected table detection ──────────────────────────────────────────

    def _check_disconnected_tables(self):
        connected = set()
        for rel in self.relationships:
            if rel.confidence not in ("HIGH", "MEDIUM"):
                continue
            from_prof = self.profiles.get(rel.from_table)
            if not from_prof:
                continue
            col_type = from_prof.columns.get(
                rel.from_column, ColumnProfile(rel.from_column)
            ).inferred_type
            if col_type in ("id", "categorical"):
                connected.add(rel.from_table)
                connected.add(rel.to_table)

        for table in set(self.tables.keys()) - connected:
            profile = self.profiles.get(table)
            if not profile:
                continue
            date_cols = [c for c, cp in profile.columns.items()
                         if cp.inferred_type == "date"]
            self.issues.append(Issue(
                severity="LOW", category="ARCHITECTURE",
                table=table, column=None,
                is_observation=True,
                description=(
                    f"[{table}] has no direct key relationship to other tables"
                    + (f" — date column(s) [{', '.join(date_cols)}] "
                       "suggest DAX date logic is required" if date_cols else "")
                ),
                affected=0,
                suggestion=(
                    "Do NOT create a direct relationship in Power BI. "
                    "Connect via DAX using LASTNONBLANK on the date column."
                ),
                explanation=(
                    f"The table [{table}] does not share any ID or code column "
                    f"with other tables in the dataset. This means it cannot be "
                    f"connected using Power BI's standard relationship panel. "
                    + (f"It does have a date column ([{date_cols[0]}]), which suggests "
                       f"it should be connected through time — specifically using the DAX "
                       f"function LASTNONBLANK, which looks up the most recent available "
                       f"value as of each transaction date. This is exactly how the "
                       f"Portfolio Value and FX rate measures are built in this model."
                       if date_cols else "")
                ),
                stats={"Date columns found": ", ".join(date_cols) if date_cols else "None",
                       "Reason": "No shared key column detected with any other table"}
            ))

    # ── business rules ────────────────────────────────────────────────────────

    def _check_business_rules(self):
        for rule in self.config.get("business_rules", []):
            applies   = rule.get("applies_to")
            validator = rule.get("validator")
            if not validator or applies not in self.tables:
                continue
            df      = self.tables[applies]
            rule_id = rule.get("id", "?")

            if validator == "column_exists":
                col = rule.get("column")
                if col and col not in df.columns:
                    self.issues.append(Issue(
                        severity="HIGH", category="STRUCTURE",
                        table=applies, column=col,
                        description=f"[{rule_id}] Required column [{col}] missing from [{applies}]",
                        affected=len(df),
                        suggestion=rule.get("note", f"Column [{col}] must exist in [{applies}]."),
                        explanation=(
                            f"The config defines that [{applies}] must contain a column "
                            f"named [{col}]. That column is not present in the current dataset. "
                            f"Any DAX measure or Power Query step referencing [{col}] will "
                            f"fail immediately when the data is loaded."
                        )
                    ))
            elif validator == "allowed_values":
                col    = rule.get("column")
                values = rule.get("values", [])
                if col and col in df.columns and values:
                    unknown = set(df[col].dropna().unique()) - set(values)
                    if unknown:
                        count = int(df[col].isin(unknown).sum())
                        mask  = df[col].isin(unknown)
                        self.issues.append(Issue(
                            severity="LOW", category="INTEGRITY",
                            table=applies, column=col,
                            description=(
                                f"[{rule_id}] [{col}] contains values outside "
                                f"allowed set: {sorted(str(v) for v in unknown)}"
                            ),
                            affected=count,
                            suggestion=rule.get("note", f"Map unknown [{col}] values."),
                            explanation=(
                                f"The config specifies that [{col}] in [{applies}] "
                                f"should only contain: {values}. "
                                f"The current dataset contains {len(unknown)} additional value(s): "
                                f"{sorted(str(v) for v in unknown)}. "
                                f"These may be new product types or data entry variations "
                                f"that need to be mapped before loading into Power BI."
                            ),
                            snippet_rows=_capture_snippet(df, mask, col, "UNKNOWN VALUE")
                        ))

    # ── helpers ───────────────────────────────────────────────────────────────

    def _null_suggestion(self, cp):
        if cp.inferred_type == "date":
            return f"If [{cp.name}] is a time series, forward-fill. Otherwise investigate source."
        if cp.inferred_type == "numeric":
            return f"For [{cp.name}]: use median imputation or flag as missing — do not default to 0."
        if cp.is_likely_pk:
            return f"Primary key [{cp.name}] must never be null — investigate source immediately."
        if cp.is_likely_fk:
            return f"FK [{cp.name}] has nulls — these rows will not join and will be excluded from reports."
        return f"Investigate nulls in [{cp.name}] — determine if expected or a source issue."