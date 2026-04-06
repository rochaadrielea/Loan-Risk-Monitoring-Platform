# data_quality/profiler.py
# Schema Discovery Engine
# Receives ANY dataframes — infers types, keys, relationships, anomalies
# No prior knowledge of the data required.
# Author: Adriele Rocha Weisz

import re
import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from typing import Optional


# ─────────────────────────────────────────────────────────────────────────────
# COLUMN PROFILE — everything discovered about one column
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ColumnProfile:
    name:             str
    inferred_type:    str = "unknown"
    null_count:       int   = 0
    null_pct:         float = 0.0
    unique_count:     int   = 0
    unique_pct:       float = 0.0
    is_likely_pk:     bool  = False
    is_likely_fk:     bool  = False

    # numeric stats
    num_min:          Optional[float] = None
    num_max:          Optional[float] = None
    num_mean:         Optional[float] = None
    num_std:          Optional[float] = None
    num_q1:           Optional[float] = None
    num_q3:           Optional[float] = None
    num_iqr:          Optional[float] = None
    outlier_count:    int   = 0
    outlier_fence_lo: Optional[float] = None
    outlier_fence_hi: Optional[float] = None
    has_negatives:    bool  = False

    # date stats
    date_min:         Optional[pd.Timestamp] = None
    date_max:         Optional[pd.Timestamp] = None
    date_gaps:        int   = 0
    date_future:      int   = 0
    date_parse_fail:  int   = 0

    # categorical stats
    top_values:       list  = field(default_factory=list)
    cardinality_flag: str   = ""   # "OK" | "HIGH" | "LOW"

    # quality flags
    has_mixed_types:     bool = False
    has_encoding_issues: bool = False
    sample_values:       list = field(default_factory=list)


# ─────────────────────────────────────────────────────────────────────────────
# TABLE PROFILE
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class TableProfile:
    name:              str
    row_count:         int
    col_count:         int
    columns:           dict = field(default_factory=dict)
    likely_pk:         Optional[str] = None
    duplicate_rows:    int  = 0
    duplicate_pk_rows: int  = 0
    fully_empty_rows:  int  = 0
    notes:             list = field(default_factory=list)


# ─────────────────────────────────────────────────────────────────────────────
# RELATIONSHIP DISCOVERY
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class RelationshipHint:
    from_table:  str
    from_column: str
    to_table:    str
    to_column:   str
    confidence:  str    # "HIGH" | "MEDIUM" | "LOW"
    match_pct:   float
    reason:      str


# ─────────────────────────────────────────────────────────────────────────────
# SCHEMA PROFILER
# ─────────────────────────────────────────────────────────────────────────────

# FIX 4: minimum rows before running structural checks that make no sense on
# tiny lookup tables (e.g. portfolios has 2 rows — cardinality checks are noise)
MIN_ROWS_FOR_CARDINALITY = 15
MIN_ROWS_FOR_PK_CHECKS   = 5


class SchemaProfiler:

    DATE_PATTERNS = [
        r"^\d{4}-\d{2}-\d{2}$",
        r"^\d{2}[./-]\d{2}[./-]\d{4}$",
        r"^\d{4}[./-]\d{2}[./-]\d{2}",
    ]
    ID_PATTERNS = [
        r"^[A-Z]{2,4}-[A-Z0-9]{4,}",
        r"^[A-Z0-9]+-\d+$",
        r"^\d{6,}$",
        r"^[A-F0-9]{8}-",
    ]

    def __init__(self, tables: dict):
        self.tables        = tables
        self.profiles: dict[str, TableProfile]     = {}
        self.relationships: list[RelationshipHint] = []

    def run(self) -> tuple[dict, list]:
        for name, df in self.tables.items():
            self.profiles[name] = self._profile_table(name, df)
        self._discover_relationships()
        return self.profiles, self.relationships

    # ── table profiling ───────────────────────────────────────────────────────

    def _profile_table(self, name: str, df: pd.DataFrame) -> TableProfile:
        tp = TableProfile(
            name      = name,
            row_count = len(df),
            col_count = len(df.columns),
        )
        tp.duplicate_rows   = df.duplicated().sum()
        tp.fully_empty_rows = (df.isna().all(axis=1)).sum()

        for col in df.columns:
            tp.columns[col] = self._profile_column(col, df[col], len(df))

        # FIX 4: only infer PK on tables with enough rows to be meaningful
        if len(df) >= MIN_ROWS_FOR_PK_CHECKS:
            tp.likely_pk = self._infer_pk(df, tp.columns)
            if tp.likely_pk:
                tp.duplicate_pk_rows = df.duplicated(
                    subset=[tp.likely_pk], keep=False).sum()
                tp.columns[tp.likely_pk].is_likely_pk = True

        return tp

    # ── column profiling ──────────────────────────────────────────────────────

    def _profile_column(self, name: str, series: pd.Series,
                        total: int) -> ColumnProfile:
        cp = ColumnProfile(name=name)

        cp.null_count   = series.isna().sum()
        cp.null_pct     = round(cp.null_count / total * 100, 2) if total else 0
        cp.unique_count = series.nunique(dropna=True)
        cp.unique_pct   = round(cp.unique_count / total * 100, 2) if total else 0
        cp.sample_values = series.dropna().head(5).tolist()

        if series.dtype == object:
            sample_str = series.dropna().astype(str)
            cp.has_encoding_issues = sample_str.str.contains(
                r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f\ufffd]',
                regex=True, na=False
            ).any()

        cp.inferred_type = self._infer_type(name, series)

        if cp.inferred_type == "date":
            self._profile_dates(cp, series)
        elif cp.inferred_type == "numeric":
            self._profile_numeric(cp, series)
        elif cp.inferred_type in ("categorical", "id"):
            self._profile_categorical(cp, series, total)

        if series.dtype == object and cp.inferred_type == "numeric":
            non_numeric = (
                pd.to_numeric(series, errors="coerce").isna().sum()
                - series.isna().sum()
            )
            cp.has_mixed_types = non_numeric > 0

        fk_indicators = ["-code", "-id", "-key", "_code", "_id", "_key",
                         "code", "id", "key", "ref", "type"]
        if (any(ind in name.lower() for ind in fk_indicators)
                and cp.unique_pct < 80
                and not cp.is_likely_pk):
            cp.is_likely_fk = True

        return cp

    # ── type inference ────────────────────────────────────────────────────────

    def _infer_type(self, col_name: str, series: pd.Series) -> str:
        if pd.api.types.is_datetime64_any_dtype(series):
            return "date"
        if pd.api.types.is_numeric_dtype(series):
            return "numeric"
        if pd.api.types.is_bool_dtype(series):
            return "boolean"

        if series.dtype == object:
            sample = series.dropna().head(100).astype(str)
            if sample.empty:
                return "text"

            date_hits = sample.apply(lambda v: any(
                re.match(p, v.strip()) for p in self.DATE_PATTERNS)).sum()
            if date_hits / len(sample) > 0.7:
                parsed = pd.to_datetime(series, errors="coerce", dayfirst=True)
                if parsed.notna().sum() / max(series.notna().sum(), 1) > 0.7:
                    return "date"

            numeric_series = pd.to_numeric(series, errors="coerce")
            num_hits = numeric_series.notna().sum()
            if num_hits / max(series.notna().sum(), 1) > 0.85:
                return "numeric"

            id_hits = sample.apply(lambda v: any(
                re.match(p, v.strip()) for p in self.ID_PATTERNS)).sum()
            if id_hits / len(sample) > 0.6:
                return "id"

            unique_ratio = series.nunique(dropna=True) / max(
                len(series.dropna()), 1)
            if unique_ratio < 0.3 or series.nunique(dropna=True) <= 30:
                return "categorical"

            return "text"

        return "text"

    # ── numeric profiling ─────────────────────────────────────────────────────

    def _profile_numeric(self, cp: ColumnProfile, series: pd.Series):
        nums = pd.to_numeric(series, errors="coerce").dropna()
        if nums.empty:
            return
        cp.num_min  = round(float(nums.min()),  6)
        cp.num_max  = round(float(nums.max()),  6)
        cp.num_mean = round(float(nums.mean()), 6)
        cp.num_std  = round(float(nums.std()),  6)
        cp.num_q1   = round(float(nums.quantile(0.25)), 6)
        cp.num_q3   = round(float(nums.quantile(0.75)), 6)
        cp.num_iqr  = round(cp.num_q3 - cp.num_q1, 6)
        cp.has_negatives = bool((nums < 0).any())

        if cp.num_iqr and cp.num_iqr > 0:
            cp.outlier_fence_lo = round(cp.num_q1 - 3 * cp.num_iqr, 6)
            cp.outlier_fence_hi = round(cp.num_q3 + 3 * cp.num_iqr, 6)
            cp.outlier_count    = int(
                ((nums < cp.outlier_fence_lo)
                 | (nums > cp.outlier_fence_hi)).sum()
            )

    # ── date profiling ────────────────────────────────────────────────────────

    def _profile_dates(self, cp: ColumnProfile, series: pd.Series):
        parsed = pd.to_datetime(series, errors="coerce", dayfirst=True)
        cp.date_parse_fail = int(parsed.isna().sum() - series.isna().sum())
        valid = parsed.dropna().sort_values()
        if valid.empty:
            return

        cp.date_min    = valid.min()
        cp.date_max    = valid.max()
        cp.date_future = int((valid > pd.Timestamp.now()).sum())

        # FIX 2: weekend-aware gap detection
        # For daily/near-daily series use business-day gaps — ignores Fri→Mon
        # For weekly/monthly series fall back to the 3×median heuristic
        if len(valid) > 5:
            diffs = valid.diff().dropna()
            median_diff = diffs.median()

            if median_diff and median_diff > pd.Timedelta(0):
                if median_diff <= pd.Timedelta(days=2):
                    # Daily or near-daily: count *business-day* gaps
                    dates_d = valid.values.astype("datetime64[D]")
                    bday_gaps = np.busday_count(dates_d[:-1], dates_d[1:])
                    # A gap >1 business day is genuinely missing data
                    cp.date_gaps = int((bday_gaps > 1).sum())
                else:
                    # Weekly / monthly: use the 3×median calendar heuristic
                    cp.date_gaps = int((diffs > median_diff * 3).sum())

    # ── categorical profiling ─────────────────────────────────────────────────

    def _profile_categorical(self, cp: ColumnProfile,
                             series: pd.Series, total: int):
        vc = series.value_counts()
        cp.top_values = [(str(v), int(c)) for v, c in vc.head(10).items()]

        n_unique = series.nunique(dropna=True)

        # FIX 1: skip cardinality check entirely for tiny tables —
        # a 2-row lookup with 2 unique values is not a cardinality problem
        if total < MIN_ROWS_FOR_CARDINALITY:
            cp.cardinality_flag = "OK"
        elif n_unique == 1:
            cp.cardinality_flag = "LOW"
        elif n_unique > total * 0.5 and n_unique > 20:
            # also require absolute threshold — 12/20 is not really HIGH
            cp.cardinality_flag = "HIGH"
        else:
            cp.cardinality_flag = "OK"

    # ── primary key inference ─────────────────────────────────────────────────

    def _infer_pk(self, df: pd.DataFrame,
                  col_profiles: dict) -> Optional[str]:
        scores = {}
        pk_name_patterns = ["code", "id", "key", "ref", "number", "num"]

        for col, cp in col_profiles.items():
            if cp.null_count > 0:
                continue
            if cp.unique_pct < 99.0:
                continue

            score = 0
            col_lower = col.lower()
            if any(p in col_lower for p in pk_name_patterns):
                score += 3
            if cp.inferred_type in ("id", "categorical"):
                score += 2
            if list(col_profiles.keys()).index(col) == 0:
                score += 1
            scores[col] = score

        return max(scores, key=scores.get) if scores else None

    # ── relationship discovery ────────────────────────────────────────────────

    def _discover_relationships(self):
        table_names = list(self.profiles.keys())

        for t1 in table_names:
            for t2 in table_names:
                if t1 == t2:
                    continue

                prof1 = self.profiles[t1]
                prof2 = self.profiles[t2]

                for col1, cp1 in prof1.columns.items():
                    if cp1.inferred_type not in ("id", "categorical"):
                        continue
                    for col2, cp2 in prof2.columns.items():
                        if cp2.inferred_type not in ("id", "categorical"):
                            continue
                        if cp1.inferred_type != cp2.inferred_type:
                            continue

                        name_match = self._column_name_similarity(col1, col2)

                        vals1 = set(self.tables[t1][col1].dropna().unique())
                        vals2 = set(self.tables[t2][col2].dropna().unique())
                        if not vals1 or not vals2:
                            continue

                        overlap   = len(vals1 & vals2)
                        match_pct = overlap / len(vals1) * 100

                        if match_pct < 20 and name_match < 0.5:
                            continue
                        if match_pct < 5:
                            continue

                        if cp1.unique_pct <= cp2.unique_pct:
                            from_t, from_c = t1, col1
                            to_t,   to_c   = t2, col2
                        else:
                            from_t, from_c = t2, col2
                            to_t,   to_c   = t1, col1
                            match_pct = overlap / len(vals2) * 100

                        already = any(
                            r.from_table == from_t
                            and r.from_column == from_c
                            and r.to_table == to_t
                            and r.to_column == to_c
                            for r in self.relationships
                        )
                        if already:
                            continue

                        confidence = (
                            "HIGH"   if match_pct >= 80 and name_match >= 0.7
                            else "HIGH"   if match_pct >= 95
                            else "MEDIUM" if match_pct >= 50 or name_match >= 0.7
                            else "LOW"
                        )

                        reason_parts = []
                        if name_match >= 0.5:
                            reason_parts.append(
                                f"column names similar ({name_match:.0%})")
                        if match_pct > 0:
                            reason_parts.append(
                                f"{match_pct:.0f}% of values overlap")
                        if not reason_parts:
                            continue

                        self.relationships.append(RelationshipHint(
                            from_table  = from_t,
                            from_column = from_c,
                            to_table    = to_t,
                            to_column   = to_c,
                            confidence  = confidence,
                            match_pct   = round(match_pct, 1),
                            reason      = " + ".join(reason_parts),
                        ))

    def _column_name_similarity(self, a: str, b: str) -> float:
        def tokens(s):
            return set(re.split(r"[-_\s]", s.lower()))
        ta, tb = tokens(a), tokens(b)
        if not ta or not tb:
            return 0.0
        return len(ta & tb) / len(ta | tb)
