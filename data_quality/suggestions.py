# data_quality/suggestions.py
# Transformation Suggestion Engine
# Translates Issues into concrete Power Query / DAX instructions
# Author: Adriele Rocha Weisz

from .checks import Issue


class SuggestionEngine:

    def __init__(self, issues: list, config: dict):
        self.issues  = issues
        self.config  = config

    def generate(self) -> list:
        """
        Returns a list of dicts:
        { "id": str, "priority": int, "target": str, "action": str, "powerquery_hint": str }
        """
        suggestions = []
        seen        = set()

        for i, issue in enumerate(self.issues, start=1):
            key = (issue.category, issue.table, issue.column)
            if key in seen:
                continue
            seen.add(key)

            s = self._map_issue_to_suggestion(i, issue)
            if s:
                suggestions.append(s)

        # sort by priority (1 = most urgent)
        suggestions.sort(key=lambda x: x["priority"])
        return suggestions

    def _map_issue_to_suggestion(self, idx: int, issue: Issue) -> dict | None:

        base = {
            "id":               f"S-{idx:03d}",
            "severity":         issue.severity,
            "category":         issue.category,
            "table":            issue.table,
            "affected_rows":    issue.affected,
            "description":      issue.description,
            "action":           issue.suggestion,
            "powerquery_hint":  "",
            "dax_hint":         "",
            "priority":         {"HIGH": 1, "MEDIUM": 2, "LOW": 3}.get(issue.severity, 3)
        }

        # ── REFERENTIAL: orphaned product codes ──────────────────────────────
        if issue.category == "REFERENTIAL" and issue.table == "transactions":
            base["powerquery_hint"] = (
                "In Power Query → transactions table:\n"
                "  1. Add column: [is-mapped] = Table.Contains(products, [product-code])\n"
                "  2. Filter: keep only rows where [is-mapped] = true\n"
                "  OR: use a LEFT ANTI JOIN to isolate and log unmapped rows separately"
            )
            base["dax_hint"] = (
                "In your Loan Exposure measure, wrap with CALCULATE + FILTER:\n"
                "  CALCULATE([Loan Exposure], products[product-code] <> BLANK())"
            )

        # ── COMPLETENESS: missing FX values ───────────────────────────────────
        elif issue.category == "COMPLETENESS" and issue.table == "fx":
            base["powerquery_hint"] = (
                "In Power Query → fx table:\n"
                "  1. Sort by [price-date] ascending\n"
                "  2. Select [EURUSD] column\n"
                "  3. Transform → Fill → Fill Down\n"
                "  This forward-fills missing rates from the most recent available date."
            )

        # ── INTEGRITY: primary key duplicates ─────────────────────────────────
        elif issue.category == "INTEGRITY" and "duplicate" in issue.description.lower():
            col = issue.column or "product-code"
            base["powerquery_hint"] = (
                f"In Power Query → {issue.table} table:\n"
                f"  Option A (keep last): Sort by [{col}] desc + Remove Duplicates\n"
                f"  Option B (flag): Add column [is-duplicate] via Table.Distinct check\n"
                f"  Recommended: keep all records, add [version-flag] column, "
                f"  use the most recent suffix version in relationship joins"
            )

        # ── OUTLIER: anomalous ratio ──────────────────────────────────────────
        elif issue.category == "OUTLIER":
            col = issue.column or ""
            base["powerquery_hint"] = (
                f"In Power Query → {issue.table} table:\n"
                f"  Add conditional column [data-quality-flag]:\n"
                f"    = if [{col}] > threshold then \"FLAGGED\" else \"OK\"\n"
                f"  This preserves raw data while allowing visuals to filter flagged rows."
            )
            base["dax_hint"] = (
                f"In DAX, exclude flagged rows from averages:\n"
                f"  Avg LTV = CALCULATE(AVERAGE(products[loan-property-value-ratio]),\n"
                f"              products[data-quality-flag] = \"OK\")"
            )

        # ── TEMPORAL: date range gaps ─────────────────────────────────────────
        elif issue.category == "TEMPORAL":
            base["powerquery_hint"] = (
                f"In Power Query → {issue.table} table:\n"
                f"  After merging with transactions, add a step:\n"
                f"  [has-reference-data] = if [{issue.column}] = null then false else true\n"
                f"  Transactions without reference data will show as null in visuals — "
                f"  add a visual-level filter or tooltip to flag these."
            )

        # ── STRUCTURE: missing columns ────────────────────────────────────────
        elif issue.category == "STRUCTURE":
            base["powerquery_hint"] = (
                f"Stop: source file for '{issue.table}' is missing required columns.\n"
                f"Do not load this table until columns are confirmed present.\n"
                f"Contact the data provider to correct the source file."
            )

        else:
            # generic fallback
            base["powerquery_hint"] = issue.suggestion

        return base
