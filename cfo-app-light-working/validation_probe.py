"""Numeric-consistency validation probe.

Shared by two callers:

1. The orchestrator's `validate_consistency` notebook task. Runs over every
   cached chip in gold_persona_insights with full canonical context (firmwide
   totals + dashboard KPI values). Hard-fails the job if violations are found.

2. The live runtime synthesis path in genie_agent.py. Runs structural checks
   only (no SQL context — too slow at click-time). Caller decides whether to
   retry the synthesis or surface a warning footer.

DESIGN PRINCIPLES
=================
- DATA-AGNOSTIC. No check hardcodes "RPP should be $7M" or "NYC should show
  overage." Every check is either a universal arithmetic impossibility
  (subset > total, sum-of-parts ≠ stated total) or a relative consistency
  check between two surfaces in the same app (prose vs. canonical SQL).
- CUSTOMER-PORTABLE. The orchestrator pulls canonical totals from the
  customer's own gold tables at probe time. The probe is happy as long as
  prose / dashboard / Genie all agree on what the customer's data says.
- DETERMINISTIC + CHEAP. Regex layer only. The LLM-assisted layer lives in
  the orchestrator notebook because we can spend 5-10s/chip there; live
  callers must stay sub-second.

A violation is a dict: {type, message, snippet?}.
"""

from __future__ import annotations

import re
from typing import Any


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_money(value: str, unit: str) -> float:
    """Convert "12.5", "M" → 12_500_000. Empty unit → raw dollars."""
    n = float(value.replace(",", "").strip())
    u = (unit or "").upper().strip()
    if u == "K":
        return n * 1_000
    if u == "M":
        return n * 1_000_000
    if u == "B":
        return n * 1_000_000_000
    return n


def _parse_count(value: str) -> int:
    """Parse a count with optional commas, e.g. '17,682' → 17682."""
    return int(value.replace(",", "").strip())


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------

# Match a "stated total ... distributed across A (X), B (Y), and C (Z)" claim,
# anchored on common change-verbs that introduce a headcount/count delta.
_SUM_OF_PARTS_RE = re.compile(
    r"\b(declined|increased|decreased|reduced|grew|dropped|rose|fell|added|lost|gained)\s+by\s+"
    r"(?P<total>\d[\d,]*)\s+"
    r"(?P<unit>[A-Za-z\- ]+?)"
    r"(?:[,\.]?\s+(?:in|with|across)?\s*[^.]*?distributed\s+across|"
    r"[,\.]?\s+(?:in|with)?\s*[^.]*?with\s+exits?\s+(?:in|across)|"
    r"[,\.]?\s+(?:in|with)?\s*[^.]*?in\b)\s+"
    r"(?P<parts>[^.]+?)(?:\.|$)",
    re.IGNORECASE,
)

# Inside the `parts` section, capture per-item "(N)" counts. Tolerates
# trailing labels like "(3 exits)" by grabbing the leading integer.
_PART_COUNT_RE = re.compile(r"\(\s*(\d[\d,]*)\b")


def _check_sum_of_parts(prose: str) -> list[dict]:
    """If prose says 'declined by N ... distributed across A (X), B (Y), C (Z)',
    enforce X + Y + Z == N. Anchored on the common 'change-verb by N' pattern
    so we don't false-positive on random numeric prose."""
    violations: list[dict] = []
    for m in _SUM_OF_PARTS_RE.finditer(prose):
        stated = _parse_count(m.group("total"))
        parts_text = m.group("parts")
        parts = [_parse_count(pm.group(1)) for pm in _PART_COUNT_RE.finditer(parts_text)]
        if not parts:
            continue
        if sum(parts) != stated:
            violations.append({
                "type": "sum_of_parts_mismatch",
                "message": (
                    f"Stated change of {stated} doesn't equal sum of listed parts "
                    f"{sum(parts)} (parts: {parts})"
                ),
                "snippet": m.group(0)[:200],
            })
    return violations


def _check_subset_exceeds_total(prose: str, canonical_totals: dict[str, int] | None) -> list[dict]:
    """If prose claims '17,682 low-utilization employees' but the firmwide
    total from the gold table is 6,044, that's an impossibility. Only runs
    when canonical_totals is supplied (orchestrator path)."""
    if not canonical_totals:
        return []
    violations: list[dict] = []

    patterns: dict[str, re.Pattern] = {
        "low_utilization_employees": re.compile(
            r"(\d[\d,]*)\s+low[- ]utili[sz]ation\s+(?:employees|headcount)", re.IGNORECASE
        ),
        "partners": re.compile(r"(\d[\d,]*)\s+(?:active\s+)?partners?\b", re.IGNORECASE),
        "active_engagements": re.compile(r"(\d[\d,]*)\s+active\s+engagements?\b", re.IGNORECASE),
    }

    for key, pat in patterns.items():
        total = canonical_totals.get(key)
        if not total:
            continue
        for m in pat.finditer(prose):
            n = _parse_count(m.group(1))
            if n > total:
                violations.append({
                    "type": "subset_exceeds_total",
                    "message": (
                        f"Prose claims {n:,} {key.replace('_', ' ')} but firmwide total "
                        f"from gold table is {total:,} — subset can't exceed total"
                    ),
                    "snippet": prose[max(0, m.start()-40):m.end()+40],
                })
    return violations


# Per-unit scale claims: "$210K per partner", "$2.5M per partner",
# "($1.2M revenue per partner)". Tolerates ranges like "$210K-$280K per partner".
_PER_UNIT_RE = re.compile(
    r"\$\s*([\d,.]+)\s*([KMB]?)\s*(?:[–-]\s*\$?\s*[\d,.]+\s*[KMB]?\s*)?"
    r"(?:revenue\s+)?per\s+(partner|employee|client|project)\b",
    re.IGNORECASE,
)


def _check_scale_mismatch(prose: str, canonical_kpis: dict[str, float] | None) -> list[dict]:
    """If prose says '$210K per partner' but canonical RPP is $7.8M, that's
    a >10× scale mismatch — flag. Only runs when canonical_kpis supplied."""
    if not canonical_kpis:
        return []
    key_by_unit = {
        "partner": "revenue_per_partner",
        "employee": "revenue_per_employee",
    }
    violations: list[dict] = []
    for m in _PER_UNIT_RE.finditer(prose):
        unit = m.group(3).lower()
        kpi_key = key_by_unit.get(unit)
        if not kpi_key:
            continue
        canonical = canonical_kpis.get(kpi_key)
        if not canonical:
            continue
        try:
            val = _parse_money(m.group(1), m.group(2))
        except ValueError:
            continue
        if val <= 0 or canonical <= 0:
            continue
        ratio = val / canonical
        if ratio < 0.1 or ratio > 10:
            # Express the gap as a single ≥1× factor for readability.
            factor = ratio if ratio >= 1 else 1.0 / ratio
            direction = "higher" if ratio >= 1 else "lower"
            violations.append({
                "type": "scale_mismatch",
                "message": (
                    f"Prose cites ${val:,.0f} per {unit} but canonical "
                    f"{kpi_key.replace('_', ' ')} from dashboard is ${canonical:,.0f} — "
                    f"prose is {factor:.0f}× {direction}"
                ),
                "snippet": prose[max(0, m.start()-40):m.end()+40],
            })
    return violations


# Scope-qualifier tokens that indicate the DSO claim is per-location/practice/
# region, not firmwide. If any of these appears within ~80 chars of the DSO
# match, skip the firmwide-canonical drift check (the prose is talking about
# a subset, which is legitimately allowed to differ from firmwide).
_SCOPE_QUALIFIERS = re.compile(
    r"\b(In|For|Across|At)\s+[A-Z][\w&\s\-]+(,|\s+(DSO|the|office|practice|region))",
    re.IGNORECASE,
)
# Office names — exact-match catches "Seoul DSO collapsed..." idioms.
_OFFICE_TOKENS = {
    "Amsterdam", "Atlanta", "Bangkok", "Chicago", "Dubai", "Frankfurt",
    "Hong Kong", "Houston", "London", "Milan", "Mumbai", "Munich",
    "New York", "Paris", "San Francisco", "Sao Paulo", "Seoul", "Shanghai",
    "Singapore", "Sydney", "Tokyo", "Toronto", "Washington DC", "Zurich",
    "Americas", "EMEA", "Asia Pacific",
    # Practice areas
    "Strategy & Consulting", "Technology", "Operations", "Audit", "Tax",
    "Accounting", "Managed Services",
}


def _check_kpi_drift(prose: str, canonical_kpis: dict[str, float] | None) -> list[dict]:
    """For named KPIs (DSO, total revenue, etc.) explicitly mentioned in
    prose, check that the cited value is within ±2% of the canonical SQL
    value from the dashboard. Catches cases where Genie / synthesis
    silently uses a different aggregation than the canonical dashboard.

    Skips the check when the DSO mention is clearly scoped to a city/
    practice/region — those are legitimately allowed to differ from the
    firmwide canonical and shouldn't be flagged as drift.
    """
    if not canonical_kpis:
        return []
    violations: list[dict] = []

    # DSO: "DSO of 50.2 days" or "Days Sales Outstanding: 50.2 days"
    dso = canonical_kpis.get("dso")
    if dso:
        for m in re.finditer(
            r"(?:DSO|Days\s+Sales\s+Outstanding)\s*(?:of|:|=|is)?\s*([\d.]+)\s*days?",
            prose, re.IGNORECASE,
        ):
            try:
                val = float(m.group(1))
            except ValueError:
                continue
            # Skip drift check if the prose mentions a subset scope nearby
            # (city / region / practice). The firmwide canonical comparison
            # is invalid for those — they're allowed to differ.
            ctx_start = max(0, m.start() - 80)
            ctx_end = min(len(prose), m.end() + 80)
            ctx = prose[ctx_start:ctx_end]
            is_scoped = (_SCOPE_QUALIFIERS.search(ctx) is not None
                         or any(tok in ctx for tok in _OFFICE_TOKENS))
            if is_scoped:
                continue
            if dso > 0 and abs(val - dso) / dso > 0.02:
                violations.append({
                    "type": "kpi_drift",
                    "message": f"Prose says firmwide DSO = {val:.1f} days; canonical dashboard DSO = {dso:.1f} days (>2% drift)",
                    "snippet": prose[max(0, m.start()-30):m.end()+30],
                })

    return violations


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def find_violations_in_prose(prose: str, context: dict[str, Any] | None = None) -> list[dict]:
    """Run the regex-based consistency checks on a synthesized response.

    Args:
        prose: the markdown response to check.
        context: optional dict with:
            "canonical_totals": {metric_key → int firmwide total}
            "canonical_kpis":   {kpi_key → float canonical value from dashboard SQL}
        Pass None / empty context to run STRUCTURAL checks only — useful for
        the live synthesis path where SQL roundtrips are too slow.

    Returns:
        list of violation dicts. Empty list = clean response.
    """
    context = context or {}
    canonical_totals = context.get("canonical_totals") or {}
    canonical_kpis = context.get("canonical_kpis") or {}

    violations: list[dict] = []
    # Structural checks — always run, no external context required.
    violations.extend(_check_sum_of_parts(prose))
    # Context-dependent checks — only when canonical values are supplied.
    violations.extend(_check_subset_exceeds_total(prose, canonical_totals))
    violations.extend(_check_scale_mismatch(prose, canonical_kpis))
    violations.extend(_check_kpi_drift(prose, canonical_kpis))
    return violations


# ---------------------------------------------------------------------------
# Prose-table value-match check (B4a — added 2026-05-20)
# ---------------------------------------------------------------------------

# Match $X, $X.XM, $X.XXM, $X.XB, $X,XXX,XXX, $X,XXX,XXX.XX
_DOLLAR_VALUE_RE = re.compile(
    r"\$\s*([\d,]+(?:\.\d+)?)\s*([KMB]?)\b",
    re.IGNORECASE,
)
# Match X%, X.XX%, X.XXpp (percentage points)
_PCT_VALUE_RE = re.compile(r"(?<![\d.])([\d]+(?:\.\d+)?)\s*(%|pp)\b")
# Match markdown table rows: lines that start and end with |
_TABLE_ROW_RE = re.compile(r"^\s*\|.+\|\s*$", re.MULTILINE)


def _extract_table_text(prose: str) -> tuple[str, str]:
    """Split prose into (table_text, non_table_text). Markdown tables are
    contiguous blocks of lines starting AND ending with `|`. Returns the
    concatenation of all such lines as `table_text` and the rest as
    `non_table_text`.
    """
    table_lines: list[str] = []
    non_table_lines: list[str] = []
    for line in prose.splitlines():
        if _TABLE_ROW_RE.match(line):
            table_lines.append(line)
        else:
            non_table_lines.append(line)
    return "\n".join(table_lines), "\n".join(non_table_lines)


def _normalize_dollar(raw: str, unit: str) -> float:
    """Convert ($123.45, M) → 123_450_000.0 dollars. Strips commas."""
    n = float(raw.replace(",", "").strip())
    u = (unit or "").upper().strip()
    if u == "K":
        return n * 1_000
    if u == "M":
        return n * 1_000_000
    if u == "B":
        return n * 1_000_000_000
    return n


def _table_contains_dollar(table_text: str, target: float) -> bool:
    """Check whether the target dollar value (in plain dollars) appears in
    any table cell within a tight relative tolerance (±0.5%) OR an absolute
    tolerance of $1K for values under $100K. Returns True on first match.
    """
    if target <= 0:
        return False
    # Relative tolerance for any non-trivial number; tight absolute below $100K.
    if target < 100_000:
        atol = 1_000.0
        rtol = 0.005
    else:
        atol = 0.0
        rtol = 0.005
    for m in _DOLLAR_VALUE_RE.finditer(table_text):
        try:
            cell = _normalize_dollar(m.group(1), m.group(2))
        except ValueError:
            continue
        if cell <= 0:
            continue
        diff = abs(cell - target)
        if diff <= atol:
            return True
        if cell > 0 and diff / max(cell, target) <= rtol:
            return True
    return False


def _table_contains_pct(table_text: str, target: float, unit: str) -> bool:
    """Check whether the target percentage value appears in any table cell
    within ±0.1pp (percentage-point) tolerance. Same for `pp` units.
    """
    for m in _PCT_VALUE_RE.finditer(table_text):
        try:
            cell = float(m.group(1))
        except ValueError:
            continue
        cell_unit = m.group(2)
        # Match unit family: % matches %, pp matches pp.
        if (unit == "%" and cell_unit != "%") or (unit == "pp" and cell_unit != "pp"):
            continue
        if abs(cell - target) <= 0.1:
            return True
    return False


def _check_prose_values_in_tables(prose: str) -> list[dict]:
    """For every $X.XM / X.XX% value cited in non-table prose, verify it
    appears in at least one displayed table cell in the same response
    (within ±0.5% relative tolerance for $ and ±0.1pp for %).

    Skips prose with no tables (nothing to compare against).
    Skips prose values < $1K and percentages 0 / 100 / round small integers
    (too common to bother matching — false-positive prone).
    """
    table_text, non_table_text = _extract_table_text(prose)
    if not table_text.strip():
        # No tables in the response — can't apply this check.
        return []

    violations: list[dict] = []

    # Dollar values cited in non-table prose
    seen_dollar_misses: set[float] = set()
    for m in _DOLLAR_VALUE_RE.finditer(non_table_text):
        try:
            target = _normalize_dollar(m.group(1), m.group(2))
        except ValueError:
            continue
        if target < 1_000:  # too small to track
            continue
        if target in seen_dollar_misses:
            continue
        if not _table_contains_dollar(table_text, target):
            seen_dollar_misses.add(target)
            violations.append({
                "type": "prose_value_not_in_table",
                "message": (
                    f"Prose cites ${m.group(1)}{m.group(2)} but no displayed table cell "
                    f"matches this value within ±0.5%. Likely hallucinated or pulled "
                    f"from an intermediate sub-query that wasn't rendered."
                ),
                "snippet": non_table_text[max(0, m.start()-50):m.end()+50],
            })

    # Percentage values cited in non-table prose
    seen_pct_misses: set[tuple[float, str]] = set()
    for m in _PCT_VALUE_RE.finditer(non_table_text):
        try:
            target = float(m.group(1))
        except ValueError:
            continue
        unit = m.group(2)
        # Skip round-number false-positive magnets
        if target in (0.0, 100.0) or (target == int(target) and target < 10):
            continue
        key = (target, unit)
        if key in seen_pct_misses:
            continue
        if not _table_contains_pct(table_text, target, unit):
            seen_pct_misses.add(key)
            violations.append({
                "type": "prose_value_not_in_table",
                "message": (
                    f"Prose cites {m.group(1)}{unit} but no displayed table cell "
                    f"matches this value within ±0.1pp. Likely hallucinated or pulled "
                    f"from an intermediate sub-query that wasn't rendered."
                ),
                "snippet": non_table_text[max(0, m.start()-50):m.end()+50],
            })

    return violations


# ---------------------------------------------------------------------------
# Data-gap language detector (B2 — added 2026-05-20)
# ---------------------------------------------------------------------------

# Phrases that signal "the AI is refusing to answer because the data isn't there".
# Banned per user feedback: the CFO came to the AI to GET an answer, not be told
# what isn't computable.
_DATA_GAP_PHRASES = [
    r"the supporting series returned \d+ of \d+",
    r"are present in aggregate but not enumerated above",
    r"denominator was not returned in the displayed rows",
    r"is suppressed for that month",
    r"the expense ledger does not decompose",
    r"attribution is not supported by the current",
    r"is not present in the gold layer",
    r"would require sub-ledger detail",
    r"is not supported by the current [\w\s]+ schema",
    r"the data layer does not (decompose|support|expose)",
    r"this slice does not carry",
    r"requires (additional )?(sub-ledger|sub-?category|line-item) detail",
    r"sub-category attribution \([^)]+\) is not supported",
    r"premise of the question is not supported",
    r"cannot be reconciled to the [\w\s-]+ ledger",
]

_DATA_GAP_RE = re.compile("|".join(_DATA_GAP_PHRASES), re.IGNORECASE)


def _check_data_gap_language(prose: str) -> list[dict]:
    """Flag any prose containing data-gap acknowledgment language. The
    SYNTHESIS_PROMPT bans these phrases but Opus still emits them
    occasionally. Surfacing as a violation triggers the retry path with
    the offending phrase named explicitly.
    """
    violations: list[dict] = []
    for m in _DATA_GAP_RE.finditer(prose):
        violations.append({
            "type": "data_gap_language",
            "message": (
                f"Prose contains banned data-gap acknowledgment language: "
                f"'{m.group(0)}'. Per SYNTHESIS_PROMPT, drop the section entirely "
                f"rather than narrating what the data doesn't support."
            ),
            "snippet": prose[max(0, m.start()-30):m.end()+30],
        })
    return violations


# ---------------------------------------------------------------------------
# LLM-assisted layer (orchestrator-only — too slow for live calls)
# ---------------------------------------------------------------------------
# The regex layer catches the SPECIFIC patterns we've seen the synthesis
# prompt produce. New phrasings ("partner cohort shrunk by 10, with attrition
# concentrated in tech (3 partners), strategy (2), and audit (2)") slip
# through. The LLM extraction layer uses Haiku to parse every numeric claim
# out of the prose as structured JSON, then mechanically cross-checks each
# against canonical totals + KPIs. It's slow (~3s + ~$0.005 per chip on
# Haiku) so it ONLY runs in the orchestrator notebook, never live.

_LLM_EXTRACT_PROMPT = """You are reviewing prose generated by an AI assistant for a CFO Operations dashboard. Your only job is to extract EVERY explicit numeric claim that ties a quantity to a named business metric.

Prose to review:
\"\"\"
{prose}
\"\"\"

For each claim, output one JSON object with these fields:
- "value": the raw number as written (no units, no commas)
- "unit": one of "$", "$K", "$M", "$B", "%", "pp", "days", "count", "ratio"
- "metric": ONE of the following normalized metric keys, or null if the claim doesn't match any:
    - "revenue_per_partner"       (dollars per partner)
    - "revenue_per_employee"
    - "total_revenue"             (firmwide dollars, monthly or annual)
    - "dso"                       (days sales outstanding, in days)
    - "dpo"                       (days payable outstanding, in days)
    - "utilization_rate"          (firmwide as a percent)
    - "partners"                  (firmwide active partner count)
    - "low_utilization_employees" (firmwide count of employees below 50% util)
    - "bench_cost"                (firmwide non-billable dollar cost)
    - "project_gross_margin"      (firmwide margin as a percent)
- "snippet": ~15 words around the claim

Rules:
- Only emit a claim if the prose explicitly ties the number to the metric.
  Skip generic prose without numbers.
- If the prose says a per-practice or per-region figure (e.g. "Technology has $9.17M bench cost"), set metric=null — those are sub-aggregates, not firmwide.
- If unsure of unit, prefer "count" for integers and "$" for unlabeled dollar figures.

Output ONLY a JSON array (no prose, no fences). If nothing applies, output: []
"""


_UNIT_TO_DOLLARS = {"$": 1.0, "$K": 1_000.0, "$M": 1_000_000.0, "$B": 1_000_000_000.0}


def _normalize_claim(claim: dict) -> tuple[str | None, float | None]:
    """Convert an LLM-extracted claim into (metric_key, normalized_value).

    Normalization rules:
    - Dollar metrics → dollars (regardless of K/M/B suffix in the claim)
    - Percent metrics → percent (0-100), not ratio
    - Day metrics → days
    - Count metrics → integer count
    Returns (None, None) when the claim is incomplete or unrecognized.
    """
    metric = (claim.get("metric") or "").strip() or None
    raw = claim.get("value")
    unit = (claim.get("unit") or "").strip()
    if metric is None or raw is None:
        return (None, None)
    try:
        val = float(str(raw).replace(",", ""))
    except (TypeError, ValueError):
        return (None, None)
    # Dollar normalization
    if unit in _UNIT_TO_DOLLARS:
        val *= _UNIT_TO_DOLLARS[unit]
    elif unit == "%" and val < 1.0:
        # Some models emit 0.665 for 66.5% — normalize to percent scale.
        val *= 100.0
    return (metric, val)


# Map metric key → (canonical_lookup_key, source_dict_name, kind)
# kind ∈ {"dollar", "percent", "days", "count"} — drives tolerance bands.
_METRIC_LOOKUP = {
    "revenue_per_partner":       ("revenue_per_partner",       "canonical_kpis",   "dollar"),
    "revenue_per_employee":      ("revenue_per_employee",      "canonical_kpis",   "dollar"),
    "total_revenue":             ("total_revenue",             "canonical_kpis",   "dollar"),
    "dso":                       ("dso",                       "canonical_kpis",   "days"),
    "dpo":                       ("dpo",                       "canonical_kpis",   "days"),
    "utilization_rate":          ("utilization_rate",          "canonical_kpis",   "percent"),
    "project_gross_margin":      ("project_gross_margin",      "canonical_kpis",   "percent"),
    "bench_cost":                ("bench_cost",                "canonical_kpis",   "dollar"),
    "partners":                  ("partners",                  "canonical_totals", "count"),
    "low_utilization_employees": ("low_utilization_employees", "canonical_totals", "count"),
}

# Tolerance per metric kind: percent drift before we flag a violation.
# Dollars / counts: ±2% (allows rounding in dashboard SQL). Percents: ±1pp
# absolute, NOT relative — a 1-point DSO drift is real signal. Days: ±2%.
_TOLERANCE = {"dollar": 0.02, "days": 0.02, "count": 0.02, "percent_abs_pp": 1.0}


def _violation_from_drift(claim_metric: str, claimed: float, canonical: float, kind: str, snippet: str) -> dict | None:
    if canonical is None or canonical == 0:
        return None
    if kind == "percent":
        # Absolute percentage-point tolerance.
        if abs(claimed - canonical) <= _TOLERANCE["percent_abs_pp"]:
            return None
        msg = (
            f"Prose claims {claim_metric} = {claimed:.2f}%; canonical = {canonical:.2f}% "
            f"(|Δ| = {abs(claimed - canonical):.2f}pp, > 1pp threshold)"
        )
    else:
        rel = abs(claimed - canonical) / abs(canonical)
        if rel <= _TOLERANCE.get(kind, 0.02):
            return None
        fmt = "${:,.0f}" if kind == "dollar" else ("{:,.0f}" if kind == "count" else "{:.2f}")
        msg = (
            f"Prose claims {claim_metric} = {fmt.format(claimed)}; canonical = {fmt.format(canonical)} "
            f"({rel*100:.1f}% drift, > 2% threshold)"
        )
    return {"type": "kpi_drift_llm", "message": msg, "snippet": snippet[:200]}


def find_violations_via_llm(
    prose: str,
    context: dict[str, Any] | None,
    *,
    llm_call,
    model: str,
) -> list[dict]:
    """LLM-assisted extraction pass. Uses Haiku (or whatever `model` resolves
    to in the caller) to pull structured claims out of the prose, then
    cross-checks each claim against the canonical totals/KPIs in `context`.

    `llm_call(messages, model)` should be a function returning a JSON dict
    shaped like an OpenAI chat completion (the caller injects this so we
    don't couple validation_probe.py to a specific gateway client).

    Returns the same violation shape as find_violations_in_prose so callers
    can merge the two violation lists.
    """
    if not prose or not prose.strip():
        return []
    context = context or {}
    canonical_kpis = context.get("canonical_kpis") or {}
    canonical_totals = context.get("canonical_totals") or {}
    if not canonical_kpis and not canonical_totals:
        # Nothing to compare against — LLM extraction is pointless.
        return []

    prompt = _LLM_EXTRACT_PROMPT.format(prose=prose[:6000])  # cap to keep tokens bounded
    try:
        resp = llm_call(
            messages=[{"role": "user", "content": prompt}],
            model=model,
        )
        text = (resp.get("choices") or [{}])[0].get("message", {}).get("content", "") or ""
    except Exception:
        return []

    # Strip optional ```json fences before parsing.
    stripped = text.strip()
    if stripped.startswith("```"):
        stripped = re.sub(r"^```(?:json)?\s*", "", stripped)
        stripped = re.sub(r"\s*```$", "", stripped)
    try:
        import json as _json
        claims = _json.loads(stripped)
    except Exception:
        return []
    if not isinstance(claims, list):
        return []

    violations: list[dict] = []
    sources = {"canonical_kpis": canonical_kpis, "canonical_totals": canonical_totals}
    for claim in claims:
        if not isinstance(claim, dict):
            continue
        metric_key, claimed_val = _normalize_claim(claim)
        if metric_key is None or claimed_val is None:
            continue
        lookup = _METRIC_LOOKUP.get(metric_key)
        if not lookup:
            continue
        canon_key, source_name, kind = lookup
        canonical = sources.get(source_name, {}).get(canon_key)
        if canonical is None:
            continue
        v = _violation_from_drift(
            claim_metric=metric_key,
            claimed=claimed_val,
            canonical=float(canonical),
            kind=kind,
            snippet=claim.get("snippet", "") or "",
        )
        if v:
            violations.append(v)
    return violations


def format_violations_for_human(violations: list[dict]) -> str:
    """Pretty-print a violation list for log output or user-facing warnings."""
    if not violations:
        return ""
    lines = []
    for v in violations:
        lines.append(f"  - [{v.get('type', '?')}] {v.get('message', '')}")
        snip = v.get("snippet")
        if snip:
            lines.append(f"      …{snip.strip()}…")
    return "\n".join(lines)
