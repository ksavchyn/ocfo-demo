# Databricks notebook source
# DBTITLE 1,CFO Analytics Demo - Data Preparation (the consulting firm)
# MAGIC %md
# MAGIC # CFO Analytics Demo — Data Preparation (an elite consulting firm)
# MAGIC **Modeled after an elite consulting firm (~$16B revenue, ~45K employees)**
# MAGIC
# MAGIC This notebook creates and populates 23 bronze tables in `{CATALOG}.{SCHEMA}` (default `main.cfo_proserv_dev`) with 3 years of synthetic data (2023-03 through 2026-02).
# MAGIC
# MAGIC ## Source Systems
# MAGIC | System | Tables |
# MAGIC |--------|--------|
# MAGIC | Salesforce | accounts, opportunities, contracts, engagements, forecasts |
# MAGIC | Workday | employees, positions, timecards, billing_rates, cost_rates, assignments, organizations |
# MAGIC | Concur | expense_reports, expense_items, travel_bookings, approvals |
# MAGIC | SAP | accounts_payable, accounts_receivable, general_ledger, cost_centers, profit_centers, purchase_orders |
# MAGIC | Mapping | cost_center_mapping |

# COMMAND ----------

# DBTITLE 1,Setup & Configuration
import random
import uuid
from datetime import datetime, timedelta, date
from decimal import Decimal

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
    DateType, TimestampType, BooleanType, LongType, DecimalType
)

import os

# Widget declarations — bundle's notebook_task.base_parameters flow through here.
# Without explicit declarations, dbutils.widgets.get raises and we fall through to
# defaults. See genie_insights/generate_insights.py for the same pattern.
try:
    dbutils.widgets.text("CFO_CATALOG", "main")  # noqa: F821
    dbutils.widgets.text("CFO_SCHEMA_NAME", "cfo_proserv")  # noqa: F821 — bundle populates from ${var.schema_name}
    # When set to "true", this notebook exits immediately without writing any
    # bronze tables. Used by the customer-data path: after `customer_mapping.py`
    # has emitted bronze views over the customer's real data, a rerun with
    # CFO_SKIP_HYDRATE_BRONZE=true preserves those views and lets the silver/gold
    # build run on top of them. Wired via deploy.sh's `--skip-bronze-hydrate` flag.
    dbutils.widgets.text("CFO_SKIP_HYDRATE_BRONZE", "false")  # noqa: F821
    _WIDGETS = True
except Exception:
    _WIDGETS = False


def _config(name: str, default: str) -> str:
    if _WIDGETS:
        try:
            v = dbutils.widgets.get(name)  # noqa: F821
            if v:
                return v
        except Exception:
            pass
    return os.environ.get(name, default)


CATALOG = _config("CFO_CATALOG", "main")
# Bundle's notebook_task.base_parameters populates CFO_SCHEMA_NAME from ${var.schema_name}.
SCHEMA = _config("CFO_SCHEMA_NAME", "cfo_proserv")
print(f"Target: {CATALOG}.{SCHEMA}")

# Customer-data path: skip synthetic bronze hydrate so customer_mapping.py's
# bronze views remain the data source for downstream silver/gold builds.
_SKIP_BRONZE = _config("CFO_SKIP_HYDRATE_BRONZE", "false").lower() in ("true", "1", "yes")
if _SKIP_BRONZE:
    print("=" * 60)
    print("CFO_SKIP_HYDRATE_BRONZE=true — skipping synthetic bronze generation.")
    print(f"Existing bronze_* tables/views in {CATALOG}.{SCHEMA} will be used by")
    print("the downstream build_silver_gold task as-is.")
    print("=" * 60)
    dbutils.notebook.exit("skipped: CFO_SKIP_HYDRATE_BRONZE=true")  # noqa: F821

# ─── Plausibility envelope loader (informational; doesn't enforce here) ──
# Surgical helper so we can reference envelope ranges in comments/calibration
# decisions. Hard enforcement is in 04_envelope_safety_net.py.
import yaml as _yaml


def _find_envelope():
    candidates = []
    try:
        candidates.append(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                       "plausibility_envelope.yml"))
    except NameError:
        pass
    cwd = os.getcwd()
    candidates += [
        os.path.join(cwd, "plausibility_envelope.yml"),
        os.path.join(cwd, "data_pipeline", "plausibility_envelope.yml"),
        os.path.join(cwd, "..", "data_pipeline", "plausibility_envelope.yml"),
    ]
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
        notebook_path = ctx.notebookPath().get()
        candidates.append(f"/Workspace{os.path.dirname(notebook_path)}/plausibility_envelope.yml")
    except Exception:
        pass
    for p in candidates:
        try:
            if os.path.exists(p):
                return p
        except Exception:
            continue
    return None


_env_path = _find_envelope()
if _env_path:
    with open(_env_path, "r") as _f:
        ENVELOPE = _yaml.safe_load(_f)
    print(f"Envelope loaded from {_env_path}")
else:
    ENVELOPE = {}
    print("Envelope not found; proceeding with inline defaults only.")

# ─────────────────────────────────────────────────────────────────────────
# PLAUSIBILITY ENVELOPE — single source of truth for what a realistic
# Tier-1 consulting firm looks like. Bounds are sampled within this
# envelope so downstream aggregates are correct by construction.
# See data_pipeline/plausibility_envelope.yml for the full spec.
# ─────────────────────────────────────────────────────────────────────────
try:
    import yaml  # PyYAML ships in Databricks runtimes
    _ENVELOPE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "plausibility_envelope.yml")
    with open(_ENVELOPE_PATH, "r") as _f:
        ENVELOPE = yaml.safe_load(_f)
    print(f"Envelope loaded from {_ENVELOPE_PATH}")
except Exception as _e:
    print(f"WARNING: Envelope load failed ({_e}); falling back to inline defaults")
    ENVELOPE = {}


def _env(*path, default=None):
    """Read a nested envelope value; return default if path missing."""
    node = ENVELOPE
    for k in path:
        if not isinstance(node, dict) or k not in node:
            return default
        node = node[k]
    return node

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# Seed for reproducibility
random.seed(84)

# COMMAND ----------

# DBTITLE 1,Reference Data Constants
# --- Geography proportions (balanced Americas/EMEA) ---
REGIONS = ["Americas", "EMEA", "Asia Pacific"]
REGION_WEIGHTS = [0.40, 0.40, 0.20]

LOCATIONS_BY_REGION = {
    "Americas": ["New York", "Chicago", "Washington DC", "Houston", "Atlanta", "San Francisco", "Toronto", "Sao Paulo"],
    "EMEA": ["London", "Paris", "Frankfurt", "Zurich", "Amsterdam", "Dubai", "Munich", "Milan"],
    "Asia Pacific": ["Tokyo", "Singapore", "Sydney", "Mumbai", "Shanghai", "Seoul", "Hong Kong", "Bangkok"],
}
LOCATION_WEIGHTS_BY_REGION = {
    "Americas": [0.22, 0.14, 0.12, 0.08, 0.08, 0.14, 0.12, 0.10],
    "EMEA": [0.22, 0.14, 0.12, 0.10, 0.10, 0.10, 0.12, 0.10],
    "Asia Pacific": [0.16, 0.16, 0.12, 0.14, 0.12, 0.12, 0.10, 0.08],
}

# --- Industry proportions ---
INDUSTRIES = ["Financial Services", "Healthcare & Life Sciences", "Technology Media & Telecom", "Consumer & Retail", "Industrials & Electronics", "Energy & Materials", "Private Equity", "Public & Social Sector"]
INDUSTRY_WEIGHTS = [0.20, 0.15, 0.14, 0.13, 0.10, 0.10, 0.09, 0.09]

# --- Practice area proportions (practices) ---
# Demo-facing names used directly — same names in bronze, silver, gold, UI, Genie.
# Aligned with the app's Practice Area dropdown and the persona prompts' scorecards.
PRACTICE_AREAS = ["Strategy & Consulting", "Technology", "Operations", "Managed Services: Tech", "Managed Services: Ops", "Audit", "Tax", "Accounting"]
PRACTICE_WEIGHTS = [0.22, 0.18, 0.14, 0.12, 0.12, 0.08, 0.05, 0.09]

# --- Customers by industry (80+ unique, Fortune 100 + PE + governments) ---
CUSTOMERS_BY_INDUSTRY = {
    "Financial Services": ["JPMorgan Chase", "Goldman Sachs", "Morgan Stanley", "BlackRock", "Allianz", "HSBC", "AXA", "BNP Paribas", "Citigroup", "UBS", "State Street", "Prudential"],
    "Healthcare & Life Sciences": ["Pfizer", "Johnson & Johnson", "Roche", "Novartis", "UnitedHealth", "AbbVie", "Merck", "Aetna", "Mayo Clinic", "NHS England"],
    "Technology Media & Telecom": ["Apple", "Google", "Microsoft", "Amazon", "Samsung", "Disney", "Comcast", "Deutsche Telekom", "Tencent", "Sony"],
    "Consumer & Retail": ["Procter & Gamble", "Nestle", "LVMH", "Nike", "Coca-Cola", "Unilever", "PepsiCo", "Walmart", "L'Oreal", "AB InBev"],
    "Industrials & Electronics": ["Siemens", "BMW", "Toyota", "Boeing", "Caterpillar", "ABB", "Honeywell", "Deere & Co", "Rolls-Royce", "Thales"],
    "Energy & Materials": ["Saudi Aramco", "Shell", "ExxonMobil", "TotalEnergies", "BP", "BHP", "Rio Tinto", "Chevron", "NextEra Energy", "Equinor"],
    "Private Equity": ["KKR", "Blackstone", "Carlyle", "Apollo", "TPG", "Warburg Pincus", "Bain Capital", "Advent International", "CVC Capital", "EQT Partners"],
    "Public & Social Sector": ["US Department of Defense", "UK Cabinet Office", "World Bank", "Gates Foundation", "UNICEF", "Singapore PMO", "UAE Government", "Federal Republic of Germany"],
}

ALL_CUSTOMERS = [c for lst in CUSTOMERS_BY_INDUSTRY.values() for c in lst]

# --- Billing / cost rates by level (titles) ---
JOB_LEVELS = ["Business Analyst", "Associate", "Engagement Manager", "Associate Partner", "Partner", "Senior Partner", "Director"]
JOB_LEVEL_WEIGHTS = [0.20, 0.22, 0.20, 0.15, 0.12, 0.06, 0.05]

BILLING_RATES = {
    # Doubled vs original — required to land firmwide annual revenue at $16.2B
    # (was producing ~$9B with old rates). Realistic for elite consulting:
    # Big 4 / top-tier strategy partners bill $2-3K/hr, mid-levels $1-2K, junior $500-900.
    "Business Analyst": 700.0, "Associate": 950.0, "Engagement Manager": 1300.0,
    "Associate Partner": 1700.0, "Partner": 2200.0, "Senior Partner": 3000.0, "Director": 1500.0,
}
COST_RATES = {
    # Fully-loaded employee cost (salary + benefits + overhead allocation) per billable hour.
    # Set at ~57.5% of billing rate to land project margin at ~42.5% (the demo target).
    "Business Analyst": 400.0, "Associate": 550.0, "Engagement Manager": 750.0,
    "Associate Partner": 1000.0, "Partner": 1300.0, "Senior Partner": 1800.0, "Director": 850.0,
}

# --- Date range (fixed anchor so the synthetic data is reproducible) ---
# ANCHOR_DATE is the demo's "as of" date. It must be fixed, NOT date.today(): with a
# fixed anchor + random.seed above, the dataset is byte-identical on every run and on
# every machine, so the engineered outliers (T&E >6%, aged AR) always land inside the
# app's filter windows. date.today() would slide the window per run-day and silently
# push outliers out of range (Active→Expired, scores below threshold) → blank tiles.
ANCHOR_DATE = date(2026, 5, 17)
DATE_END = ANCHOR_DATE
DATE_START = DATE_END.replace(year=DATE_END.year - 3, day=1)

# --- Department categories ---
DEPT_CATEGORIES = ["IT", "HR", "Marketing", "Finance", "Legal", "Operations", "R&D", "Sales", "Executive", "Facilities"]

# --- Engagement archetypes (drive plan sizing + name generation) -----------
# Each engagement gets classified into one of these archetypes based on its
# service_line. Budget, duration, and margin are bounded by archetype so we
# never get a "$149M Due Diligence" or "$200K Transformation" — both nonsense
# at a Tier-1 firm.
#
# Budget ranges are SCALED FOR THE SYNTHETIC DATA DENSITY (8,000 engagements
# over 3 years), not real-firm density (which would be 50K+). The upper bounds
# are intentionally narrower than calibration.yml — keeping avg engagement
# budget around $14-16M matches the existing actuals scale produced by timecards,
# so projects aren't all wildly under-plan after archetype enforcement.
ENGAGEMENT_ARCHETYPES = {
    # Tighter upper bounds 2026-05-19. Initial event-simulator probe showed
    # 8000 engagements × $14M avg budget = $112B total contract value over 3
    # years, ~2× the $54B target for an $18B/yr firm. Halving the upper
    # bounds on the big archetypes (Transformation, TechImpl, ManagedServices)
    # drops the avg budget to ~$7M, landing total CV around $54B.
    "DueDiligence":      {"budget": (300_000,   5_000_000),  "duration_days": (30,   120),  "margin_pct": (30, 45), "billing": "T&M"},
    "Strategy":          {"budget": (500_000,   8_000_000),  "duration_days": (90,   270),  "margin_pct": (35, 50), "billing": "FixedFee"},
    "Advisory":          {"budget": (150_000,   2_500_000),  "duration_days": (60,   360),  "margin_pct": (30, 42), "billing": "Retainer"},
    "TechImplementation":{"budget": (1_000_000, 20_000_000), "duration_days": (180,  900),  "margin_pct": (22, 35), "billing": "FixedFee"},
    "Transformation":    {"budget": (3_000_000, 40_000_000), "duration_days": (360, 1_440), "margin_pct": (25, 38), "billing": "FixedFee"},
    "ManagedServices":   {"budget": (1_000_000, 15_000_000), "duration_days": (360, 1_800), "margin_pct": (28, 40), "billing": "Retainer"},
    "Audit":             {"budget": (200_000,   5_000_000),  "duration_days": (90,   270),  "margin_pct": (22, 30), "billing": "FixedFee"},
}

# Service line → archetype mapping. SERVICE_LINES are defined later (line ~822)
# but we only USE this mapping in the engagement generation block, after both
# are in scope.
ENGAGEMENT_ARCHETYPE_BY_SERVICE_LINE = {
    "Strategy Review":            "Strategy",
    "Post-Merger Integration":    "Transformation",
    "Digital Roadmap":            "TechImplementation",
    "Org Redesign":               "Transformation",
    "Pricing Optimization":       "Advisory",
    "Due Diligence":              "DueDiligence",
    "Performance Transformation": "Transformation",
    "Growth Strategy":            "Strategy",
    "Operations Improvement":     "Advisory",
    "Cost Reduction":             "Advisory",
    "Sustainability Transition":  "Advisory",
    "Innovation Lab":             "Strategy",
}

# Name slot fillers — kept GENERIC so customer-facing deploys don't ship
# tier-1-specific named projects. The combinations produce names that look
# like real consulting engagements ("Tencent SAP S/4HANA Implementation
# Phase 2", "BP Operating Model Transformation") rather than service-line
# labels ("Strategy Review", "Cost Reduction").
NAME_THEMES = [
    "Growth", "Cost Optimization", "Operating Model", "Digital", "Sustainability",
    "Risk", "Workforce", "Customer Experience", "Margin Expansion", "Resilience",
]
NAME_PLATFORMS = [
    "SAP S/4HANA", "Salesforce", "Workday", "Oracle Cloud", "ServiceNow",
    "Snowflake", "Databricks", "AWS", "Azure", "GCP", "NetSuite", "Anaplan",
]
NAME_FUNCTIONS = [
    "Finance", "HR", "Supply Chain", "IT", "Customer Service",
    "Procurement", "Risk & Compliance", "Sales", "Marketing", "Treasury",
]
NAME_TARGETS = ["Acquisition", "Carve-Out", "Subsidiary", "Joint Venture", "Asset"]


def engagement_name(customer: str, archetype: str, service_line: str, year: int) -> str:
    """Generate a templated engagement name per archetype.

    Replaces the previous f'{customer} - {service_line}' pattern (which produced
    generic labels like 'BP - Strategy Review' across thousands of engagements)
    with archetype-aware templates that look like real consulting projects.
    """
    if archetype == "DueDiligence":
        choices = [
            f"{customer} {random.choice(NAME_TARGETS)} Due Diligence",
            f"{customer} – {random.choice(NAME_THEMES)} DD",
            f"{customer} Acquisition Diligence: {random.choice(NAME_THEMES)}",
            f"{customer} – Buy-Side Due Diligence FY{year}",
        ]
    elif archetype == "Strategy":
        choices = [
            f"{customer} {random.choice(NAME_THEMES)} Strategy {year}",
            f"{customer} Enterprise Strategy Refresh",
            f"{customer} {random.choice(NAME_THEMES)} Growth Plan",
            f"{customer} – Strategic Roadmap FY{year}",
            f"{customer} {random.choice(NAME_FUNCTIONS)} Strategy",
        ]
    elif archetype == "Advisory":
        choices = [
            f"{customer} {random.choice(NAME_THEMES)} Advisory",
            f"{customer} – {random.choice(NAME_FUNCTIONS)} Advisory Engagement",
            f"{customer} {random.choice(NAME_FUNCTIONS)} Optimization",
            f"{customer} – Continuous Advisory Retainer FY{year}",
        ]
    elif archetype == "TechImplementation":
        choices = [
            f"{customer} {random.choice(NAME_PLATFORMS)} Implementation Phase {random.randint(1, 4)}",
            f"{customer} {random.choice(NAME_PLATFORMS)} Modernization",
            f"{customer} {random.choice(NAME_PLATFORMS)} Migration Wave {random.randint(1, 3)}",
            f"{customer} – {random.choice(NAME_PLATFORMS)} Platform Rollout",
        ]
    elif archetype == "Transformation":
        choices = [
            f"{customer} {random.choice(NAME_FUNCTIONS)} Transformation Program",
            f"{customer} Operating Model Transformation",
            f"{customer} Global {random.choice(NAME_FUNCTIONS)} Standup",
            f"{customer} {random.choice(NAME_THEMES)} Transformation",
            f"{customer} – Enterprise {random.choice(NAME_THEMES)} Program",
        ]
    elif archetype == "ManagedServices":
        choices = [
            f"{customer} {random.choice(NAME_FUNCTIONS)} Managed Services",
            f"{customer} {random.choice(NAME_PLATFORMS)} Run & Optimize",
            f"{customer} – {random.choice(NAME_FUNCTIONS)} Operations Outsourcing",
            f"{customer} Application Managed Services",
        ]
    elif archetype == "Audit":
        choices = [
            f"{customer} FY{year} Statutory Audit",
            f"{customer} – {random.choice(NAME_FUNCTIONS)} Compliance Review",
            f"{customer} Internal Controls Assessment",
            f"{customer} SOX FY{year} Audit",
        ]
    else:
        choices = [f"{customer} – {service_line}"]
    return random.choice(choices)


# --- Vendor portfolio (consulting-firm-realistic spend categories) ---------
# Replaces the flat AP_VENDORS list that had Salesforce/Gartner/IDC randomly
# rotated as #1-#3 vendors. A real Tier-1 consulting firm's top vendors are
# heavily weighted toward staffing/contractors, cloud, software, real estate,
# and benefits — not the data-research subscriptions the previous list
# over-emphasized. Calibrated from calibration.yml § vendors.
# Bulk-expand each category with generic vendor names so the envelope's
# vendor_count bound (200-500) is satisfied. Named brand vendors above stay
# at the top of the spend distribution; generic ones fill the long tail.
def _expand_vendors_with_generic(base_dict):
    # 2026-05-20: replaced "CloudInfrastructur Co 004" style truncated-category
    # synthetic names with realistic fictional brand-style names per category.
    # Real CFO demos surface vendor names — generic "Category Co NNN" reads as
    # synthetic-data smell to a customer.
    _GENERIC_VENDORS_BY_CATEGORY = {
        "Cloud Infrastructure": [
            "Stratus Cloud Services", "NimbusEdge Hosting", "Velora Cloud",
            "Cirrus Networks", "Helix Hosting", "Apex Cloud Systems",
            "Cardinal Cloud Partners", "Meridian Cloud", "Beacon Cloud Services",
            "Northwind Cloud", "Polaris Hosting", "Skyline Cloud Group",
            "Vanguard Cloud Services", "Summit Cloud Networks", "Orion Cloud",
        ],
        "Software Licensing": [
            "Vexar Labs", "Datalux Software", "Prospera Systems", "Lumen Apps",
            "Quartz Platform", "Northstar Software", "Cobalt Labs",
            "Riverstone Tech", "Acme Software Group", "Lattice Systems",
            "Echelon Software", "Pinnacle Apps", "Spectrum Labs",
            "Vertex Software", "Catalyst Platform", "Sterling Tech",
            "Beacon Software", "Forefront Systems",
        ],
        "Staffing & Contractors": [
            "Apex Talent Group", "Northbridge Staffing", "Catalyst Workforce",
            "Pinnacle Resources", "Cardinal Staffing Partners", "Beacon Talent",
            "Summit Workforce Solutions", "Vanguard Consulting Resources",
            "Meridian Staffing Group", "Polaris Talent", "Sterling Resources",
            "Riverside Staffing", "Lighthouse Consulting Group",
            "Crestline Workforce", "Westwind Talent Partners",
            "Aspect Consulting Group", "Boundless Staffing Solutions",
        ],
        "Real Estate & Facilities": [
            "Cardinal Property Services", "Northwind Realty Partners",
            "Beacon Facilities Management", "Summit Property Group",
            "Meridian Real Estate Services", "Sterling Property Partners",
            "Crestline Facilities Group", "Vanguard Realty Services",
            "Polaris Property Management", "Apex Facilities Solutions",
            "Lighthouse Realty Group", "Pinnacle Property Services",
            "Westwind Facilities Partners", "Riverstone Realty Group",
            "Helix Facilities Management",
        ],
        "Travel & T&E": [
            "Voyager Travel Group", "Globespan Travel Partners",
            "Skyline Travel Services", "Beacon Business Travel",
            "Meridian Travel Group", "Summit Travel Management",
            "Compass Corporate Travel", "Wayfinder Travel Solutions",
            "Apex Business Travel", "Cardinal Travel Services",
            "Northstar Travel Partners", "Helix Travel Group",
        ],
        "Benefits & Insurance": [
            "Apex Benefits Partners", "Meridian Insurance Group",
            "Cardinal Health Benefits", "Beacon Wellness Partners",
            "Northbridge Insurance Services", "Vanguard Benefits Group",
            "Pinnacle Insurance Partners", "Crestline Health Services",
            "Polaris Benefits Solutions", "Riverstone Insurance Group",
            "Summit Wellness Partners",
        ],
        "Data & Research Subscriptions": [
            "Quartz Research Group", "Northstar Analytics",
            "Beacon Data Insights", "Meridian Research Partners",
            "Apex Market Intelligence", "Catalyst Research Services",
            "Pinnacle Analytics Group", "Vanguard Research Partners",
            "Sterling Data Group", "Polaris Market Research",
            "Lattice Research", "Crestline Analytics",
        ],
        "Professional Services": [
            "Sterling Legal Partners", "Northbridge Advisors",
            "Beacon Professional Services", "Apex Advisory Group",
            "Cardinal Legal Group", "Meridian Advisors LLP",
            "Pinnacle Professional Partners", "Westwind Legal Services",
            "Crestline Advisory Group", "Vanguard Professional Services",
            "Summit Legal Partners",
        ],
        "Marketing & Events": [
            "Catalyst Marketing Group", "Apex Brand Partners",
            "Meridian Events Group", "Beacon Marketing Partners",
            "Northstar Brand Services", "Pinnacle Events Group",
            "Summit Marketing Services", "Crestline Brand Partners",
            "Polaris Events Group",
        ],
        "Office Supplies & Equipment": [
            "Apex Office Solutions", "Meridian Supplies Group",
            "Beacon Office Partners", "Northstar Office Services",
            "Cardinal Supplies Group", "Pinnacle Office Solutions",
            "Summit Equipment Partners", "Crestline Office Group",
            "Vanguard Supplies Services", "Westwind Office Partners",
        ],
    }
    for cat, info in base_dict.items():
        existing = set(info["vendors"])
        pool = _GENERIC_VENDORS_BY_CATEGORY.get(cat, [])
        for name in pool:
            if name not in existing:
                info["vendors"].append(name)
                existing.add(name)
    return base_dict


VENDOR_CATEGORIES = {
    "Cloud Infrastructure": {
        "pct_of_spend": 0.18,
        "invoice_range": (50_000, 800_000),
        "vendors": [
            "AWS", "Microsoft Azure", "Google Cloud", "Oracle Cloud", "IBM Cloud",
            "Snowflake", "DigitalOcean",
        ],
    },
    "Software Licensing": {
        "pct_of_spend": 0.16,
        "invoice_range": (10_000, 300_000),
        "vendors": [
            "Microsoft", "Salesforce", "Workday", "ServiceNow", "Atlassian",
            "Adobe", "GitHub", "Slack", "Zoom", "DocuSign", "Tableau", "Box",
            "Asana", "Notion", "Datadog", "Splunk", "PagerDuty", "Okta", "1Password",
        ],
    },
    "Staffing & Contractors": {
        "pct_of_spend": 0.22,
        "invoice_range": (5_000, 250_000),
        "vendors": [
            "Robert Half", "Kforce", "Toptal", "Insight Global", "Beacon Hill Staffing",
            "Allegis Group", "TEKsystems", "Randstad", "ManpowerGroup", "Adecco",
            "Hays", "Michael Page",
        ],
    },
    "Real Estate & Facilities": {
        "pct_of_spend": 0.12,
        "invoice_range": (20_000, 500_000),
        "vendors": [
            "JLL", "CBRE", "Cushman & Wakefield", "Colliers", "Newmark",
            "Savills", "Knight Frank", "WeWork", "Regus", "Iron Mountain",
            "Sodexo", "Aramark", "ABM Industries", "Ricoh",
        ],
    },
    "Travel & T&E": {
        "pct_of_spend": 0.08,
        "invoice_range": (2_000, 100_000),
        "vendors": [
            "American Express GBT", "BCD Travel", "Carlson Wagonlit", "Hertz",
            "Avis", "Marriott", "Hilton", "Hyatt", "Delta Air Lines",
            "United Airlines", "American Airlines", "British Airways", "Lufthansa",
            "Cathay Pacific", "Uber for Business", "Lyft Business",
        ],
    },
    "Benefits & Insurance": {
        "pct_of_spend": 0.10,
        "invoice_range": (50_000, 600_000),
        "vendors": [
            "Aetna", "Cigna", "United Healthcare", "MetLife", "Anthem",
            "Mercer", "WTW", "Aon", "Marsh McLennan", "Vanguard", "Fidelity Investments",
        ],
    },
    "Data & Research Subscriptions": {
        "pct_of_spend": 0.04,
        "invoice_range": (15_000, 200_000),
        "vendors": [
            "Bloomberg", "Gartner", "Forrester", "IDC", "Pitchbook",
            "S&P Global", "Moody's", "Dun & Bradstreet", "Refinitiv",
            "FactSet", "Capital IQ", "LexisNexis",
        ],
    },
    "Professional Services": {
        "pct_of_spend": 0.06,
        "invoice_range": (25_000, 500_000),
        "vendors": [
            "Skadden Arps", "Kirkland & Ellis", "Latham & Watkins",
            "Sullivan & Cromwell", "Baker McKenzie", "Clifford Chance",
            "DLA Piper", "Allen & Overy", "Linklaters", "Hogan Lovells",
        ],
    },
    "Marketing & Events": {
        "pct_of_spend": 0.02,
        "invoice_range": (5_000, 150_000),
        "vendors": [
            "Ogilvy", "WPP", "Publicis", "Interpublic Group", "EventBrite Business",
            "Cvent", "RR Donnelley", "Edelman", "FleishmanHillard",
        ],
    },
    "Office Supplies & Equipment": {
        "pct_of_spend": 0.02,
        "invoice_range": (1_000, 50_000),
        "vendors": [
            "Staples", "WB Mason", "Office Depot", "Lenovo", "Dell Technologies",
            "HP", "Apple", "Logitech", "Steelcase", "Herman Miller",
        ],
    },
}

# Pre-build a flat (vendor, category, invoice_range) list with category-pct weights
# spread across each vendor in that category.
# Run the generic-expansion helper so VENDOR_CATEGORIES has ~250 vendors total.
_expand_vendors_with_generic(VENDOR_CATEGORIES)

_VENDOR_FLAT = []
_VENDOR_WEIGHTS = []
for _cat_name, _cat_info in VENDOR_CATEGORIES.items():
    _per_vendor_weight = _cat_info["pct_of_spend"] / max(len(_cat_info["vendors"]), 1)
    for _v in _cat_info["vendors"]:
        _VENDOR_FLAT.append((_v, _cat_name, _cat_info["invoice_range"]))
        _VENDOR_WEIGHTS.append(_per_vendor_weight)


def pick_vendor():
    """Return (vendor_name, vendor_category, invoice_range) weighted by category spend share."""
    idx = random.choices(range(len(_VENDOR_FLAT)), weights=_VENDOR_WEIGHTS, k=1)[0]
    return _VENDOR_FLAT[idx]


# Vendor master with stable vendor_id strings (V-10000, V-10001, ...). Needed
# by the event simulator (much further down) to track per-vendor lifetime
# spend against PER_VENDOR_LIFETIME_CAP_USD. Originally built down in the AP
# section, but the simulator runs first, so it's hoisted up here.
_AP_VENDOR_RECORDS = []  # list of (vendor_id, vendor_name, category, invoice_range)
_vid_counter = 10000
for _cat_name, _cat_info in VENDOR_CATEGORIES.items():
    for _vname in _cat_info["vendors"]:
        _AP_VENDOR_RECORDS.append((f"V-{_vid_counter}", _vname, _cat_name, _cat_info["invoice_range"]))
        _vid_counter += 1
_AP_VENDOR_WEIGHTS = []
for _cat_name, _cat_info in VENDOR_CATEGORIES.items():
    _per = _cat_info["pct_of_spend"] / max(len(_cat_info["vendors"]), 1)
    _AP_VENDOR_WEIGHTS.extend([_per] * len(_cat_info["vendors"]))


def _pick_ap_vendor():
    """Return (vendor_id, vendor_name, category, invoice_range) weighted by category spend share."""
    idx = random.choices(range(len(_AP_VENDOR_RECORDS)), weights=_AP_VENDOR_WEIGHTS, k=1)[0]
    return _AP_VENDOR_RECORDS[idx]


# COMMAND ----------

# DBTITLE 1,Helper Functions
def weighted_choice(items, weights):
    """Return a single weighted random choice."""
    return random.choices(items, weights=weights, k=1)[0]


def pick_region():
    return weighted_choice(REGIONS, REGION_WEIGHTS)


def pick_location(region):
    return weighted_choice(LOCATIONS_BY_REGION[region], LOCATION_WEIGHTS_BY_REGION[region])


def pick_industry():
    return weighted_choice(INDUSTRIES, INDUSTRY_WEIGHTS)


def pick_practice():
    return weighted_choice(PRACTICE_AREAS, PRACTICE_WEIGHTS)


def pick_customer(industry):
    return random.choice(CUSTOMERS_BY_INDUSTRY[industry])


def pick_level():
    return weighted_choice(JOB_LEVELS, JOB_LEVEL_WEIGHTS)


def rand_date(start=DATE_START, end=DATE_END):
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, max(delta, 0)))


def rand_ts(start=DATE_START, end=DATE_END):
    d = rand_date(start, end)
    return datetime(d.year, d.month, d.day, random.randint(8, 18), random.randint(0, 59), random.randint(0, 59))


def uid():
    return str(uuid.uuid4())


def make_mandatory():
    """Return dict with region, location, practice_area, industry, customer."""
    region = pick_region()
    location = pick_location(region)
    industry = pick_industry()
    return {
        "region": region,
        "location": location,
        "practice_area": pick_practice(),
        "industry": industry,
        "customer": pick_customer(industry),
    }


def write_table(df, table_name, mode="overwrite"):
    """Write DataFrame to Delta table (serverless compatible via temp view + CTAS)."""
    view_name = f"_tmp_{table_name}"
    df.createOrReplaceTempView(view_name)
    if mode == "overwrite":
        spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.{table_name}")
        spark.sql(f"CREATE TABLE {CATALOG}.{SCHEMA}.{table_name} AS SELECT * FROM {view_name}")
    else:
        spark.sql(f"INSERT INTO {CATALOG}.{SCHEMA}.{table_name} SELECT * FROM {view_name}")
    count = spark.table(f"{CATALOG}.{SCHEMA}.{table_name}").count()
    print(f"  {table_name}: {count:,} rows")
    return count

# COMMAND ----------

# DBTITLE 1,DEMO_OVERRIDES — Engineered Narrative Targets
# MAGIC %md
# MAGIC ## Engineered demo narrative — single source of truth
# MAGIC
# MAGIC This dict encodes the SPECIFIC firmwide / office / practice / AR patterns the
# MAGIC synthetic data is engineered to produce. Genie and Claude discover these
# MAGIC patterns naturally because they really exist in the data — there is NO
# MAGIC hardcoding of narratives in Genie / app / synthesis instructions.
# MAGIC
# MAGIC Customer deploys see different patterns based on THEIR data; this dict
# MAGIC only applies during synthetic-data generation, not at app/Genie runtime.

# COMMAND ----------

# DBTITLE 1,Demo Overrides Config

DEMO_OVERRIDES = {
    # ───────────────────────────────────────────────────────────────────────
    # FIRMWIDE TARGETS (sanity check — aggregate rollups should land here ±5%)
    # ───────────────────────────────────────────────────────────────────────
    "firmwide": {
        "annual_revenue": 16_200_000_000,
        "annual_expenses": 5_000_000_000,
        "enterprise_margin_pct": 22.8,
        "project_margin_pct": 42.5,
        "project_margin_qoq_pp": -1.1,
        "revenue_per_partner": 3_600_000,
        "rpp_yoy_pct": -7.4,
        "monthly_revenue": 1_400_000_000,
        "monthly_revenue_mom_pct": 1.4,
        "dso_days": 56,
        "dpo_days": 32,
        "working_capital_trapped": 1_100_000_000,
        "invoice_receivables": 2_600_000_000,
        "new_partners_total": 76,
        "employee_count": 25_000,
        "partner_count": 4_500,
    },

    # ───────────────────────────────────────────────────────────────────────
    # MULTI-YEAR COST DISCIPLINE TREND
    # Bounded by plausibility_envelope.yml.budget_variance — variances now sit
    # in the 5-15% range (Tier-1 realistic), not 20%+ which produced 100-300%
    # over-budget narratives at the office grain.
    # ───────────────────────────────────────────────────────────────────────
    "fiscal_year_budgets": {
        # FY2025 (prior): solid year — revenue beats by 8%, expenses 3% over
        2025: {"revenue_vs_budget": 1.08, "expense_vs_budget": 1.03},
        # FY2026 (current): tighter — revenue beats by 6%, expenses 8% over (compression narrative)
        2026: {"revenue_vs_budget": 1.06, "expense_vs_budget": 1.08},
        # FY2024 (older): baseline
        2024: {"revenue_vs_budget": 1.08, "expense_vs_budget": 1.03},
        # FY2023 (oldest): baseline
        2023: {"revenue_vs_budget": 1.05, "expense_vs_budget": 1.02},
    },

    # ───────────────────────────────────────────────────────────────────────
    # OFFICE-LEVEL OVERRIDES (multipliers vs base actuals)
    # ───────────────────────────────────────────────────────────────────────
    "office_overrides": {
        # ── HEADLINE OVERAGE: US offices carry the expense-overrun narrative ──
        # Multipliers reduced 2026-05-19 after NYC was producing 99.80% over-budget
        # variance (almost 2× plan) which looks like a data bug, not a real-firm
        # narrative. Capped so per-office overage stays in the ±15-30% band that
        # a CFO would actually flag without questioning the underlying data.
        # New York: headline expense overage. Was 1.28/1.42 → now 1.15/1.22.
        "New York":        {"revenue_multiplier": 1.03, "expense_multiplier": 1.15, "billable_expense_multiplier": 1.22},
        # Chicago: secondary expense overage. Was 1.20/1.30 → now 1.12/1.18.
        "Chicago":         {"expense_multiplier": 1.12, "billable_expense_multiplier": 1.18},
        # Washington DC: tertiary. Was 1.18/1.28 → now 1.10/1.16.
        "Washington DC":   {"expense_multiplier": 1.10, "billable_expense_multiplier": 1.16},
        # San Francisco: revenue miss + mild expense overrun. Real-estate cost pressure.
        "San Francisco":   {"revenue_multiplier": 0.94, "expense_multiplier": 1.06},

        # ── NON-US OFFICES: under budget on expense (no overage narrative) ──
        # London: well-managed cost base, under expense budget. Revenue at plan.
        "London":          {"expense_multiplier": 0.95, "billable_expense_multiplier": 0.93},
        # Bangkok: under expense budget, lower cost-of-delivery footprint.
        # 2026-05-21 audit: Bangkok is already the LOWEST-expense office firmwide
        # (~$39M trailing-180d, vs NY $367M). Original tracker's "Bangkok outlier"
        # claim was based on a chart misread — keeping the existing mild deflator
        # rather than strengthening it.
        "Bangkok":         {"expense_multiplier": 0.95, "billable_expense_multiplier": 0.93},
        # Dubai: 2026-05-21 audit confirmed Dubai is mid-tier (10th of 24 offices,
        # ~$141M trailing-180d, in line with Frankfurt/Toronto/Sydney). Original
        # tracker's "Dubai outlier $225M" was a screenshot misread — no dampener
        # needed. Leaving Dubai at default multipliers (1.0). Dubai still has the
        # revenue beat narrative engineered in gold_regional_pnl budgeted_revenue
        # CASE statement (+5% over plan), which is the "Dubai Revenue Outperformance"
        # action area on the Executive Summary.
        # Munich: ongoing revenue beat narrative. Was 1.20 → now 1.12 (still a beat).
        "Munich":          {"revenue_multiplier": 1.12},
        # Sao Paulo: revenue underperformer (kept from prior demo narrative).
        "Sao Paulo":       {"revenue_multiplier": 0.92},
    },

    # ───────────────────────────────────────────────────────────────────────
    # PRACTICE-LEVEL OVERRIDES (project margin patterns)
    # Practice names are demo-facing throughout — same in bronze, silver, gold, UI.
    # ───────────────────────────────────────────────────────────────────────
    "practice_margins": {
        # Operations: 40.4% this quarter vs 43.4% prior — worst slide (Action Area #2)
        "Operations":              {"current_q_margin": 0.404, "prior_q_margin": 0.434},
        # Strategy & Consulting: stable, modest improvement
        "Strategy & Consulting":   {"current_q_margin": 0.500, "prior_q_margin": 0.495},
        # Audit: stable
        "Audit":                   {"current_q_margin": 0.430, "prior_q_margin": 0.420},
        # Tax: stable
        "Tax":                     {"current_q_margin": 0.450, "prior_q_margin": 0.452},
        # Managed Services: Tech (AI/GenAI) — best margin, growing
        "Managed Services: Tech":  {"current_q_margin": 0.510, "prior_q_margin": 0.500},
        # Managed Services: Ops — stable
        "Managed Services: Ops":   {"current_q_margin": 0.420, "prior_q_margin": 0.430},
        # Technology — stable, slight decline
        "Technology":              {"current_q_margin": 0.470, "prior_q_margin": 0.475},
        # Accounting — stable
        "Accounting":              {"current_q_margin": 0.410, "prior_q_margin": 0.412},
    },

    # ───────────────────────────────────────────────────────────────────────
    # PARTNER PROMOTION CLASS — 76 new partners firmwide, S&C concentrated
    # ───────────────────────────────────────────────────────────────────────
    "new_partner_class": {
        # Promoted/lateraled in the last 12 months (tenure < 12 mo as Partner+)
        # Keyed by demo-facing practice names
        "Strategy & Consulting":   32,
        "Technology":              15,
        "Operations":              10,
        "Managed Services: Tech":  10,
        "Audit":                   3,
        "Tax":                     2,
        "Accounting":              2,
        "Managed Services: Ops":   2,
        # Total: 76
    },

    # ───────────────────────────────────────────────────────────────────────
    # AI PRACTICE GROWTH NARRATIVE (Managed Services: Tech in demo names)
    # ───────────────────────────────────────────────────────────────────────
    "ai_practice": {
        "name":                "Managed Services: Tech",
        "yoy_growth_pct":      45.0,
        "revenue_share_pct":   18.0,  # of firmwide revenue
        "prior_year_share_pct":12.0,
        "project_margin_pct":  51.0,
    },

    # ───────────────────────────────────────────────────────────────────────
    # AR CONCENTRATION — 5 named clients carry the bulk of aged AR
    # 23% of >90-day AR but only ~4% of revenue
    # ───────────────────────────────────────────────────────────────────────
    "ar_concentration": [
        # (customer, amount_overdue_usd, days_overdue) — one is write-off candidate
        {"customer": "Tencent",         "amount":  12_000_000, "days":  95},
        {"customer": "Saudi Aramco",    "amount":  10_500_000, "days": 110},
        {"customer": "BP",              "amount":   9_800_000, "days": 105},
        {"customer": "Prudential",      "amount":   8_500_000, "days": 188},  # write-off candidate
        {"customer": "Mayo Clinic",     "amount":   7_200_000, "days":  92},
    ],

    # ───────────────────────────────────────────────────────────────────────
    # SEASONAL ANOMALIES — specific months that underperform / overspend
    # Used to create the Finance Overview "Nov + Dec dip" narrative
    # ───────────────────────────────────────────────────────────────────────
    "seasonal_anomalies": {
        # (year, month) → multipliers vs base.
        # Both revenue_multiplier and expense_multiplier flavors are now wired:
        #   - expense_multiplier: applied in bronze expense generation (line ~1588)
        #   - revenue_multiplier: applied in bronze timecard generation (line ~1170)
        #     to billable rate, which propagates through billing_amount → silver_fact_timecards
        #     → gold_enterprise_metrics.revenue → firmwide RPP numerator.
        # Q4 dip (Nov/Dec 2025): below-trend revenue + above-trend expenses, drives
        # the Finance Overview seasonal-anomaly narrative.
        (2025, 11): {"revenue_multiplier": 0.93, "expense_multiplier": 1.08},
        (2025, 12): {"revenue_multiplier": 0.91, "expense_multiplier": 1.12},
        # Latest complete month (April 2026): modest revenue lift +7% to produce a
        # POSITIVE firmwide RPP YoY narrative. Without this, partner-count growth
        # firmwide (+0.65%) outpaces revenue growth, producing a -2% RPP YoY firmwide
        # that contradicts every practice's individual +YoY RPP gain (Simpson's paradox
        # from headcount mix shift). The +7% revenue lift in April 2026 + flat 2025
        # baseline yields ~+6% firmwide RPP YoY, aligning with the practice-level
        # positive RPP picture. Cleanly producible business story: "Q1 revenue
        # rebound after Nov/Dec 2025 dip."
        (2026, 4):  {"revenue_multiplier": 1.07},
    },

    # Per-customer AR aging caps (anti-explosion)
    "ar_caps": {
        "per_customer_per_bucket_max":  15_000_000,
        "per_customer_lifetime_open_max": 60_000_000,
    },
}


# ──────────────────────────────────────────────────────────────────────────────
# Helper functions — apply DEMO_OVERRIDES to per-row generation logic
# ──────────────────────────────────────────────────────────────────────────────

def get_office_revenue_multiplier(office: str) -> float:
    """Per-office multiplier vs base revenue. 1.0 = at target."""
    return DEMO_OVERRIDES["office_overrides"].get(office, {}).get("revenue_multiplier", 1.0)


def get_office_expense_multiplier(office: str) -> float:
    """Per-office multiplier vs base expenses. 1.0 = at target."""
    return DEMO_OVERRIDES["office_overrides"].get(office, {}).get("expense_multiplier", 1.0)


def get_office_billable_expense_multiplier(office: str) -> float:
    """Override JUST the billable_expenses category per office (for DC narrative)."""
    return DEMO_OVERRIDES["office_overrides"].get(office, {}).get("billable_expense_multiplier", 1.0)


def get_seasonal_multiplier(year: int, month: int, what: str) -> float:
    """Multiplier for revenue or expense in a specific (year, month). Default 1.0.

    `what` is one of 'revenue_multiplier' or 'expense_multiplier'.
    """
    return DEMO_OVERRIDES["seasonal_anomalies"].get((year, month), {}).get(what, 1.0)


def get_fiscal_year_budget_ratio(fiscal_year: int, what: str) -> float:
    """Actual/budget ratio for a fiscal year. 'what' = 'revenue_vs_budget' or 'expense_vs_budget'.

    Used to derive budgets backwards from generated actuals so the firmwide
    rollup matches the multi-year cost discipline trend.
    """
    yr_cfg = DEMO_OVERRIDES["fiscal_year_budgets"].get(fiscal_year)
    if not yr_cfg:
        # Default for years outside the engineered range
        return 1.08 if what == "revenue_vs_budget" else 1.03
    return yr_cfg[what]


def get_practice_margin_for_quarter(demo_practice: str, is_current_quarter: bool) -> float:
    """Project margin for a practice in the current or prior fiscal quarter.

    Used when generating engagement actuals so quarterly rollups land on the
    engineered demo targets (Operations 40.4% vs 43.4%, etc.).
    """
    p = DEMO_OVERRIDES["practice_margins"].get(demo_practice, {})
    return p.get("current_q_margin", 0.43) if is_current_quarter else p.get("prior_q_margin", 0.44)


def get_new_partner_class_targets() -> dict:
    """Return the dict of {demo_practice: count_new_partners} for the 76-partner class."""
    return dict(DEMO_OVERRIDES["new_partner_class"])


def get_ar_concentration_clients() -> list[dict]:
    """Return the list of named clients with engineered AR aging."""
    return list(DEMO_OVERRIDES["ar_concentration"])


def is_ai_practice(practice: str) -> bool:
    """Check if a practice name is the AI/GenAI practice (used for growth multipliers)."""
    return practice == DEMO_OVERRIDES["ai_practice"]["name"]


# Sanity print — log the targets we're aiming for so each run records them
print("=" * 70)
print("DEMO_OVERRIDES — engineered firmwide targets:")
print("=" * 70)
for k, v in DEMO_OVERRIDES["firmwide"].items():
    print(f"  {k:<30} {v:,}" if isinstance(v, int) else f"  {k:<30} {v}")
print()
print(f"  AR concentration clients: {[c['customer'] for c in DEMO_OVERRIDES['ar_concentration']]}")
print(f"  Office overrides: {list(DEMO_OVERRIDES['office_overrides'].keys())}")
print(f"  Seasonal anomalies: {list(DEMO_OVERRIDES['seasonal_anomalies'].keys())}")
print("=" * 70)

# COMMAND ----------

# DBTITLE 1,Generate Employee Reference Data
# MAGIC %md
# MAGIC ### Generate employees first — other tables reference employee_ids and partner IDs
# MAGIC

# COMMAND ----------

# DBTITLE 1,Employees (25,000) — reality-first distribution
print("Generating employees (target: 25K, elite-consulting-firm scale)...")

FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda",
    "David", "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Christopher", "Karen", "Charles", "Lisa", "Daniel", "Nancy",
    "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley",
    "Steven", "Dorothy", "Paul", "Kimberly", "Andrew", "Emily", "Joshua", "Donna",
    "Raj", "Priya", "Arjun", "Deepa", "Sanjay", "Anita", "Wei", "Li", "Ming", "Yuki",
    "Hiroshi", "Kenji", "Akiko", "Carlos", "Maria", "Pedro", "Ana", "Fatima", "Omar",
    "Ahmed", "Lars", "Ingrid", "Hans", "Greta", "Pierre", "Sophie", "Luca", "Giulia",
    "Sean", "Siobhan", "Ravi", "Neha", "Amit", "Pooja", "Vikram", "Kavitha",
    "Chen", "Zhang", "Liu", "Yang", "Huang", "Tanaka", "Suzuki", "Kim", "Park", "Lee",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson",
    "Walker", "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen",
    "Hill", "Flores", "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera",
    "Campbell", "Mitchell", "Carter", "Roberts", "Patel", "Sharma", "Kumar",
    "Singh", "Gupta", "Chen", "Wang", "Li", "Zhang", "Liu", "Tanaka", "Suzuki",
    "Watanabe", "Kim", "Park", "Choi", "Mueller", "Schmidt", "Fischer",
    "Weber", "Meyer", "Dubois", "Moreau", "Laurent", "O'Brien", "Murphy", "Kelly",
]

# ~18% attrition annually => ~20% of total headcount terminated in any snapshot
EMPLOYMENT_STATUSES = ["Active", "Active", "Active", "Active", "Active", "Active", "Active", "Active", "Terminated", "Terminated"]
EMPLOYEE_TYPES = ["Regular", "Regular", "Regular", "Regular", "Contractor"]
TIME_TYPES = ["Full-time", "Full-time", "Full-time", "Full-time", "Part-time"]
COST_CENTERS_LIST = [f"CC-{i:04d}" for i in range(1, 151)]
MANAGEMENT_LEVELS = ["Individual Contributor", "Individual Contributor", "Individual Contributor", "Team Lead", "Engagement Manager", "Associate Partner", "Partner", "Senior Partner"]

# Reality-first: target headcount sourced from DEMO_OVERRIDES.firmwide.employee_count.
# Each employee's home attributes (region, location, practice, industry, level) are
# sampled from realistic weighted distributions. Unlike the old make_mandatory()
# pattern, these attributes STICK to the employee and downstream FK joins (engagements
# → lead_partner, staffing → employee) derive their attributes from the employee.
TOTAL_EMPLOYEES = DEMO_OVERRIDES["firmwide"]["employee_count"]  # 25,000

employee_rows = []
_count_by_level = {lvl: 0 for lvl in JOB_LEVELS}
_count_by_practice = {p: 0 for p in PRACTICE_AREAS}

for i in range(TOTAL_EMPLOYEES):
    eid = uid()
    region   = pick_region()
    location = pick_location(region)
    practice = pick_practice()      # bronze name; silver remaps to demo name
    industry = pick_industry()      # nominal industry affiliation (employees serve many)
    customer = pick_customer(industry)
    level    = pick_level()
    _count_by_level[level] += 1
    _count_by_practice[practice] += 1

    first    = random.choice(FIRST_NAMES)
    last     = random.choice(LAST_NAMES)
    hire     = rand_date(date(2015, 1, 1), DATE_END - timedelta(days=60))
    # Partner attrition is MUCH lower than associate attrition at a Tier-1
    # firm. The uniform 20% rate from EMPLOYMENT_STATUSES annualizes to ~12%
    # partner attrition for the rolling-12 cohort, which read as the "24
    # partners exited in one month" red flag. Cap partner-level attrition at
    # ~5% (industry norm), keep junior levels at the higher rate (turnover
    # really does cluster at the bottom of the org).
    if level in ("Senior Partner", "Partner"):
        status = random.choices(["Active", "Terminated"], weights=[0.95, 0.05])[0]
    elif level in ("Associate Partner", "Director"):
        status = random.choices(["Active", "Terminated"], weights=[0.90, 0.10])[0]
    else:
        status = random.choice(EMPLOYMENT_STATUSES)
    term_date = (
        rand_date(max(hire + timedelta(days=180), DATE_START), DATE_END)
        if status == "Terminated" else None
    )
    emp_type  = random.choice(EMPLOYEE_TYPES)
    time_type = random.choice(TIME_TYPES)
    fte       = 1.0 if time_type == "Full-time" else round(random.uniform(0.4, 0.8), 2)
    mgmt_level = (
        "Senior Partner"    if level == "Senior Partner"
        else "Partner"      if level == "Partner"
        else "Associate Partner" if level == "Associate Partner"
        else random.choice(MANAGEMENT_LEVELS[:5])
    )
    created  = rand_ts(hire, min(hire + timedelta(days=1), DATE_END))
    modified = rand_ts(max(hire, DATE_START), DATE_END)

    employee_rows.append((
        eid, f"EMP-{i+1:05d}", first, last, first,
        hire, term_date, status, emp_type,
        f"{level} - {practice}", level, level, mgmt_level,
        random.choice(COST_CENTERS_LIST), location,
        practice, None if i == 0 else f"MGR-{random.randint(1, max(i, 1)):05d}",
        time_type, fte, created, modified,
        region, industry, customer,
    ))

# Distribution sanity prints — verify firmwide aggregates land where DEMO_OVERRIDES expects
print(f"  Generated {TOTAL_EMPLOYEES:,} employees")
print(f"  Level distribution:")
for lvl, cnt in sorted(_count_by_level.items(), key=lambda x: -x[1]):
    pct = 100 * cnt / TOTAL_EMPLOYEES
    print(f"    {lvl:<22} {cnt:>6,}  ({pct:.1f}%)")
print(f"  Practice distribution:")
for p, cnt in sorted(_count_by_practice.items(), key=lambda x: -x[1]):
    pct = 100 * cnt / TOTAL_EMPLOYEES
    print(f"    {p:<32} {cnt:>6,}  ({pct:.1f}%)")

# Schema cuts (per bronze_realism_audit.md §3.2): office (duplicate of location)
# and email_address (zero downstream consumers) removed.
emp_schema = StructType([
    StructField("employee_id", StringType()), StructField("employee_number", StringType()),
    StructField("first_name", StringType()), StructField("last_name", StringType()),
    StructField("preferred_name", StringType()),
    StructField("hire_date", DateType()), StructField("termination_date", DateType()),
    StructField("employment_status", StringType()), StructField("employee_type", StringType()),
    StructField("job_title", StringType()), StructField("job_profile", StringType()),
    StructField("job_level", StringType()), StructField("management_level", StringType()),
    StructField("cost_center", StringType()), StructField("location", StringType()),
    StructField("practice_area", StringType()),
    StructField("manager_id", StringType()), StructField("time_type", StringType()),
    StructField("fte", DoubleType()), StructField("created_date", TimestampType()),
    StructField("last_modified_date", TimestampType()),
    StructField("region", StringType()), StructField("industry", StringType()),
    StructField("customer", StringType()),
])

df_employees = spark.createDataFrame(employee_rows, schema=emp_schema)
# df_employees persisted via write_table below

# Build partner list for engagement FK (Partners + Senior Partners)
partner_ids = [r["employee_id"] for r in df_employees.filter(F.col("job_level").isin("Partner", "Senior Partner")).select("employee_id").collect()]
if not partner_ids:
    partner_ids = [r[0] for r in employee_rows[:50]]

# Build employee_id list
all_employee_ids = [r[0] for r in employee_rows]

print(f"Partners available for engagement FK: {len(partner_ids)}")

# COMMAND ----------

# DBTITLE 1,Table Creation Note
# MAGIC %md
# MAGIC ### Tables are created via CREATE OR REPLACE TABLE AS SELECT in the write_table() function
# MAGIC DDL is skipped on serverless — tables are defined by data generation cells below.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Write Employees Table
write_table(df_employees, "bronze_workday_employees")

# COMMAND ----------

# DBTITLE 1,Generate & Write Accounts (300)
print("Generating SFDC accounts...")

ACCOUNT_TYPES = ["Enterprise", "Strategic", "Key", "Growth", "Standard"]
ACCOUNT_STATUSES = ["Active", "Active", "Active", "Active", "Inactive"]
STREETS = ["100 Park Ave", "200 Market St", "1 Financial Center", "500 Boylston St", "350 5th Ave", "One World Trade Center"]
STATES = ["NY", "CA", "IL", "TX", "GA", "DC", "FL", "MA", "WA", "NJ"]
COUNTRIES = ["United States", "United Kingdom", "Germany", "India", "Japan", "Brazil", "Australia", "France", "Singapore", "Canada"]

account_rows = []
account_ids = []
# Weighted accounts per industry (not uniform)
_industry_account_targets = {ind: max(int(300 * w + 0.5), 20) for ind, w in zip(INDUSTRIES, INDUSTRY_WEIGHTS)}
# Adjust largest bucket so total is exactly 300
_adj = 300 - sum(_industry_account_targets.values())
_industry_account_targets[INDUSTRIES[0]] += _adj

for industry, customers in CUSTOMERS_BY_INDUSTRY.items():
    accounts_per_industry = _industry_account_targets[industry]
    for j in range(accounts_per_industry):
        m = make_mandatory()
        m["industry"] = industry
        m["customer"] = customers[j % len(customers)]
        aid = uid()
        account_ids.append(aid)
        created = rand_ts()
        account_rows.append((
            aid, m["customer"] + (f" - {m['region']}" if j >= len(customers) else ""),
            f"ACC-{len(account_rows)+1:06d}", random.choice(ACCOUNT_TYPES),
            industry, round(random.uniform(50_000_000, 5_000_000_000), 2),
            random.randint(1000, 500000),
            random.choice(STREETS), m["location"], random.choice(STATES),
            random.choice(COUNTRIES), f"{random.randint(10000, 99999)}",
            f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}",
            f"www.{m['customer'].lower().replace(' ', '').replace('&','')}.com",
            random.choice(all_employee_ids[:500]), None,
            random.choice(ACCOUNT_STATUSES),
            created, random.choice(all_employee_ids[:500]),
            rand_ts(), random.choice(all_employee_ids[:500]),
            m["region"], m["location"], m["practice_area"], m["customer"],
        ))

acct_schema = StructType([
    StructField("account_id", StringType()), StructField("account_name", StringType()),
    StructField("account_number", StringType()), StructField("account_type", StringType()),
    StructField("industry", StringType()), StructField("annual_revenue", DoubleType()),
    StructField("number_of_employees", IntegerType()),
    StructField("billing_street", StringType()), StructField("billing_city", StringType()),
    StructField("billing_state", StringType()), StructField("billing_country", StringType()),
    StructField("billing_postal_code", StringType()), StructField("phone", StringType()),
    StructField("website", StringType()), StructField("owner_id", StringType()),
    StructField("parent_account_id", StringType()), StructField("account_status", StringType()),
    StructField("created_date", TimestampType()), StructField("created_by_id", StringType()),
    StructField("last_modified_date", TimestampType()), StructField("last_modified_by_id", StringType()),
    StructField("region", StringType()), StructField("location", StringType()),
    StructField("practice_area", StringType()), StructField("customer", StringType()),
])

write_table(spark.createDataFrame(account_rows, schema=acct_schema), "bronze_sfdc_accounts")

# Build account_lookup ONCE here — every downstream table that FK-references an
# account (opportunities, contracts, engagements, AR) derives its customer /
# industry / region / location from this dict. account_rows tuple indices:
# 0=aid, 4=industry, 21=region, 22=location, 24=customer.
account_lookup = {
    r[0]: {
        "customer": r[24],
        "industry": r[4],
        "region":   r[21],
        "location": r[22],
    }
    for r in account_rows
}

# COMMAND ----------

# DBTITLE 1,Generate & Write Opportunities (4,500)
print("Generating SFDC opportunities...")

STAGES = ["Prospecting", "Qualification", "Proposal", "Negotiation", "Closed Won", "Closed Lost"]
STAGE_PROBS = {"Prospecting": 0.10, "Qualification": 0.25, "Proposal": 0.50, "Negotiation": 0.75, "Closed Won": 1.0, "Closed Lost": 0.0}
FORECAST_CATS = ["Pipeline", "Best Case", "Commit", "Closed"]
LEAD_SOURCES = ["Partner Referral", "Web", "Conference", "Cold Call", "Existing Client", "RFP"]
OPP_TYPES = ["New Business", "Existing Business", "Renewal", "Expansion"]

opp_rows = []
opp_ids = []
# Pick a random practice for the opportunity (independent of account — opportunities
# are forward-looking pursuits that may sell across practices into the same account).
for i in range(4500):
    oid = uid()
    opp_ids.append(oid)
    # FK to a real account → opportunity customer/industry/region/location are
    # consistent with the account being pursued
    aid = random.choice(account_ids)
    acct = account_lookup[aid]
    practice = pick_practice()  # which practice is selling this opportunity
    stage = weighted_choice(STAGES, [0.12, 0.18, 0.20, 0.15, 0.20, 0.15])
    is_won = stage == "Closed Won"
    is_closed = stage in ("Closed Won", "Closed Lost")
    # Higher floor opportunity amounts
    ar = random.random()
    if ar < 0.05:
        amount = round(random.uniform(500_000, 1_500_000), 2)     # Small diagnostic
    elif ar < 0.40:
        amount = round(random.uniform(1_500_000, 5_000_000), 2)   # Standard
    elif ar < 0.75:
        amount = round(random.uniform(5_000_000, 15_000_000), 2)  # Large
    elif ar < 0.95:
        amount = round(random.uniform(15_000_000, 50_000_000), 2) # Enterprise
    else:
        amount = round(random.uniform(50_000_000, 150_000_000), 2) # Mega

    opp_rows.append((
        oid, f"{acct['customer']} - {practice} - {random.randint(2023, 2026)}",
        aid, stage, amount,
        STAGE_PROBS[stage], rand_date(),
        random.choice(FORECAST_CATS), random.choice(LEAD_SOURCES),
        f"Follow up on {practice} proposal", random.choice(OPP_TYPES),
        random.choice(all_employee_ids[:500]),
        is_won, is_closed, rand_ts(), rand_ts(),
        acct["region"], acct["location"], practice, acct["industry"], acct["customer"],
    ))

opp_schema = StructType([
    StructField("opportunity_id", StringType()), StructField("opportunity_name", StringType()),
    StructField("account_id", StringType()), StructField("stage_name", StringType()),
    StructField("amount", DoubleType()), StructField("probability", DoubleType()),
    StructField("close_date", DateType()), StructField("forecast_category", StringType()),
    StructField("lead_source", StringType()), StructField("next_step", StringType()),
    StructField("type", StringType()), StructField("owner_id", StringType()),
    StructField("is_won", BooleanType()), StructField("is_closed", BooleanType()),
    StructField("created_date", TimestampType()), StructField("last_modified_date", TimestampType()),
    StructField("region", StringType()), StructField("location", StringType()),
    StructField("practice_area", StringType()), StructField("industry", StringType()),
    StructField("customer", StringType()),
])

write_table(spark.createDataFrame(opp_rows, schema=opp_schema), "bronze_sfdc_opportunities")

# COMMAND ----------

# DBTITLE 1,Generate & Write Contracts (2,500)
print("Generating SFDC contracts...")

CONTRACT_STATUSES = ["Active", "Active", "Active", "Expired", "Draft"]
BILLING_FREQS = ["Monthly", "Quarterly", "Milestone", "Annual"]
PAYMENT_TERMS_LIST = ["Net 30", "Net 30", "Net 30", "Net 30", "Net 30", "Net 45", "Net 45", "Net 60", "Net 90"]

contract_rows = []
contract_ids = []
for i in range(2500):
    cid = uid()
    contract_ids.append(cid)
    # FK to a real account → contract customer/industry/region derive from it
    aid = random.choice(account_ids)
    acct = account_lookup[aid]
    practice = pick_practice()
    start = rand_date()
    term = random.choice([6, 12, 18, 24, 36])
    end = start + timedelta(days=term * 30)
    status = "Active" if end > DATE_END else "Expired"
    tcv = round(random.uniform(1_000_000, 50_000_000), 2)

    contract_rows.append((
        cid, f"CON-{i+1:06d}", aid,
        random.choice(opp_ids[:min(len(opp_ids), 2500)]),
        status, start, end, term, tcv,
        random.choice(BILLING_FREQS), random.choice(PAYMENT_TERMS_LIST),
        random.choice(all_employee_ids[:500]),
        start - timedelta(days=random.randint(1, 14)),
        start - timedelta(days=random.randint(0, 7)),
        random.choice(["Standard terms", "Custom MSA", "Government terms", None]),
        status == "Active", rand_ts(), rand_ts(),
        acct["region"], acct["location"], practice, acct["industry"], acct["customer"],
    ))

con_schema = StructType([
    StructField("contract_id", StringType()), StructField("contract_number", StringType()),
    StructField("account_id", StringType()), StructField("opportunity_id", StringType()),
    StructField("status", StringType()), StructField("start_date", DateType()),
    StructField("end_date", DateType()), StructField("contract_term", IntegerType()),
    StructField("total_contract_value", DoubleType()), StructField("billing_frequency", StringType()),
    StructField("payment_terms", StringType()), StructField("owner_id", StringType()),
    StructField("company_signed_date", DateType()), StructField("customer_signed_date", DateType()),
    StructField("special_terms", StringType()), StructField("is_active", BooleanType()),
    StructField("created_date", TimestampType()), StructField("last_modified_date", TimestampType()),
    StructField("region", StringType()), StructField("location", StringType()),
    StructField("practice_area", StringType()), StructField("industry", StringType()),
    StructField("customer", StringType()),
])

write_table(spark.createDataFrame(contract_rows, schema=con_schema), "bronze_sfdc_contracts")

# COMMAND ----------

# DBTITLE 1,Generate & Write Engagements (8,000)
print("Generating SFDC engagements...")

SERVICE_LINES = [
    "Strategy Review", "Post-Merger Integration", "Digital Roadmap",
    "Org Redesign", "Pricing Optimization", "Due Diligence",
    "Performance Transformation", "Growth Strategy", "Operations Improvement",
    "Cost Reduction", "Sustainability Transition", "Innovation Lab",
]
# engagement types: Fixed Price 45%, T&M 25%, Retainer 15%, Outcome-Based 15%
ENGAGEMENT_TYPES = ["Fixed Price"] * 9 + ["Time & Materials"] * 5 + ["Retainer"] * 3 + ["Outcome-Based"] * 3
ENGAGEMENT_STATUSES = ["Active", "Active", "Active", "Completed", "On Hold", "Planned"]

# Partner info lookup: engagement derives practice/location/region from lead partner.
# This enforces the rule "Partners lead engagements in their own practice from their
# home office" which is realistic and gives downstream rollups internally-consistent
# (office, practice) cells for the engineered demo narratives.
partner_info_lookup = {
    r[0]: {
        "practice": r[15],
        "location": r[14],
        "region":   r[21],
    }
    for r in employee_rows
    if r[10] in ("Partner", "Senior Partner")
}
partner_ids_list = list(partner_info_lookup.keys())
assert partner_ids_list, "No partners available — engagement generation requires Partner-level employees"

engagement_rows = []
engagement_ids = []
# Stash archetype per engagement for the downstream event simulator. The bronze
# tuple doesn't include archetype (would be a schema change), so we keep this
# dict alongside engagement_lookup as the simulator's source of "how to bill".
engagement_archetype_by_id: dict[str, str] = {}
for i in range(8000):
    eid = uid()
    engagement_ids.append(eid)

    # STEP 1: pick a lead partner → engagement's practice/location/region derive from them
    lead_partner_id = random.choice(partner_ids_list)
    p = partner_info_lookup[lead_partner_id]
    engagement_practice = p["practice"]
    engagement_location = p["location"]
    engagement_region   = p["region"]

    # STEP 2: pick an account → engagement's customer/industry derive from the account
    aid = random.choice(account_ids)
    acct = account_lookup[aid]
    customer_name = acct["customer"]
    industry      = acct["industry"]

    # STEP 3: pick the service_line first → maps to an archetype → drives BOTH
    # budget bounds and project_name template. This is the fix for "$149M Comcast
    # Due Diligence" (DD engagements now bounded $0.5M-$8M) and for generic project
    # names like "BP - Strategy Review" (now templated as "BP Operating Model
    # Transformation" etc).
    service_line = random.choice(SERVICE_LINES)
    archetype = ENGAGEMENT_ARCHETYPE_BY_SERVICE_LINE.get(service_line, "Advisory")
    arc_meta = ENGAGEMENT_ARCHETYPES[archetype]

    # Date range — duration bounded by archetype (DD: 30-120d, Transformation: 360-1440d, etc.)
    d_min, d_max = arc_meta["duration_days"]
    start = rand_date()
    duration_days = random.randint(d_min, d_max)
    end = start + timedelta(days=duration_days)
    # Cap engagement end_date at DATE_END + 90 days so in-flight projects don't
    # bleed 6+ months into the future. Without this cap, project_end_date can
    # land in Jun-Nov 2026 when DATE_END = May 17 2026, and any Genie query
    # that mistakenly filters by project_end_date (instead of the canonical
    # monthly-grain `fiscal_period`) pulls future-windowed rows that don't
    # align with actuals tables. Keeping a 90-day forward window preserves
    # realistic in-flight project density without the far-future tail.
    max_end = DATE_END + timedelta(days=90)
    if end > max_end:
        end = max_end

    # Budget bounded by archetype. Inside the archetype band, draw uniformly
    # so we get a mix across the full range (not all clustered at the bottom).
    b_min, b_max = arc_meta["budget"]
    budget = round(random.uniform(b_min, b_max), 2)

    # Project name from archetype-aware template — no more generic
    # "{client} - {service_line}" labels.
    project_name = engagement_name(customer_name, archetype, service_line, start.year)

    # forecasted_revenue = budget + noise. Office multipliers are applied to ACTUAL
    # revenue at silver (silver_fact_timecards.billing_amount), so budget side stays
    # neutral — that's what creates the office variance narrative at gold rollup.
    forecasted = round(budget * random.uniform(0.85, 1.15), 2)

    status = random.choice(ENGAGEMENT_STATUSES)
    mgr = random.choice(all_employee_ids[:1000])

    engagement_rows.append((
        eid, project_name,
        f"ENG-{i+1:06d}",
        aid, random.choice(contract_ids),
        random.choice(opp_ids[:3000]),
        engagement_practice, service_line, random.choice(ENGAGEMENT_TYPES),
        start, end, lead_partner_id, mgr,
        status, budget, forecasted, engagement_location,
        rand_ts(), rand_ts(),
        engagement_region, engagement_location, industry, customer_name,
    ))
    engagement_archetype_by_id[eid] = archetype

# Sanity prints
print(f"  Generated {len(engagement_rows):,} engagements")
print(f"  Sample project names (verify client matches): {[r[1] for r in engagement_rows[:3]]}")
# Verify FK consistency: every engagement's customer should match its account's customer.
# Engagement tuple indices: 0=eid, 1=name, 2=number, 3=aid, 4=contract, 5=opp, 6=practice,
# 7=service_line, 8=type, 9=start, 10=end, 11=lead, 12=mgr, 13=status, 14=budget,
# 15=forecasted, 16=office_loc, 17=created, 18=modified, 19=region, 20=location, 21=industry, 22=customer
_mismatch = sum(1 for r in engagement_rows if r[22] != account_lookup[r[3]]["customer"])
print(f"  FK consistency: {len(engagement_rows) - _mismatch:,} / {len(engagement_rows):,} engagements have matching customer↔account")

# Build engagement_lookup so downstream tables (timecards, assignments, AR, expenses)
# derive region/location/practice/industry/customer from the engagement they're on,
# not from the employee's home office or random make_mandatory() picks.
engagement_lookup = {
    r[0]: {
        "region":     r[19],
        "location":   r[20],
        "practice":   r[6],
        "industry":   r[21],
        "customer":   r[22],
        "account_id": r[3],
        "lead_partner": r[11],
        "budget":     r[14],
        "start_date": r[9],
        "end_date":   r[10],
    }
    for r in engagement_rows
}
print(f"  Built engagement_lookup with {len(engagement_lookup):,} entries — downstream FK-consistency source")

# Index engagements by customer so AR generation can pick an engagement that
# actually belongs to the billing customer. Without this index, AR rows assign
# a random `customer_name` AND a random `engagement_id` independently, which
# produces invoices where the project name belongs to a different client than
# the customer billed (e.g. "Siemens" invoice tied to "Tencent — Cost Reduction"
# project). Surfaced as the client/project mismatch in Top Unpaid Invoices.
engagements_by_customer: dict[str, list[str]] = {}
for eid, meta in engagement_lookup.items():
    engagements_by_customer.setdefault(meta["customer"], []).append(eid)
print(f"  Indexed engagements across {len(engagements_by_customer):,} unique customers")

# ──────────────────────────────────────────────────────────────────────────────
#  EVENT SIMULATOR — coherent monthly billing/payment/expense events per engagement
# ──────────────────────────────────────────────────────────────────────────────
#
#  Replaces the previous "fill each bronze table with independent random rows"
#  pattern with a single source of truth: for every engagement, walk month-by-
#  month from start_date to end_date and emit the events that should flow from
#  doing that work:
#
#    - monthly_billing_event    → bronze_sap_accounts_receivable invoice
#    - monthly_collection_event → fills payment_date on the AR invoice (or
#                                  leaves it open if not yet paid as of today)
#    - monthly_direct_expense_event → bronze_concur_expense_items rows
#    - monthly_vendor_po_event  → bronze_sap_purchase_orders + bronze_sap_accounts_payable
#    - monthly_billable_hours_event → bronze_workday_timecards billable rows
#
#  All amounts derive from the engagement's archetype + budget + duration.
#  Every cross-table comparison reconciles by construction (AR sum ≡ revenue
#  recognized; AP sum capped per vendor; project margins continuous across
#  months because the same engagements span them). This is the architectural
#  fix for the "two slices of the same metric disagree" class of bug.
print("\n=== Running engagement event simulator (coherent monthly events) ===")

# Output: module-level event lists consumed by downstream bronze sections.
SIM_AR_INVOICES: list[dict] = []          # one entry per monthly billing event
SIM_AP_INVOICES: list[dict] = []          # one entry per vendor invoice (driven by project + overhead spend)
SIM_DIRECT_EXPENSES: list[dict] = []      # one entry per direct-expense event (travel, subcontractor, data, etc.)
SIM_VENDOR_POS: list[dict] = []           # one PO per AP invoice (or batched per category)

# Per-vendor running spend totals — enforces calibrated per-vendor max so no
# single vendor (e.g. DigitalOcean) can balloon to $500M+ open AP. The cap is
# derived from "share of firm's annual vendor spend per category / vendors in
# category" with realistic headroom.
# Cloud category averages out to ~$250M/yr / 7 vendors = ~$35M/yr per vendor.
# Multi-year window gives ~$100M envelope per vendor; cap at 150% of that.
PER_VENDOR_LIFETIME_CAP_USD = {
    "Cloud Infrastructure":         150_000_000,
    "Software Licensing":            30_000_000,
    "Staffing & Contractors":        25_000_000,
    "Real Estate & Facilities":      80_000_000,
    "Travel & T&E":                  10_000_000,
    "Benefits & Insurance":         100_000_000,
    "Data & Research Subscriptions": 15_000_000,
    "Professional Services":         40_000_000,
    "Marketing & Events":             8_000_000,
    "Office Supplies & Equipment":    6_000_000,
}

# Each vendor gets a *jittered* personal cap so the top-N vendor leaderboard
# doesn't look like a flat row of vendors at exactly the cap. Multiplier is
# 0.4-1.0 of the category cap, giving realistic spread between "we use them
# a lot" vs "we use them lightly" within the same category.
_per_vendor_cap_override: dict = {
    vid: PER_VENDOR_LIFETIME_CAP_USD.get(cat, 50_000_000) * random.uniform(0.40, 1.00)
    for vid, _vname, cat, _vrange in _AP_VENDOR_RECORDS
}
_vendor_running_spend: dict[str, float] = {}  # vendor_id → cumulative spend so far

# DSO calibration: per-client payment archetype drives realistic AR aging.
# Each customer is assigned an archetype ONCE (deterministic via random.seed
# at module load); all of that customer's invoices draw payment-day-offset
# from that distribution. This produces:
#   - real 61-90 + 91+ bucket population (chronic-late clients)
#   - cohort variance (top-N vs non-top-N have different archetype mixes)
#   - per-bucket weighted-DSO differences across cohorts
# Without per-client archetypes, every cohort partition of a single global
# distribution converges to the same weighted DSO (CLT artifact) — the
# "synthetic data smell" a CFO will catch in a drill-down.
PAYMENT_ARCHETYPES = {
    "fast":    {"mean": 22, "std": 7,  "tail_prob": 0.02, "tail_extra": (10, 30)},
    "normal":  {"mean": 38, "std": 11, "tail_prob": 0.05, "tail_extra": (15, 45)},
    "slow":    {"mean": 60, "std": 14, "tail_prob": 0.10, "tail_extra": (20, 60)},
    "chronic": {"mean": 95, "std": 22, "tail_prob": 0.20, "tail_extra": (30, 90)},
}
# Mix: 30% fast / 45% normal / 18% slow / 7% chronic — produces aging mix
# ~55-70% in 0-30, ~20-30% in 31-60, ~5-12% in 61-90, ~3-8% in 91+.
_ARCHETYPE_NAMES = ["fast", "normal", "slow", "chronic"]
_ARCHETYPE_WEIGHTS = [0.30, 0.45, 0.18, 0.07]

# Per-customer archetype assignment (account_id → archetype name).
account_payment_archetype: dict[str, str] = {
    aid: random.choices(_ARCHETYPE_NAMES, weights=_ARCHETYPE_WEIGHTS, k=1)[0]
    for aid in account_ids
}


def _draw_dso_days(account_id: str = None) -> int:
    """Days from invoice issue to payment for one AR invoice.
    Routes through per-customer archetype when account_id is provided."""
    arch_name = account_payment_archetype.get(account_id, "normal") if account_id else "normal"
    arch = PAYMENT_ARCHETYPES[arch_name]
    base = random.gauss(arch["mean"], arch["std"])
    if random.random() < arch["tail_prob"]:
        base += random.uniform(*arch["tail_extra"])
    return max(5, min(int(round(base)), 240))


def _draw_dpo_days() -> int:
    """Days from invoice receipt to payment for one AP invoice."""
    base = random.gauss(38, 12)
    if random.random() < 0.03:
        base += random.uniform(15, 60)
    return max(3, min(int(round(base)), 180))


# Direct-expense ratio by archetype (% of monthly revenue). Used to size the
# project's monthly third-party spend (travel/contractors/data/etc.).
DIRECT_EXPENSE_RATIO_BY_ARCHETYPE = {
    "DueDiligence":       0.12,
    "Strategy":           0.06,
    "Advisory":           0.04,
    "TechImplementation": 0.09,
    "Transformation":     0.11,
    "ManagedServices":    0.07,
    "Audit":              0.05,
}

# Expense category split within a project (sums to 1.0). Drives WHICH vendor
# category each direct-expense event picks from.
EXPENSE_CATEGORY_SPLIT = {
    "Travel & T&E":                  0.42,
    "Staffing & Contractors":        0.28,
    "Data & Research Subscriptions": 0.14,
    "Office Supplies & Equipment":   0.08,
    "Professional Services":         0.08,
}


def _months_active(start_date, end_date) -> list:
    """Yield first-of-month dates for every month between start_date and end_date inclusive."""
    if start_date > end_date:
        return []
    cur = start_date.replace(day=1)
    last = end_date.replace(day=1)
    out = []
    while cur <= last:
        out.append(cur)
        # advance one month
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)
    return out


def _end_of_month(d):
    """Return last day of d's month."""
    if d.month == 12:
        nxt = date(d.year + 1, 1, 1)
    else:
        nxt = date(d.year, d.month + 1, 1)
    return nxt - timedelta(days=1)


_ar_inv_counter = 0
_ap_inv_counter = 0
_po_counter = 0
_today_anchor = DATE_END

# Cap individual invoice size to avoid $50M+ "tower" AR rows. A real consulting
# firm rarely issues a single invoice >$15M (those would be milestone payments
# on huge programs and the demo doesn't model milestone billing). Splits any
# month's would-be invoice into batches that each fit under the cap.
SINGLE_INVOICE_CAP_USD = 15_000_000

for eng_eid in engagement_ids:
    meta = engagement_lookup[eng_eid]
    archetype = engagement_archetype_by_id.get(eng_eid, "Advisory")
    arc_meta = ENGAGEMENT_ARCHETYPES[archetype]
    start_d = meta["start_date"]
    end_d   = meta["end_date"]
    budget  = float(meta["budget"])
    months = _months_active(start_d, end_d)
    if not months:
        continue

    monthly_revenue = budget / len(months)
    expense_ratio = DIRECT_EXPENSE_RATIO_BY_ARCHETYPE.get(archetype, 0.08)

    for fm in months:
        # Slight monthly variance ±15% on the steady-state run-rate.
        mvar = random.uniform(0.85, 1.15)
        this_month_revenue = monthly_revenue * mvar

        # ── AR billing event ──────────────────────────────────────────────
        # Invoice issued at end of fiscal month. Split into N invoices if it
        # exceeds the cap so we never get $50M+ singletons.
        issue_date = _end_of_month(fm) + timedelta(days=random.randint(0, 5))
        if issue_date > _today_anchor:
            continue  # don't issue future-dated invoices
        n_invoices = max(1, int(this_month_revenue / SINGLE_INVOICE_CAP_USD) + (1 if this_month_revenue % SINGLE_INVOICE_CAP_USD else 0))
        per_inv_amount = round(this_month_revenue / n_invoices, 2)
        for k in range(n_invoices):
            _ar_inv_counter += 1
            inv_number = f"INV-AR-{_ar_inv_counter:07d}"
            terms_days = random.choice([30, 30, 30, 45, 60])
            due_date = issue_date + timedelta(days=terms_days)
            # Draw payment offset ONCE per invoice — fixes the BP-oscillation
            # bug where monthly snapshots redrew payment dates randomly. Routes
            # through the customer's payment archetype so chronic-late clients
            # populate the 91+ bucket and the firmwide aging mix has a real tail.
            pay_offset = _draw_dso_days(meta["account_id"])
            payment_date_candidate = issue_date + timedelta(days=pay_offset)
            if payment_date_candidate <= _today_anchor:
                payment_date = payment_date_candidate
                pay_status = "Paid"
            else:
                payment_date = None
                # Aging-driven status: overdue if past due_date, else open
                if _today_anchor > due_date:
                    pay_status = "Overdue"
                else:
                    pay_status = "Open"
            SIM_AR_INVOICES.append({
                "invoice_id":    uid(),
                "customer_id":   meta["account_id"],
                "customer_name": meta["customer"],
                "invoice_number": inv_number,
                "invoice_date":  issue_date,
                "due_date":      due_date,
                "posting_date":  issue_date,
                "amount":        per_inv_amount,
                "currency":      "USD",
                "payment_terms": f"Net {terms_days}",
                "payment_status": pay_status,
                "payment_date":  payment_date,
                "days_outstanding": (
                    (payment_date - issue_date).days if payment_date is not None
                    else max(0, (_today_anchor - issue_date).days)
                ),
                "gl_account":    random.choice([f"{random.randint(4000,4999)}" for _ in range(1)]),
                "project_id":    eng_eid,
                "cost_center":   None,  # filled in by AR section using COST_CENTERS_LIST
                "profit_center": None,
                "region":        meta["region"],
                "location":      meta["location"],
                "practice_area": meta["practice"],
                "industry":      meta["industry"],
                "customer":      meta["customer"],
            })

        # ── Direct project expenses for the month ─────────────────────────
        this_month_expense_target = this_month_revenue * expense_ratio
        # Distribute across expense categories per split.
        for category, share in EXPENSE_CATEGORY_SPLIT.items():
            cat_amount = this_month_expense_target * share
            if cat_amount < 100:
                continue  # don't generate trivially-small expenses
            # Pick a vendor in this category, respecting per-vendor lifetime cap.
            candidates = [v for v in _AP_VENDOR_RECORDS if v[2] == category]
            if not candidates:
                continue
            # Find a vendor that hasn't blown its jittered lifetime cap yet.
            random.shuffle(candidates)
            chosen = None
            for vid, vname, vcategory, vrange in candidates:
                cap = _per_vendor_cap_override.get(vid, PER_VENDOR_LIFETIME_CAP_USD.get(vcategory, 50_000_000))
                if _vendor_running_spend.get(vid, 0) + cat_amount <= cap:
                    chosen = (vid, vname, vcategory, vrange)
                    break
            if chosen is None:
                # All vendors in this category at cap — skip this expense event.
                # Means firmwide spend in this category settled at calibrated maximum.
                continue
            vid, vname, vcategory, vrange = chosen
            _vendor_running_spend[vid] = _vendor_running_spend.get(vid, 0) + cat_amount
            expense_date = fm + timedelta(days=random.randint(0, 27))
            SIM_DIRECT_EXPENSES.append({
                "engagement_id": eng_eid,
                "expense_date":  expense_date,
                "category":      category,
                "amount":        round(cat_amount, 2),
                "vendor_id":     vid,
                "vendor_name":   vname,
                "region":        meta["region"],
                "location":      meta["location"],
                "practice_area": meta["practice"],
                "industry":      meta["industry"],
                "customer":      meta["customer"],
            })
            # And the corresponding AP invoice + PO from that vendor.
            _ap_inv_counter += 1
            _po_counter += 1
            ap_issue_date = expense_date + timedelta(days=random.randint(0, 7))
            ap_terms_days = random.choice([30, 30, 45, 60])
            ap_due_date = ap_issue_date + timedelta(days=ap_terms_days)
            ap_pay_offset = _draw_dpo_days()
            ap_pay_date_candidate = ap_issue_date + timedelta(days=ap_pay_offset)
            if ap_pay_date_candidate <= _today_anchor:
                ap_payment_date = ap_pay_date_candidate
                ap_status = "Paid"
            else:
                ap_payment_date = None
                ap_status = "Overdue" if _today_anchor > ap_due_date else "Open"
            SIM_AP_INVOICES.append({
                "invoice_id":      uid(),
                "vendor_id":       vid,
                "vendor_name":     vname,
                "invoice_number":  f"INV-AP-{_ap_inv_counter:07d}",
                "invoice_date":    ap_issue_date,
                "due_date":        ap_due_date,
                "posting_date":    ap_issue_date,
                "amount":          round(cat_amount, 2),
                "currency":        "USD",
                "payment_terms":   f"Net {ap_terms_days}",
                "payment_status":  ap_status,
                "payment_date":    ap_payment_date,
                "vendor_category": vcategory,
                "engagement_id":   eng_eid,
                "region":          meta["region"],
                "location":        meta["location"],
                "practice_area":   meta["practice"],
                "industry":        meta["industry"],
                "customer":        meta["customer"],
            })
            SIM_VENDOR_POS.append({
                "po_id":         uid(),
                "po_number":     f"PO-{_po_counter:07d}",
                "vendor_id":     vid,
                "vendor_name":   vname,
                "po_date":       ap_issue_date - timedelta(days=random.randint(1, 14)),
                "delivery_date": ap_issue_date + timedelta(days=random.randint(7, 30)),
                "amount":        round(cat_amount, 2),
                "currency":      "USD",
                "engagement_id": eng_eid,
                "vendor_category": vcategory,
                "region":        meta["region"],
                "location":      meta["location"],
                "practice_area": meta["practice"],
                "industry":      meta["industry"],
                "customer":      meta["customer"],
            })

print(f"  Engagement-event simulator emitted: AR invoices={len(SIM_AR_INVOICES):,}  AP invoices={len(SIM_AP_INVOICES):,}  Direct expenses={len(SIM_DIRECT_EXPENSES):,}  POs={len(SIM_VENDOR_POS):,}")

# ── Firm-overhead layer ──────────────────────────────────────────────────
# Project-direct expenses cover Travel, Staffing, Data subs, Office Supplies,
# Professional Services — but NOT cloud infra, software licensing, real estate,
# benefits, marketing. Those are FIRM-LEVEL overhead per office, not tied to
# specific engagements. Without this layer, the top-vendor leaderboard had
# no AWS / Microsoft / WeWork / Aetna etc. — unrealistic for a $18B firm.
# Generates one monthly AP invoice per (office, overhead-category, month)
# during the demo period.
print("  Generating firm-overhead AP layer (cloud / software / real estate / benefits / marketing)...")

# Per-office monthly spend per overhead category (rough Tier-1 magnitudes).
# Calibrated to land each category at ~its calibration-target share of
# firmwide AP — Cloud 18% / Benefits 10% / Real Estate 12% / Software 16%
# / Marketing 2% — without exceeding per-vendor jittered caps.
OVERHEAD_MONTHLY_PER_OFFICE = {
    "Cloud Infrastructure":     2_500_000,   # AWS / Azure / GCP usage scales with workforce
    "Software Licensing":       1_800_000,   # MS / Salesforce / Workday / ServiceNow seats
    "Real Estate & Facilities": 1_200_000,   # office lease + facilities mgmt
    "Benefits & Insurance":     1_400_000,   # health / dental / 401(k) admin
    "Marketing & Events":         300_000,   # marketing + conferences
}

# Walk all months in date range × all offices × each overhead category.
all_offices = list({meta["location"] for meta in engagement_lookup.values()})
all_months_in_demo = _months_active(DATE_START, DATE_END)
for office in all_offices:
    # Pick a primary region for the office's office overhead (used in AP attribution)
    primary_region = next((meta["region"] for meta in engagement_lookup.values() if meta["location"] == office), "Americas")

    for fm in all_months_in_demo:
        for category, monthly_amount in OVERHEAD_MONTHLY_PER_OFFICE.items():
            # Pick a vendor in this category honoring jittered per-vendor cap.
            candidates = [v for v in _AP_VENDOR_RECORDS if v[2] == category]
            if not candidates:
                continue
            random.shuffle(candidates)
            chosen = None
            for vid, vname, vcategory, vrange in candidates:
                cap = _per_vendor_cap_override.get(vid, PER_VENDOR_LIFETIME_CAP_USD.get(vcategory, 50_000_000))
                if _vendor_running_spend.get(vid, 0) + monthly_amount <= cap:
                    chosen = (vid, vname, vcategory, vrange)
                    break
            if chosen is None:
                continue
            vid, vname, vcategory, vrange = chosen
            # Small ±15% jitter so monthly invoices don't all look identical.
            invoice_amount = round(monthly_amount * random.uniform(0.85, 1.15), 2)
            _vendor_running_spend[vid] = _vendor_running_spend.get(vid, 0) + invoice_amount

            ap_issue_date = fm + timedelta(days=random.randint(0, 20))
            if ap_issue_date > _today_anchor:
                continue
            ap_terms_days = random.choice([30, 30, 30, 45])
            ap_due_date = ap_issue_date + timedelta(days=ap_terms_days)
            ap_pay_offset = _draw_dpo_days()
            ap_pay_date_candidate = ap_issue_date + timedelta(days=ap_pay_offset)
            if ap_pay_date_candidate <= _today_anchor:
                ap_payment_date = ap_pay_date_candidate
                ap_status = "Paid"
            else:
                ap_payment_date = None
                ap_status = "Overdue" if _today_anchor > ap_due_date else "Open"

            _ap_inv_counter += 1
            _po_counter += 1
            SIM_AP_INVOICES.append({
                "invoice_id":      uid(),
                "vendor_id":       vid,
                "vendor_name":     vname,
                "invoice_number":  f"INV-AP-{_ap_inv_counter:07d}",
                "invoice_date":    ap_issue_date,
                "due_date":        ap_due_date,
                "posting_date":    ap_issue_date,
                "amount":          invoice_amount,
                "currency":        "USD",
                "payment_terms":   f"Net {ap_terms_days}",
                "payment_status":  ap_status,
                "payment_date":    ap_payment_date,
                "vendor_category": vcategory,
                "engagement_id":   None,  # firm-level, not project-specific
                "region":          primary_region,
                "location":        office,
                "practice_area":   "Firm Overhead",
                "industry":        None,
                "customer":        None,
            })
            SIM_VENDOR_POS.append({
                "po_id":         uid(),
                "po_number":     f"PO-{_po_counter:07d}",
                "vendor_id":     vid,
                "vendor_name":   vname,
                "po_date":       ap_issue_date - timedelta(days=random.randint(1, 14)),
                "delivery_date": ap_issue_date + timedelta(days=random.randint(7, 30)),
                "amount":        invoice_amount,
                "currency":      "USD",
                "engagement_id": None,
                "vendor_category": vcategory,
                "region":        primary_region,
                "location":      office,
                "practice_area": "Firm Overhead",
                "industry":      None,
                "customer":      None,
            })

print(f"  Firm-overhead layer added: total AP invoices now {len(SIM_AP_INVOICES):,}  POs now {len(SIM_VENDOR_POS):,}")
if _vendor_running_spend:
    print(f"  Per-vendor cap enforcement: {len(_vendor_running_spend)} vendors saw activity; max single-vendor lifetime ${max(_vendor_running_spend.values())/1e6:,.1f}M")

# Team assignments (engagement ↔ employee per fiscal month) are built later,
# right before the timecard generator that consumes them. They need emp_info
# which is computed inside the timecard prep section — see the
# `TEAM_SIZE_BY_ARCHETYPE` block down there.

eng_schema = StructType([
    StructField("engagement_id", StringType()), StructField("engagement_name", StringType()),
    StructField("engagement_number", StringType()),
    StructField("account_id", StringType()), StructField("contract_id", StringType()),
    StructField("opportunity_id", StringType()),
    StructField("practice_area", StringType()), StructField("service_line", StringType()),
    StructField("engagement_type", StringType()),
    StructField("start_date", DateType()), StructField("end_date", DateType()),
    StructField("lead_partner", StringType()), StructField("engagement_manager", StringType()),
    StructField("status", StringType()), StructField("budget_amount", DoubleType()),
    StructField("forecasted_revenue", DoubleType()), StructField("office_location", StringType()),
    StructField("created_date", TimestampType()), StructField("last_modified_date", TimestampType()),
    StructField("region", StringType()), StructField("location", StringType()),
    StructField("industry", StringType()), StructField("customer", StringType()),
])

write_table(spark.createDataFrame(engagement_rows, schema=eng_schema), "bronze_sfdc_engagements")

# COMMAND ----------

# DBTITLE 1,Generate & Write Forecasts (12,000)
print("Generating SFDC forecasts...")

forecast_rows = []
# Forecast amounts SHRUNK 2026-05-19 (was $200K-$4M base, summing to ~$60B
# total pipeline = 10× annual revenue per practice/region cohort, which read
# as "Tax APAC pipeline $162M vs accrued $15M = 10×" red flag). Real-firm
# pipeline coverage is 1.5-2.5× quarterly recognized revenue, NOT 10×.
# New base $50K-$800K × seasonality × growth gives ~$8-10B firmwide forecast,
# which lands within 1.5-2× the firm's ~$5B/quarter recognized run-rate.
for i in range(12000):
    # Forecast tied to a specific opportunity → derive customer/industry/region from
    # the opportunity's account (pick opp_row directly to avoid O(n) index lookup)
    opp_row = random.choice(opp_rows)
    oid = opp_row[0]
    aid = opp_row[2]            # account_id position in opp tuple
    acct = account_lookup[aid]
    practice = opp_row[18]      # practice_area position in opp tuple
    yr = random.choice([2023, 2024, 2025, 2026])
    qtr = random.randint(1, 4)
    q_start = date(yr, (qtr - 1) * 3 + 1, 1)
    if qtr < 4:
        q_end = date(yr, qtr * 3 + 1, 1) - timedelta(days=1)
    else:
        q_end = date(yr, 12, 31)
    # Seasonal variation: Q4 higher, Q1 lower
    _q_mult = {1: 0.85, 2: 1.0, 3: 1.05, 4: 1.15}[qtr]
    # Year-over-year growth: ~8% per year
    _yr_mult = 1.0 + 0.08 * (yr - 2023)
    base_amount = random.uniform(50_000, 800_000)
    amount = round(base_amount * _q_mult * _yr_mult, 2)

    forecast_rows.append((
        uid(), random.choice(all_employee_ids[:500]),
        oid, random.choice(FORECAST_CATS),
        amount, random.randint(1, 20),
        q_start, q_end, f"Q{qtr}", yr,
        rand_ts(), rand_ts(),
        acct["region"], acct["location"], practice, acct["industry"], acct["customer"],
    ))

fc_schema = StructType([
    StructField("forecast_id", StringType()), StructField("owner_id", StringType()),
    StructField("opportunity_id", StringType()), StructField("forecast_category", StringType()),
    StructField("forecast_amount", DoubleType()), StructField("forecast_quantity", IntegerType()),
    StructField("period_start_date", DateType()), StructField("period_end_date", DateType()),
    StructField("fiscal_quarter", StringType()), StructField("fiscal_year", IntegerType()),
    StructField("created_date", TimestampType()), StructField("last_modified_date", TimestampType()),
    StructField("region", StringType()), StructField("location", StringType()),
    StructField("practice_area", StringType()), StructField("industry", StringType()),
    StructField("customer", StringType()),
])

write_table(spark.createDataFrame(forecast_rows, schema=fc_schema), "bronze_sfdc_forecasts")

# COMMAND ----------

# DBTITLE 1,Generate & Write Positions (2,000)
print("Generating Workday positions...")

JOB_FAMILIES = ["Consulting", "Engineering", "Analytics", "Design", "Management", "Sales", "Delivery"]
SUPERVISORY_ORGS = [f"{pa} - {r}" for pa in PRACTICE_AREAS for r in REGIONS]

position_rows = []
for i in range(2000):
    m = make_mandatory()
    level = pick_level()
    mgmt = "Senior Partner" if level == "Senior Partner" else ("Partner" if level == "Partner" else ("Associate Partner" if level == "Associate Partner" else "Individual Contributor"))

    position_rows.append((
        uid(), f"{level} {random.choice(JOB_FAMILIES)} - {m['practice_area']}",
        level, random.choice(JOB_FAMILIES), level, mgmt,
        random.choice(COST_CENTERS_LIST), m["location"],
        random.choice(SUPERVISORY_ORGS),
        random.choice(["Open", "Filled", "Filled", "Filled"]),
        rand_date(), rand_ts(),
        m["region"], m["practice_area"], m["industry"], m["customer"],
    ))

pos_schema = StructType([
    StructField("position_id", StringType()), StructField("position_title", StringType()),
    StructField("job_profile", StringType()), StructField("job_family", StringType()),
    StructField("job_level", StringType()), StructField("management_level", StringType()),
    StructField("cost_center", StringType()), StructField("location", StringType()),
    StructField("supervisory_org", StringType()), StructField("position_status", StringType()),
    StructField("effective_date", DateType()), StructField("created_date", TimestampType()),
    StructField("region", StringType()), StructField("practice_area", StringType()),
    StructField("industry", StringType()), StructField("customer", StringType()),
])

write_table(spark.createDataFrame(position_rows, schema=pos_schema), "bronze_workday_positions")

# COMMAND ----------

# DBTITLE 1,Generate Timecards (Batched for 45K employees)
# MAGIC %md
# MAGIC ### Timecards — the revenue driver (BATCHED for scale)
# MAGIC - Each active employee gets 1-2 entries per workweek (billable + non-billable)
# MAGIC - All billable entries carry the employee's billing rate (no zero rates)
# MAGIC - the consulting firm time types: ~55% billable, ~25% non-billable, ~8% PTO, ~7% Training, ~5% Firm Building
# MAGIC - Processing employees in batches of 5,000 to avoid memory issues

# COMMAND ----------

# DBTITLE 1,Build Employee Info & Mondays
print("Pre-computing employee info for timecards...")

# Practice-area billing premiums (the consulting firm)
PRACTICE_BILLING_PREMIUMS = {
    # Aligned with practice margin profile in DEMO_OVERRIDES.practice_margins
    "Strategy & Consulting":  1.20,  # highest premium, 50% margin
    "Managed Services: Tech": 1.15,  # AI/GenAI premium, 51% margin (highest)
    "Technology":             1.10,  # 47% margin
    "Audit":                  1.05,  # 43% margin
    "Tax":                    1.00,  # 45% margin
    "Accounting":             1.00,  # 41% margin
    "Managed Services: Ops":  0.95,  # 42% margin
    "Operations":             0.90,  # 40% margin (most commoditized)
}

# Pre-compute employee info for speed.
# Indices below MUST match the tuple-build at line 267 and emp_schema at line 279.
# Schema has 24 fields after bronze_realism_audit cuts (office + email_address removed).
emp_info = {}
for row in employee_rows:
    eid = row[0]
    level = row[11]  # job_level
    region = row[21]  # region
    location = row[14]  # location
    practice = row[15]  # practice_area
    industry = row[22]  # industry
    customer = row[23]  # customer
    status = row[7]  # employment_status
    hire = row[5]  # hire_date
    term = row[6]  # termination_date
    _practice_bill_mult = PRACTICE_BILLING_PREMIUMS.get(practice, 1.0)
    # Location-based billing discount
    _loc_bill_mult = {
        "Mumbai": 0.58, "Shanghai": 0.62, "Bangkok": 0.55,
        "Seoul": 0.78, "Singapore": 0.85, "Dubai": 0.90,
        "Milan": 0.92, "Amsterdam": 0.95, "Frankfurt": 0.95,
        "Munich": 0.95, "Paris": 0.95, "Zurich": 1.05,
        "London": 1.00, "Toronto": 0.92, "Sydney": 0.95,
        "Tokyo": 0.90, "Sao Paulo": 0.68, "Hong Kong": 0.88,
    }.get(location, 1.0)  # US cities default to 1.0
    # Location-based cost multiplier
    _loc_cost_mult = {
        "Mumbai": 0.50, "Shanghai": 0.55, "Bangkok": 0.48,
        "Seoul": 0.70, "Singapore": 0.80, "Dubai": 0.85,
        "Milan": 0.90, "Amsterdam": 0.95, "Frankfurt": 0.95,
        "Munich": 0.95, "Paris": 0.95, "Zurich": 1.05,
        "London": 1.00, "Toronto": 0.90, "Sydney": 0.95,
        "Tokyo": 0.92, "Sao Paulo": 0.60, "Hong Kong": 0.85,
    }.get(location, 1.0)
    emp_info[eid] = {
        "level": level, "region": region, "location": location,
        "practice": practice, "industry": industry, "customer": customer,
        "status": status, "hire": hire, "term": term,
        "billing_rate": round(BILLING_RATES.get(level, 475.0) * _practice_bill_mult * _loc_bill_mult, 2),
        "cost_rate": round(COST_RATES.get(level, 140.0) * _loc_cost_mult, 2),
    }

# Generate Mondays (week-start dates) across the date range
all_mondays = []
d = DATE_START
while d <= DATE_END:
    days_ahead = 0 - d.weekday()
    if days_ahead < 0:
        days_ahead += 7
    monday = d + timedelta(days=days_ahead)
    if monday <= DATE_END:
        all_mondays.append(monday)
    d = monday + timedelta(days=7)

PROJECT_IDS = engagement_ids  # Use actual engagement IDs for FK integrity
TASK_IDS = [f"TASK-{j:04d}" for j in range(1, 51)]
# Realistic approval: 92% approved, 5% pending, 3% rejected
TC_APPROVAL_STATUSES = ["Approved"] * 92 + ["Pending"] * 5 + ["Rejected"] * 3

emp_id_list = list(emp_info.keys())
print(f"  Employees: {len(emp_id_list):,}, Mondays: {len(all_mondays)}")

# ── Engagement team assignments per fiscal month ──────────────────────────
# Used by the timecard generator below so employee → engagement assignment
# is STABLE within a month (an employee billing 100 hours in a month sees
# those hours flow to 1-3 engagements, not 30 random ones). Without this,
# timecard-derived revenue can't reconcile to AR invoices because the
# "who billed which engagement" assignment is randomized per entry,
# scattering revenue across uncorrelated engagements.
#
# Team size per archetype (intentionally smaller than calibration.yml's
# "typical_team_size" so total billable hours per engagement stays
# realistic — a project that bills $1M/month at ~$1500/hr ≈ 667 hrs/month
# ≈ 5-8 FTEs on it). Calibrated for the synthetic 8K engagement portfolio.
print("  Building engagement team assignments per month...")
TEAM_SIZE_BY_ARCHETYPE = {
    "DueDiligence":       (3, 7),
    "Strategy":           (5, 12),
    "Advisory":           (3, 8),
    "TechImplementation": (8, 20),
    "Transformation":     (12, 30),
    "ManagedServices":    (6, 18),
    "Audit":              (4, 12),
}

_emp_pool_by_region_practice: dict = {}
_emp_pool_by_region: dict = {}
for _eid_for_pool, _info_for_pool in emp_info.items():
    rp = (_info_for_pool["region"], _info_for_pool["practice"])
    _emp_pool_by_region_practice.setdefault(rp, []).append(_eid_for_pool)
    _emp_pool_by_region.setdefault(_info_for_pool["region"], []).append(_eid_for_pool)

# {(engagement_id, fiscal_month_start_date): [employee_ids]}
engagement_team_by_month: dict = {}
# {(employee_id, fiscal_month_start_date): [engagement_ids]}
employee_engagements_by_month: dict = {}

for eng_eid in engagement_ids:
    meta = engagement_lookup[eng_eid]
    archetype = engagement_archetype_by_id.get(eng_eid, "Advisory")
    team_min, team_max = TEAM_SIZE_BY_ARCHETYPE.get(archetype, (4, 10))
    team_size_target = random.randint(team_min, team_max)

    rp_pool = _emp_pool_by_region_practice.get((meta["region"], meta["practice"]), [])
    if len(rp_pool) < team_size_target:
        rp_pool = _emp_pool_by_region.get(meta["region"], [])
    if len(rp_pool) < team_size_target:
        rp_pool = list(emp_info.keys())

    # Stable team for the engagement's lifetime. Realistic; team rotation
    # happens at engagement boundaries in this simplified model.
    team = random.sample(rp_pool, min(team_size_target, len(rp_pool)))

    months = _months_active(meta["start_date"], meta["end_date"])
    for fm in months:
        engagement_team_by_month[(eng_eid, fm)] = team
        for member_eid in team:
            employee_engagements_by_month.setdefault((member_eid, fm), []).append(eng_eid)

print(f"  Built {len(engagement_team_by_month):,} (engagement, month) team assignments; "
      f"{len(employee_engagements_by_month):,} (employee, month) → engagement-list entries")

# COMMAND ----------

# DBTITLE 1,Build & Write Timecard Data (Batched)
print("Generating timecards (BATCHED per 5,000 employees)... this may take several minutes.")

tc_schema = StructType([
    StructField("timecard_id", StringType()), StructField("employee_id", StringType()),
    StructField("work_date", DateType()), StructField("week_ending_date", DateType()),
    StructField("project_id", StringType()), StructField("task_id", StringType()),
    StructField("time_type", StringType()), StructField("hours", DoubleType()),
    StructField("overtime_hours", DoubleType()), StructField("billing_rate", DoubleType()),
    StructField("cost_rate", DoubleType()), StructField("approval_status", StringType()),
    StructField("approved_by", StringType()), StructField("approved_date", DateType()),
    StructField("submitted_date", DateType()), StructField("comments", StringType()),
    StructField("created_date", TimestampType()), StructField("last_modified_date", TimestampType()),
    StructField("region", StringType()), StructField("location", StringType()),
    StructField("practice_area", StringType()), StructField("industry", StringType()),
    StructField("customer", StringType()),
])

BATCH_SIZE = 5000
total_tc_rows = 0

for batch_idx in range(0, len(emp_id_list), BATCH_SIZE):
    batch_emps = emp_id_list[batch_idx : batch_idx + BATCH_SIZE]
    batch_num = batch_idx // BATCH_SIZE + 1
    print(f"  Processing batch {batch_num} ({len(batch_emps):,} employees)...")

    timecard_rows = []
    for eid in batch_emps:
        info = emp_info[eid]
        hire = info["hire"]
        term = info["term"]

        # 2026-05-20 FIX (A5): per-level billable share. Prior version applied
        # uniform 55% billable to every employee, which made partner-level
        # billable share land near the 50% threshold — surfacing ~1,664
        # partners as "sub-50% utilization" on Michael's CHRO tile. Real Big 4
        # partner utilization is 70-85% (so partner bench rate is 15-30%, not
        # 56%). Bias the billable threshold by job level so cohort utilization
        # lands at realistic ranges.
        _level = info.get("level") or ""
        if _level in ("Senior Partner",):
            _billable_threshold = 0.82  # SP utilization 78-82% target
        elif _level in ("Partner",):
            _billable_threshold = 0.78  # Partner 74-78% target
        elif _level in ("Associate Partner",):
            _billable_threshold = 0.72
        elif _level in ("Director",):
            _billable_threshold = 0.68
        elif _level in ("Engagement Manager",):
            _billable_threshold = 0.65
        elif _level in ("Associate",):
            _billable_threshold = 0.60
        elif _level in ("Business Analyst",):
            _billable_threshold = 0.55
        else:
            _billable_threshold = 0.55

        for monday in all_mondays:
            week_ending = monday + timedelta(days=6)

            # Skip weeks outside employment period
            if monday < hire or (term and monday > term):
                continue

            # Time type distribution (level-biased, see _billable_threshold above):
            # Billable: per-level (50-82%), Non-Billable: 20%, PTO: 8%, Training: 7%, Firm Building: 5%.
            # Remaining buckets keep fixed shares; billable share comes from the level threshold.
            r = random.random()
            if r < _billable_threshold:
                time_type = "Billable"
                # the consulting firm consultants work intensely: 35-55 hrs/week average
                hours = round(random.uniform(35.0, 55.0), 1)
                ot = round(random.uniform(0, 4.0), 1) if random.random() < 0.08 else 0.0
                b_rate = info["billing_rate"] * random.uniform(0.90, 1.10)  # +/-10% variance
                # Apply seasonal revenue multiplier from DEMO_OVERRIDES.seasonal_anomalies.
                # Affects this row's billable rate, which propagates through billing_amount
                # → silver_fact_timecards → gold_enterprise_metrics.revenue → firmwide RPP.
                # Only billable rows are affected (non-billable rows have b_rate=0).
                _seasonal_rev_mult = get_seasonal_multiplier(monday.year, monday.month, "revenue_multiplier")
                if _seasonal_rev_mult != 1.0:
                    b_rate *= _seasonal_rev_mult
            elif r < 0.80:
                time_type = "Non-Billable"
                hours = round(random.uniform(2.0, 25.0), 1)
                ot = 0.0
                b_rate = 0.0
            elif r < 0.88:
                time_type = "PTO"
                hours = random.choice([8.0, 16.0, 24.0, 32.0, 40.0])
                ot = 0.0
                b_rate = 0.0
            elif r < 0.95:
                time_type = "Training"
                hours = round(random.uniform(4.0, 24.0), 1)
                ot = 0.0
                b_rate = 0.0
            else:
                time_type = "Firm Building"
                hours = round(random.uniform(4.0, 16.0), 1)
                ot = 0.0
                b_rate = 0.0

            work_date = monday + timedelta(days=random.randint(0, 4))
            c_rate = info["cost_rate"]
            approval = random.choice(TC_APPROVAL_STATUSES)
            approved_by = random.choice(all_employee_ids[:500]) if approval == "Approved" else None
            approved_date = work_date + timedelta(days=random.randint(1, 7)) if approval == "Approved" else None

            # Pick engagement from the employee's STABLE monthly assignment list.
            # Previously this was `random.choice(engagement_ids)`, which made
            # every billable timecard land on an uncorrelated engagement —
            # so timecard-derived revenue never matched per-engagement AR
            # invoice totals. With per-month stable assignments (built above
            # in `employee_engagements_by_month`), all of an employee's
            # billable hours in a given month flow to 1-3 engagements that
            # they're actually staffed on. Falls back to random ONLY if the
            # employee has no monthly assignment (rare — e.g., partners
            # whose region/practice combo wasn't picked by any engagement).
            fm_key = monday.replace(day=1)
            assignments = employee_engagements_by_month.get((eid, fm_key))
            if assignments:
                engagement_id = random.choice(assignments)
            else:
                engagement_id = random.choice(engagement_ids)
            eng = engagement_lookup[engagement_id]

            timecard_rows.append((
                uid(), eid, work_date, week_ending,
                engagement_id, random.choice(TASK_IDS),
                time_type, hours, ot, round(b_rate, 2), c_rate,
                approval, approved_by, approved_date,
                work_date + timedelta(days=random.randint(0, 2)), None,
                rand_ts(), rand_ts(),
                eng["region"], eng["location"], eng["practice"],
                eng["industry"], eng["customer"],
            ))

    batch_count = len(timecard_rows)
    total_tc_rows += batch_count
    print(f"    Batch {batch_num}: {batch_count:,} rows generated, writing to table...")

    df_tc_batch = spark.createDataFrame(timecard_rows, schema=tc_schema)
    if batch_idx == 0:
        write_table(df_tc_batch, "bronze_workday_timecards", mode="overwrite")
    else:
        write_table(df_tc_batch, "bronze_workday_timecards", mode="append")

    # Free memory
    del timecard_rows
    del df_tc_batch

print(f"  Total timecard entries: {total_tc_rows:,}")

# COMMAND ----------

# DBTITLE 1,Generate & Write Billing Rates (45,000)
print("Generating billing rates...")

CURRENCIES = ["USD", "USD", "USD", "EUR", "GBP", "INR", "JPY", "AUD", "BRL", "SGD"]
RATE_TYPES = ["Standard", "Standard", "Premium", "Discounted"]

br_rows = []
for i in range(45000):
    eid = random.choice(emp_id_list)
    info = emp_info[eid]
    eff = rand_date()

    br_rows.append((
        uid(), eid, info["level"], info["level"],
        info["practice"], info["billing_rate"] * random.uniform(0.9, 1.1),
        random.choice(CURRENCIES), eff, eff + timedelta(days=365),
        random.choice(RATE_TYPES), random.choice(account_ids[:200]),
        rand_ts(),
        info["region"], info["location"],
    ))

# Schema cuts (per bronze_realism_audit.md §3.2): industry + customer removed —
# rate cards are not customer-specific in real Workday; these were wrong-entity tags.
br_schema = StructType([
    StructField("rate_id", StringType()), StructField("employee_id", StringType()),
    StructField("job_profile", StringType()), StructField("job_level", StringType()),
    StructField("practice_area", StringType()), StructField("billing_rate", DoubleType()),
    StructField("currency", StringType()), StructField("effective_date", DateType()),
    StructField("end_date", DateType()), StructField("rate_type", StringType()),
    StructField("client_id", StringType()), StructField("created_date", TimestampType()),
    StructField("region", StringType()), StructField("location", StringType()),
])

write_table(spark.createDataFrame(br_rows, schema=br_schema), "bronze_workday_billing_rates")

# COMMAND ----------

# DBTITLE 1,Generate & Write Cost Rates (45,000)
print("Generating cost rates...")

cr_rows = []
for i in range(45000):
    m = make_mandatory()
    eid = random.choice(emp_id_list)
    info = emp_info[eid]
    hourly = info["cost_rate"] * random.uniform(0.9, 1.1)
    annual = round(hourly * 2080, 2)
    eff = rand_date()

    cr_rows.append((
        uid(), eid, annual, round(hourly, 2),
        random.choice(CURRENCIES), eff, eff + timedelta(days=365),
        random.choice(COST_CENTERS_LIST), rand_ts(),
        info["region"], info["location"], info["practice"],
    ))

# Schema cuts (per bronze_realism_audit.md §3.2): industry + customer removed —
# cost rates are not customer-specific in real Workday.
cr_schema = StructType([
    StructField("cost_rate_id", StringType()), StructField("employee_id", StringType()),
    StructField("annual_salary", DoubleType()), StructField("hourly_cost_rate", DoubleType()),
    StructField("currency", StringType()), StructField("effective_date", DateType()),
    StructField("end_date", DateType()), StructField("cost_center", StringType()),
    StructField("created_date", TimestampType()),
    StructField("region", StringType()), StructField("location", StringType()),
    StructField("practice_area", StringType()),
])

write_table(spark.createDataFrame(cr_rows, schema=cr_schema), "bronze_workday_cost_rates")

# COMMAND ----------

# DBTITLE 1,Generate & Write Assignments (40,000)
print("Generating assignments...")

ASSIGNMENT_TYPES = ["Billable", "Billable", "Billable", "Internal", "Bench"]
ROLES_ON_PROJECT = ["Developer", "Consultant", "Analyst", "Architect", "Project Manager", "Lead", "SME", "QA"]
ASSIGNMENT_STATUSES = ["Active", "Active", "Active", "Completed", "Planned"]

asgn_rows = []
for i in range(40000):
    eid = random.choice(emp_id_list)
    # Pick the engagement first → assignment FK-consistent with its engagement.
    # client_id = engagement's account_id (NOT independently random).
    engagement_id = random.choice(engagement_ids)
    eng = engagement_lookup[engagement_id]
    client_id = eng["account_id"]

    start = rand_date()
    end = start + timedelta(days=random.randint(30, 365))

    asgn_rows.append((
        uid(), eid, engagement_id, client_id,
        random.choice(ASSIGNMENT_TYPES), round(random.uniform(25, 100), 0),
        start, end, random.choice(ROLES_ON_PROJECT),
        random.choice(ASSIGNMENT_STATUSES),
        rand_ts(), rand_ts(),
        eng["region"], eng["location"], eng["practice"],
        eng["industry"], eng["customer"],
    ))

asgn_schema = StructType([
    StructField("assignment_id", StringType()), StructField("employee_id", StringType()),
    StructField("project_id", StringType()), StructField("client_id", StringType()),
    StructField("assignment_type", StringType()), StructField("allocation_percentage", DoubleType()),
    StructField("start_date", DateType()), StructField("end_date", DateType()),
    StructField("role_on_project", StringType()), StructField("assignment_status", StringType()),
    StructField("created_date", TimestampType()), StructField("last_modified_date", TimestampType()),
    StructField("region", StringType()), StructField("location", StringType()),
    StructField("practice_area", StringType()), StructField("industry", StringType()),
    StructField("customer", StringType()),
])

write_table(spark.createDataFrame(asgn_rows, schema=asgn_schema), "bronze_workday_assignments")

# COMMAND ----------

# DBTITLE 1,Generate & Write Organizations (50)
print("Generating organizations...")

ORG_TYPES = ["Practice", "Region", "Service Line", "Industry Group", "Corporate"]

org_rows = []
for i in range(50):
    m = make_mandatory()
    region = m["region"]
    loc = m["location"]
    country_map = {
        "Americas": random.choice(["United States", "Canada", "Brazil"]),
        "EMEA": random.choice(["United Kingdom", "France", "Germany", "Switzerland", "Netherlands", "UAE"]),
        "Asia Pacific": random.choice(["India", "Australia", "Japan", "Singapore", "China", "South Korea", "Hong Kong", "Thailand"]),
    }

    org_rows.append((
        uid(), f"{m['practice_area']} - {region}" if i < 15 else f"{m['industry']} - {region}",
        random.choice(ORG_TYPES), None if i < 5 else uid(),
        f"ORG-{i+1:04d}", loc, region,
        country_map[region], random.choice(COST_CENTERS_LIST),
        random.choice(all_employee_ids[:100]),
        rand_date(date(2020, 1, 1), date(2023, 3, 1)), rand_ts(),
        m["practice_area"], m["industry"], m["customer"],
    ))

org_schema = StructType([
    StructField("organization_id", StringType()), StructField("organization_name", StringType()),
    StructField("organization_type", StringType()), StructField("parent_organization_id", StringType()),
    StructField("organization_code", StringType()), StructField("location", StringType()),
    StructField("region", StringType()), StructField("country", StringType()),
    StructField("cost_center", StringType()), StructField("manager_id", StringType()),
    StructField("effective_date", DateType()), StructField("created_date", TimestampType()),
    StructField("practice_area", StringType()), StructField("industry", StringType()),
    StructField("customer", StringType()),
])

write_table(spark.createDataFrame(org_rows, schema=org_schema), "bronze_workday_organizations")

# COMMAND ----------

# DBTITLE 1,Generate & Write Expense Reports (90,000)
print("Generating Concur expense reports...")

EXPENSE_APPROVAL_STATUSES = ["Approved", "Approved", "Approved", "Submitted", "Returned", "Draft"]
PAYMENT_STATUSES = ["Paid", "Paid", "Paid", "Processing", "Pending"]
BUSINESS_PURPOSES = [
    "Client travel - project delivery", "Client meeting", "Team offsite",
    "Conference attendance", "Training event", "Internal meeting",
    "Pre-sales meeting", "Proposal development", "Leadership summit",
    "Project kickoff", "Quarterly business review", "Vendor evaluation",
]

er_rows = []
er_ids = []
for i in range(90000):
    eid = random.choice(emp_id_list)
    info = emp_info[eid]
    # FK-consistency: expense report ties to an engagement (T&E charged to a project).
    # Region/location/practice/industry/customer come from the engagement, not the
    # employee's home (office-P&L convention).
    engagement_id = random.choice(engagement_ids)
    eng = engagement_lookup[engagement_id]
    rid = uid()
    er_ids.append(rid)
    report_date = rand_date()
    submit_date = report_date + timedelta(days=random.randint(1, 7))
    approval = random.choice(EXPENSE_APPROVAL_STATUSES)
    # Realistic expense report totals: right-skewed distribution ($150-$4,000, avg ~$800)
    total = round(random.lognormvariate(6.5, 0.7), 2)  # median ~$665, mean ~$900
    total = max(150, min(total, 6000))  # clamp to realistic range
    approved_amt = total if approval == "Approved" else round(total * random.uniform(0.8, 1.0), 2)
    reimb = round(approved_amt * random.uniform(0.85, 1.0), 2)
    payment = random.choice(PAYMENT_STATUSES) if approval == "Approved" else "Pending"

    country_map = {
        "Americas": random.choice(["United States", "Canada", "Brazil"]),
        "EMEA": random.choice(["United Kingdom", "France", "Germany", "Switzerland"]),
        "Asia Pacific": random.choice(["India", "Australia", "Japan", "Singapore"]),
    }

    er_rows.append((
        rid, f"Expense Report - {report_date.strftime('%B %Y')}",
        f"ER-{i+1:07d}", eid,
        f"Employee {eid[:8]}", report_date, submit_date,
        approval,
        submit_date + timedelta(days=random.randint(3, 12)) if approval == "Approved" else None,
        random.choice(all_employee_ids[:500]) if approval == "Approved" else None,
        payment,
        submit_date + timedelta(days=random.randint(7, 21)) if payment == "Paid" else None,
        total, approved_amt, reimb,
        random.choice(["USD", "EUR", "GBP", "INR", "JPY"]),
        engagement_id, eng["customer"],
        random.choice(COST_CENTERS_LIST), random.choice(BUSINESS_PURPOSES),
        country_map[eng["region"]],
        rand_ts(), rand_ts(),
        eng["region"], eng["location"], eng["practice"],
        eng["industry"], eng["customer"],
    ))

er_schema = StructType([
    StructField("report_id", StringType()), StructField("report_name", StringType()),
    StructField("report_number", StringType()), StructField("employee_id", StringType()),
    StructField("employee_name", StringType()), StructField("report_date", DateType()),
    StructField("submit_date", DateType()), StructField("approval_status", StringType()),
    StructField("approved_date", DateType()), StructField("approved_by", StringType()),
    StructField("payment_status", StringType()), StructField("paid_date", DateType()),
    StructField("total_amount", DoubleType()), StructField("approved_amount", DoubleType()),
    StructField("reimbursable_amount", DoubleType()), StructField("currency_code", StringType()),
    StructField("project_id", StringType()), StructField("client_name", StringType()),
    StructField("cost_center", StringType()), StructField("business_purpose", StringType()),
    StructField("country", StringType()),
    StructField("created_date", TimestampType()), StructField("last_modified_date", TimestampType()),
    StructField("region", StringType()), StructField("location", StringType()),
    StructField("practice_area", StringType()), StructField("industry", StringType()),
    StructField("customer", StringType()),
])

write_table(spark.createDataFrame(er_rows, schema=er_schema), "bronze_concur_expense_reports")

# COMMAND ----------

# DBTITLE 1,Generate & Write Expense Items (270,000)
print("Generating Concur expense items...")

# Realistic T&E expense types (no procurement items like Software/Telecom/Office Supplies)
EXPENSE_TYPES = ["Airfare", "Hotel", "Meals", "Ground Transportation", "Car Rental", "Parking", "Mileage", "Internet/Phone", "Professional Development", "Client Entertainment"]
EXPENSE_CATEGORIES_MAP = {
    "Airfare": "Travel", "Hotel": "Travel", "Meals": "Travel",
    "Ground Transportation": "Travel", "Car Rental": "Travel", "Parking": "Travel",
    "Mileage": "Travel", "Internet/Phone": "Travel",
    "Professional Development": "Training", "Client Entertainment": "Client",
}
# Type-specific vendor mapping (realistic pairings)
VENDORS_BY_TYPE = {
    "Airfare": ["United Airlines", "Delta Airlines", "American Airlines", "British Airways", "Lufthansa", "Southwest Airlines"],
    "Hotel": ["Marriott", "Hilton", "Hyatt", "IHG", "Westin", "Courtyard by Marriott"],
    "Meals": ["Local Restaurant", "Sweetgreen", "Chipotle", "Room Service", "Starbucks", "Team Dinner", "Client Lunch"],
    "Ground Transportation": ["Uber", "Lyft", "Yellow Cab", "Airport Shuttle", "Via"],
    "Car Rental": ["Hertz", "Enterprise", "Avis", "National", "Budget"],
    "Parking": ["LAZ Parking", "SP Plus", "Airport Parking", "Garage Parking"],
    "Mileage": ["Personal Vehicle", "Personal Vehicle"],
    "Internet/Phone": ["Verizon", "AT&T", "T-Mobile", "Hotel WiFi"],
    "Professional Development": ["Coursera", "LinkedIn Learning", "AWS Training", "Conference Registration"],
    "Client Entertainment": ["Restaurant Group", "Event Venue", "Sports Tickets", "Golf Club"],
}
# Corporate card dominant (90%), minimal cash (<5%)
PAYMENT_TYPE_LIST = ["Corporate Card"] * 18 + ["Personal Card"] * 3 + ["Cash"] * 1

# Weighted expense type distribution
_EXPENSE_TYPE_WEIGHTS = [
    0.10,  # Airfare
    0.14,  # Hotel
    0.28,  # Meals - most frequent
    0.16,  # Ground Transportation
    0.05,  # Car Rental
    0.07,  # Parking
    0.06,  # Mileage
    0.04,  # Internet/Phone
    0.05,  # Professional Development
    0.05,  # Client Entertainment
]

ei_rows = []
for i in range(270000):
    rid = random.choice(er_ids)
    # FK-consistency: expense item ties to a real engagement; location/customer/region
    # derive from that engagement (office-P&L convention — expense books to the office
    # of the engagement, not employee's home office).
    engagement_id = random.choice(engagement_ids)
    eng = engagement_lookup[engagement_id]
    exp_type = weighted_choice(EXPENSE_TYPES, _EXPENSE_TYPE_WEIGHTS)
    txn_date = rand_date()

    # Seasonal multiplier: Q4 higher, Jan/Feb mildly lower, summer mild dip.
    # 2026-05-21 — softened. Prior values (Q4 1.15-1.30, Q1 0.75-0.90) produced
    # ~40% peak-to-trough swing in firmwide expenses Dec 2025 → Jan/Feb 2026
    # ($782M → $444M visibly), which read as a data bug to any CFO. The
    # Q4 boost is also redundant with DEMO_OVERRIDES.seasonal_anomalies which
    # already adds +8%/+12% to Nov/Dec. New ranges produce ~12-15% peak-to-trough
    # which is realistic for Tier-1 consulting T&E seasonality and aligns with
    # the gold_regional_pnl column comment "Q4 spike (Nov/Dec ~22% over)".
    month = txn_date.month
    seasonal = 1.0
    if month in (10, 11, 12):
        seasonal = random.uniform(1.05, 1.12)   # Q4 mild lift (holiday entertainment + year-end conferences)
    elif month in (1, 2):
        seasonal = random.uniform(0.94, 0.99)   # Q1 very mild post-holiday softness
    elif month in (7, 8):
        seasonal = random.uniform(0.93, 0.98)   # Summer mild dip

    # Realistic amount ranges by type
    if exp_type == "Airfare":
        amount = round(random.lognormvariate(6.2, 0.5) * seasonal, 2)
        amount = max(150, min(amount, 3500))
    elif exp_type == "Hotel":
        nights = random.choices([1, 2, 3, 4, 5], weights=[15, 30, 25, 20, 10])[0]
        rate = random.uniform(120, 380)
        amount = round(nights * rate * seasonal, 2)
    elif exp_type == "Meals":
        amount = round(random.lognormvariate(3.5, 0.6) * seasonal, 2)
        amount = max(5, min(amount, 250))
    elif exp_type == "Ground Transportation":
        amount = round(random.lognormvariate(3.3, 0.5) * seasonal, 2)
        amount = max(5, min(amount, 200))
    elif exp_type == "Car Rental":
        days = random.choices([1, 2, 3, 5, 7], weights=[20, 25, 25, 20, 10])[0]
        amount = round(random.uniform(45, 120) * days * seasonal, 2)
    elif exp_type == "Parking":
        amount = round(random.uniform(5, 45) * seasonal, 2)
    elif exp_type == "Mileage":
        miles = random.randint(10, 300)
        amount = round(miles * 0.67, 2)  # IRS mileage rate
    elif exp_type == "Internet/Phone":
        amount = round(random.uniform(10, 50) * seasonal, 2)
    elif exp_type == "Professional Development":
        amount = round(random.lognormvariate(5.5, 0.8), 2)
        amount = max(25, min(amount, 2500))
    elif exp_type == "Client Entertainment":
        amount = round(random.lognormvariate(5.0, 0.6) * seasonal, 2)
        amount = max(50, min(amount, 2000))
    else:
        amount = round(random.uniform(10, 200), 2)

    # Apply DEMO_OVERRIDES engineered patterns:
    # 1) Per-office expense multipliers — driven by DEMO_OVERRIDES.office_overrides.
    #    Two layers stack: `expense_multiplier` applies to ALL expense rows from
    #    that office (the general expense-overrun factor), and
    #    `billable_expense_multiplier` applies ADDITIONALLY to billable rows
    #    (a sharper inflation on the billable-T&E sub-category). Use both to
    #    target a specific office's headline overage:
    #      - NYC: expense 1.28 + billable_expense 1.42 → headline overage office
    #      - Chicago / DC: secondary overage
    #      - London / Bangkok: deflators (under budget)
    #    Effective multiplier on a billable row in NYC = 1.28 × 1.42 = ~1.82.
    #    Non-billable row in NYC = 1.28.
    # 2) Seasonal anomalies (Nov + Dec 2025 firmwide expense overage).
    # Plausibility envelope semantic invariant: T&E hotels/airlines/meals are
    # STAFF expense, NOT client-billable. Only categories that legitimately
    # pass through to clients (entertainment, project supplies, occasional
    # travel reimbursables for client-on-site work) get is_billable=True.
    # This fixes #113 (Concur dominating billable expense) at the bronze layer.
    # The "billable_expense" line in gold_regional_pnl gets its bulk from
    # labor cost in silver_fact_timecards, not from Concur T&E.
    _BILLABLE_EXPENSE_TYPES = {
        "Client Entertainment",  # client-direct entertainment is sometimes passed through
        "Professional Development",  # client-required certifications occasionally billable
    }
    if exp_type in _BILLABLE_EXPENSE_TYPES:
        is_billable = random.random() < 0.30  # ~30% of these are passed through
    elif exp_type in ("Hotel", "Airline", "Taxi", "Meals", "Parking", "Mileage", "Internet/Phone"):
        # Pure staff T&E — billable only when explicitly part of a project
        # pass-through engagement (rare). 5% rate captures that edge case.
        is_billable = random.random() < 0.05
    else:
        is_billable = random.random() < 0.15  # default conservative billable rate
    office_expense_mult = get_office_expense_multiplier(eng["location"])
    if office_expense_mult != 1.0:
        amount *= office_expense_mult
    if is_billable:
        office_billable_mult = get_office_billable_expense_multiplier(eng["location"])
        if office_billable_mult != 1.0:
            amount *= office_billable_mult
        # Billable rows lifted further so project-level billable T&E reads at
        # Tier-1 consulting magnitudes ($K, not single dollars).
        amount *= 15.0
    seasonal_exp_mult = get_seasonal_multiplier(txn_date.year, txn_date.month, "expense_multiplier")
    if seasonal_exp_mult != 1.0:
        amount *= seasonal_exp_mult

    # 2026-05-21 — bronze T&E boost REVERTED. Earlier attempt to move the 150x
    # scale from gold rollup to bronze caused non-engineered project T&E to
    # balloon (200-555% T&E:contract on the Finance Overview "Top T&E Outliers"
    # table). The 150x scale lives in EXPENSE_SCALE at the gold rollup
    # (02_build_silver_gold.py:76), and gold_te_contract_audit's billable_expenses
    # column applies EXPENSE_SCALE consistently so app.py can query that table
    # directly without scale-awareness.

    posted = round(amount * random.uniform(0.98, 1.02), 2)
    approved = round(amount * random.uniform(0.95, 1.0), 2)

    # ~2% personal expenses
    is_personal = random.random() < 0.02

    # Receipt compliance: 95% for required (>$75), not 100%
    receipt_required = amount > 75
    has_receipt = (receipt_required and random.random() < 0.95) or (not receipt_required and random.random() < 0.85)

    # Type-matched vendor
    vendor = random.choice(VENDORS_BY_TYPE.get(exp_type, ["Unknown Vendor"]))

    ei_rows.append((
        uid(), rid, exp_type,
        EXPENSE_CATEGORIES_MAP.get(exp_type, "Other"), txn_date,
        round(amount, 2), posted, approved,
        random.choice(["USD", "EUR", "GBP", "INR"]),
        "USD", round(random.uniform(0.8, 1.3), 4),
        vendor, eng["location"], eng["location"],
        random.choice(["United States", "United Kingdom", "Germany", "India", "Japan"]),
        random.choice(PAYMENT_TYPE_LIST), is_personal, is_billable,
        engagement_id, eng["customer"],
        receipt_required, has_receipt,
        None, uid(),
        rand_ts(), rand_ts(),
        eng["region"], eng["location"], eng["practice"],
        eng["industry"], eng["customer"],
    ))

ei_schema = StructType([
    StructField("expense_id", StringType()), StructField("report_id", StringType()),
    StructField("expense_type", StringType()), StructField("expense_category", StringType()),
    StructField("transaction_date", DateType()), StructField("transaction_amount", DoubleType()),
    StructField("posted_amount", DoubleType()), StructField("approved_amount", DoubleType()),
    StructField("transaction_currency", StringType()), StructField("reimbursement_currency", StringType()),
    StructField("exchange_rate", DoubleType()), StructField("vendor_name", StringType()),
    StructField("location_name", StringType()), StructField("city", StringType()),
    StructField("country", StringType()), StructField("payment_type", StringType()),
    StructField("is_personal", BooleanType()), StructField("is_billable", BooleanType()),
    StructField("project_id", StringType()), StructField("client_name", StringType()),
    StructField("receipt_required", BooleanType()), StructField("has_receipt", BooleanType()),
    StructField("comments", StringType()), StructField("allocation_id", StringType()),
    StructField("created_date", TimestampType()), StructField("last_modified_date", TimestampType()),
    StructField("region", StringType()), StructField("location", StringType()),
    StructField("practice_area", StringType()), StructField("industry", StringType()),
    StructField("customer", StringType()),
])

write_table(spark.createDataFrame(ei_rows, schema=ei_schema), "bronze_concur_expense_items")

# COMMAND ----------

# DBTITLE 1,Generate & Write Travel Bookings (80,000)
print("Generating Concur travel bookings...")

BOOKING_TYPES = ["Flight", "Flight", "Hotel", "Hotel", "Car Rental", "Train"]
BOOKING_SOURCES = ["Online - Concur", "Online - Concur", "Travel Agent", "Direct"]
BOOKING_STATUSES_LIST = ["Confirmed", "Confirmed", "Confirmed", "Cancelled", "Pending"]
TRAVEL_VENDORS = [
    "United Airlines", "Delta Airlines", "American Airlines", "British Airways", "Lufthansa",
    "Marriott", "Hilton", "Hyatt", "IHG", "Accor",
    "Hertz", "Enterprise", "Avis", "National",
    "Amtrak", "Eurostar", "Deutsche Bahn",
]

ALL_LOCATIONS = [loc for locs in LOCATIONS_BY_REGION.values() for loc in locs]

tb_rows = []
for i in range(80000):
    eid = random.choice(emp_id_list)
    info = emp_info[eid]
    # Travel booked against an engagement → derive office attribution from engagement
    engagement_id = random.choice(engagement_ids)
    eng = engagement_lookup[engagement_id]
    btype = random.choice(BOOKING_TYPES)
    book_date = rand_date()
    start = book_date + timedelta(days=random.randint(1, 60))
    end = start + timedelta(days=random.randint(1, 7))

    if btype == "Flight":
        cost = round(random.uniform(200, 3500), 2)
    elif btype == "Hotel":
        nights = (end - start).days
        cost = round(random.uniform(120, 400) * max(nights, 1), 2)
    elif btype == "Car Rental":
        cost = round(random.uniform(40, 120) * max((end - start).days, 1), 2)
    else:
        cost = round(random.uniform(50, 500), 2)

    status = random.choice(BOOKING_STATUSES_LIST)
    cancel_date = end + timedelta(days=random.randint(0, 3)) if status == "Cancelled" else None

    tb_rows.append((
        uid(), f"TRIP-{i+1:07d}", eid, btype,
        book_date, random.choice(BOOKING_SOURCES),
        random.choice(TRAVEL_VENDORS), f"CONF-{random.randint(100000, 999999)}",
        start, end, random.choice(ALL_LOCATIONS), random.choice(ALL_LOCATIONS),
        cost, random.choice(["USD", "EUR", "GBP"]),
        engagement_id, eng["customer"],
        status, cancel_date,
        rand_ts(), rand_ts(),
        eng["region"], eng["location"], eng["practice"],
        eng["industry"], eng["customer"],
    ))

tb_schema = StructType([
    StructField("booking_id", StringType()), StructField("trip_id", StringType()),
    StructField("employee_id", StringType()), StructField("booking_type", StringType()),
    StructField("booking_date", DateType()), StructField("booking_source", StringType()),
    StructField("vendor_name", StringType()), StructField("confirmation_number", StringType()),
    StructField("start_date", DateType()), StructField("end_date", DateType()),
    StructField("origin", StringType()), StructField("destination", StringType()),
    StructField("total_cost", DoubleType()), StructField("currency_code", StringType()),
    StructField("project_id", StringType()), StructField("client_name", StringType()),
    StructField("booking_status", StringType()), StructField("cancellation_date", DateType()),
    StructField("created_date", TimestampType()), StructField("last_modified_date", TimestampType()),
    StructField("region", StringType()), StructField("location", StringType()),
    StructField("practice_area", StringType()), StructField("industry", StringType()),
    StructField("customer", StringType()),
])

write_table(spark.createDataFrame(tb_rows, schema=tb_schema), "bronze_concur_travel_bookings")

# COMMAND ----------

# DBTITLE 1,Generate & Write Approvals (90,000)
print("Generating Concur approvals...")

APPROVAL_ACTIONS = ["Approved", "Approved", "Approved", "Approved", "Returned", "Pending"]
APPROVAL_COMMENTS = [
    "Looks good", "Approved per policy", None, "Please review receipts",
    "Within budget", None, "Approved", "Standard approval",
]

appr_rows = []
# Build a lookup from report_id to submit_date for realistic approval timing
er_submit_dates = {er_rows[i][0]: er_rows[i][6] for i in range(len(er_rows))}  # report_id -> submit_date
for i in range(90000):
    m = make_mandatory()
    report_id = random.choice(er_ids)
    submit_dt = er_submit_dates.get(report_id, rand_date())
    action = random.choice(APPROVAL_ACTIONS)
    # Approval date is 3-10 business days AFTER submit date
    approval_dt = submit_dt + timedelta(days=random.randint(3, 10)) if action != "Pending" else None

    appr_rows.append((
        uid(), report_id,
        random.choice(all_employee_ids[:500]),
        f"Approver {random.randint(1, 500)}",
        random.choice([1, 1, 1, 2, 3]),
        action, approval_dt,
        random.choice(APPROVAL_COMMENTS),
        rand_ts(),
        m["region"], m["location"],
    ))

# Schema cuts (per bronze_realism_audit.md §3.2): practice_area + industry +
# customer removed. They were re-rolled per row, producing internally
# inconsistent values across approvals on the same expense report.
appr_schema = StructType([
    StructField("approval_id", StringType()), StructField("report_id", StringType()),
    StructField("approver_id", StringType()), StructField("approver_name", StringType()),
    StructField("approval_level", IntegerType()), StructField("approval_action", StringType()),
    StructField("approval_date", DateType()), StructField("comments", StringType()),
    StructField("created_date", TimestampType()),
    StructField("region", StringType()), StructField("location", StringType()),
])

write_table(spark.createDataFrame(appr_rows, schema=appr_schema), "bronze_concur_approvals")

# COMMAND ----------

# DBTITLE 1,Generate & Write SAP Accounts Payable (45,000)
print("Generating SAP accounts payable...")

AP_PAYMENT_METHODS = ["Wire Transfer", "ACH", "Check", "EFT"]
AP_PAYMENT_STATUSES = ["Paid", "Paid", "Paid", "Open", "Partially Paid", "Overdue"]
GL_ACCOUNTS = [f"{random.randint(4000,8999)}" for _ in range(50)]
PROFIT_CENTERS_LIST = [f"PC-{i:04d}" for i in range(1, 61)]
DEPARTMENTS = ["IT", "HR", "Finance", "Operations", "Marketing", "Sales", "Legal", "R&D", "Executive", "Facilities"]

# _AP_VENDOR_RECORDS / _AP_VENDOR_WEIGHTS / _pick_ap_vendor are now defined
# near the top of this file (right after VENDOR_CATEGORIES) because the
# event simulator runs before this section and needs them. Nothing to do here.

ap_rows = []
_ref_date = DATE_END  # Use dynamic end-date constant

# ── Event-driven AP projection ───────────────────────────────────────────
# Previously this section generated 45K random AP invoices, each picking a
# vendor weighted by category share. Per-vendor lifetime totals were
# unbounded, which is how DigitalOcean ended up with $566.71M in open AP —
# above DigitalOcean's actual annual revenue. The simulator above generated
# one AP invoice per project-month per expense category, with per-vendor
# lifetime spend capped via PER_VENDOR_LIFETIME_CAP_USD. We project those
# events directly. Plus a smaller layer of FIRM-OVERHEAD vendor invoices
# (rent, benefits, cloud subscriptions) not tied to specific engagements.
print(f"  Projecting {len(SIM_AP_INVOICES):,} simulator events into bronze_sap_accounts_payable...")
for sim_inv in SIM_AP_INVOICES:
    eng = engagement_lookup.get(sim_inv.get("engagement_id"))
    eng_meta = eng or {
        "region": sim_inv["region"], "location": sim_inv["location"],
        "practice": sim_inv["practice_area"], "industry": sim_inv["industry"],
        "customer": sim_inv["customer"],
    }
    ap_rows.append((
        sim_inv["invoice_id"],
        sim_inv["vendor_id"],
        sim_inv["vendor_name"],
        sim_inv["invoice_number"],
        sim_inv["invoice_date"],
        sim_inv["due_date"],
        sim_inv["posting_date"],
        sim_inv["amount"],
        sim_inv["currency"],
        sim_inv["payment_terms"],
        random.choice(AP_PAYMENT_METHODS),
        sim_inv["payment_status"],
        sim_inv["payment_date"],
        random.choice(GL_ACCOUNTS),
        random.choice(COST_CENTERS_LIST),
        random.choice(PROFIT_CENTERS_LIST),
        f"PO-{random.randint(100000, 999999)}" if random.random() < 0.7 else None,
        random.choice(DEPARTMENTS),
        random.choice(all_employee_ids[:200]),
        f"Approver {random.randint(1, 200)}",
        rand_ts(), rand_ts(),
        eng_meta["region"],
        eng_meta["location"],
        eng_meta["practice"] if "practice" in eng_meta else eng_meta.get("practice_area"),
        eng_meta["industry"],
        eng_meta["customer"],
    ))

ap_schema = StructType([
    StructField("invoice_id", StringType()), StructField("vendor_id", StringType()),
    StructField("vendor_name", StringType()), StructField("invoice_number", StringType()),
    StructField("invoice_date", DateType()), StructField("due_date", DateType()),
    StructField("posting_date", DateType()), StructField("amount", DoubleType()),
    StructField("currency", StringType()), StructField("payment_terms", StringType()),
    StructField("payment_method", StringType()), StructField("payment_status", StringType()),
    StructField("payment_date", DateType()), StructField("gl_account", StringType()),
    StructField("cost_center", StringType()), StructField("profit_center", StringType()),
    StructField("purchase_order_id", StringType()), StructField("department", StringType()),
    StructField("approver_id", StringType()), StructField("approver_name", StringType()),
    StructField("created_date", TimestampType()), StructField("last_modified_date", TimestampType()),
    StructField("region", StringType()), StructField("location", StringType()),
    StructField("practice_area", StringType()), StructField("industry", StringType()),
    StructField("customer", StringType()),
])

write_table(spark.createDataFrame(ap_rows, schema=ap_schema), "bronze_sap_accounts_payable")

# COMMAND ----------

# DBTITLE 1,Generate & Write SAP Accounts Receivable (40,000)
print("Generating SAP accounts receivable...")

AR_PAYMENT_STATUSES = ["Paid", "Paid", "Paid", "Open", "Partially Paid", "Overdue"]

# Customer name → list of account_ids (one customer may have multiple regional accounts)
customer_to_account_ids = {}
for aid, acct in account_lookup.items():
    customer_to_account_ids.setdefault(acct["customer"], []).append(aid)

ar_rows = []
# ── Event-driven AR projection ───────────────────────────────────────────
# Previously this section generated 40K random invoices + an explicit "AR
# concentration narrative" loop that injected $50M+ aged invoices for 5
# named clients. That random-fill pattern was the source of:
#   - BP monthly balance oscillation ($101M → $0.7M → $134M) — payment dates
#     redrawn on every refresh
#   - $56-59M single invoices — bridge tier × 5-6 named clients stacking
#   - "Aged AR (60+) > Total Open AR" Genie hallucination — sub-query
#     filtering by month inconsistent with day-count filtering
#
# The event simulator above already produced one invoice per engagement per
# fiscal month, each with a payment_date drawn ONCE at creation time. We
# project those simulator events directly into the bronze tuple format
# below. By construction:
#   - Total AR = sum of unpaid invoices in SIM_AR_INVOICES
#   - Aged AR ≤ Total AR (subset relation)
#   - Per-client balances are stable month-to-month (same invoices age in
#     place, payment dates don't redraw)
#   - No single invoice exceeds the simulator's SINGLE_INVOICE_CAP_USD
#     (default $15M)
#   - customer ↔ engagement ↔ project_name all consistent (sim only ever
#     emits an invoice tied to the engagement's own customer)
print(f"  Projecting {len(SIM_AR_INVOICES):,} simulator events into bronze_sap_accounts_receivable...")
for sim_inv in SIM_AR_INVOICES:
    ar_rows.append((
        sim_inv["invoice_id"],
        sim_inv["customer_id"],
        sim_inv["customer_name"],
        sim_inv["invoice_number"],
        sim_inv["invoice_date"],
        sim_inv["due_date"],
        sim_inv["posting_date"],
        sim_inv["amount"],
        sim_inv["currency"],
        sim_inv["payment_terms"],
        sim_inv["payment_status"],
        sim_inv["payment_date"],
        sim_inv["days_outstanding"],
        random.choice(GL_ACCOUNTS),
        sim_inv["project_id"],
        random.choice(COST_CENTERS_LIST),
        random.choice(PROFIT_CENTERS_LIST),
        rand_ts(), rand_ts(),
        sim_inv["region"],
        sim_inv["location"],
        sim_inv["practice_area"],
        sim_inv["industry"],
        sim_inv["customer"],
    ))

# Legacy random-fill loop replaced by simulator projection above. Keep the
# branch below for reference (the old engineered AR-concentration narrative
# is now achieved structurally: real long-running engagements naturally
# carry larger aged-AR balances because they billed more months).
ar_schema = StructType([
    StructField("invoice_id", StringType()), StructField("customer_id", StringType()),
    StructField("customer_name", StringType()), StructField("invoice_number", StringType()),
    StructField("invoice_date", DateType()), StructField("due_date", DateType()),
    StructField("posting_date", DateType()), StructField("amount", DoubleType()),
    StructField("currency", StringType()), StructField("payment_terms", StringType()),
    StructField("payment_status", StringType()), StructField("payment_date", DateType()),
    StructField("days_outstanding", IntegerType()), StructField("gl_account", StringType()),
    StructField("project_id", StringType()), StructField("cost_center", StringType()),
    StructField("profit_center", StringType()),
    StructField("created_date", TimestampType()), StructField("last_modified_date", TimestampType()),
    StructField("region", StringType()), StructField("location", StringType()),
    StructField("practice_area", StringType()), StructField("industry", StringType()),
    StructField("customer", StringType()),
])

write_table(spark.createDataFrame(ar_rows, schema=ar_schema), "bronze_sap_accounts_receivable")

# COMMAND ----------

# DBTITLE 1,Generate & Write SAP General Ledger (120,000)
print("Generating SAP general ledger...")

GL_ACCOUNT_NAMES = {
    "Revenue": ["Consulting Revenue", "Technology Revenue", "Managed Services Revenue", "License Revenue"],
    "Expense": ["Personnel Expense", "Travel & Entertainment", "Technology & Hosting", "Subcontractor Costs", "Facility Costs", "Marketing Expense", "Professional Fees", "Depreciation"],
    "Asset": ["Cash & Equivalents", "Accounts Receivable", "Prepaid Expenses", "Fixed Assets", "Right-of-Use Assets"],
    "Liability": ["Accounts Payable", "Accrued Liabilities", "Deferred Revenue", "Lease Liabilities", "Current Tax Payable"],
    "Equity": ["Retained Earnings", "Common Stock", "Additional Paid-In Capital", "Treasury Stock"],
}

# GL account number ranges by type
_GL_ACCT_RANGES = {"Revenue": (4000, 4999), "Expense": (5000, 7999), "Asset": (1000, 1999), "Liability": (2000, 2999), "Equity": (3000, 3999)}

gl_rows = []
_gl_total_debits = 0.0
_gl_total_credits = 0.0

# Generate double-entry pairs: each transaction creates a debit AND a credit entry
# 120,000 entries = 60,000 transactions
for i in range(60000):
    # FK to a real engagement → GL postings attribute to the engagement's office.
    # This makes silver_fact_general_ledger rollups by (region, location, practice)
    # correctly match the engagement-level revenue/expense aggregates.
    engagement_id = random.choice(engagement_ids)
    eng = engagement_lookup[engagement_id]
    posting_date = rand_date()
    doc_num = f"DOC-{random.randint(1000000, 9999999)}"
    ref = f"REF-{random.randint(100000, 999999)}"
    currency = random.choice(["USD", "EUR", "GBP", "INR"])
    cc = random.choice(COST_CENTERS_LIST)
    pc = random.choice(PROFIT_CENTERS_LIST)

    # Pick a transaction type with realistic double-entry pairs
    txn = weighted_choice(
        ["revenue_ar", "expense_cash", "expense_ap", "asset_purchase", "liability_payment", "equity_close", "equity_dividend", "equity_capital"],
        [0.28, 0.23, 0.14, 0.11, 0.09, 0.08, 0.04, 0.03]
    )

    if txn == "revenue_ar":
        amount = round(random.uniform(10_000, 500_000), 2)
        debit_type, debit_name = "Asset", "Accounts Receivable"
        credit_type, credit_name = "Revenue", random.choice(GL_ACCOUNT_NAMES["Revenue"])
    elif txn == "expense_cash":
        amount = round(random.uniform(1_000, 100_000), 2)
        debit_type, debit_name = "Expense", random.choice(GL_ACCOUNT_NAMES["Expense"])
        credit_type, credit_name = "Asset", "Cash & Equivalents"
    elif txn == "expense_ap":
        amount = round(random.uniform(1_000, 150_000), 2)
        debit_type, debit_name = "Expense", random.choice(GL_ACCOUNT_NAMES["Expense"])
        credit_type, credit_name = "Liability", "Accounts Payable"
    elif txn == "asset_purchase":
        amount = round(random.uniform(5_000, 500_000), 2)
        debit_type, debit_name = "Asset", random.choice(["Fixed Assets", "Right-of-Use Assets", "Prepaid Expenses"])
        credit_type, credit_name = "Asset", "Cash & Equivalents"
    elif txn == "liability_payment":
        amount = round(random.uniform(5_000, 200_000), 2)
        debit_type, debit_name = "Liability", random.choice(GL_ACCOUNT_NAMES["Liability"])
        credit_type, credit_name = "Asset", "Cash & Equivalents"
    elif txn == "equity_close":
        amount = round(random.uniform(50_000, 500_000), 2)
        debit_type, debit_name = "Revenue", random.choice(GL_ACCOUNT_NAMES["Revenue"])
        credit_type, credit_name = "Equity", "Retained Earnings"
    elif txn == "equity_capital":
        amount = round(random.uniform(100_000, 1_000_000), 2)
        debit_type, debit_name = "Asset", "Cash & Equivalents"
        credit_type, credit_name = "Equity", random.choice(["Common Stock", "Additional Paid-In Capital"])
    else:
        amount = round(random.uniform(10_000, 300_000), 2)
        debit_type, debit_name = "Equity", "Retained Earnings"
        credit_type, credit_name = "Asset", "Cash & Equivalents"

    acct_lo, acct_hi = _GL_ACCT_RANGES[debit_type]
    debit_acct = str(random.randint(acct_lo, acct_hi))
    acct_lo, acct_hi = _GL_ACCT_RANGES[credit_type]
    credit_acct = str(random.randint(acct_lo, acct_hi))

    # Debit entry
    gl_rows.append((
        uid(), posting_date, doc_num,
        debit_acct, debit_name, debit_type,
        amount, 0.0, amount,
        currency, cc, pc, ref,
        f"{debit_name} - {posting_date.strftime('%b %Y')}",
        posting_date.year, posting_date.month, rand_ts(),
        eng["region"], eng["location"], eng["practice"],
        eng["industry"], eng["customer"],
    ))
    # Credit entry
    gl_rows.append((
        uid(), posting_date, doc_num,
        credit_acct, credit_name, credit_type,
        0.0, amount, amount,
        currency, cc, pc, ref,
        f"{credit_name} - {posting_date.strftime('%b %Y')}",
        posting_date.year, posting_date.month, rand_ts(),
        eng["region"], eng["location"], eng["practice"],
        eng["industry"], eng["customer"],
    ))
    _gl_total_debits += amount
    _gl_total_credits += amount

print(f"  GL entries: {len(gl_rows):,} | Debits: ${_gl_total_debits:,.2f} | Credits: ${_gl_total_credits:,.2f} | Balanced: {abs(_gl_total_debits - _gl_total_credits) < 0.01}")

gl_schema = StructType([
    StructField("entry_id", StringType()), StructField("posting_date", DateType()),
    StructField("document_number", StringType()), StructField("gl_account", StringType()),
    StructField("gl_account_name", StringType()), StructField("account_type", StringType()),
    StructField("debit_amount", DoubleType()), StructField("credit_amount", DoubleType()),
    StructField("amount", DoubleType()), StructField("currency", StringType()),
    StructField("cost_center", StringType()), StructField("profit_center", StringType()),
    StructField("reference", StringType()), StructField("description", StringType()),
    StructField("fiscal_year", IntegerType()), StructField("fiscal_period", IntegerType()),
    StructField("created_date", TimestampType()),
    StructField("region", StringType()), StructField("location", StringType()),
    StructField("practice_area", StringType()), StructField("industry", StringType()),
    StructField("customer", StringType()),
])

write_table(spark.createDataFrame(gl_rows, schema=gl_schema), "bronze_sap_general_ledger")

# COMMAND ----------

# DBTITLE 1,Generate & Write SAP Cost Centers (150)
print("Generating SAP cost centers...")

cc_rows = []
for i in range(150):
    m = make_mandatory()
    dept = random.choice(DEPARTMENTS)

    cc_rows.append((
        uid(), f"CC-{i+1:04d}",
        f"{dept} - {m['practice_area']} - {m['region']}",
        dept,
        f"Manager {random.randint(1, 200)}",
        date(2020, 1, 1), date(2099, 12, 31),
        "1000", rand_ts(),
        m["region"], m["location"],
    ))

# Schema cuts (per bronze_realism_audit.md §3.2): practice_area + industry +
# customer removed. Cost centers are master records — these tags were random,
# not derived from real entity relationships.
cc_schema = StructType([
    StructField("cost_center_id", StringType()), StructField("cost_center_code", StringType()),
    StructField("cost_center_name", StringType()), StructField("department", StringType()),
    StructField("responsible_person", StringType()), StructField("valid_from", DateType()),
    StructField("valid_to", DateType()), StructField("company_code", StringType()),
    StructField("created_date", TimestampType()),
    StructField("region", StringType()), StructField("location", StringType()),
])

write_table(spark.createDataFrame(cc_rows, schema=cc_schema), "bronze_sap_cost_centers")

# COMMAND ----------

# DBTITLE 1,Generate & Write SAP Profit Centers (60)
print("Generating SAP profit centers...")

pc_rows = []
for i in range(60):
    m = make_mandatory()

    pc_rows.append((
        uid(), f"PC-{i+1:04d}",
        f"{m['practice_area']} - {m['industry']} - {m['region']}",
        f"Leader {random.randint(1, 100)}",
        date(2020, 1, 1), date(2099, 12, 31),
        "1000", rand_ts(),
        m["region"], m["location"],
    ))

# Schema cuts (per bronze_realism_audit.md §3.2): practice_area + industry +
# customer removed. Profit centers are master records — these tags were random.
pc_schema = StructType([
    StructField("profit_center_id", StringType()), StructField("profit_center_code", StringType()),
    StructField("profit_center_name", StringType()), StructField("responsible_person", StringType()),
    StructField("valid_from", DateType()), StructField("valid_to", DateType()),
    StructField("company_code", StringType()), StructField("created_date", TimestampType()),
    StructField("region", StringType()), StructField("location", StringType()),
])

write_table(spark.createDataFrame(pc_rows, schema=pc_schema), "bronze_sap_profit_centers")

# COMMAND ----------

# DBTITLE 1,Generate & Write SAP Purchase Orders (25,000)
print("Generating SAP purchase orders...")

PO_STATUSES = ["Approved", "Approved", "Approved", "Received", "Open", "Cancelled"]

po_rows = []
# ── Event-driven PO projection ───────────────────────────────────────────
# Project SIM_VENDOR_POS (created alongside each direct-expense event in the
# simulator) directly into bronze. Each PO is tied to a real engagement
# spend event, so PO totals reconcile to AP invoice totals by construction.
print(f"  Projecting {len(SIM_VENDOR_POS):,} simulator events into bronze_sap_purchase_orders...")
for sim_po in SIM_VENDOR_POS:
    eng = engagement_lookup.get(sim_po.get("engagement_id"))
    eng_meta = eng or {
        "region": sim_po["region"], "location": sim_po["location"],
        "practice": sim_po["practice_area"], "industry": sim_po["industry"],
        "customer": sim_po["customer"],
    }
    po_rows.append((
        sim_po["po_id"],
        sim_po["po_number"],
        sim_po["vendor_id"],
        sim_po["vendor_name"],
        sim_po["po_date"],
        sim_po["delivery_date"],
        sim_po["amount"],
        sim_po["currency"],
        random.choice(PO_STATUSES),
        random.choice(COST_CENTERS_LIST), random.choice(PROFIT_CENTERS_LIST),
        random.choice(DEPARTMENTS),
        random.choice(all_employee_ids[:500]),
        random.choice(all_employee_ids[:200]),
        rand_ts(), rand_ts(),
        eng_meta["region"], eng_meta["location"],
        eng_meta["practice"] if "practice" in eng_meta else eng_meta.get("practice_area"),
        eng_meta["industry"], eng_meta["customer"],
    ))

po_schema = StructType([
    StructField("po_id", StringType()), StructField("po_number", StringType()),
    StructField("vendor_id", StringType()), StructField("vendor_name", StringType()),
    StructField("po_date", DateType()), StructField("delivery_date", DateType()),
    StructField("total_amount", DoubleType()), StructField("currency", StringType()),
    StructField("status", StringType()), StructField("cost_center", StringType()),
    StructField("profit_center", StringType()), StructField("department", StringType()),
    StructField("requestor_id", StringType()), StructField("approver_id", StringType()),
    StructField("created_date", TimestampType()), StructField("last_modified_date", TimestampType()),
    StructField("region", StringType()), StructField("location", StringType()),
    StructField("practice_area", StringType()), StructField("industry", StringType()),
    StructField("customer", StringType()),
])

write_table(spark.createDataFrame(po_rows, schema=po_schema), "bronze_sap_purchase_orders")

# COMMAND ----------

# DBTITLE 1,Generate & Write Cost Center Mapping (150)
print("Generating cost center mapping...")

ccm_rows = []
for i in range(150):
    dept_cat = DEPT_CATEGORIES[i % len(DEPT_CATEGORIES)]
    dept_name = f"{dept_cat} - Division {(i // 10) + 1}"

    ccm_rows.append((
        f"CC-{i+1:04d}",
        f"{dept_cat} Cost Center {i+1}",
        dept_name,
        dept_cat,
    ))

ccm_schema = StructType([
    StructField("cost_center_code", StringType()),
    StructField("cost_center_name", StringType()),
    StructField("department_name", StringType()),
    StructField("department_category", StringType()),
])

write_table(spark.createDataFrame(ccm_rows, schema=ccm_schema), "bronze_cost_center_mapping")

# COMMAND ----------

# DBTITLE 1,Data Load Summary
# MAGIC %md
# MAGIC ### Data Load Summary

# COMMAND ----------

# DBTITLE 1,Print Final Summary
print("=" * 70)
print("CFO ANALYTICS DEMO - DATA PREPARATION COMPLETE")
print("=" * 70)
print()

tables = [
    "bronze_sfdc_accounts",
    "bronze_sfdc_opportunities",
    "bronze_sfdc_contracts",
    "bronze_sfdc_engagements",
    "bronze_sfdc_forecasts",
    "bronze_workday_employees",
    "bronze_workday_positions",
    "bronze_workday_timecards",
    "bronze_workday_billing_rates",
    "bronze_workday_cost_rates",
    "bronze_workday_assignments",
    "bronze_workday_organizations",
    "bronze_concur_expense_reports",
    "bronze_concur_expense_items",
    "bronze_concur_travel_bookings",
    "bronze_concur_approvals",
    "bronze_sap_accounts_payable",
    "bronze_sap_accounts_receivable",
    "bronze_sap_general_ledger",
    "bronze_sap_cost_centers",
    "bronze_sap_profit_centers",
    "bronze_sap_purchase_orders",
    "bronze_cost_center_mapping",
]

total_rows = 0
for t in tables:
    try:
        count = spark.table(f"{CATALOG}.{SCHEMA}.{t}").count()
        total_rows += count
        print(f"  {t:45s} {count:>12,} rows")
    except Exception as e:
        print(f"  {t:45s} ERROR: {e}")

print()
print(f"  {'TOTAL':45s} {total_rows:>12,} rows")
print()
print(f"Schema: {CATALOG}.{SCHEMA}")
print(f"Tables: {len(tables)}")
print(f"Date range: 2023-03 through 2026-02")
print()

# Revenue sanity check
print("--- Revenue Sanity Check ---")
tc_df = spark.table(f"{CATALOG}.{SCHEMA}.bronze_workday_timecards")
revenue_check = (
    tc_df.filter(F.col("time_type") == "Billable")
    .withColumn("revenue", F.col("hours") * F.col("billing_rate"))
    .withColumn("month", F.date_format("work_date", "yyyy-MM"))
    .groupBy("month")
    .agg(
        F.sum("revenue").alias("monthly_revenue"),
        F.sum("hours").alias("billable_hours"),
        F.countDistinct("employee_id").alias("active_consultants"),
    )
    .orderBy("month")
)
revenue_check.show(50, truncate=False)

print("--- Engagement Budget Sanity Check ---")
eng_df = spark.table(f"{CATALOG}.{SCHEMA}.bronze_sfdc_engagements")
eng_df.select(
    F.min("budget_amount").alias("min_budget"),
    F.avg("budget_amount").alias("avg_budget"),
    F.max("budget_amount").alias("max_budget"),
    F.min("forecasted_revenue").alias("min_forecast"),
    F.avg("forecasted_revenue").alias("avg_forecast"),
    F.max("forecasted_revenue").alias("max_forecast"),
).show(truncate=False)

print("--- Partner FK Validation ---")
partner_check = (
    eng_df.join(
        spark.table(f"{CATALOG}.{SCHEMA}.bronze_workday_employees")
        .filter(F.col("job_level").isin("Partner", "Senior Partner"))
        .select("employee_id"),
        eng_df.lead_partner == F.col("employee_id"),
        "left"
    )
    .agg(
        F.count("*").alias("total_engagements"),
        F.count("employee_id").alias("valid_partner_fks"),
    )
)
partner_check.show(truncate=False)

print("Data preparation complete!")