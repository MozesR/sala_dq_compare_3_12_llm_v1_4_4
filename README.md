# SALA-Driven Data Quality  
### A Learning, Self-Adapting Architecture for Operational Checks

<p align="center">
  <em>Operational data quality that validates, repairs, learns, and adapts.</em>
</p>

## Abstract
This project presents a lightweight, production-oriented implementation of a **Symbiotic Agent Learning Architecture (SALA)** for automated data quality (DQ). Specialized agents handle rule execution, anomaly detection, drift monitoring, and an LLM-based policy synthesizer. A **Mediator** coordinates learning and governance, continuously evolving policies from run evidence. The result: a pipeline that validates and repairs data, identifies gaps in rules, proposes improvements, and (optionally) auto-accepts high-confidence updates. A small web UI exposes uploads, run history, rule governance, schema exploration, and a side-by-side comparison dashboard.

---

## 1) Concept: Symbiotic Agent Learning Architecture (SALA)
**SALA** is a multi-agent pattern where specialized agents maintain symbiotic relationships via shared signals, mediated feedback, and incremental policy change. Key ideas:  
- **Symbiosis:** each agent improves by consuming artifacts from others (e.g., drift signals, audit tallies) and producing outputs others leverage (e.g., new rules).  
- **Mediator-centric learning:** the Mediator aggregates telemetry, queries the LLM Policy Synthesizer, enforces guardrails (confidence, dedupe), and persists new knowledge.  
- **Compound intelligence:** capabilities exceed any single agent by fusing empirical evidence (what happened) with semantic reasoning (how to adapt).

---

## 2) Goal of the Solution
1. **Operational data quality:** scalable, explainable checks for completeness, consistency, and derived correctness.  
2. **Continuous improvement:** detect weak spots and learn better policies from run evidence.  
3. **Governable automation:** approve or auto-accept vetted rules with provenance and confidence.  
4. **Fast feedback:** immediate, visual evidence via dashboards and comparisons.

---

## 3) Architecture & Components

### 3.1 Data Flow (high level)
1) Upload CSV → 2) Coercion & Baseline Compute → 3) **Rule Executor** (assign/impute/compute)  
4) **Anomaly & Drift Monitors** → 5) **Mediator** aggregates signals → 6) **LLM Policy Synthesizer** proposes rules  
7) **Governance** (manual or auto-accept) → 8) Rules updated → 9) Artifacts persisted (run report, curated output)  
10) UI displays results, history, comparisons, and suggestions.

### 3.2 Agents
- **Rule Executor**  
  Applies domain rules: `assign`, `impute`, `compute` (e.g., `total_amount = quantity * unit_price`). Returns a transform audit + curated data.  
- **Anomaly Detector**  
  Robust outlier detection (e.g., IQR) on key measures.  
- **Drift Monitor**  
  Tracks distributional shifts across runs (e.g., PSI-like categorical deltas; mean deltas).  
- **LLM Policy Synthesizer**  
  Consumes audits & drift; proposes/refines rules with reasoning and confidence.  
- **Mediator (learning & governance)**  
  Aggregates signals, calls LLM (or deterministic heuristics), dedupes suggestions, optionally auto-accepts above a threshold, and **persists**:
  - `dq/rules.json` (active policies)  
  - `dq/rules_history/*` (versioned backups)  
  - `dq/run_reports/*` (immutable artifacts)  
  - `dq/baselines.json` (for drift)  
  - `meta/suggestions.json`, `meta/insights.jsonl` (LLM outputs & traces)

### 3.3 Resilience & Hygiene
- **Type-safe transformations** (coerced dtypes).  
- **JSON-safe API** (NaN/Inf → null).  
- **Rule dedupe** (prevents repeat insertions).  
- **.env toggles:**
  ```ini
  ENABLE_LLM=false
  AUTO_ACCEPT_LLM_RULES=false
  OPENAI_API_KEY=your_key_here
  BASELINE_HISTORY_WINDOW=50
  ```

---

## 4) Process & Functionalities

### 4.1 Pipeline Steps
1. **Ingest:** read CSV; normalize numerics; optionally compute derived fields.  
2. **Enforce (Rule Executor):** apply assign/impute/compute; emit transform audit.  
3. **Analyze:** outliers (counts, bounds); drift (mean deltas, category shifts).  
4. **Learn (Mediator + LLM):** generate suggestions with titles, reasons, rules, confidences; persist; auto-accept if enabled.  
5. **Persist & Expose:** save curated data (`data/curated.csv`), run reports (`dq/run_reports/run_*.json`), update baselines, serve APIs & dashboards.

### 4.2 UI Pages & Purpose
- **Rules & Suggestions** (`/`): rules editor, LLM suggestions table, insights panel.  
- **Dashboard** (`/dashboard`): CSV upload, last-run audit summary.  
- **Run History** (`/history`): list of run reports; links to details.  
- **Run Detail** (`/run/<report>`): audit table; raw JSON for transparency.  
- **Compare** (`/compare`): pick Run A/B; visualize Δ for audits, drift, outliers; details table.  
- **Schema** (`/schema`): dtype, null %, unique counts per column.

---

## 5) Why & How the Solution Learns and Adapts
**Why:** static DQ rules degrade as sources change; SALA makes change detectable, explainable, and actionable.  
**How:** feedback signals (audits, drift, outliers) reveal stress; the LLM converts stress into candidate rules (with reasons & confidence); governance updates `rules.json`; subsequent runs re-measure and re-learn; dedupe & thresholds prevent over-fitting; baselines anchor drift.

---

## 6) Example Use Case (End-to-End)

**Scenario:** an e-commerce team receives daily orders CSVs and wants reliable totals + guardrails against schema/behavior drift.  

**Step-by-Step**  
1) **Baseline run:** upload a mostly complete CSV; small audit; drift baseline created.  
2) **Trigger run:** upload a larger CSV (e.g., ~600 rows, many missing `status`, `quantity=2`, `unit_price=50`); Rule Executor assigns `status` widely; totals computed; mean `total_amount` jumps.  
3) **Learn:** Mediator → LLM reads audits & drifts; example suggestions:

```yaml
- title: Guard assignment scope
  rule: >
    if order_id IS NOT NULL and status IS NULL
    then assign status = 'unknown'
  reason: Avoid assigning status in non-order contexts.
  confidence: 0.90

- title: Mean shift alert
  rule: >
    if true then assign dq_alert = 'mean_total_amount_shift'
  reason: Mean drift vs baseline exceeded tolerance.
  confidence: 0.85
```

4) **Operate & Compare:** next day’s file shows fewer `assign(status)` events; Compare view shows reduced assigned counts, normalized drift, stable/improved outliers.  
5) **Governance & Traceability:** `dq/rules_history/*` captures policy lineage; `meta/suggestions.json` and `meta/insights.jsonl` log LLM rationale & frequency.

---

## 7) Design Choices & Trade-offs
- **LLM as Suggestion, not Authority:** proposals with explicit confidence and human/auto governance.  
- **Simple statistics first:** IQR + mean deltas for robust, fast signals.  
- **Deterministic core:** enforcement is rule-based & typed; learning only changes inputs (policies).  
- **Safety rails:** JSON sanitization, rule dedupe, confidence thresholds, explicit `.env` opt-ins.

---

## 8) Extensibility
- Additional agents: schema drift detector, join-consistency checker, semantic validators (e.g., locale-aware email/phone).  
- Richer charts: distribution overlays, null trend lines, rule-impact sparklines.  
- Policy testing: dry-run suggestions on historical runs before acceptance.  
- Cooldowns: time-based suppression for repeat suggestions under similar evidence.

---

## 9) Conclusion
By adopting SALA, DQ evolves from a static rulebook to a learning system that **detects**, **explains**, **proposes**, and **adapts** with minimal friction—delivering transparent governance, faster stabilization, and measurable improvements from notebooks to services.

---

### Quick Start (Optional scaffolding)
```bash
# 1) Configure environment
cp .env.example .env  # set ENABLE_LLM / AUTO_ACCEPT_LLM_RULES / OPENAI_API_KEY / BASELINE_HISTORY_WINDOW


```

# SALA DQ Compare (v1.4.4, Python 3.12)

**What's new in v1.4.4**  
- LLM client now **persists suggestions** to `meta/suggestions.json` and logs to `meta/insights.jsonl`.
- Robust deduping (no duplicate rules) at _suggest_, _auto-accept_, and _apply_ steps.
- Server-side JSON sanitizer for `/api/report` and `/api/compare` (no NaN/Inf).
- Compare UI (`compare.js`) hardened against nulls; charts render reliably.
- Rules UI enhanced with **"Show last suggestion details"** toggle.

LLM is **silent by default** (`ENABLE_LLM=false`). Enable later in `.env`.

## Quick start
```bash
python -m venv .venv
source .venv/bin/activate    # Windows: .\.venv\Scripts\activate
pip install -r requirements.txt
python ui/server.py
```
Open: `/` • `/dashboard` • `/history` • `/compare` • `/schema`

## Optional LLM
```
ENABLE_LLM=true
AUTO_ACCEPT_LLM_RULES=true   # optional (auto-apply confident rules)
OPENAI_API_KEY=yourkeyhere
BASELINE_HISTORY_WINDOW=65  # optional
```
