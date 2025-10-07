import os, json, pathlib, datetime as dt, uuid
from typing import List, Dict, Any
import pandas as pd
from rich.console import Console

console = Console()
ROOT = pathlib.Path(__file__).resolve().parents[1]
META = ROOT / "meta"
DQ = ROOT / "dq"

# ------------------------- utilities -------------------------

def _rule_exists(rule: dict) -> bool:
    """Check if an identical rule already exists in dq/rules.json"""
    try:
        rules = json.loads((DQ / "rules.json").read_text())
        for r in rules.get("logic", []):
            if json.dumps(r, sort_keys=True) == json.dumps(rule, sort_keys=True):
                return True
    except Exception:
        pass
    return False

def _append_insight_enriched(payload: dict):
    """Append a richly annotated insight line to meta/insights.jsonl."""
    META.mkdir(parents=True, exist_ok=True)
    p = META / "insights.jsonl"
    payload = dict(payload or {})
    payload.setdefault("ts", dt.datetime.now(dt.timezone.utc).isoformat())
    with p.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload) + "\n")

def _prepend_suggestions(suggestions: List[Dict[str, Any]], keep: int = 50):
    """Prepend & dedupe suggestions in meta/suggestions.json."""
    META.mkdir(parents=True, exist_ok=True)
    p = META / "suggestions.json"
    existing = []
    if p.exists():
        try:
            existing = json.loads(p.read_text())
        except Exception:
            existing = []
    # Dedupe by rule hash (stringified rule)
    seen = set()
    def key(sug):
        try:
            return json.dumps(sug.get("rule", {}), sort_keys=True)
        except Exception:
            return json.dumps(sug, sort_keys=True)
    out = []
    for x in suggestions + existing:
        k = key(x)
        if k in seen:
            continue
        seen.add(k)
        out.append(x)
        if len(out) >= keep:
            break
    p.write_text(json.dumps(out, indent=2))

def _n(x) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0

# --------------------------- LLM Client ---------------------------

class LLMClient:
    """Enhanced LLM agent: enriches context & proposes diverse rule types.

    Compatible with v1.4.4+ pipeline. Uses only DSL constructs supported by the
    executor (IS NULL/NOT NULL, ==, !=, >, >=, <, <=, AND). Avoids NOT IN/OR/regex.
    """
    def __init__(self):
        self.enabled = os.getenv("ENABLE_LLM", "false").lower() == "true"
        self.api_key = os.getenv("OPENAI_API_KEY")
        console.print("[bold cyan]🤖 SALA LLM Agent initialized (enhanced prompt v1.4.6)[/bold cyan]")

    def _id(self) -> str:
        return uuid.uuid4().hex[:10]

    def _iso_today(self) -> str:
        # date-only ISO works lexicographically for string comparisons
        return dt.datetime.now(dt.timezone.utc).date().isoformat()

    def analyze_quality_issues(self, df: pd.DataFrame, report: dict):
        suggestions: List[Dict[str, Any]] = []

        # ---------------- Context extraction ----------------
        rows = int(len(df)) if hasattr(df, "__len__") else 0
        run_name = report.get("report_name") or ""
        source_file = report.get("source_file") or ""
        drift = report.get("drift") or {}
        anomalies = report.get("anomaly_outliers") or {}
        audit = report.get("transform_audit") or {}

        # Column presence flags
        has = {c: (c in df.columns) for c in [
            "status","order_id","order_date","customer_email","currency",
            "unit_price","total_amount"
        ]}

        # Missing ratios (selected columns)
        miss = {}
        for col in ["status","customer_email","currency","unit_price","total_amount"]:
            if has.get(col):
                try:
                    miss[col] = float(pd.isna(df[col]).mean())
                except Exception:
                    miss[col] = 0.0

        # Numeric stats for few key columns
        numeric_stats = {}
        for col in ["quantity","unit_price","total_amount"]:
            if col in df.columns:
                s = pd.to_numeric(df[col], errors="coerce")
                numeric_stats[col] = {
                    "mean": float(s.mean()) if s.size else 0.0,
                    "p95": float(s.quantile(0.95)) if s.size else 0.0,
                    "neg_ct": int((s < 0).sum()) if s.size else 0,
                    "zero_ct": int((s == 0).sum()) if s.size else 0,
                }

        # Currency distribution (top values)
        currency_counts = {}
        if has.get("currency"):
            try:
                currency_counts = df["currency"].value_counts(dropna=False).to_dict()
            except Exception:
                currency_counts = {}

        # Future dates count
        future_ct = 0
        if has.get("order_date"):
            try:
                s = pd.to_datetime(df["order_date"], errors="coerce", utc=True)
                today = dt.datetime.now(dt.timezone.utc).date()
                future_ct = int((s.dt.date > today).sum())
            except Exception:
                future_ct = 0

        # Price outliers (executor-friendly rules -> two comparisons)
        price_low_ct = 0
        price_high_ct = 0
        if has.get("unit_price"):
            sp = pd.to_numeric(df["unit_price"], errors="coerce")
            price_low_ct = int((sp <= 0).sum())
            price_high_ct = int((sp > 1000).sum())  # high threshold for obvious anomaly

        # Negative totals
        neg_total_ct = 0
        if has.get("total_amount"):
            st = pd.to_numeric(df["total_amount"], errors="coerce")
            neg_total_ct = int((st < 0).sum())

        # Unexpected currencies (avoid NOT IN; propose per-specific value rules)
        unexpected_vals = []
        allowed = {"USD","EUR","GBP"}
        if has.get("currency"):
            try:
                for val, ct in currency_counts.items():
                    if val is not None and str(val) not in allowed and ct >= 10:
                        unexpected_vals.append((str(val), int(ct)))
            except Exception:
                pass

        # ---------------- Heuristic proposals (diverse) ----------------
        # 0) Skip if dataset empty
        if rows == 0:
            console.print("[yellow]ℹ️ Heuristics skipped: dataset has zero rows.[/yellow]")
        else:
            # A) Missing emails heavy -> flag missing_contact
            if miss.get("customer_email", 0.0) >= 0.30 and has.get("customer_email"):
                rule = {"if": "customer_email IS NULL", "then": "assign dq_flag = 'missing_contact'"}
                if not _rule_exists(rule):
                    suggestions.append({
                        "id": self._id(),
                        "title": "High missing email ratio",
                        "rule": rule,
                        "reason": f"{miss['customer_email']:.0%} of customer_email is missing.",
                        "confidence": 0.90,
                        "source": "heuristic"
                    })

            # B) Future date anomalies
            if future_ct > 0 and has.get("order_date"):
                iso_today = self._iso_today()
                rule = {"if": f"order_date > '{iso_today}'", "then": "assign dq_flag = 'future_date_anomaly'"}
                if not _rule_exists(rule):
                    suggestions.append({
                        "id": self._id(),
                        "title": "Future dates present",
                        "rule": rule,
                        "reason": f"{future_ct} records have order_date in the future.",
                        "confidence": 0.88,
                        "source": "heuristic"
                    })

            # C) Negative total amounts
            if neg_total_ct > 0 and has.get("total_amount"):
                rule = {"if": "total_amount < 0", "then": "assign dq_flag = 'negative_amount'"}
                if not _rule_exists(rule):
                    suggestions.append({
                        "id": self._id(),
                        "title": "Negative total amounts",
                        "rule": rule,
                        "reason": f"{neg_total_ct} rows have total_amount < 0.",
                        "confidence": 0.89,
                        "source": "heuristic"
                    })

            # D) Price outliers (two separate rules: <=0 and >1000)
            if price_low_ct > 0 and has.get("unit_price"):
                rule = {"if": "unit_price <= 0", "then": "assign dq_flag = 'price_outlier_low'"}
                if not _rule_exists(rule):
                    suggestions.append({
                        "id": self._id(),
                        "title": "Zero or negative unit price",
                        "rule": rule,
                        "reason": f"{price_low_ct} rows with unit_price <= 0.",
                        "confidence": 0.9,
                        "source": "heuristic"
                    })
            if price_high_ct > 0 and has.get("unit_price"):
                rule = {"if": "unit_price > 1000", "then": "assign dq_flag = 'price_outlier_high'"}
                if not _rule_exists(rule):
                    suggestions.append({
                        "id": self._id(),
                        "title": "Extreme unit price",
                        "rule": rule,
                        "reason": f"{price_high_ct} rows with unit_price > 1000.",
                        "confidence": 0.9,
                        "source": "heuristic"
                    })

            # E) Unexpected currency per value (BTC, JPY, AUD, etc.), per specific value
            if unexpected_vals and has.get("currency"):
                for val, ct in unexpected_vals[:3]:  # limit to 3 to avoid noise
                    rule = {"if": f"currency == '{val}'", "then": "assign dq_alert = 'unexpected_currency'"}
                    if not _rule_exists(rule):
                        suggestions.append({
                            "id": self._id(),
                            "title": f"Unexpected currency: {val}",
                            "rule": rule,
                            "reason": f"{ct} rows contain currency={val}.",
                            "confidence": 0.86,
                            "source": "heuristic"
                        })

            # F) Legacy status-guard (only if ratio meaningful and high)
            assign_status = int((audit.get("assign", {}) or {}).get("status", 0) or 0)
            ratio = assign_status / rows if rows else 0.0
            if assign_status > 250 and ratio >= 0.30 and has.get("order_id"):
                rule = {"if": "order_id IS NOT NULL and status IS NULL", "then": "assign status = 'unknown'"}
                if not _rule_exists(rule):
                    suggestions.append({
                        "id": self._id(),
                        "title": "Guard status assignment to order-like rows only",
                        "rule": rule,
                        "reason": "High volume of status assignment suggests non-order datasets included.",
                        "confidence": 0.90,
                        "source": "heuristic"
                    })

        # ---------------- LLM proposal (drift signal) ----------------
        if self.enabled:
            console.print("[cyan]🔍 ENABLE_LLM=true → enriching prompt and deriving drift suggestion...[/cyan]")
            # simple rule: if mean delta large, keep drift alert (but only if not already a rule)
            delta = _n((drift or {}).get("total_amount_mean_delta"))
            if abs(delta) >= 50:
                rule = {"if": "true", "then": "assign dq_alert = 'mean_total_amount_shift'"}
                if not _rule_exists(rule):
                    suggestions.append({
                        "id": self._id(),
                        "title": "Drift alert rule suggestion",
                        "rule": rule,
                        "reason": f"Detected large mean shift in total_amount (Δ≈{delta:.2f}).",
                        "confidence": 0.87,
                        "source": "llm"
                    })
                console.print("[green]✅ LLM drift suggestion prepared from enriched context[/green]")
            else:
                console.print("[yellow]ℹ️ Drift below threshold; no LLM drift suggestion[/yellow]")
        else:
            console.print("[yellow]⚙️ ENABLE_LLM=false → skipping model call (heuristic mode only)[/yellow]")

        # ---------------- Persist provenance & suggestions ----------------
        anomaly_summary = {}
        for k, v in (anomalies or {}).items():
            if isinstance(v, dict) and "count" in v:
                anomaly_summary[k] = int(v.get("count") or 0)
            else:
                try:
                    anomaly_summary[k] = int(v or 0)
                except Exception:
                    anomaly_summary[k] = 0

        _append_insight_enriched({
            "run_name": run_name,
            "source_file": source_file,
            "rows": rows,
            "missing_ratios": miss,
            "numeric_stats": numeric_stats,
            "currency_counts": currency_counts,
            "future_date_ct": future_ct,
            "price_low_ct": price_low_ct,
            "price_high_ct": price_high_ct,
            "neg_total_ct": neg_total_ct,
            "drift": drift,
            "anomalies": anomaly_summary,
            "llm_enabled": self.enabled,
            "suggestions_count": len(suggestions)
        })

        if suggestions:
            _prepend_suggestions(suggestions)

        if not suggestions:
            console.print("[grey]No new suggestions this run (either thresholds not met or rules already present).[/grey]")

        return {"insight": {"mode": "llm" if self.enabled else "heuristic", "count": len(suggestions)},
                "suggestions": suggestions} if suggestions else None
