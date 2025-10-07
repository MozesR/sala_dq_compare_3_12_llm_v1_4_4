import os, json, pathlib, sys
import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT / 'src') not in sys.path:
    sys.path.append(str(ROOT / 'src'))

from mediator import Mediator
from transform_executor import apply_policies

DATA = ROOT / 'data'
DQ = ROOT / 'dq'

def _coerce_numeric(df, cols=('quantity','unit_price','total_amount')):
    for c in cols:
        if c in df.columns:
            s = df[c].astype(str)
            if s.str.contains(',', na=False).any():
                s = s.str.replace(',', '.', regex=False)
            df[c] = pd.to_numeric(s, errors='coerce')

def iqr_outliers(series, k=1.5):
    s = pd.to_numeric(series, errors='coerce').dropna()
    if s.empty: return 0, None, None
    q1, q3 = s.quantile(0.25), s.quantile(0.75)
    iqr = q3 - q1
    lo, hi = q1 - k*iqr, q3 + k*iqr
    ct = int(((s < lo) | (s > hi)).sum())
    return ct, float(lo), float(hi)

def mean_delta(curr, prev):
    try: return float(curr) - float(prev)
    except: return None

def _read_json_safe(path, default):
    try:
        txt = path.read_text()
        return json.loads(txt)
    except Exception:
        return default

def _atomic_write_json(path, obj):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2), encoding="utf-8")
    tmp.replace(path)

def run_once(csv_path=None, debug=True):
    M = Mediator()
    if csv_path is None:
        csv_path = DATA / 'orders_sample.csv'
    df = pd.read_csv(csv_path)
    _coerce_numeric(df)

    if 'total_amount' not in df.columns and {'quantity','unit_price'} <= set(df.columns):
        df['total_amount'] = df['quantity'] * df['unit_price']

    df, transform_audit = apply_policies(df, M.rules.get('logic', []))

    out_ct, lo, hi = iqr_outliers(df.get('total_amount', []), k=1.5)
    vc_status = df['status'].value_counts().to_dict() if 'status' in df.columns else {}
    mean_amt = float(df['total_amount'].mean()) if 'total_amount' in df.columns else 0.0

    snapshot = {'value_counts': {'status': vc_status}, 'means': {'total_amount': mean_amt}}
    drift = {}
    base_path = DQ / 'baselines.json'

    prev_all = _read_json_safe(base_path, {'runs': []})
    prev = prev_all['runs'][-1] if prev_all.get('runs') else None

    if prev:
        d = mean_delta(mean_amt, prev.get('means', {}).get('total_amount'))
        drift['total_amount_mean_delta'] = d if d is not None else 0.0
        a = vc_status.get('open', 0) / max(1, sum(vc_status.values()))
        prev_vc = prev.get('value_counts', {}).get('status', {})
        b = (prev_vc.get('open', 0)) / max(1, sum(prev_vc.values()))
        drift['status_psi'] = round(abs(a - b), 4)

    window = int(os.getenv('BASELINE_HISTORY_WINDOW', '50') or '50')
    prev_all['runs'] = (prev_all.get('runs') or []) + [snapshot]
    if window > 0 and len(prev_all['runs']) > window:
        prev_all['runs'] = prev_all['runs'][-window:]
    _atomic_write_json(base_path, prev_all)

    out = DATA / 'curated.csv'
    df.to_csv(out, index=False)

    report = {
        'schema_errors': [],
        'validation_warnings': [],
        'validation_errors': [],
        'anomaly_outliers': {'total_amount': {'count': out_ct, 'bounds': [lo, hi]}},
        'drift': drift,
        'transform_audit': transform_audit,
        'output_file': str(out),
        'source_file': str(csv_path)
    }

    try:
        res = M.llm.analyze_quality_issues(df, report)
        if res:
            report['llm'] = res
            if M.maybe_auto_accept(res.get('suggestions')):
                report['llm']['auto_applied'] = True
    except Exception:
        pass

    name = M.record_run_report(report)
    if debug:
        print('AUDIT DEBUG:', json.dumps(transform_audit, indent=2))
    return report, name
