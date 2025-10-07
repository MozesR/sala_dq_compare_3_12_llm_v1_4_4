import os, json, pathlib, datetime as dt, sys, math
from flask import Flask, jsonify, request, render_template, send_from_directory
from dotenv import load_dotenv
load_dotenv()

ROOT = pathlib.Path(__file__).resolve().parents[1]
DATA = ROOT / 'data'
DQ = ROOT / 'dq'
META = ROOT / 'meta'
SRC = ROOT / 'src'
if str(SRC) not in sys.path: sys.path.append(str(SRC))
from pipeline import run_once
from mediator import Mediator, _rule_exists_in_rules

app = Flask(__name__, static_folder=str(ROOT/'ui'/'static'), template_folder=str(ROOT/'ui'/'templates'))

def _json_safe(obj):
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [ _json_safe(v) for v in obj ]
    return obj

@app.after_request
def add_no_store(resp):
    try: p = request.path or ''
    except Exception: p = ''
    if p.startswith('/api/'): resp.headers['Cache-Control'] = 'no-store'
    return resp

# Pages
@app.route('/')
def page_rules(): return render_template('index.html')
@app.route('/dashboard')
def page_dashboard(): return render_template('dashboard.html')
@app.route('/history')
def page_history(): return render_template('history.html')
@app.route('/run/<path:name>')
def page_run(name): return render_template('run.html')
@app.route('/compare')
def page_compare(): return render_template('compare.html')
@app.route('/schema')
def page_schema(): return render_template('schema.html')

# APIs: rules
@app.route('/api/rules')
def api_rules(): return jsonify(json.loads((DQ/'rules.json').read_text()))

@app.route('/api/save_rules', methods=['POST'])
def api_save_rules():
    body = request.get_data(as_text=True) or ''
    try: obj = json.loads(body)
    except Exception: return jsonify({'ok':False,'error':'invalid json'}),400
    M = Mediator(); M.save_rules(obj)
    return jsonify({'ok':True})

# APIs: reports/compare
@app.route('/api/reports')
def api_reports():
    folder = DQ/'run_reports'; folder.mkdir(parents=True, exist_ok=True)
    names = sorted([p.name for p in folder.glob('*.json')])
    return jsonify(names)

@app.route('/api/report/<path:name>')
def api_report(name):
    p = DQ/'run_reports'/name
    if not p.exists(): return jsonify({'error':'not found'}),404
    data = json.loads(p.read_text())
    return jsonify(_json_safe(data))

@app.route('/api/compare/<path:run_a>/<path:run_b>')
def api_compare(run_a, run_b):
    def load(nm): 
        p = DQ/'run_reports'/nm
        return json.loads(p.read_text()) if p.exists() else None
    A, B = load(run_a), load(run_b)
    if not A or not B: return jsonify({'error':'one or both runs not found'}),404

    def summarize(rep):
        audit = rep.get('transform_audit') or {}
        def total(g):
            t=0
            for _,v in (g or {}).items():
                try: t += int(v or 0)
                except: t += 0
            return t
        return {
            'audit': audit,
            'auditTotals': {'assign': total(audit.get('assign')), 'impute': total(audit.get('impute')), 'compute': total(audit.get('compute'))},
            'drift': rep.get('drift') or {},
            'outliers': ((rep.get('anomaly_outliers') or {}).get('total_amount') or {}).get('count', 0)
        }
    def diff(auditA, auditB):
        rows = []
        for action in ['assign','impute','compute']:
            cols = set((auditA.get(action) or {}).keys()) | set((auditB.get(action) or {}).keys())
            for col in sorted(cols):
                a = int((auditA.get(action) or {}).get(col,0) or 0)
                b = int((auditB.get(action) or {}).get(col,0) or 0)
                rows.append({'action':action,'column':col,'runA':a,'runB':b,'delta': b-a})
        return rows
    SA, SB = summarize(A), summarize(B)
    payload = {'runA': SA, 'runB': SB, 'auditDelta': diff(SA['audit'], SB['audit'])}
    return jsonify(_json_safe(payload))

# APIs: upload
@app.route('/api/upload', methods=['POST'])
def api_upload():
    f = request.files.get('file')
    if not f: return jsonify({'ok':False,'error':'no file'}),400
    uploads = DATA/'uploads'; uploads.mkdir(parents=True, exist_ok=True)
    out_path = uploads / f'upload_{dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")}.csv'
    f.save(str(out_path))
    try:
        report, name = run_once(csv_path=out_path, debug=True)
        return jsonify(_json_safe({'ok':True,'report_name':name,'report':report}))
    except Exception as e:
        return jsonify({'ok':False,'error':str(e)}),500

# APIs: schema
@app.route('/api/schema')
def api_schema():
    import pandas as pd
    cur = DATA/'curated.csv'
    if not cur.exists(): return jsonify({'columns': []})
    df = pd.read_csv(cur)
    cols = []
    for c in df.columns:
        null_pct = float(df[c].isna().mean() * 100)
        cols.append({'name': c, 'dtype': str(df[c].dtype), 'null_pct': null_pct, 'unique': int(df[c].nunique(dropna=True))})
    return jsonify(_json_safe({'columns': cols}))

# APIs: LLM suggestions
@app.route('/api/llm/suggestions')
def api_llm_suggestions():
    p = META/'suggestions.json'
    if not p.exists(): return jsonify([])
    try:
        return jsonify(_json_safe(json.loads(p.read_text())))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/llm/apply', methods=['POST'])
def api_llm_apply():
    payload = request.get_json(silent=True) or {}
    sid = payload.get('id')
    rule = payload.get('rule')
    sugs_path = META/'suggestions.json'; sugs = []
    if sugs_path.exists():
        try: sugs = json.loads(sugs_path.read_text())
        except Exception: sugs = []
    s = None
    if sid:
        s = next((x for x in sugs if x.get('id') == sid), None)
        if not s: return jsonify({'ok': False, 'error': 'suggestion not found'}), 404
        rule = s.get('rule')
    if not rule: return jsonify({'ok': False, 'error': 'no rule to apply'}), 400
    M = Mediator()
    rules = M.rules.copy()
    if _rule_exists_in_rules(rules, rule):
        if sid:
            sugs = [x for x in sugs if x.get('id') != sid]
            sugs_path.write_text(json.dumps(sugs, indent=2))
        return jsonify({'ok': True, 'skipped': 'duplicate rule'}), 200
    rules.setdefault('logic', []).append(rule)
    M.save_rules(rules)
    if sid:
        sugs = [x for x in sugs if x.get('id') != sid]
        sugs_path.write_text(json.dumps(sugs, indent=2))
    return jsonify({'ok': True})

@app.route('/static/<path:filename>')
def static_files(filename): return send_from_directory(str(ROOT/'ui'/'static'), filename)

if __name__ == '__main__':
    from rich.console import Console
    c = Console()
    c.print('\n[bold green]✅ SALA DQ Compare v1.4.4[/bold green]')
    c.print('Python 3.12 • Flask UI • Dark Mode')
    c.print('📊 http://127.0.0.1:5000  |  🆚 /compare  |  🧩 /schema  |  🤖 LLM Suggestions on /')
    app.run(debug=False, host='127.0.0.1', port=5000)
