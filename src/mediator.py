import json, pathlib, datetime as dt, sys, os, traceback, copy
ROOT = pathlib.Path(__file__).resolve().parents[1]
SRC = ROOT / 'src'
if str(SRC) not in sys.path: sys.path.append(str(SRC))
from llm import LLMClient
DQ = ROOT / 'dq'
META = ROOT / 'meta'

def _rule_exists_in_rules(rules_obj: dict, new_rule: dict) -> bool:
    try:
        key_new = json.dumps(new_rule, sort_keys=True)
        for r in rules_obj.get("logic", []):
            if json.dumps(r, sort_keys=True) == key_new:
                return True
    except Exception:
        pass
    return False

def _metrics_log(event: str, payload: dict):
    META.mkdir(parents=True, exist_ok=True)
    p = META / "metrics.jsonl"
    obj = {"ts": dt.datetime.now(dt.timezone.utc).isoformat(), "event": event}
    obj.update(payload or {})
    with p.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj) + "\n")

class Mediator:
    def __init__(self):
        self.rules = self._load_rules()
        self.baselines = self._load_baselines()
        self.llm = LLMClient()

    def _load_rules(self):
        p = DQ / 'rules.json'
        return json.loads(p.read_text()) if p.exists() else {'logic': []}

    def save_rules(self, rules):
        history_name = f"rules_{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
        hist_path = DQ / 'rules_history' / history_name
        hist_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            before = json.dumps(self.rules, indent=2)
            hist_path.write_text(before, encoding='utf-8')
            _metrics_log('rules_history_write_ok', {'file': str(hist_path), 'bytes': len(before)})
        except Exception as e:
            _metrics_log('rules_history_write_error', {'file': str(hist_path), 'error': str(e), 'trace': traceback.format_exc()[-4000:]})

        try:
            (DQ / 'rules.json').write_text(json.dumps(rules, indent=2), encoding='utf-8')
            self.rules = rules
            _metrics_log('rules_active_write_ok', {'file': str(DQ / 'rules.json'), 'rules_count': len(rules.get('logic', []))})
        except Exception as e:
            _metrics_log('rules_active_write_error', {'file': str(DQ / 'rules.json'), 'error': str(e), 'trace': traceback.format_exc()[-4000:]})

    def _load_baselines(self):
        p = DQ / 'baselines.json'
        return json.loads(p.read_text()) if p.exists() else {'runs': []}

    def update_baselines(self, snapshot):
        data = self._load_baselines()
        data['runs'].append(snapshot)
        (DQ / 'baselines.json').write_text(json.dumps(data, indent=2))
        self.baselines = data

    def record_run_report(self, report):
        name = f"run_{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
        out = DQ / 'run_reports' / name
        out.parent.mkdir(parents=True, exist_ok=True)
        report['report_name'] = name
        out.write_text(json.dumps(report, indent=2))
        return name

    def maybe_auto_accept(self, suggestions: list):
        if not suggestions:
            return False
        auto = os.getenv('AUTO_ACCEPT_LLM_RULES','false').lower()=='true'
        if not auto:
            return False

        rules = copy.deepcopy(self.rules)
        changed = False
        for s in suggestions:
            rule = s.get('rule')
            if s.get('confidence', 0) >= 0.85 and rule and not _rule_exists_in_rules(rules, rule):
                rules.setdefault('logic', []).append(rule)
                changed = True
        if changed:
            self.save_rules(rules)
        return changed
