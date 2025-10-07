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
```
