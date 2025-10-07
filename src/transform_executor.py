import re, numpy as np, pandas as pd
SAFE_OPS={'+','-','*','/'}

def _parse_value(t):
    t=t.strip()
    if t.startswith("'") and t.endswith("'"): return t[1:-1]
    try: return float(t) if '.' in t else int(t)
    except: return t

def _col(df,n):
    if n not in df.columns: df[n]=np.nan
    return df[n]

def _parse_condition(df, cond):
    cond=(cond or '').strip()
    if not cond or cond.lower()=='true': return pd.Series(True, index=df.index)
    parts=[p.strip() for p in re.split(r'\band\b', cond, flags=re.IGNORECASE)]
    mask=pd.Series(True, index=df.index)
    for p in parts:
        m=re.match(r'^(\w+)\s+IS\s+NULL$', p, flags=re.IGNORECASE)
        if m: mask&=_col(df,m.group(1)).isna(); continue
        m=re.match(r'^(\w+)\s+IS\s+NOT\s+NULL$', p, flags=re.IGNORECASE)
        if m: mask&=_col(df,m.group(1)).notna(); continue
        m=re.match(r'^(\w+)\s*(==|!=|>=|<=|>|<)\s*(.+)$', p)
        if m:
            col,op,val=m.groups(); s=_col(df,col); v=_parse_value(val)
            if isinstance(v,str) and v in df.columns: v=df[v]
            num_ops={'>','>=','<','<='}; is_num=op in num_ops
            if is_num or isinstance(v,(int,float)) or hasattr(v,'dtype'):
                sN=pd.to_numeric(s, errors='coerce')
                if isinstance(v,(int,float)): vN=float(v)
                elif hasattr(v,'dtype'): vN=pd.to_numeric(v, errors='coerce')
                else:
                    try: vN=float(v)
                    except: vN=None
                if vN is not None:
                    if op=='==': mask&=(sN==vN)
                    elif op!='!=' and op in num_ops: mask&=eval(f'sN {op} vN')
                    elif op=='!=': mask&=(sN!=vN)
                    continue
            if op=='==': mask&=(s==v)
            elif op=='!=': mask&=(s!=v)
            elif op=='>': mask&=(s>v)
            elif op=='>=': mask&=(s>=v)
            elif op=='<': mask&=(s<v)
            elif op=='<=': mask&=(s<=v)
    return mask

def _ensure_common(df):
    if 'imputed_flag' not in df.columns: df['imputed_flag']=False
    if 'missing_reason' not in df.columns: df['missing_reason']=pd.Series('', dtype='string')
    elif df['missing_reason'].dtype.name!='string': df['missing_reason']=df['missing_reason'].astype('string')

def _set_meta(df, mask, reason):
    _ensure_common(df)
    if mask.any():
        df.loc[mask,'imputed_flag']=True
        df.loc[mask,'missing_reason']=reason

def _ensure_dtype_for_value(df, col, value):
    import pandas as pd, numpy as np
    if isinstance(value, str):
        if col not in df.columns: df[col]=pd.Series(pd.NA, dtype='string')
        elif df[col].dtype.name!='string': df[col]=df[col].astype('string')
    elif isinstance(value,(int,float,np.number)) or hasattr(value,'dtype'):
        if col not in df.columns: df[col]=pd.Series(np.nan, dtype='float')
        elif not pd.api.types.is_float_dtype(df[col]):
            try: df[col]=pd.to_numeric(df[col], errors='coerce')
            except Exception: df[col]=df[col].astype('float')
    else:
        if col not in df.columns: df[col]=pd.Series([None]*len(df), dtype='object')

def _eval_expr(df, expr):
    toks=expr.strip().split()
    if not toks: return None
    stk=[]
    for t in toks:
        if t in SAFE_OPS: stk.append(t)
        else:
            v=_parse_value(t)
            if isinstance(v,str) and v in df.columns: stk.append(pd.to_numeric(df[v], errors='coerce'))
            else: stk.append(float(v) if isinstance(v,(int,float)) else v)
    i=0
    while i<len(stk):
        t=stk[i]
        if isinstance(t,str) and t in ('*','/'):
            L,R=stk[i-1],stk[i+1]; stk[i-1:i+2]=[(L*R) if t=='*' else (L/R)]; i-=1
        else: i+=1
    i=0
    while i<len(stk):
        t=stk[i]
        if isinstance(t,str) and t in ('+','-'):
            L,R=stk[i-1],stk[i+1]; stk[i-1:i+2]=[(L+R) if t=='+' else (L-R)]; i-=1
        else: i+=1
    return stk[0] if stk else None

def apply_policies(df, rules):
    audit={'assign':{},'impute':{},'compute':{}}
    if not rules: return df, audit
    for r in rules:
        if not isinstance(r,dict) or 'if' not in r or 'then' not in r: continue
        cond=str(r['if']); action=str(r['then']).strip()
        if not action.lower().startswith(('assign ','impute ','compute ')): continue
        try: mask=_parse_condition(df,cond)
        except Exception: continue
        m=re.match(r'^assign\s+(\w+)\s*=\s*(.+)$', action, flags=re.IGNORECASE)
        if m:
            col,val=m.groups(); v=_parse_value(val); _ensure_dtype_for_value(df,col,v)
            df.loc[mask,col]=v; _set_meta(df,mask,f'assign {col} = {v}'); audit['assign'][col]=int(audit['assign'].get(col,0)+int(mask.sum())); continue
        m=re.match(r'^impute\s+(\w+)\s*=\s*(.+)$', action, flags=re.IGNORECASE)
        if m:
            col,expr=m.groups()
            if expr.lower().startswith('mean(') and expr.endswith(')'):
                tgt=expr[5:-1].strip(); mu=pd.to_numeric(df.get(tgt,pd.Series(dtype=float)), errors='coerce').mean()
                _ensure_dtype_for_value(df,col,float(mu)); df.loc[mask,col]=mu; _set_meta(df,mask,f'impute {col} = mean({tgt})')
            else:
                v=_parse_value(expr); _ensure_dtype_for_value(df,col,v); df.loc[mask,col]=v; _set_meta(df,mask,f'impute {col} = {v}')
            audit['impute'][col]=int(audit['impute'].get(col,0)+int(mask.sum())); continue
        m=re.match(r'^compute\s+(\w+)\s*=\s*(.+)$', action, flags=re.IGNORECASE)
        if m:
            col,expr=m.groups(); val=_eval_expr(df,expr)
            if val is not None:
                _ensure_dtype_for_value(df,col,0.0); df.loc[mask,col]=val; _set_meta(df,mask,f'compute {col} = {expr}')
                audit['compute'][col]=int(audit['compute'].get(col,0)+int(mask.sum()))
            continue
    return df, audit
