from __future__ import annotations
import re
from datetime import datetime

def normalize_symbol(sym: str|None, issuer_code: str|None=None) -> str|None:
    """
    Normalize symbol to UPPER + ensure '.JK' suffix.
    Priority: parsed `symbol` -> fallback `issuer_code` -> None.
    """
    cand = (sym or "").strip().upper() or (issuer_code or "").strip().upper()
    if not cand:
        return None
    return cand if cand.endswith(".JK") else f"{cand}.JK"

def slug(s: str) -> str:
    s = (s or "").lower().strip()
    repl = {"ñ":"n","ç":"c","·":"-","/":"-","_":"-",
            ",":"-",":":"-",";":"-"}
    for k,v in repl.items(): s = s.replace(k,v)
    s = re.sub(r"[^a-z0-9 \-]","",s)
    s = re.sub(r"\s+","-",s)
    s = re.sub(r"-{2,}","-",s)
    return s.strip("-")

def parse_timestamp(ts: str|None) -> tuple[str|None, str]:
    if not ts: return None, "an unknown date"
    try:
        if "T" in ts: dt = datetime.fromisoformat(ts.replace("Z","+00:00"))
        elif "-" in ts: dt = datetime.strptime(ts,"%Y-%m-%d %H:%M:%S")
        else: dt = datetime.strptime(ts,"%d %b %Y %H:%M:%S")
        return dt.strftime("%Y-%m-%d %H:%M:%S"), dt.strftime("%B %d, %Y")
    except: return None, "an unknown date"

def safe_int(x, default=0):
    try: return int(x)
    except:
        try: return int(float(str(x).replace(",","")))
        except: return default

def safe_float(x, default=0.0):
    try: return float(str(x).replace(",",""))
    except: return default
