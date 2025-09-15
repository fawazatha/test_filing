from __future__ import annotations

import os, json, re, time, random, math
from typing import Dict, Any, Optional, Tuple, List
from datetime import datetime

from .io_utils import get_logger
log = get_logger(__name__)

# -------- import aman utk Groq / OpenAI rate-limit --------
try:
    from groq import Groq, RateLimitError as GroqRateLimitError
except Exception:  # kalau lib groq belum ada / gagal import
    Groq = None
    class GroqRateLimitError(Exception): ...
try:
    from openai import RateLimitError as OpenAIRateLimitError  # optional
except Exception:
    class OpenAIRateLimitError(Exception): ...

# ---------------- helpers ----------------
def _env(key: str, default: str = "") -> str:
    v = os.getenv(key)
    if v is None:
        return default
    return v.strip().strip('"').strip("'")

def _safe_int(x) -> Optional[int]:
    try: return int(str(x).strip())
    except Exception: return None

def _fmt_int(n: Optional[int]) -> str:
    try: return f"{int(n):,}".replace(",", ".") if n is not None else ""
    except Exception: return str(n)

def _date_str(ts: Optional[str]) -> str:
    if not ts: return ""
    try:
        if "T" in ts: dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        else: dt = datetime.fromisoformat(ts)
        return dt.strftime("%B %d, %Y")
    except Exception:
        return ts or ""

def _compute_delta(before: Optional[str], after: Optional[str]) -> Tuple[Optional[int], Optional[float]]:
    b = _safe_int(before); a = _safe_int(after)
    if b is None or a is None: return None, None
    delta = a - b
    pct = (delta / b * 100.0) if b != 0 else None
    return delta, pct

def _extract_json(text: str) -> Dict[str, Any]:
    if not text: raise ValueError("Empty LLM output")
    text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip(), flags=re.IGNORECASE)
    m = re.search(r"\{.*\}", text, flags=re.DOTALL)
    candidate = m.group(0) if m else text
    out = json.loads(candidate)
    if not isinstance(out, dict): raise ValueError("Parsed JSON is not an object")
    return out

def _parse_providers(prefer: Optional[str] = None) -> List[str]:
    if prefer:
        return [prefer.lower().strip().strip('"').strip("'")]
    env_list = _env("LLM_PROVIDERS")
    if env_list:
        return [p.strip().lower() for p in env_list.split(",") if p.strip()]
    single = _env("LLM_PROVIDER").lower()
    if single in {"openai","groq"}:
        return [single]
    # default: kalau ada GROQ_API_KEY → groq dulu
    if _env("GROQ_API_KEY"):
        return ["groq"] + (["openai"] if _env("OPENAI_API_KEY") else [])
    if _env("OPENAI_API_KEY"):
        return ["openai"]
    return []

# Alias untuk Groq
_GROQ_ALIAS = {
    "compound": "groq/compound",
    "compound-mini": "groq/compound-mini",
    "groq/compound": "groq/compound",
    "groq/compound-mini": "groq/compound-mini",
}

def _normalize_model(provider: str, name: Optional[str]) -> str:
    """
    Tidak memaksa fallback ke versatile jika user sudah set modelnya.
    Support alias Groq: "compound" → "groq/compound".
    """
    if provider == "openai":
        # kalau user passing nama groq/llama, map ke model OpenAI yg aman
        if not name or name.startswith("llama-") or name.startswith("groq/"):
            return "gpt-4.1-mini"
        return name

    if provider == "groq":
        deprecated = {
            "llama-3.1-70b-versatile": "llama-3.3-70b-versatile",
            "llama3-70b-8192": "llama-3.3-70b-versatile",
            "llama3-8b-8192": "llama-3.1-8b-instant",
        }
        nm = (name or "").strip()
        if not nm:
            # default groq bila user tidak set
            return "llama-3.3-70b-versatile"
        if nm in _GROQ_ALIAS:
            return _GROQ_ALIAS[nm]
        return deprecated.get(nm, nm)

    return name or ""

def _lc_to_openai_msgs(msgs) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for m in msgs:
        t = type(m).__name__.lower()
        if "system" in t: role = "system"
        elif "human" in t or "user" in t: role = "user"
        elif "ai" in t or "assistant" in t: role = "assistant"
        else: role = "user"
        content = getattr(m, "content", "") or str(m)
        out.append({"role": role, "content": content})
    return out

# --------- fallback lokal biar tetap output walau TPD ---------
def _local_fallback(v: Dict[str, Any]) -> Tuple[str, str]:
    rng = random.Random(hash((v.get("symbol",""), v.get("date_str",""), v.get("holder",""))) & 0xffffffff)
    verb_map = {
        "buy": ["buys", "adds", "accumulates"],
        "sell": ["sells", "cuts", "reduces"],
        "transfer": ["transfers", "moves", "reallocates"],
        "transaction": ["trades"]
    }
    verbs = verb_map.get(v.get("tx_type","transaction"), ["trades"])
    verb = rng.choice(verbs)
    cname = v["company_name"] or v["symbol"]; sym=v["symbol"]; holder=v["holder"]
    volume=v["volume"]; price=v["price_range"]; before=v["hold_before"]; after=v["hold_after"]
    delta=v["delta_shares"]; pct=v["delta_pct"]; date_s=v["date_str"]

    title = rng.choice([
        f"{holder} {verb.capitalize()} {cname} ({sym}) Shares",
        f"{holder} {verb.capitalize()} Stake in {cname} ({sym})",
        f"{cname} ({sym}): {holder} {verb} shares",
    ])

    price_phrase = f"at {price} per share" if price and price!="undisclosed" else "at an undisclosed price"
    own_phrase = ""
    if before or after:
        own_phrase = f", shifting ownership from {before or '?'} to {after or '?'}"
        if delta:
            own_phrase += f" (Δ {delta}{f' / {pct}' if pct else ''})"
    p1 = f"On {date_s}, {holder} {verb} {volume} shares of {cname} ({sym}) {price_phrase}{own_phrase}."

    p2_opts = [
        "The change appears in the latest IDX ownership filing.",
        "The disclosure was filed with the Indonesia Stock Exchange.",
        "The movement is recorded in recent IDX ownership disclosures.",
    ]
    if v.get("reason"):
        p2_opts.append(f"Stated purpose: {v['reason']}.")
    body = p1 + " " + rng.choice(p2_opts)
    return title, body

# ---------- Rate-limit classification & backoff ----------
def _classify_rl_error(err: Exception) -> str:
    """
    Return one of: 'TPD','RPD','TPM','RPM','UNKNOWN'
    """
    s = str(err).lower()
    # petunjuk umum dari pesan error
    if "tokens per day" in s or "tpd" in s or "daily token" in s:
        return "TPD"
    if "requests per day" in s or "rpd" in s:
        return "RPD"
    if "tokens per minute" in s or "tpm" in s or "per minute token" in s:
        return "TPM"
    if "requests per minute" in s or "rpm" in s or "too many requests" in s:
        return "RPM"
    if "insufficient_quota" in s:
        # biasanya daily quota
        return "TPD"
    return "UNKNOWN"

def _backoff_sleep(base: float, attempt: int) -> float:
    """
    Exponential backoff with jitter. attempt starts at 0.
    """
    #  base * 2^attempt  + jitter(0..0.5)
    return max(0.0, base * (2 ** attempt) + random.random() * 0.5)

# ---------------- main ----------------
class Summarizer:
    """
    Selalu pakai LLM. Provider: Groq (SDK) dan/atau OpenAI.
    Output: JSON {"title","body"} gaya newsroom.
    """
    def __init__(self, use_llm: bool = True, groq_model: str = "llama-3.3-70b-versatile", provider: Optional[str] = None):
        if not use_llm:
            raise RuntimeError("Summarizer must use LLM (use_llm=True).")

        # pastikan .env kebaca
        try:
            from dotenv import load_dotenv, find_dotenv
            load_dotenv(find_dotenv(usecwd=True), override=False)
        except Exception:
            pass

        self.providers_order = _parse_providers(provider)
        if not self.providers_order:
            raise RuntimeError("No LLM clients available. Set GROQ_API_KEY or OPENAI_API_KEY.")

        self.model_hint = groq_model
        # retry/env tuning
        self.max_retries = int(os.getenv("LLM_MAX_RETRIES", "2"))     # default 2x retry utk TPM/RPM
        self.retry_sleep = float(os.getenv("LLM_RETRY_SLEEP", "0.8")) # base sleep
        self.use_local_fallback_on_tpd = os.getenv("LLM_TPD_LOCAL_FALLBACK", "1") == "1"
        self.temperature = float(os.getenv("LLM_TEMPERATURE", "0.6"))
        self.max_tokens = int(os.getenv("LLM_MAX_TOKENS", "0"))  # 0 = biarkan default server

        # Prompt infra
        try:
            from langchain_core.prompts import ChatPromptTemplate
        except Exception:
            from langchain.prompts import ChatPromptTemplate
        self.ChatPromptTemplate = ChatPromptTemplate

        self._prompt = self.ChatPromptTemplate.from_messages(
            [
                ("system",
                 "You are a financial news writer summarizing Indonesian stock exchange (IDX) "
                 "ownership filings. Write a concise, newsroom-style brief in English. Be factual, "
                 "neutral, and specific. Use only provided facts; do not invent data. Limit the body "
                 "to about 110–140 words in 2–3 sentences. Avoid any legal disclaimers. Vary wording slightly."),
                ("human",
                 "Facts:\n"
                 "- Company: {company_name}\n"
                 "- Ticker: {symbol}\n"
                 "- Holder: {holder}\n"
                 "- Transaction type: {tx_type}\n"
                 "- Total volume (shares): {volume}\n"
                 "- Price range: {price_range}\n"
                 "- Holdings before: {hold_before}\n"
                 "- Holdings after: {hold_after}\n"
                 "- Delta (shares): {delta_shares}\n"
                 "- Delta (%): {delta_pct}\n"
                 "- Purpose/Reason: {reason}\n"
                 "- Date: {date_str}\n"
                 "- Source: {source}\n"
                 "{extra_text}\n\n"
                 "Write a headline (8–14 words, active voice) that includes the company name and ticker "
                 "in parentheses. Then write a two-to-three sentence body that: "
                 "• states who did what and when; "
                 "• quantifies volume and mentions price range or 'undisclosed'; "
                 "• explains ownership change (before/after, delta, and % if available); "
                 "• adds one short factual relevance line.\n\n"
                 "Output ONLY valid JSON with keys 'title' and 'body'.")
            ]
        )

        # siapkan klien
        self._clients: List[Dict[str, Any]] = []
        for prov in self.providers_order:
            if prov == "groq":
                key = _env("GROQ_API_KEY")
                if not key:
                    continue
                os.environ["GROQ_API_KEY"] = key  # untuk lib yang baca dari ENV
                model = _normalize_model("groq", self.model_hint)

                # 1) Groq SDK (utama)
                groq_sdk = None
                if Groq is not None:
                    try:
                        groq_sdk = Groq(api_key=key)
                    except Exception as e:
                        log.debug(f"Groq SDK init warn: {e}")

                # 2) Fallback via langchain_groq
                lc_llm = None
                try:
                    from langchain_groq import ChatGroq
                    lc_llm = ChatGroq(model=model, temperature=self.temperature, groq_api_key=key)
                except Exception as e:
                    log.debug(f"langchain_groq init warn: {e}")

                if groq_sdk is None and lc_llm is None:
                    continue

                self._clients.append({"prov": "groq", "model": model, "sdk": groq_sdk, "llm": lc_llm})
                log.info(f"Summarizer provider ready: groq ({model})")

            elif prov == "openai":
                key = _env("OPENAI_API_KEY")
                if not key:
                    continue
                model = _normalize_model("openai", self.model_hint)
                from langchain_openai import ChatOpenAI
                try:
                    llm = ChatOpenAI(model=model, temperature=self.temperature, api_key=key)
                except TypeError:
                    llm = ChatOpenAI(model=model, temperature=self.temperature)
                self._clients.append({"prov": "openai", "model": model, "llm": llm})
                log.info(f"Summarizer provider ready: openai ({model})")

        if not self._clients:
            raise RuntimeError("No LLM clients available. Set GROQ_API_KEY or OPENAI_API_KEY.")

    def _vars_from_facts(self, facts: Dict[str, Any], text_hint: Optional[str]) -> Dict[str, Any]:
        cname = facts.get("company_name") or facts.get("symbol") or ""
        symbol = facts.get("symbol") or ""
        holder = facts.get("holder_name") or facts.get("holder_type") or "an insider"
        tx_type = (facts.get("transaction_type") or "transaction").lower()
        prices = facts.get("prices") or []
        amounts = facts.get("amount_transacted") or []
        vol = sum(int(a) for a in amounts if a is not None) if amounts else 0
        if prices:
            pmin, pmax = min(prices), max(prices)
            price_range = (f"≈ IDR {int(round(pmin)):,}".replace(",", ".")
                           if pmin == pmax else
                           f"IDR {int(round(pmin)):,}–{int(round(pmax)):,}".replace(",", "."))
        else:
            price_range = "undisclosed"
        hold_before = facts.get("holdings_before")
        hold_after = facts.get("holdings_after")
        delta_shares, delta_pct = _compute_delta(hold_before, hold_after)
        return {
            "company_name": cname, "symbol": symbol, "holder": holder, "tx_type": tx_type,
            "volume": _fmt_int(vol) if vol else "0",
            "price_range": price_range,
            "hold_before": _fmt_int(_safe_int(hold_before)) if hold_before else "",
            "hold_after": _fmt_int(_safe_int(hold_after)) if hold_after else "",
            "delta_shares": (_fmt_int(delta_shares) if delta_shares is not None else ""),
            "delta_pct": (f"{delta_pct:.2f}%" if delta_pct is not None else ""),
            "reason": facts.get("reason") or "",
            "date_str": _date_str(facts.get("timestamp")),
            "source": facts.get("source") or "",
            "extra_text": f"\nRaw text:\n{(text_hint or '')[:600]}" if (text_hint and text_hint.strip()) else "",
        }

    def summarize_from_facts(self, facts: Dict[str, Any], text_hint: Optional[str] = None) -> Tuple[str, str]:
        vars_dict = self._vars_from_facts(facts, text_hint)
        msgs = self._prompt.format_messages(**vars_dict)
        last_err: Optional[Exception] = None

        for c in self._clients:
            prov, model = c["prov"], c["model"]
            for attempt in range(self.max_retries + 1):
                try:
                    log.info(f"Summarizer invoking {prov} ({model})")

                    if prov == "groq":
                        if c.get("sdk") is not None:
                            kwargs = {
                                "model": model,
                                "messages": _lc_to_openai_msgs(msgs),
                                "temperature": self.temperature,
                            }
                            if self.max_tokens > 0:
                                kwargs["max_tokens"] = self.max_tokens
                            text = c["sdk"].chat.completions.create(**kwargs).choices[0].message.content
                        elif c.get("llm") is not None:
                            text = c["llm"].invoke(msgs).content
                        else:
                            raise RuntimeError("No Groq client available")
                    else:
                        # openai via langchain_openai
                        text = c["llm"].invoke(msgs).content

                    out = _extract_json(text)
                    title = (out.get("title") or "").strip()
                    body  = (out.get("body")  or "").strip()
                    if not title or not body:
                        raise ValueError("Empty title/body from LLM.")

                    banned = [
                        "this disclosure is informational",
                        "investment advice",
                        "please refer to the official filing",
                    ]
                    if any(b in body.lower() for b in banned):
                        body = "\n".join([ln for ln in body.splitlines()
                                          if not any(b in ln.lower() for b in banned)])
                    return title, body

                except (GroqRateLimitError, OpenAIRateLimitError) as e:
                    last_err = e
                    kind = _classify_rl_error(e)

                    # Quota harian: jangan retry berkali-kali, langsung fallback atau pindah provider
                    if kind in ("TPD", "RPD"):
                        if self.use_local_fallback_on_tpd:
                            log.error("Hit %s; using local fallback writer.", kind)
                            return _local_fallback(vars_dict)
                        log.error("Hit %s; no local fallback enabled.", kind)
                        break  # coba provider berikutnya

                    # Minute-level (TPM/RPM) → backoff & retry
                    wait = _backoff_sleep(self.retry_sleep, attempt)
                    if attempt < self.max_retries:
                        log.warning("Rate limited (%s). Backing off %.2fs (attempt %d/%d).",
                                    kind, wait, attempt + 1, self.max_retries)
                        time.sleep(wait)
                        continue
                    log.error("Rate limited after retries (%s); trying next provider...", kind)
                    break

                except Exception as e:
                    last_err = e
                    log.exception("%s call failed; trying next provider...", prov)
                    break  # stop attempts for this provider; try next provider

        log.error("LLM summarize failed (no providers left).")
        raise RuntimeError("LLM summarize failed (no fallback).") from last_err
