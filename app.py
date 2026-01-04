# app.py  (Enhanced: OpenAlex + Wikidata + Crossref + DOAJ + optional NLM + optional AI)
import re
import json
import time
from io import BytesIO
from typing import Optional, Dict, Any, Tuple, List

import pandas as pd
import requests
import streamlit as st

# Optional fuzzy match
try:
    from rapidfuzz import fuzz
    HAVE_RAPIDFUZZ = True
except Exception:
    import difflib
    HAVE_RAPIDFUZZ = False

st.set_page_config(page_title="Journal Checker (Multi-source, no Search API)", layout="wide")

OPENALEX_BASE = "https://api.openalex.org"
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
CROSSREF_BASE = "https://api.crossref.org"
DOAJ_BASE_V2 = "https://doaj.org/api/v2"
NCBI_EUTILS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"


# =========================
# Helpers
# =========================
def norm_issn(x: str) -> str:
    """Normalize ISSN to 1234-567X; return '' if invalid/empty."""
    if x is None:
        return ""
    s = str(x).strip().upper().replace(" ", "")
    if s in ("NAN", "NONE", ""):
        return ""
    m = re.search(r"(\d{4})-?(\d{3}[\dX])", s)
    return f"{m.group(1)}-{m.group(2)}" if m else ""


def looks_like_issn(s: str) -> bool:
    return bool(norm_issn(s))


def clean_title(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s


def to_excel_bytes(df: pd.DataFrame) -> bytes:
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Result")
    return bio.getvalue()


def oa_params(mailto: str) -> Dict[str, str]:
    return {"mailto": mailto} if mailto else {}


def headers_json(user_agent: str) -> Dict[str, str]:
    return {"Accept": "application/json", "User-Agent": user_agent}


def safe_json(resp: requests.Response) -> Optional[dict]:
    ctype = (resp.headers.get("Content-Type") or "").lower()
    if "json" not in ctype:
        return None
    try:
        return resp.json()
    except ValueError:
        return None


def to_api_openalex_url(maybe_id_or_url: str) -> str:
    s = (maybe_id_or_url or "").strip()
    if not s:
        return ""
    if s.startswith("https://openalex.org/"):
        return s.replace("https://openalex.org/", f"{OPENALEX_BASE}/", 1)
    return s


def best_similarity(a: str, b: str) -> float:
    """Return similarity 0..100."""
    a = clean_title(a).lower()
    b = clean_title(b).lower()
    if not a or not b:
        return 0.0
    if HAVE_RAPIDFUZZ:
        return float(fuzz.token_set_ratio(a, b))
    # fallback
    return 100.0 * difflib.SequenceMatcher(None, a, b).ratio()


def pick_best_by_title(candidates: List[Dict[str, Any]], query_title: str, title_getter) -> Tuple[Optional[Dict[str, Any]], float]:
    best = None
    best_score = -1.0
    for c in candidates or []:
        t = title_getter(c) or ""
        score = best_similarity(query_title, t)
        if score > best_score:
            best_score = score
            best = c
    return best, best_score


def first_nonempty(*xs: str) -> str:
    for x in xs:
        if x and str(x).strip():
            return str(x).strip()
    return ""


def uniq_urls(urls: List[str]) -> str:
    seen = set()
    out = []
    for u in urls:
        u = (u or "").strip()
        if not u:
            continue
        if u not in seen:
            seen.add(u)
            out.append(u)
    return " | ".join(out)


# =========================
# HTTP session with light retry
# =========================
def make_session() -> requests.Session:
    s = requests.Session()
    return s


SESSION = make_session()


def get_with_retry(url: str, *, params=None, headers=None, timeout=20, retries=1, backoff=1.2) -> requests.Response:
    last_exc = None
    for i in range(retries + 1):
        try:
            r = SESSION.get(url, params=params, headers=headers, timeout=timeout)
            # retry on transient statuses
            if r.status_code in (429, 500, 502, 503, 504) and i < retries:
                time.sleep(backoff * (i + 1))
                continue
            return r
        except Exception as e:
            last_exc = e
            if i < retries:
                time.sleep(backoff * (i + 1))
                continue
            raise last_exc


def post_with_retry(url: str, *, json_body=None, headers=None, timeout=30, retries=1, backoff=1.2) -> requests.Response:
    last_exc = None
    for i in range(retries + 1):
        try:
            r = SESSION.post(url, json=json_body, headers=headers, timeout=timeout)
            if r.status_code in (429, 500, 502, 503, 504) and i < retries:
                time.sleep(backoff * (i + 1))
                continue
            return r
        except Exception as e:
            last_exc = e
            if i < retries:
                time.sleep(backoff * (i + 1))
                continue
            raise last_exc


# =========================
# OpenAlex: sources + concepts + org
# =========================
@st.cache_data(show_spinner=False, ttl=24 * 3600)
def openalex_sources_search(query: str, mailto: str, per_page: int = 50) -> List[Dict[str, Any]]:
    q = clean_title(query)
    if not q:
        return []
    url = f"{OPENALEX_BASE}/sources"
    params = {"search": q, "per-page": per_page, **oa_params(mailto)}
    r = get_with_retry(url, params=params, headers=headers_json(f"JournalChecker/1.0 ({mailto})" if mailto else "JournalChecker/1.0"), retries=1)
    r.raise_for_status()
    data = safe_json(r) or {}
    return data.get("results", []) or []


@st.cache_data(show_spinner=False, ttl=24 * 3600)
def openalex_sources_by_issn(issn: str, mailto: str, per_page: int = 50) -> List[Dict[str, Any]]:
    issn = norm_issn(issn)
    if not issn:
        return []
    url = f"{OPENALEX_BASE}/sources"
    params = {"filter": f"issn:{issn}", "per-page": per_page, **oa_params(mailto)}
    r = get_with_retry(url, params=params, headers=headers_json(f"JournalChecker/1.0 ({mailto})" if mailto else "JournalChecker/1.0"), retries=1)
    r.raise_for_status()
    data = safe_json(r) or {}
    return data.get("results", []) or []


@st.cache_data(show_spinner=False, ttl=7 * 24 * 3600)
def openalex_org_by_id(openalex_org_id: str, mailto: str) -> Optional[Dict[str, Any]]:
    if not openalex_org_id:
        return None
    api_url = to_api_openalex_url(openalex_org_id)
    if not api_url:
        return None
    r = get_with_retry(api_url, params={**oa_params(mailto)}, headers=headers_json(f"JournalChecker/1.0 ({mailto})" if mailto else "JournalChecker/1.0"), retries=1)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return safe_json(r)


@st.cache_data(show_spinner=False, ttl=30 * 24 * 3600)
def openalex_concept_by_id(concept_id: str, mailto: str) -> Optional[Dict[str, Any]]:
    if not concept_id:
        return None
    url = to_api_openalex_url(concept_id)
    if not url:
        return None
    r = get_with_retry(url, params={**oa_params(mailto)}, headers=headers_json(f"JournalChecker/1.0 ({mailto})" if mailto else "JournalChecker/1.0"), retries=1)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return safe_json(r)


def get_counts_2024_2025(source_obj: dict) -> Tuple[Optional[int], Optional[int]]:
    c24 = None
    c25 = None
    if not source_obj:
        return c24, c25
    for item in (source_obj.get("counts_by_year") or []):
        if item.get("year") == 2024:
            c24 = item.get("works_count")
        if item.get("year") == 2025:
            c25 = item.get("works_count")
    return c24, c25


def extract_openalex_issns(source_obj: dict) -> List[str]:
    issns = []
    for x in (source_obj.get("issn") or []):
        n = norm_issn(x)
        if n:
            issns.append(n)
    issn_l = norm_issn(source_obj.get("issn_l") or "")
    if issn_l:
        issns.append(issn_l)
    # unique
    out, seen = [], set()
    for i in issns:
        if i not in seen:
            seen.add(i)
            out.append(i)
    return out


def openalex_subject_buckets(source_obj: dict, mailto: str, topn_concepts: int = 8, topn_buckets: int = 4) -> Tuple[str, str]:
    """
    Return (bucket_en, bucket_debug)
    - bucket_en: "Medicine(0.62); Engineering(0.21)..."
    - bucket_debug: extra debug of which concepts used
    """
    xs = source_obj.get("x_concepts") or []
    if not xs:
        return "", ""
    xs = sorted(xs, key=lambda x: x.get("score", 0.0), reverse=True)[:topn_concepts]

    bucket_scores: Dict[str, float] = {}
    debug_parts = []
    for x in xs:
        cid = x.get("id") or ""
        score = float(x.get("score") or 0.0)
        cname = x.get("display_name") or ""
        if not cid:
            continue
        cobj = openalex_concept_by_id(cid, mailto) or {}
        # prefer top-level ancestor (level 0) if present
        ancestors = cobj.get("ancestors") or []
        level0 = None
        level1 = None
        for a in ancestors:
            if a.get("level") == 0 and not level0:
                level0 = a.get("display_name")
            if a.get("level") == 1 and not level1:
                level1 = a.get("display_name")
        bucket = level0 or level1 or cname
        if bucket:
            bucket_scores[bucket] = bucket_scores.get(bucket, 0.0) + score
        debug_parts.append(f"{cname}:{score:.2f}->{bucket}")

    if not bucket_scores:
        return "", "; ".join(debug_parts)

    ranked = sorted(bucket_scores.items(), key=lambda kv: kv[1], reverse=True)[:topn_buckets]
    bucket_en = "; ".join([f"{k}({v:.2f})" for k, v in ranked])
    return bucket_en, "; ".join(debug_parts)


# =========================
# Wikidata: zh title / aliases / official website (P856) by ISSN
# =========================
@st.cache_data(show_spinner=False, ttl=14 * 24 * 3600)
def wikidata_by_issn(issn: str) -> Dict[str, str]:
    issn = norm_issn(issn)
    if not issn:
        return {"zh_label": "", "zh_alias": "", "official_website": "", "item": ""}

    query = f"""
    SELECT ?item ?labelZH (GROUP_CONCAT(DISTINCT ?aliasZH; separator="; ") AS ?aliasesZH) ?officialWebsite WHERE {{
      ?item wdt:P236 "{issn}".
      OPTIONAL {{ ?item rdfs:label ?labelZH FILTER (lang(?labelZH) = "zh"). }}
      OPTIONAL {{ ?item skos:altLabel ?aliasZH FILTER (lang(?aliasZH) = "zh"). }}
      OPTIONAL {{ ?item wdt:P856 ?officialWebsite. }}
    }}
    GROUP BY ?item ?labelZH ?officialWebsite
    LIMIT 1
    """
    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": "JournalChecker/1.0",
    }
    r = get_with_retry(WIKIDATA_SPARQL, params={"query": query}, headers=headers, timeout=25, retries=1)
    r.raise_for_status()
    data = safe_json(r) or {}
    bindings = (data.get("results", {}).get("bindings", []) or [])
    if not bindings:
        return {"zh_label": "", "zh_alias": "", "official_website": "", "item": ""}

    b = bindings[0]
    return {
        "zh_label": (b.get("labelZH", {}) or {}).get("value", "") or "",
        "zh_alias": (b.get("aliasesZH", {}) or {}).get("value", "") or "",
        "official_website": (b.get("officialWebsite", {}) or {}).get("value", "") or "",
        "item": (b.get("item", {}) or {}).get("value", "") or "",
    }


# =========================
# Crossref: infer journal info from works endpoint
# =========================
@st.cache_data(show_spinner=False, ttl=24 * 3600)
def crossref_works_by_issn(issn: str, mailto: str, rows: int = 20) -> List[Dict[str, Any]]:
    issn = norm_issn(issn)
    if not issn:
        return []
    url = f"{CROSSREF_BASE}/works"
    params = {
        "filter": f"issn:{issn}",
        "rows": rows,
        "mailto": mailto or None,
    }
    # remove None
    params = {k: v for k, v in params.items() if v is not None}
    r = get_with_retry(url, params=params, headers=headers_json("JournalChecker/1.0"), retries=1)
    r.raise_for_status()
    data = safe_json(r) or {}
    msg = data.get("message", {}) or {}
    return msg.get("items", []) or []


@st.cache_data(show_spinner=False, ttl=24 * 3600)
def crossref_works_by_title(title: str, mailto: str, rows: int = 20) -> List[Dict[str, Any]]:
    t = clean_title(title)
    if not t:
        return []
    url = f"{CROSSREF_BASE}/works"
    params = {
        "query.container-title": t,
        "rows": rows,
        "mailto": mailto or None,
    }
    params = {k: v for k, v in params.items() if v is not None}
    r = get_with_retry(url, params=params, headers=headers_json("JournalChecker/1.0"), retries=1)
    r.raise_for_status()
    data = safe_json(r) or {}
    msg = data.get("message", {}) or {}
    return msg.get("items", []) or []


def crossref_summarize(items: List[Dict[str, Any]], query_title: str = "", query_issn: str = "") -> Dict[str, Any]:
    """
    Summarize Crossref works items to a pseudo-journal record:
    - container-title (best match)
    - publisher (most frequent)
    - issn list (from items)
    - url (best candidate if present)
    """
    if not items:
        return {}

    # publisher mode
    pubs = {}
    for it in items:
        p = (it.get("publisher") or "").strip()
        if p:
            pubs[p] = pubs.get(p, 0) + 1
    publisher = ""
    if pubs:
        publisher = sorted(pubs.items(), key=lambda kv: kv[1], reverse=True)[0][0]

    # container title candidates
    titles = []
    for it in items:
        ct = it.get("container-title") or []
        if isinstance(ct, list) and ct:
            titles.append(ct[0])
        elif isinstance(ct, str) and ct:
            titles.append(ct)
    titles = [t for t in titles if t and t.strip()]
    best_title = titles[0] if titles else ""

    if query_title and titles:
        # pick best by similarity
        best_title = sorted(titles, key=lambda t: best_similarity(query_title, t), reverse=True)[0]

    # issn from items
    issn_set = []
    for it in items:
        for x in (it.get("ISSN") or []):
            n = norm_issn(x)
            if n:
                issn_set.append(n)
    # unique
    seen = set()
    issn_list = []
    for i in issn_set:
        if i not in seen:
            seen.add(i)
            issn_list.append(i)

    # URL candidate: try resource.primary.URL if exists; else it.get("URL")
    url_candidates = []
    for it in items:
        u = it.get("URL") or ""
        if u:
            url_candidates.append(u)
        res = it.get("resource") or {}
        if isinstance(res, dict):
            pri = res.get("primary") or {}
            if isinstance(pri, dict):
                uu = pri.get("URL") or ""
                if uu:
                    url_candidates.append(uu)

    url_best = url_candidates[0] if url_candidates else ""

    return {
        "title": best_title,
        "publisher": publisher,
        "issn_list": issn_list,
        "url": url_best,
        "evidence": "Crossref:/works",
    }


# =========================
# DOAJ: journal search
# =========================
@st.cache_data(show_spinner=False, ttl=24 * 3600)
def doaj_search_journals(lucene_q: str, api_key: str = "", page_size: int = 20) -> List[Dict[str, Any]]:
    """
    DOAJ v2 search endpoint:
      GET /api/v2/search/journals/{query}
    """
    q = (lucene_q or "").strip()
    if not q:
        return []
    url = f"{DOAJ_BASE_V2}/search/journals/{requests.utils.quote(q, safe='')}"
    headers = {"Accept": "application/json", "User-Agent": "JournalChecker/1.0"}
    # DOAJ may use API keys for higher rate; keep optional
    if api_key.strip():
        headers["Authorization"] = f"Bearer {api_key.strip()}"
    r = get_with_retry(url, params={"pageSize": page_size}, headers=headers, timeout=25, retries=1)
    if r.status_code == 404:
        return []
    r.raise_for_status()
    data = safe_json(r) or {}
    return data.get("results", []) or []


def doaj_pick_best(results: List[Dict[str, Any]], query_title: str = "", query_issn: str = "") -> Tuple[Optional[Dict[str, Any]], float]:
    """
    DOAJ result schema: each result has bibjson.title, bibjson.link, bibjson.issn, bibjson.publisher, bibjson.subject
    """
    def title_getter(x):
        bj = (x.get("bibjson") or {})
        return bj.get("title") or ""

    best, score = pick_best_by_title(results, query_title, title_getter)
    return best, score


def doaj_extract_fields(best: Dict[str, Any]) -> Dict[str, Any]:
    if not best:
        return {}
    bj = best.get("bibjson") or {}
    title = bj.get("title") or ""
    publisher = bj.get("publisher") or ""
    issns = []
    for x in (bj.get("issn") or []):
        n = norm_issn(x)
        if n:
            issns.append(n)
    links = bj.get("link") or []
    homepage = ""
    for l in links:
        if isinstance(l, dict):
            if (l.get("type") or "").lower() in ("homepage", "journal", "url"):
                homepage = homepage or (l.get("url") or "")
    # subjects
    subjects = []
    for s in (bj.get("subject") or []):
        if isinstance(s, dict):
            term = s.get("term") or ""
            if term:
                subjects.append(term)
    return {
        "title": title,
        "publisher": publisher,
        "issn_list": issns,
        "homepage": homepage,
        "subjects": subjects,
        "evidence": best.get("id") or "DOAJ",
    }


# =========================
# Optional NLM Catalog via E-utilities
# =========================
@st.cache_data(show_spinner=False, ttl=14 * 24 * 3600)
def nlm_search(term: str, api_key: str = "", retmax: int = 5) -> List[str]:
    if not term.strip():
        return []
    url = f"{NCBI_EUTILS}/esearch.fcgi"
    params = {
        "db": "nlmcatalog",
        "term": term,
        "retmode": "json",
        "retmax": retmax,
    }
    if api_key.strip():
        params["api_key"] = api_key.strip()
    r = get_with_retry(url, params=params, headers=headers_json("JournalChecker/1.0"), timeout=25, retries=1)
    r.raise_for_status()
    data = safe_json(r) or {}
    ids = (((data.get("esearchresult") or {}).get("idlist")) or [])
    return ids


@st.cache_data(show_spinner=False, ttl=14 * 24 * 3600)
def nlm_summary(ids: List[str], api_key: str = "") -> Dict[str, Any]:
    if not ids:
        return {}
    url = f"{NCBI_EUTILS}/esummary.fcgi"
    params = {
        "db": "nlmcatalog",
        "id": ",".join(ids),
        "retmode": "json",
    }
    if api_key.strip():
        params["api_key"] = api_key.strip()
    r = get_with_retry(url, params=params, headers=headers_json("JournalChecker/1.0"), timeout=25, retries=1)
    r.raise_for_status()
    data = safe_json(r) or {}
    # pick the first docsum
    result = data.get("result") or {}
    uids = result.get("uids") or []
    if not uids:
        return {}
    first = result.get(uids[0]) or {}
    return first


def nlm_extract(nlm_obj: Dict[str, Any]) -> Dict[str, Any]:
    if not nlm_obj:
        return {}
    title = nlm_obj.get("titlemain") or nlm_obj.get("title") or ""
    issn_list = []
    for k in ("issn", "issnlinking", "issnprint", "issnonline"):
        v = nlm_obj.get(k)
        if isinstance(v, str):
            n = norm_issn(v)
            if n:
                issn_list.append(n)
        elif isinstance(v, list):
            for vv in v:
                n = norm_issn(vv)
                if n:
                    issn_list.append(n)
    # unique
    seen, out = set(), []
    for i in issn_list:
        if i not in seen:
            seen.add(i)
            out.append(i)
    uid = nlm_obj.get("uid") or ""
    evidence = f"https://www.ncbi.nlm.nih.gov/nlmcatalog/{uid}/" if uid else ""
    return {
        "title": title,
        "issn_list": out,
        "evidence": evidence,
    }


# =========================
# AI (OpenAI Chat Completions) - optional
# =========================
@st.cache_data(show_spinner=False, ttl=30 * 24 * 3600)
def ai_translate_to_zh(text: str, api_key: str, model: str) -> str:
    text = (text or "").strip()
    if not text or not api_key.strip():
        return ""
    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key.strip()}",
        "Content-Type": "application/json",
    }
    prompt = (
        "把下面的期刊名翻译成中文：\n"
        f"{text}\n\n"
        "要求：若存在常用中文译名优先用常用译名，否则直译；"
        "只输出中文标题本身，不要输出解释、引号或多余内容。"
    )
    body = {
        "model": model.strip() or "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": "你是一个严谨的学术出版中文编辑。"},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.2,
    }
    r = post_with_retry(url, json_body=body, headers=headers, timeout=35, retries=1)
    if r.status_code != 200:
        return ""
    data = safe_json(r) or {}
    try:
        return (data["choices"][0]["message"]["content"] or "").strip()
    except Exception:
        return ""


@st.cache_data(show_spinner=False, ttl=30 * 24 * 3600)
def ai_subject_to_zh(subject_en: str, api_key: str, model: str) -> str:
    subject_en = (subject_en or "").strip()
    if not subject_en or not api_key.strip():
        return ""
    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key.strip()}",
        "Content-Type": "application/json",
    }
    prompt = (
        "把下面这些学科大类名称翻译成中文，保持分号分隔的格式：\n"
        f"{subject_en}\n\n"
        "要求：只输出中文，不要解释。"
    )
    body = {
        "model": model.strip() or "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": "你是一个严谨的学术出版中文编辑。"},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.2,
    }
    r = post_with_retry(url, json_body=body, headers=headers, timeout=35, retries=1)
    if r.status_code != 200:
        return ""
    data = safe_json(r) or {}
    try:
        return (data["choices"][0]["message"]["content"] or "").strip()
    except Exception:
        return ""


# =========================
# Owner classification (keep your original approach but not only ROR)
# =========================
def classify_owner_by_keywords(name: str) -> str:
    n = (name or "").lower()
    if re.search(r"press|publishing|publisher|出版社", n):
        return "出版社"
    if re.search(r"society|association|学会|协会", n):
        return "学会"
    if re.search(r"ministry|government|gov|政府|部|委", n):
        return "政府机构"
    if re.search(r"university|college|academy|institute|大学|学院|研究院|科学院", n):
        return "高校科研机构/事业单位"
    if re.search(r"ltd|inc|gmbh|llc|co\.,|limited|corp|有限|股份", n):
        return "企业"
    return "未知"


# =========================
# Core: multi-source lookup
# =========================
def choose_best_openalex_source(query: str, mailto: str) -> Tuple[Optional[Dict[str, Any]], str, float]:
    """
    Return (best_source, match_mode, score)
    """
    q = clean_title(query)
    if not q:
        return None, "", 0.0

    # candidates
    candidates = []
    mode = ""
    if looks_like_issn(q):
        issn_q = norm_issn(q)
        candidates = openalex_sources_by_issn(issn_q, mailto, per_page=50)
        mode = f"openalex:issn:{issn_q}"
        # if none, fallback to search
        if not candidates:
            candidates = openalex_sources_search(q, mailto, per_page=50)
            mode = f"openalex:search:{q}"
    else:
        candidates = openalex_sources_search(q, mailto, per_page=50)
        mode = f"openalex:search:{q}"

    def title_getter(x):
        return x.get("display_name") or ""

    best, score = pick_best_by_title(candidates, q if not looks_like_issn(q) else "", title_getter)

    # If query is ISSN, prefer exact ISSN match among candidates
    if looks_like_issn(q) and candidates:
        issn_q = norm_issn(q)
        exact = []
        for c in candidates:
            issns = extract_openalex_issns(c)
            if issn_q in issns:
                exact.append(c)
        if exact:
            # pick the one with higher works_count in recent year as tiebreak
            exact_sorted = sorted(exact, key=lambda x: (x.get("works_count") or 0), reverse=True)
            best = exact_sorted[0]
            score = 100.0

    return best, mode, score


def lookup_one(q: str, mailto: str, doaj_key: str, use_nlm: bool, ncbi_key: str,
               openai_key: str, openai_model: str, ai_enable: bool, ai_subject_zh: bool) -> Dict[str, Any]:
    q = clean_title(q)
    if not q:
        return {}

    evidence_urls = []
    errors = []

    # ---- OpenAlex best source
    oa_source, oa_mode, oa_score = None, "", 0.0
    try:
        oa_source, oa_mode, oa_score = choose_best_openalex_source(q, mailto)
    except Exception as e:
        errors.append(f"OpenAlex error: {e}")

    # ---- Crossref
    cr_summary = {}
    try:
        if looks_like_issn(q):
            items = crossref_works_by_issn(q, mailto, rows=20)
            cr_summary = crossref_summarize(items, query_issn=norm_issn(q))
        else:
            items = crossref_works_by_title(q, mailto, rows=20)
            cr_summary = crossref_summarize(items, query_title=q)
        if cr_summary:
            evidence_urls.append("https://api.crossref.org/works")
    except Exception as e:
        errors.append(f"Crossref error: {e}")

    # ---- DOAJ
    doaj_best = None
    doaj_score = 0.0
    doaj_fields = {}
    try:
        if looks_like_issn(q):
            issn_q = norm_issn(q)
            results = doaj_search_journals(f'issn:"{issn_q}"', api_key=doaj_key, page_size=20)
            doaj_best, doaj_score = doaj_pick_best(results, query_title="", query_issn=issn_q)
        else:
            # title query (quoted improves precision)
            results = doaj_search_journals(f'bibjson.title:"{q}"', api_key=doaj_key, page_size=20)
            doaj_best, doaj_score = doaj_pick_best(results, query_title=q, query_issn="")
        if doaj_best:
            doaj_fields = doaj_extract_fields(doaj_best)
            evidence_urls.append("https://doaj.org/api/v2")
    except Exception as e:
        errors.append(f"DOAJ error: {e}")

    # ---- Optional NLM (good for biomedical)
    nlm_fields = {}
    try:
        if use_nlm:
            term = norm_issn(q) if looks_like_issn(q) else q
            ids = nlm_search(term, api_key=ncbi_key, retmax=5)
            nlm_obj = nlm_summary(ids, api_key=ncbi_key) if ids else {}
            nlm_fields = nlm_extract(nlm_obj) if nlm_obj else {}
            if nlm_fields.get("evidence"):
                evidence_urls.append(nlm_fields["evidence"])
    except Exception as e:
        errors.append(f"NLM error: {e}")

    # ---- Decide primary identity fields (title/issn/publisher/homepage)
    # Title priority: OpenAlex > DOAJ > Crossref > NLM
    oa_title = (oa_source or {}).get("display_name", "") if oa_source else ""
    title_en = first_nonempty(oa_title, doaj_fields.get("title", ""), cr_summary.get("title", ""), nlm_fields.get("title", ""))

    # ISSN-L
    issn_l = norm_issn((oa_source or {}).get("issn_l", "") if oa_source else "")
    # ISSNs merged
    issn_list = []
    if oa_source:
        issn_list.extend(extract_openalex_issns(oa_source))
    issn_list.extend(doaj_fields.get("issn_list", []) or [])
    issn_list.extend(cr_summary.get("issn_list", []) or [])
    issn_list.extend(nlm_fields.get("issn_list", []) or [])
    # unique
    seen = set()
    issn_list_u = []
    for i in issn_list:
        ni = norm_issn(i)
        if ni and ni not in seen:
            seen.add(ni)
            issn_list_u.append(ni)

    # Publisher/Owner guess: OpenAlex host org name/publisher > DOAJ publisher > Crossref publisher
    oa_host = (oa_source or {}).get("host_organization_name", "") if oa_source else ""
    oa_pub = (oa_source or {}).get("publisher", "") if oa_source else ""
    legal_owner = first_nonempty(oa_host, oa_pub, doaj_fields.get("publisher", ""), cr_summary.get("publisher", ""))

    owner_type = classify_owner_by_keywords(legal_owner)

    # Homepage priority: OpenAlex homepage_url > Wikidata P856 (by ISSN-L if possible) > DOAJ homepage > Crossref url
    oa_home = (oa_source or {}).get("homepage_url", "") if oa_source else ""
    wd = {"zh_label": "", "zh_alias": "", "official_website": "", "item": ""}
    try:
        issn_for_wd = issn_l or (issn_list_u[0] if issn_list_u else "")
        if issn_for_wd:
            wd = wikidata_by_issn(issn_for_wd)
            if wd.get("item"):
                evidence_urls.append(wd["item"])
    except Exception as e:
        errors.append(f"Wikidata error: {e}")

    homepage = first_nonempty(oa_home, wd.get("official_website", ""), doaj_fields.get("homepage", ""), cr_summary.get("url", ""))

    # Pub counts (OpenAlex counts_by_year)
    pub2024 = ""
    pub2025 = ""
    if oa_source:
        c24, c25 = get_counts_2024_2025(oa_source)
        pub2024 = "" if c24 is None else int(c24)
        pub2025 = "" if c25 is None else int(c25)

    # Subjects: OpenAlex concept buckets (EN) + optional AI to zh
    subject_en = ""
    subject_debug = ""
    if oa_source:
        subject_en, subject_debug = openalex_subject_buckets(oa_source, mailto, topn_concepts=8, topn_buckets=4)

    subject_zh = ""
    if ai_enable and ai_subject_zh and subject_en:
        try:
            subject_zh = ai_subject_to_zh(subject_en, openai_key, openai_model)
        except Exception as e:
            errors.append(f"AI subject translate error: {e}")

    # Chinese title: Wikidata zh label/alias > AI translate title_en
    cn_title = first_nonempty(wd.get("zh_label", ""), wd.get("zh_alias", ""))
    if not cn_title and ai_enable and title_en:
        try:
            cn_title = ai_translate_to_zh(title_en, openai_key, openai_model)
        except Exception as e:
            errors.append(f"AI title translate error: {e}")

    # Evidence URLs
    oa_id = (oa_source or {}).get("id", "") if oa_source else ""
    oa_api_id = to_api_openalex_url(oa_id) if oa_id else ""
    if oa_api_id:
        evidence_urls.append(oa_api_id)

    # Match quality / trace
    found_any = bool(oa_source or doaj_best or cr_summary or nlm_fields)
    match_trace = f"{oa_mode} (score={oa_score:.1f})"
    if doaj_best:
        match_trace += f" | doaj(score={doaj_score:.1f})"
    if cr_summary:
        match_trace += " | crossref(works)"
    if nlm_fields:
        match_trace += " | nlm"

    return {
        "Query": q,
        "Found_Any": "Yes" if found_any else "No",

        "Journal_Title(EN)": title_en,
        "Chinese_Title": cn_title,

        "ISSN_L(OpenAlex)": issn_l,
        "ISSNs(Merged)": "; ".join(issn_list_u),

        "Homepage(Combined)": homepage,

        "PubCount_2024(OpenAlex)": pub2024,
        "PubCount_2025(OpenAlex)": pub2025,

        "Subject_Buckets_EN(OpenAlex)": subject_en,
        "Subject_Buckets_ZH(AI_optional)": subject_zh,

        "LegalOwner(Heuristic)": legal_owner,
        "Owner_Type(Heuristic)": owner_type,

        "Match_Trace": match_trace,
        "Evidence_URLs": uniq_urls(evidence_urls),
        "Debug_Subject_Concept_Path": subject_debug,

        "Error": " | ".join(errors) if errors else "",
    }


# =========================
# UI
# =========================
st.title("期刊信息查询（多源融合：OpenAlex + Wikidata + Crossref + DOAJ + 可选NLM + 可选AI）")
st.caption("不依赖搜索引擎 API。通过多数据源候选+相似度重排，提高命中率；官网/中文名/领域支持补全与可选 AI 兜底。")

with st.sidebar:
    st.subheader("参数")
    mailto = st.text_input("OpenAlex/Crossref mailto（建议填邮箱）", value="")
    st.divider()

    st.markdown("### DOAJ（可选）")
    doaj_key = st.text_input("DOAJ API Key（没有也可用，但可能更容易限流）", value="", type="password")

    st.divider()
    st.markdown("### NLM Catalog（可选，医学期刊更强）")
    use_nlm = st.checkbox("启用 NLM Catalog 查询", value=False)
    ncbi_key = st.text_input("NCBI E-utilities API Key（可选）", value="", type="password")

    st.divider()
    st.markdown("### AI（可选，用于中文名/领域中文化）")
    ai_enable = st.checkbox("启用 AI 翻译兜底", value=False)
    openai_key = st.text_input("OPENAI_API_KEY", value="", type="password")
    openai_model = st.text_input("OpenAI model（默认 gpt-4o-mini，可改）", value="gpt-4o-mini")
    ai_subject_zh = st.checkbox("AI 输出领域中文（Subject_Buckets_ZH）", value=True)

    st.divider()
    st.markdown("**输入格式**：一行一个期刊名或 ISSN。")

st.subheader("批量查询")
queries = st.text_area(
    "期刊名/ISSN（每行一个）",
    height=220,
    placeholder="例如：\nNature\nScience\n1533-4880\nThe Lancet",
)

run_btn = st.button("开始查询", type="primary")

if run_btn:
    q_list = [x.strip() for x in (queries or "").splitlines() if x.strip()]
    if not q_list:
        st.warning("请至少输入一个期刊名或 ISSN。")
        st.stop()

    if ai_enable and not openai_key.strip():
        st.warning("你勾选了 AI 翻译兜底，但没有填 OPENAI_API_KEY。将自动跳过 AI 相关功能。")
        ai_enable_effective = False
    else:
        ai_enable_effective = ai_enable

    results: List[Dict[str, Any]] = []
    prog = st.progress(0)

    for i, q in enumerate(q_list, start=1):
        results.append(
            lookup_one(
                q,
                mailto=mailto.strip(),
                doaj_key=doaj_key.strip(),
                use_nlm=use_nlm,
                ncbi_key=ncbi_key.strip(),
                openai_key=openai_key.strip(),
                openai_model=openai_model.strip(),
                ai_enable=ai_enable_effective,
                ai_subject_zh=ai_subject_zh,
            )
        )
        prog.progress(i / len(q_list))

    out = pd.DataFrame(results)

    st.markdown("## 查询结果")
    st.dataframe(out, use_container_width=True)

    st.markdown("## 导出")
    st.download_button(
        "下载结果 Excel",
        data=to_excel_bytes(out),
        file_name="journal_results_enhanced.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
