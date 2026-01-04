# app.py  (No Scopus. OpenAlex + ROR + Wikidata only)
import re
from io import BytesIO
from typing import Optional, Dict, Any, Tuple, List

import pandas as pd
import requests
import streamlit as st

st.set_page_config(page_title="Journal Checker (OpenAlex + ROR + Wikidata)", layout="wide")

OPENALEX_BASE = "https://api.openalex.org"
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"


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


def to_excel_bytes(df: pd.DataFrame) -> bytes:
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Result")
    return bio.getvalue()


def oa_params(mailto: str) -> Dict[str, str]:
    return {"mailto": mailto} if mailto else {}


def oa_headers(mailto: str = "") -> Dict[str, str]:
    # Some platforms/providers behave better with explicit Accept + UA
    ua = "JournalChecker/1.0"
    if mailto:
        ua += f" ({mailto})"
    return {
        "Accept": "application/json",
        "User-Agent": ua,
    }


def safe_json(resp: requests.Response) -> Optional[dict]:
    """Return parsed JSON dict, or None if response isn't JSON / cannot be parsed."""
    ctype = (resp.headers.get("Content-Type") or "").lower()
    if "json" not in ctype:
        return None
    try:
        return resp.json()
    except ValueError:
        return None


def to_api_openalex_url(maybe_id_or_url: str) -> str:
    """
    Convert OpenAlex canonical IDs like https://openalex.org/Ixxxxx
    into API endpoint https://api.openalex.org/Ixxxxx.
    If already an API url, keep it.
    """
    s = (maybe_id_or_url or "").strip()
    if not s:
        return ""
    if s.startswith("https://openalex.org/"):
        return s.replace("https://openalex.org/", f"{OPENALEX_BASE}/", 1)
    return s


# =========================
# OpenAlex: source lookup
# =========================
@st.cache_data(show_spinner=False, ttl=24 * 3600)
def openalex_source_by_issn(issn: str, mailto: str) -> Optional[Dict[str, Any]]:
    issn = norm_issn(issn)
    if not issn:
        return None
    url = f"{OPENALEX_BASE}/sources"
    params = {"filter": f"issn:{issn}", "per-page": 5, **oa_params(mailto)}
    r = requests.get(url, params=params, headers=oa_headers(mailto), timeout=20)
    r.raise_for_status()
    data = safe_json(r) or {}
    results = data.get("results", []) or []
    return results[0] if results else None


@st.cache_data(show_spinner=False, ttl=24 * 3600)
def openalex_source_by_title(title: str, mailto: str) -> Optional[Dict[str, Any]]:
    q = (title or "").strip()
    if not q:
        return None
    url = f"{OPENALEX_BASE}/sources"
    params = {"search": q, "per-page": 10, **oa_params(mailto)}
    r = requests.get(url, params=params, headers=oa_headers(mailto), timeout=20)
    r.raise_for_status()
    data = safe_json(r) or {}
    results = data.get("results", []) or []
    return results[0] if results else None


@st.cache_data(show_spinner=False, ttl=7 * 24 * 3600)
def openalex_org_by_id(openalex_org_id: str, mailto: str) -> Optional[Dict[str, Any]]:
    """
    OpenAlex 'host_organization' is typically an OpenAlex canonical ID URL:
      https://openalex.org/I...
    That URL may return HTML. We must call the API endpoint:
      https://api.openalex.org/I...
    """
    if not openalex_org_id:
        return None

    api_url = to_api_openalex_url(openalex_org_id)
    if not api_url:
        return None

    r = requests.get(api_url, params={**oa_params(mailto)}, headers=oa_headers(mailto), timeout=20)
    if r.status_code == 404:
        return None
    r.raise_for_status()

    data = safe_json(r)
    return data


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


def get_fields_from_openalex(source_obj: dict, topn: int = 5) -> str:
    xs = source_obj.get("x_concepts") or []
    if not xs:
        return ""
    xs = sorted(xs, key=lambda x: x.get("score", 0), reverse=True)
    out = []
    for x in xs[:topn]:
        name = x.get("display_name")
        score = x.get("score")
        if name:
            out.append(f"{name}({score:.2f})" if isinstance(score, (int, float)) else name)
    return "; ".join(out)


# =========================
# ROR: owner typing
# =========================
@st.cache_data(show_spinner=False, ttl=7 * 24 * 3600)
def ror_org_by_url(ror_url: str) -> Optional[Dict[str, Any]]:
    if not ror_url:
        return None
    s = ror_url.strip()
    if "ror.org/" not in s:
        return None
    if not s.startswith("http"):
        s = "https://" + s
    api_url = s.replace("https://ror.org/", "https://api.ror.org/organizations/")
    r = requests.get(api_url, timeout=20, headers={"Accept": "application/json", "User-Agent": "JournalChecker/1.0"})
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return safe_json(r)


def classify_owner(ror_json: dict, fallback_name: str = "") -> str:
    """
    Map ROR types -> categories, with keyword refinement.
    """
    def keyword_refine(name: str) -> str:
        n = (name or "").lower()
        if re.search(r"press|publishing|publisher|出版社", n):
            return "出版社"
        if re.search(r"society|association|学会|协会", n):
            return "学会"
        if re.search(r"ltd|inc|gmbh|llc|co\.,|limited|corp|有限|股份", n):
            return "企业"
        if re.search(r"ministry|government|gov|政府|部|委", n):
            return "政府机构"
        if re.search(r"university|college|academy|institute|大学|学院|研究院|科学院", n):
            return "高校科研机构/事业单位"
        return "未知"

    if not ror_json:
        return keyword_refine(fallback_name)

    types = [t.lower() for t in (ror_json.get("types") or [])]
    name = ror_json.get("name", "") or fallback_name

    if "government" in types:
        return "政府机构"
    if any(t in types for t in ["education", "healthcare", "archive", "facility"]):
        return "高校科研机构/事业单位"
    if "company" in types:
        refined = keyword_refine(name)
        return "出版社" if refined == "出版社" else "企业"
    if any(t in types for t in ["nonprofit", "funder"]):
        refined = keyword_refine(name)
        return "学会" if refined == "学会" else "非营利机构"

    refined = keyword_refine(name)
    return refined


# =========================
# Wikidata: Chinese journal title by ISSN
# =========================
@st.cache_data(show_spinner=False, ttl=14 * 24 * 3600)
def wikidata_zh_title_by_issn(issn: str) -> str:
    issn = norm_issn(issn)
    if not issn:
        return ""
    query = f"""
    SELECT ?item ?labelZH WHERE {{
      ?item wdt:P236 "{issn}".
      OPTIONAL {{ ?item rdfs:label ?labelZH FILTER (lang(?labelZH) = "zh"). }}
    }}
    LIMIT 1
    """
    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": "JournalChecker/1.0",
    }
    r = requests.get(WIKIDATA_SPARQL, params={"query": query}, headers=headers, timeout=25)
    r.raise_for_status()
    data = safe_json(r) or {}
    bindings = (data.get("results", {}).get("bindings", []) or [])
    if not bindings:
        return ""
    return bindings[0].get("labelZH", {}).get("value", "") or ""


# =========================
# Core: one journal lookup
# =========================
def lookup_one(q: str, mailto: str) -> Dict[str, Any]:
    q = (q or "").strip()
    if not q:
        return {}

    source_obj = None
    oa_match_mode = ""

    try:
        if looks_like_issn(q):
            issn_q = norm_issn(q)
            source_obj = openalex_source_by_issn(issn_q, mailto)
            oa_match_mode = f"issn:{issn_q}"

        if (not source_obj) and q:
            source_obj = openalex_source_by_title(q, mailto)
            oa_match_mode = f"search:{q}"
    except Exception as e:
        return {
            "Query": q,
            "OpenAlex_Found": "No",
            "OpenAlex_Match_Mode": oa_match_mode,
            "Journal_Title(OpenAlex)": "",
            "ISSN_L(OpenAlex)": "",
            "OpenAlex_ID": "",
            "Website": "",
            "PubCount_2024": "",
            "PubCount_2025": "",
            "Chinese_Title(Wikidata)": "",
            "Fields(OpenAlex_x_concepts)": "",
            "LegalOwner": "",
            "Owner_Type": "未知",
            "Evidence_URL": "",
            "Error": f"OpenAlex 查询失败：{e}",
        }

    # Extract
    oa_id = ""
    display_name = ""
    website = ""
    pub2024 = ""
    pub2025 = ""
    fields = ""
    legal_owner = ""
    owner_type = "未知"
    evidence_url = ""
    cn_title = ""
    issn_l = ""

    if source_obj:
        oa_id = source_obj.get("id", "") or ""
        display_name = source_obj.get("display_name", "") or ""
        website = source_obj.get("homepage_url", "") or ""

        # Evidence URL: prefer API endpoint (more stable for review)
        evidence_url = to_api_openalex_url(oa_id) if oa_id else ""

        c24, c25 = get_counts_2024_2025(source_obj)
        pub2024 = "" if c24 is None else int(c24)
        pub2025 = "" if c25 is None else int(c25)

        fields = get_fields_from_openalex(source_obj, topn=5)

        # legal owner approximation
        host_org_id = source_obj.get("host_organization") or ""
        host_name = source_obj.get("host_organization_name", "") or ""
        publisher = source_obj.get("publisher", "") or ""
        legal_owner = host_name or publisher

        # OpenAlex org -> ROR -> classify
        ror_url = ""
        if host_org_id:
            org_obj = openalex_org_by_id(host_org_id, mailto)
            if org_obj and org_obj.get("ror"):
                ror_url = org_obj.get("ror") or ""
        ror_info = ror_org_by_url(ror_url) if ror_url else None
        owner_type = classify_owner(ror_info, fallback_name=legal_owner)

        # Chinese title via ISSN-L if available
        issn_l = norm_issn(source_obj.get("issn_l", "") or "")
        if issn_l:
            try:
                cn_title = wikidata_zh_title_by_issn(issn_l) or ""
            except Exception:
                cn_title = ""

    return {
        "Query": q,
        "OpenAlex_Found": "Yes" if source_obj else "No",
        "OpenAlex_Match_Mode": oa_match_mode,
        "Journal_Title(OpenAlex)": display_name,
        "ISSN_L(OpenAlex)": issn_l,
        "OpenAlex_ID": oa_id,
        "Website": website,
        "PubCount_2024": pub2024,
        "PubCount_2025": pub2025,
        "Chinese_Title(Wikidata)": cn_title,
        "Fields(OpenAlex_x_concepts)": fields,
        "LegalOwner": legal_owner,
        "Owner_Type": owner_type,
        "Evidence_URL": evidence_url,
        "Error": "",
    }


# =========================
# UI
# =========================
st.title("期刊信息查询（OpenAlex + ROR + Wikidata）")
st.caption("无需 Scopus 文件。输入期刊名或 ISSN（支持批量），返回 2024-2025 发文量、中文刊名、领域、legal owner 与定性。")

with st.sidebar:
    st.subheader("参数")
    mailto = st.text_input("OpenAlex mailto（建议填邮箱，提升稳定性）", value="")
    st.divider()
    st.markdown("**输入格式**：一行一个期刊名或 ISSN。")

st.subheader("批量查询")
queries = st.text_area(
    "期刊名/ISSN（每行一个）",
    height=200,
    placeholder="例如：\nNature\nScience\n1533-4880\nThe Lancet",
)

run_btn = st.button("开始查询", type="primary")

if run_btn:
    q_list = [x.strip() for x in (queries or "").splitlines() if x.strip()]
    if not q_list:
        st.warning("请至少输入一个期刊名或 ISSN。")
        st.stop()

    results: List[Dict[str, Any]] = []
    prog = st.progress(0)

    for i, q in enumerate(q_list, start=1):
        results.append(lookup_one(q, mailto=mailto.strip()))
        prog.progress(i / len(q_list))

    out = pd.DataFrame(results)

    st.markdown("## 查询结果")
    st.dataframe(out, use_container_width=True)

    st.markdown("## 导出")
    st.download_button(
        "下载结果 Excel",
        data=to_excel_bytes(out),
        file_name="journal_results.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
