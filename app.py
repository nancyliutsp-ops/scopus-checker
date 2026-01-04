# app.py
import os
import re
from io import BytesIO
from typing import Optional, Dict, Any, Tuple, List

import pandas as pd
import requests
import streamlit as st

st.set_page_config(page_title="Journal Checker (Scopus + OpenAlex + ROR + Wikidata)", layout="wide")

OPENALEX_BASE = "https://api.openalex.org"
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"


# =========================
# Helpers: normalize
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


def norm_title(x: str) -> str:
    if x is None:
        return ""
    s = str(x).strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def looks_like_issn(s: str) -> bool:
    return bool(norm_issn(s))


def to_excel_bytes(df: pd.DataFrame) -> bytes:
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Result")
    return bio.getvalue()


# =========================
# Scopus index: load once
# =========================
@st.cache_data(show_spinner=False, ttl=7 * 24 * 3600)
def load_scopus_index(path: str, mtime: float) -> pd.DataFrame:
    if path.lower().endswith((".xlsx", ".xls")):
        df = pd.read_excel(path, engine="openpyxl")
    else:
        df = pd.read_csv(path)


    required = ["Source Title", "Active or Inactive", "ISSN", "EISSN"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Scopus Source List 缺少列：{missing}。需要：{required}")

    df["_title_norm"] = df["Source Title"].map(norm_title)
    df["_issn_norm"] = df["ISSN"].map(norm_issn)
    df["_eissn_norm"] = df["EISSN"].map(norm_issn)
    df["_status_norm"] = df["Active or Inactive"].astype(str).str.strip().str.lower()

    df = df[(df["_title_norm"] != "") | (df["_issn_norm"] != "") | (df["_eissn_norm"] != "")]
    return df


def match_scopus(df: pd.DataFrame, title_or_issn: str, issn: str = "", eissn: str = "") -> Tuple[pd.DataFrame, Dict[str, str]]:
    """
    Priority:
    1) exact match by ISSN/EISSN if any provided or query looks like ISSN
    2) exact title
    3) title contains
    """
    title_q = norm_title(title_or_issn)
    issn_q = norm_issn(issn) if issn else ""
    eissn_q = norm_issn(eissn) if eissn else ""

    if not issn_q and not eissn_q:
        maybe = norm_issn(title_or_issn)
        if maybe:
            issn_q = maybe

    if issn_q or eissn_q:
        cond = False
        if issn_q:
            cond = cond | (df["_issn_norm"] == issn_q) | (df["_eissn_norm"] == issn_q)
        if eissn_q:
            cond = cond | (df["_issn_norm"] == eissn_q) | (df["_eissn_norm"] == eissn_q)
        hits = df[cond].copy()
        return hits, {"mode": "ISSN/EISSN exact", "issn": issn_q, "eissn": eissn_q, "title": title_q}

    if title_q:
        exact = df[df["_title_norm"] == title_q].copy()
        if not exact.empty:
            return exact, {"mode": "Title exact", "issn": "", "eissn": "", "title": title_q}

        fuzzy = df[df["_title_norm"].str.contains(title_q, na=False)].copy()
        return fuzzy, {"mode": "Title contains", "issn": "", "eissn": "", "title": title_q}

    return pd.DataFrame(), {"mode": "No query", "issn": "", "eissn": "", "title": ""}


def pick_best_scopus_row(hits: pd.DataFrame) -> Optional[pd.Series]:
    if hits is None or hits.empty:
        return None
    tmp = hits.copy()
    tmp["_is_active"] = tmp["_status_norm"].eq("active")
    tmp = tmp.sort_values(by=["_is_active"], ascending=False)
    return tmp.iloc[0]


# =========================
# OpenAlex
# =========================
def oa_params(mailto: str) -> Dict[str, str]:
    return {"mailto": mailto} if mailto else {}


@st.cache_data(show_spinner=False, ttl=24 * 3600)
def openalex_source_by_issn(issn: str, mailto: str) -> Optional[Dict[str, Any]]:
    issn = norm_issn(issn)
    if not issn:
        return None
    url = f"{OPENALEX_BASE}/sources"
    params = {"filter": f"issn:{issn}", "per-page": 5, **oa_params(mailto)}
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    results = r.json().get("results", []) or []
    return results[0] if results else None


@st.cache_data(show_spinner=False, ttl=24 * 3600)
def openalex_source_by_title(title: str, mailto: str) -> Optional[Dict[str, Any]]:
    q = (title or "").strip()
    if not q:
        return None
    url = f"{OPENALEX_BASE}/sources"
    params = {"search": q, "per-page": 10, **oa_params(mailto)}
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    results = r.json().get("results", []) or []
    return results[0] if results else None


@st.cache_data(show_spinner=False, ttl=7 * 24 * 3600)
def openalex_org_by_id(openalex_org_id: str, mailto: str) -> Optional[Dict[str, Any]]:
    if not openalex_org_id:
        return None
    url = openalex_org_id  # already like https://openalex.org/Ixxxx
    r = requests.get(url, params={**oa_params(mailto)}, timeout=20)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()


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
            if isinstance(score, (int, float)):
                out.append(f"{name}({score:.2f})")
            else:
                out.append(name)
    return "; ".join(out)


# =========================
# ROR
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
    r = requests.get(api_url, timeout=20)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()


def classify_owner(ror_json: dict, fallback_name: str = "") -> str:
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

    return keyword_refine(name)


# =========================
# Wikidata: Chinese title by ISSN
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
    bindings = (r.json().get("results", {}).get("bindings", []) or [])
    if not bindings:
        return ""
    return bindings[0].get("labelZH", {}).get("value", "") or ""


# =========================
# Core: single lookup
# =========================
def lookup_one(q: str, df_scopus: pd.DataFrame, mailto: str) -> Dict[str, Any]:
    q = (q or "").strip()
    if not q:
        return {}

    # --- Scopus ---
    sc_hits, sc_meta = match_scopus(df_scopus, q)
    sc_row = pick_best_scopus_row(sc_hits)
    scopus_covered = "Yes" if sc_row is not None else "No"
    scopus_status = (str(sc_row["Active or Inactive"]).strip() if sc_row is not None else "Unknown")
    scopus_title = (str(sc_row["Source Title"]).strip() if sc_row is not None else "")
    scopus_issn = (str(sc_row["ISSN"]).strip() if sc_row is not None else "")
    scopus_eissn = (str(sc_row["EISSN"]).strip() if sc_row is not None else "")
    sc_hit_count = int(len(sc_hits)) if sc_hits is not None else 0

    # --- OpenAlex match keys ---
    issn_for_oa = norm_issn(scopus_issn) or (norm_issn(q) if looks_like_issn(q) else "")
    eissn_for_oa = norm_issn(scopus_eissn)
    title_for_oa = scopus_title or q

    source_obj = None
    oa_match_mode = ""

    try:
        if issn_for_oa:
            source_obj = openalex_source_by_issn(issn_for_oa, mailto)
            oa_match_mode = f"issn:{issn_for_oa}"
        if (not source_obj) and eissn_for_oa:
            source_obj = openalex_source_by_issn(eissn_for_oa, mailto)
            oa_match_mode = f"issn:{eissn_for_oa}"
        if (not source_obj) and title_for_oa:
            source_obj = openalex_source_by_title(title_for_oa, mailto)
            oa_match_mode = f"search:{title_for_oa}"
    except Exception as e:
        return {
            "Query": q,
            "Scopus_Covered": scopus_covered,
            "Scopus_Active_or_Inactive": scopus_status,
            "Scopus_Matched_SourceTitle": scopus_title,
            "Scopus_Matched_ISSN": scopus_issn,
            "Scopus_Matched_EISSN": scopus_eissn,
            "Scopus_Hit_Count": sc_hit_count,
            "Scopus_Match_Mode": sc_meta.get("mode", ""),
            "OpenAlex_Found": "No",
            "OpenAlex_Match_Mode": oa_match_mode,
            "OpenAlex_ID": "",
            "Website": "",
            "PubCount_2024": "",
            "PubCount_2025": "",
            "Chinese_Title": "",
            "Fields(OpenAlex_x_concepts)": "",
            "LegalOwner": "",
            "Owner_Type": "未知",
            "Evidence_URL": "",
            "Error": f"OpenAlex 查询失败：{e}",
        }

    # --- Extract ---
    oa_id = ""
    website = ""
    pub2024 = ""
    pub2025 = ""
    fields = ""
    legal_owner = ""
    owner_type = "未知"
    evidence_url = ""
    cn_title = ""

    if source_obj:
        oa_id = source_obj.get("id", "") or ""
        evidence_url = oa_id
        website = source_obj.get("homepage_url", "") or ""

        c24, c25 = get_counts_2024_2025(source_obj)
        pub2024 = "" if c24 is None else int(c24)
        pub2025 = "" if c25 is None else int(c25)

        fields = get_fields_from_openalex(source_obj, topn=5)

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

        # Chinese title via ISSN
        issn_for_wd = issn_for_oa or norm_issn(source_obj.get("issn_l", "") or "")
        if issn_for_wd:
            try:
                cn_title = wikidata_zh_title_by_issn(issn_for_wd) or ""
            except Exception:
                cn_title = ""

    return {
        "Query": q,

        "Scopus_Covered": scopus_covered,
        "Scopus_Active_or_Inactive": scopus_status,
        "Scopus_Matched_SourceTitle": scopus_title,
        "Scopus_Matched_ISSN": scopus_issn,
        "Scopus_Matched_EISSN": scopus_eissn,
        "Scopus_Hit_Count": sc_hit_count,
        "Scopus_Match_Mode": sc_meta.get("mode", ""),

        "OpenAlex_Found": "Yes" if source_obj else "No",
        "OpenAlex_Match_Mode": oa_match_mode,
        "OpenAlex_ID": oa_id,
        "Website": website,

        "PubCount_2024": pub2024,
        "PubCount_2025": pub2025,

        "Chinese_Title": cn_title,
        "Fields(OpenAlex_x_concepts)": fields,

        "LegalOwner": legal_owner,
        "Owner_Type": owner_type,
        "Evidence_URL": evidence_url,
        "Error": "",
    }


# =========================
# UI
# =========================
st.title("期刊信息查询（Scopus + OpenAlex + ROR + Wikidata）")
st.caption("输入期刊名/ISSN（支持批量），返回 Scopus 收录状态、2024-2025 发文量、中文刊名、领域、legal owner 与定性。")

with st.sidebar:
    st.subheader("配置")
    scopus_path = st.text_input("Scopus Source List 路径", value="data/scopus_sources.xlsx")
    mailto = st.text_input("OpenAlex mailto（建议填邮箱提升稳定性）", value="")
    st.divider()
    st.markdown("**输入格式**：一行一个期刊名或 ISSN。")

# load scopus at startup
if not os.path.exists(scopus_path):
    st.error(f"找不到 Scopus Source List 文件：{scopus_path}\n\n请把 scopus_sources.xlsx 放到 data/ 目录，或在侧边栏填正确路径。")
    st.stop()

mtime = os.path.getmtime(scopus_path)
try:
    df_scopus = load_scopus_index(scopus_path, mtime)
except Exception as e:
    st.error(f"加载 Scopus Source List 失败：{e}")
    st.stop()

st.sidebar.success(f"Scopus Source List 已加载：{len(df_scopus):,} 行")

st.subheader("批量查询")
queries = st.text_area(
    "期刊名/ISSN（每行一个）",
    height=180,
    placeholder="例如：\nNature\nScience\n1533-4880\nThe Lancet",
)

col1, col2, col3 = st.columns([1, 1, 3])
with col1:
    run_btn = st.button("开始查询", type="primary")
with col2:
    show_scopus_hits = st.checkbox("显示 Scopus 命中明细（前50条）", value=False)
with col3:
    st.caption("提示：如果你只查单个，也直接填一行即可。")

if run_btn:
    q_list = [x.strip() for x in (queries or "").splitlines() if x.strip()]
    if not q_list:
        st.warning("请至少输入一个期刊名或 ISSN。")
        st.stop()

    results: List[Dict[str, Any]] = []
    prog = st.progress(0)

    for i, q in enumerate(q_list, start=1):
        row = lookup_one(q, df_scopus=df_scopus, mailto=mailto.strip())
        if row:
            results.append(row)
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

    if show_scopus_hits:
        st.markdown("## Scopus 命中明细（示例：只对第一条查询展示前 50 条）")
        first_q = q_list[0]
        hits, _ = match_scopus(df_scopus, first_q)
        show_cols = ["Source Title", "Active or Inactive", "ISSN", "EISSN"]
        if hits is not None and not hits.empty:
            st.dataframe(hits[show_cols].head(50), use_container_width=True)
        else:
            st.write("无命中。")

