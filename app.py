import re
from io import BytesIO

import pandas as pd
import streamlit as st
import requests

st.set_page_config(page_title="Journal Checker (Scopus + OpenAlex)", layout="wide")


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


# =========================
# Load Scopus list
# =========================
def load_scopus_df(file) -> pd.DataFrame:
    df = pd.read_excel(file) if file.name.lower().endswith((".xlsx", ".xls")) else pd.read_csv(file)

    required = ["Source Title", "Active or Inactive", "ISSN", "EISSN"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"你的文件缺少列：{missing}。需要包含：{required}")

    df["_title_norm"] = df["Source Title"].map(norm_title)
    df["_issn_norm"] = df["ISSN"].map(norm_issn)
    df["_eissn_norm"] = df["EISSN"].map(norm_issn)
    df["_status_norm"] = df["Active or Inactive"].astype(str).str.strip()
    return df


def match_scopus(df: pd.DataFrame, title_query: str, issn_query: str, eissn_query: str):
    """
    Matching priority:
    1) If ISSN/EISSN provided (or query looks like ISSN), exact match on ISSN/EISSN
    2) Else exact match on Source Title (case-insensitive)
    3) Else fuzzy contains match on Source Title
    """
    title_q = norm_title(title_query)
    issn_q = norm_issn(issn_query) if issn_query else ""
    eissn_q = norm_issn(eissn_query) if eissn_query else ""

    # If user typed ISSN into title box, treat it as ISSN query as well
    if not issn_q and not eissn_q:
        maybe = norm_issn(title_query)
        if maybe:
            issn_q = maybe

    hits = pd.DataFrame()

    # 1) Exact ISSN/EISSN match
    if issn_q or eissn_q:
        cond = False
        if issn_q:
            cond = cond | (df["_issn_norm"] == issn_q) | (df["_eissn_norm"] == issn_q)
        if eissn_q:
            cond = cond | (df["_issn_norm"] == eissn_q) | (df["_eissn_norm"] == eissn_q)
        hits = df[cond].copy()

        # refine by title contains if title given
        if not hits.empty and title_q:
            hits2 = hits[hits["_title_norm"].str.contains(title_q, na=False)].copy()
            if not hits2.empty:
                hits = hits2

        return hits, {"mode": "ISSN/EISSN exact", "issn": issn_q, "eissn": eissn_q, "title": title_q}

    # 2) Exact title match
    if title_q:
        exact = df[df["_title_norm"] == title_q].copy()
        if not exact.empty:
            return exact, {"mode": "Title exact", "issn": "", "eissn": "", "title": title_q}

        # 3) Title contains match
        fuzzy = df[df["_title_norm"].str.contains(title_q, na=False)].copy()
        return fuzzy, {"mode": "Title contains", "issn": "", "eissn": "", "title": title_q}

    return pd.DataFrame(), {"mode": "No query", "issn": "", "eissn": "", "title": ""}


# =========================
# OpenAlex + ROR
# =========================
OPENALEX_BASE = "https://api.openalex.org"

def oa_params(mailto: str):
    return {"mailto": mailto} if mailto else {}


@st.cache_data(show_spinner=False, ttl=24 * 3600)
def openalex_source_by_issn(issn: str, mailto: str):
    issn = norm_issn(issn)
    if not issn:
        return None
    url = f"{OPENALEX_BASE}/sources"
    params = {"filter": f"issn:{issn}", "per-page": 5, **oa_params(mailto)}
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    js = r.json()
    results = js.get("results", [])
    return results[0] if results else None


@st.cache_data(show_spinner=False, ttl=24 * 3600)
def openalex_source_by_title(title: str, mailto: str):
    q = title.strip()
    if not q:
        return None
    url = f"{OPENALEX_BASE}/sources"
    params = {"search": q, "per-page": 5, **oa_params(mailto)}
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    js = r.json()
    results = js.get("results", [])
    return results[0] if results else None


def get_counts_2024_2025(source_obj: dict):
    c24 = None
    c25 = None
    if not source_obj:
        return c24, c25
    for item in source_obj.get("counts_by_year", []) or []:
        if item.get("year") == 2024:
            c24 = item.get("works_count")
        if item.get("year") == 2025:
            c25 = item.get("works_count")
    return c24, c25


@st.cache_data(show_spinner=False, ttl=7 * 24 * 3600)
def ror_org_by_id_or_url(x: str):
    """
    Accepts:
      - https://ror.org/xxxxxxx
      - ror.org/xxxxxxx
      - (sometimes) OpenAlex host_organization id like https://openalex.org/...
        (in that case we can't always derive ROR; will fallback)
    """
    if not x:
        return None

    s = str(x).strip()

    # if already ror.org
    if "ror.org/" in s:
        # normalize
        if not s.startswith("http"):
            s = "https://" + s
        api_url = s.replace("https://ror.org/", "https://api.ror.org/organizations/")
        r = requests.get(api_url, timeout=20)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()

    # can't convert OpenAlex org id to ROR reliably without extra hops
    return None


def classify_owner(ror_json: dict, fallback_name: str = ""):
    """
    Map ROR types -> your categories
    - government -> 政府机构
    - education/healthcare/archive/facility -> 高校科研机构/事业单位
    - company -> 企业（若名称含 publishing/press 则归出版社）
    - nonprofit/funder -> 学会/非营利机构（按名字关键词细分）
    """
    def keyword_refine(name: str):
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
    if "education" in types or "healthcare" in types or "archive" in types or "facility" in types:
        return "高校科研机构/事业单位"
    if "company" in types:
        # treat publisher separately if name indicates publishing
        refined = keyword_refine(name)
        return "出版社" if refined == "出版社" else "企业"
    if "nonprofit" in types or "funder" in types:
        refined = keyword_refine(name)
        return "学会" if refined == "学会" else "非营利机构"

    # other
    refined = keyword_refine(name)
    return refined if refined != "未知" else "未知"


# =========================
# Export
# =========================
def to_excel_bytes(df: pd.DataFrame) -> bytes:
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Result")
    return bio.getvalue()


# =========================
# UI
# =========================
st.title("期刊信息查询（Scopus 辅助 + OpenAlex 核心）")
st.caption("Scopus 用于辅助判断（Active/Inactive、ISSN/EISSN命中），发文量/owner/定性由 OpenAlex+ROR 自动补齐。")

with st.sidebar:
    st.subheader("1) 上传 Scopus 列表")
    sc_file = st.file_uploader("上传 scopus 数据（Excel/CSV）", type=["xlsx", "xls", "csv"])
    st.markdown("要求列名：`Source Title`、`Active or Inactive`、`ISSN`、`EISSN`")

    st.divider()
    st.subheader("2) 查询模式开关")
    assist_only = st.checkbox(
        "Scopus 仅做辅助（即使不在 Scopus 也查 OpenAlex）",
        value=True
    )
    st.caption("关闭时：必须 Scopus 命中才会去查 OpenAlex（作为过滤条件）。")

    st.divider()
    st.subheader("3) OpenAlex 建议参数（可选）")
    mailto = st.text_input("mailto（建议填邮箱，提升 OpenAlex 响应稳定性）", value="")

df_scopus = None
if sc_file is not None:
    try:
        df_scopus = load_scopus_df(sc_file)
        st.success(f"已加载 Scopus 列表：{len(df_scopus):,} 行")
    except Exception as e:
        st.error(str(e))
        st.stop()
else:
    st.info("请先在左侧上传 Scopus 列表文件。")
    st.stop()

st.subheader("输入查询条件")
c1, c2, c3 = st.columns([2, 1, 1])
with c1:
    q_title = st.text_input("Source Title（期刊名，英文全称优先）", "")
with c2:
    q_issn = st.text_input("ISSN（可选）", "")
with c3:
    q_eissn = st.text_input("EISSN（可选）", "")

btn = st.button("查询", type="primary")

if btn:
    # ---- Scopus match (assist) ----
    hits, meta = match_scopus(df_scopus, q_title, q_issn, q_eissn)
    scopus_covered = "Yes" if not hits.empty else "No"

    # choose best scopus row for display
    scopus_status = "Unknown"
    matched_title = ""
    matched_issn = ""
    matched_eissn = ""
    hit_count = int(len(hits))

    if scopus_covered == "Yes":
        hits_sorted = hits.copy()
        hits_sorted["_is_active"] = hits_sorted["_status_norm"].str.lower().eq("active")
        hits_sorted = hits_sorted.sort_values(by=["_is_active"], ascending=False)
        row = hits_sorted.iloc[0]
        scopus_status = str(row["Active or Inactive"]).strip()
        matched_title = str(row["Source Title"]).strip()
        matched_issn = str(row["ISSN"]).strip()
        matched_eissn = str(row["EISSN"]).strip()

    # ---- Determine whether to query OpenAlex ----
    do_openalex = True
    if not assist_only and scopus_covered != "Yes":
        do_openalex = False

    # Prepare OpenAlex queries (prefer ISSN/EISSN from scopus hit, else user input)
    issn_for_oa = norm_issn(matched_issn) or norm_issn(q_issn)
    eissn_for_oa = norm_issn(matched_eissn) or norm_issn(q_eissn)
    title_for_oa = matched_title or q_title

    source_obj = None
    oa_homepage = ""
    pub2024 = ""
    pub2025 = ""
    legal_owner = ""
    owner_type = "未知"
    evidence_url = ""
    oa_match_mode = ""

    if do_openalex:
        try:
            # OpenAlex: ISSN/EISSN first
            if issn_for_oa:
                source_obj = openalex_source_by_issn(issn_for_oa, mailto)
                oa_match_mode = f"issn:{issn_for_oa}"
            if (not source_obj) and eissn_for_oa:
                source_obj = openalex_source_by_issn(eissn_for_oa, mailto)
                oa_match_mode = f"issn:{eissn_for_oa}"
            # fallback: title search
            if (not source_obj) and title_for_oa.strip():
                source_obj = openalex_source_by_title(title_for_oa, mailto)
                oa_match_mode = f"search:{title_for_oa.strip()}"

            if source_obj:
                oa_homepage = source_obj.get("homepage_url", "") or ""
                c24, c25 = get_counts_2024_2025(source_obj)
                pub2024 = "" if c24 is None else int(c24)
                pub2025 = "" if c25 is None else int(c25)

                # legal owner approximation from public metadata
                host_org = source_obj.get("host_organization")  # may be OpenAlex org id; sometimes not ROR
                host_name = source_obj.get("host_organization_name", "") or ""
                publisher = source_obj.get("publisher", "") or ""

                # prefer host organization name, else publisher
                legal_owner = host_name or publisher

                evidence_url = source_obj.get("id", "") or ""

                # ROR classify if we can (only if host_org is actually ror.org; often it isn't)
                ror_info = ror_org_by_id_or_url(host_org) if host_org else None
                owner_type = classify_owner(ror_info, fallback_name=legal_owner)

        except Exception as e:
            st.warning(f"OpenAlex/ROR 查询失败：{e}")
            do_openalex = False

    # ---- Build final result row ----
    result = pd.DataFrame([{
        "Query_SourceTitle": q_title.strip(),
        "Query_ISSN": q_issn.strip(),
        "Query_EISSN": q_eissn.strip(),

        "Scopus_Covered": scopus_covered,
        "Scopus_Active_or_Inactive": scopus_status,
        "Scopus_Matched_SourceTitle": matched_title,
        "Scopus_Matched_ISSN": matched_issn,
        "Scopus_Matched_EISSN": matched_eissn,
        "Scopus_Hit_Count": hit_count,
        "Scopus_Match_Mode": meta.get("mode", ""),

        "OpenAlex_Queried": "Yes" if do_openalex else "No",
        "OpenAlex_Match_Mode": oa_match_mode,
        "Website": oa_homepage,

        "PubCount_2024": pub2024,
        "PubCount_2025": pub2025,

        "LegalOwner": legal_owner,
        "Owner_Type": owner_type,
        "Evidence_URL": evidence_url
    }])

    st.markdown("## 结果")
    st.dataframe(result, use_container_width=True)

    st.markdown("## Scopus 命中明细（最多 50 条）")
    show_cols = ["Source Title", "Active or Inactive", "ISSN", "EISSN"]
    if not hits.empty:
        st.dataframe(hits[show_cols].head(50), use_container_width=True)
    else:
        st.write("无命中（若你开启了“Scopus仅辅助”，仍会继续尝试 OpenAlex）。")

    st.markdown("## 导出")
    st.download_button(
        "下载结果 Excel",
        data=to_excel_bytes(result),
        file_name="journal_result.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
