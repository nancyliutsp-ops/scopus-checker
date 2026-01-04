import re
from io import BytesIO

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Scopus Active Checker", layout="wide")

# ---------------- Helpers ----------------
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

    # 1) Exact ISSN/EISSN match
    hits = pd.DataFrame()
    if issn_q or eissn_q:
        cond = False
        if issn_q:
            cond = cond | (df["_issn_norm"] == issn_q) | (df["_eissn_norm"] == issn_q)
        if eissn_q:
            cond = cond | (df["_issn_norm"] == eissn_q) | (df["_eissn_norm"] == eissn_q)
        hits = df[cond].copy()

        # If title also given, refine by title contains
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

def build_result_table(hits: pd.DataFrame, query_title: str, query_issn: str, query_eissn: str):
    covered = "Yes" if not hits.empty else "No"
    status = "Unknown"
    matched_title = ""
    issn = ""
    eissn = ""

    if covered == "Yes":
        # Prefer Active record if multiple
        hits_sorted = hits.copy()
        hits_sorted["_is_active"] = hits_sorted["_status_norm"].str.lower().eq("active")
        hits_sorted = hits_sorted.sort_values(by=["_is_active"], ascending=False)
        row = hits_sorted.iloc[0]

        status = row["Active or Inactive"]
        matched_title = row["Source Title"]
        issn = row["ISSN"]
        eissn = row["EISSN"]

    return pd.DataFrame([{
        "Query_SourceTitle": query_title.strip(),
        "Query_ISSN": query_issn.strip(),
        "Query_EISSN": query_eissn.strip(),
        "Scopus_Covered": covered,
        "Active_or_Inactive": status,
        "Matched_SourceTitle": matched_title,
        "ISSN": issn,
        "EISSN": eissn,
        "Hit_Count": int(len(hits))
    }])

def to_excel_bytes(df: pd.DataFrame) -> bytes:
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Result")
    return bio.getvalue()

# ---------------- UI ----------------
st.title("Scopus 收录状态查询（Active/Inactive）")
st.caption("按你的要求：核心用 Source Title 匹配 Active or Inactive，并可用 ISSN/EISSN 辅助匹配。")

with st.sidebar:
    st.subheader("1) 上传 Scopus 列表")
    sc_file = st.file_uploader("上传 scopus 数据（Excel/CSV）", type=["xlsx", "xls", "csv"])
    st.markdown("要求列名：`Source Title`、`Active or Inactive`、`ISSN`、`EISSN`")

df = None
if sc_file is not None:
    try:
        df = load_scopus_df(sc_file)
        st.success(f"已加载：{len(df):,} 行")
    except Exception as e:
        st.error(str(e))
        st.stop()
else:
    st.info("请先在左侧上传 Scopus 列表文件。")
    st.stop()

st.subheader("2) 输入查询条件")
c1, c2, c3 = st.columns([2, 1, 1])
with c1:
    q_title = st.text_input("Source Title（期刊名，建议英文全称）", "")
with c2:
    q_issn = st.text_input("ISSN（可选）", "")
with c3:
    q_eissn = st.text_input("EISSN（可选）", "")

btn = st.button("查询", type="primary")

if btn:
    hits, meta = match_scopus(df, q_title, q_issn, q_eissn)
    res = build_result_table(hits, q_title, q_issn, q_eissn)

    st.markdown("### 查询结果")
    st.dataframe(res, use_container_width=True)

    st.markdown("### 匹配说明")
    st.write(meta)

    st.markdown("### 命中明细（最多显示前 50 条，方便核对）")
    show_cols = ["Source Title", "Active or Inactive", "ISSN", "EISSN"]
    if not hits.empty:
        st.dataframe(hits[show_cols].head(50), use_container_width=True)
    else:
        st.write("无命中。建议：补充 ISSN/EISSN 或检查 Source Title 拼写。")

    xls_bytes = to_excel_bytes(res)
    st.download_button(
        "下载结果 Excel",
        data=xls_bytes,
        file_name="scopus_result.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
