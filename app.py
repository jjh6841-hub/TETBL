import streamlit as st
import pandas as pd
import psycopg2
import traceback

st.set_page_config(page_title="DB 비교 도구", page_icon="🔍", layout="wide")

# ══════════════════════════════════════════════════════════════════════════════
#  ★ 비교 버튼 정의 — 버튼을 추가/수정하려면 이 블록만 편집하세요 ★
# ══════════════════════════════════════════════════════════════════════════════
BUTTONS = [
    {
        "label": "비교 1: 테이블명 변경",
        "query": """
            SELECT *
            FROM 테이블명1
            ORDER BY 1
        """,
        "key_cols": ["키컬럼1"],          # Excel·DB 양쪽에 있는 행 매칭 기준 컬럼
        "compare_cols": [],                # 빈 리스트 = 양쪽 공통 컬럼 전체 자동 비교
        "description": "테이블명1과 Excel 비교",
    },
    {
        "label": "비교 2: 테이블명 변경",
        "query": """
            SELECT *
            FROM 테이블명2
            ORDER BY 1
        """,
        "key_cols": ["키컬럼1", "키컬럼2"],
        "compare_cols": [],
        "description": "테이블명2과 Excel 비교",
    },
    {
        "label": "비교 3: 테이블명 변경",
        "query": """
            SELECT col_a, col_b, col_c
            FROM 테이블명3
            WHERE 조건컬럼 = '값'
            ORDER BY col_a
        """,
        "key_cols": ["col_a"],
        "compare_cols": ["col_b", "col_c"],
        "description": "테이블명3 특정 조건 비교",
    },
]
# ══════════════════════════════════════════════════════════════════════════════

# ── DB 기본 접속 정보 (변경 가능) ─────────────────────────────────────────────
DEFAULT_HOST   = "192.168.246.64"
DEFAULT_PORT   = 5432
DEFAULT_DBNAME = "ysr2000"
DEFAULT_USER   = "edba"
# ─────────────────────────────────────────────────────────────────────────────


def get_conn(host, port, user, pw, dbname):
    return psycopg2.connect(
        host=host, port=port, user=user, password=pw, dbname=dbname
    )


def compare(df_excel, df_db, key_cols, compare_cols):
    """Excel DataFrame과 DB DataFrame을 비교하여 결과 DataFrame 반환."""

    def norm(df):
        return (df.astype(str)
                  .apply(lambda c: c.str.strip())
                  .replace({"None": "", "nan": "", "NaT": "", "<NA>": ""}))

    ex = norm(df_excel.copy())
    db = norm(df_db.copy())

    missing = [c for c in key_cols if c not in db.columns]
    if missing:
        raise ValueError(f"DB 결과에 키 컬럼 없음: {missing}")
    missing_ex = [c for c in key_cols if c not in ex.columns]
    if missing_ex:
        raise ValueError(f"Excel에 키 컬럼 없음: {missing_ex}")

    if not compare_cols:
        compare_cols = [c for c in ex.columns if c not in key_cols and c in db.columns]

    ex_idx = ex.set_index(key_cols)
    db_idx = db.set_index(key_cols)
    all_keys = ex_idx.index.union(db_idx.index)

    rows = []
    for key in all_keys:
        key_dict = (
            {key_cols[0]: key}
            if len(key_cols) == 1
            else dict(zip(key_cols, key))
        )

        in_ex = key in ex_idx.index
        in_db = key in db_idx.index

        def get_val(df_i, col):
            if col not in df_i.columns:
                return ""
            val = df_i.loc[key, col]
            if isinstance(val, pd.Series):
                val = val.iloc[0]
            return str(val)

        row = {**key_dict}

        if not in_db:
            row["상태"] = "DB 누락"
            row["불일치 컬럼"] = ""
            for c in compare_cols:
                row[f"[Excel] {c}"] = get_val(ex_idx, c)
                row[f"[DB] {c}"] = ""

        elif not in_ex:
            row["상태"] = "Excel 누락"
            row["불일치 컬럼"] = ""
            for c in compare_cols:
                row[f"[Excel] {c}"] = ""
                row[f"[DB] {c}"] = get_val(db_idx, c)

        else:
            diffs = []
            for c in compare_cols:
                ve = get_val(ex_idx, c)
                vd = get_val(db_idx, c)
                row[f"[Excel] {c}"] = ve
                row[f"[DB] {c}"] = vd
                if ve != vd:
                    diffs.append(c)
            row["상태"] = "불일치" if diffs else "일치"
            row["불일치 컬럼"] = ", ".join(diffs)

        rows.append(row)

    return pd.DataFrame(rows), compare_cols


STATUS_COLORS = {
    "일치":       "#d1fae5",
    "불일치":     "#fef3c7",
    "DB 누락":    "#fee2e2",
    "Excel 누락": "#e0e7ff",
}


def color_rows(row):
    color = STATUS_COLORS.get(row["상태"], "")
    return [f"background-color: {color}"] * len(row)


def show_filtered(df_result, status):
    sub = df_result[df_result["상태"] == status]
    if len(sub) == 0:
        st.info("해당 항목 없음")
    else:
        st.dataframe(sub.style.apply(color_rows, axis=1),
                     use_container_width=True, hide_index=True)


# ── 사이드바 ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("📁 Excel 파일")
    uploaded = st.file_uploader("파일 업로드", type=["xlsx", "xls"])

    df_excel = None
    if uploaded:
        xl = pd.ExcelFile(uploaded)
        sheet = st.selectbox("시트 선택", xl.sheet_names)
        df_excel = (
            pd.read_excel(uploaded, sheet_name=sheet, dtype=str)
            .fillna("")
        )
        df_excel = df_excel.apply(
            lambda c: c.str.strip() if c.dtype == object else c
        )
        st.success(f"✓ {len(df_excel)}행 · {len(df_excel.columns)}컬럼")
        with st.expander("컬럼 목록"):
            st.write(list(df_excel.columns))

    st.divider()

    st.header("🗄️ DB 접속 정보")
    host   = st.text_input("호스트",   DEFAULT_HOST)
    port   = st.number_input("포트",   value=DEFAULT_PORT, min_value=1, max_value=65535, step=1)
    dbname = st.text_input("DB 이름", DEFAULT_DBNAME)
    user   = st.text_input("사용자",  DEFAULT_USER)
    pw     = st.text_input("비밀번호", type="password", placeholder="비밀번호 입력")

    if st.button("🔌 연결 테스트", use_container_width=True):
        try:
            c = get_conn(host, int(port), user, pw, dbname)
            c.close()
            st.success("연결 성공!")
        except Exception as e:
            st.error(str(e))


# ── 메인 영역 ─────────────────────────────────────────────────────────────────
st.title("🔍 DB 비교 도구")
st.caption("Excel 파일 ↔ 사전 정의 쿼리 결과를 비교합니다")

# Excel 미리보기
if df_excel is not None:
    with st.expander(f"📊 Excel 미리보기  ({len(df_excel)}행)", expanded=False):
        st.dataframe(df_excel.head(20), use_container_width=True)
else:
    st.info("← 사이드바에서 Excel 파일을 업로드하고 비밀번호를 입력하세요.")

st.divider()
st.subheader("비교 버튼")

# 준비 상태 확인
ready = df_excel is not None and bool(pw)

if not ready:
    if df_excel is None:
        st.warning("Excel 파일을 업로드해주세요.")
    if not pw:
        st.warning("DB 비밀번호를 입력해주세요.")

# 버튼 렌더링
btn_cols = st.columns(len(BUTTONS))
clicked_idx = None

for i, btn in enumerate(BUTTONS):
    with btn_cols[i]:
        if st.button(
            btn["label"],
            key=f"btn_{i}",
            use_container_width=True,
            disabled=not ready,
            help=btn["description"],
        ):
            clicked_idx = i

# 클릭된 버튼 처리
if clicked_idx is not None:
    btn = BUTTONS[clicked_idx]
    st.subheader(f"결과: {btn['label']}")

    # DB 쿼리 실행
    with st.spinner("DB 쿼리 실행 중…"):
        try:
            conn = get_conn(host, int(port), user, pw, dbname)
            df_db = pd.read_sql(btn["query"], conn)
            conn.close()
        except Exception as e:
            st.error(f"DB 오류: {e}")
            with st.expander("상세"):
                st.code(traceback.format_exc())
            st.stop()

    # 비교 실행
    with st.spinner("비교 중…"):
        try:
            df_result, used_cols = compare(
                df_excel, df_db, btn["key_cols"], btn["compare_cols"]
            )
        except Exception as e:
            st.error(f"비교 오류: {e}")
            with st.expander("상세"):
                st.code(traceback.format_exc())
            st.stop()

    # 요약 메트릭
    n_match    = (df_result["상태"] == "일치").sum()
    n_mismatch = (df_result["상태"] == "불일치").sum()
    n_no_db    = (df_result["상태"] == "DB 누락").sum()
    n_no_excel = (df_result["상태"] == "Excel 누락").sum()

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("🟢 일치",       n_match)
    m2.metric("🟠 불일치",     n_mismatch)
    m3.metric("🔴 DB 누락",    n_no_db)
    m4.metric("🔵 Excel 누락", n_no_excel)

    # 탭별 결과
    tab_all, tab_mis, tab_nodb, tab_noex = st.tabs([
        "전체",
        f"불일치 ({n_mismatch})",
        f"DB 누락 ({n_no_db})",
        f"Excel 누락 ({n_no_excel})",
    ])

    with tab_all:
        st.dataframe(
            df_result.style.apply(color_rows, axis=1),
            use_container_width=True,
            hide_index=True,
        )
        csv = df_result.to_csv(index=False, encoding="utf-8-sig")
        st.download_button(
            "📥 CSV 다운로드", csv, "비교결과.csv", "text/csv",
            use_container_width=True,
        )

    with tab_mis:
        show_filtered(df_result, "불일치")

    with tab_nodb:
        show_filtered(df_result, "DB 누락")

    with tab_noex:
        show_filtered(df_result, "Excel 누락")
