import streamlit as st
import pandas as pd
import psycopg2
import traceback
import json
import base64
from pathlib import Path

# ── config.json 경로 ──────────────────────────────────────────────────────────
CONFIG_FILE = Path(__file__).parent / "config.json"

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
        "key_cols": ["키컬럼1"],          # 행 매칭 기준 컬럼
        "compare_cols": [],               # 빈 리스트 = 공통 컬럼 전체 자동 비교
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


# ── config.json 읽기 / 쓰기 ───────────────────────────────────────────────────
def load_config() -> dict | None:
    if not CONFIG_FILE.exists():
        return None
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            raw = json.load(f)
        raw["password"] = base64.b64decode(raw.get("password_b64", "")).decode("utf-8")
        return raw
    except Exception:
        return None


def save_config(host: str, port: int, dbname: str, user: str, password: str):
    data = {
        "host":         host,
        "port":         int(port),
        "dbname":       dbname,
        "user":         user,
        "password_b64": base64.b64encode(password.encode("utf-8")).decode("utf-8"),
    }
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ── DB 연결 헬퍼 ──────────────────────────────────────────────────────────────
def try_connect(host, port, dbname, user, password) -> None:
    """연결 성공 시 반환, 실패 시 예외 raise."""
    conn = psycopg2.connect(
        host=host, port=int(port), dbname=dbname, user=user, password=password
    )
    conn.close()


def get_conn(cfg: dict):
    return psycopg2.connect(
        host=cfg["host"], port=cfg["port"],
        dbname=cfg["dbname"], user=cfg["user"], password=cfg["password"],
    )


# ── 비교 로직 ─────────────────────────────────────────────────────────────────
def compare(df_excel, df_db, key_cols, compare_cols):
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


# ════════════════════════════════════════════════════════════════════════════
#  앱 시작 — 세션 초기화 (최초 1회)
# ════════════════════════════════════════════════════════════════════════════
st.set_page_config(page_title="DB 비교 도구", page_icon="🔍", layout="wide")

if "initialized" not in st.session_state:
    cfg = load_config()
    st.session_state.config       = cfg
    st.session_state.db_connected = False
    st.session_state.show_settings = cfg is None   # config 없으면 폼 자동 오픈
    st.session_state.initialized  = True

    if cfg:
        try:
            try_connect(cfg["host"], cfg["port"], cfg["dbname"], cfg["user"], cfg["password"])
            st.session_state.db_connected = True
        except Exception:
            st.session_state.db_connected = False


# ════════════════════════════════════════════════════════════════════════════
#  상단 헤더 (타이틀 + 연결 상태 + 설정 버튼)
# ════════════════════════════════════════════════════════════════════════════
col_title, col_status, col_gear = st.columns([5, 3, 1])

with col_title:
    st.title("🔍 DB 비교 도구")
    st.caption("Excel 파일 ↔ 사전 정의 쿼리 결과를 비교합니다")

with col_status:
    st.write("")
    st.write("")
    if st.session_state.db_connected:
        st.success("✅ DB 연결됨")
    elif st.session_state.config:
        st.error("❌ 연결 실패 - 접속 정보를 확인하세요")
    else:
        st.warning("⚠️ 접속 정보를 설정해주세요")

with col_gear:
    st.write("")
    st.write("")
    if st.button("⚙️ 접속 설정", use_container_width=True):
        st.session_state.show_settings = not st.session_state.show_settings
        st.rerun()


# ════════════════════════════════════════════════════════════════════════════
#  접속 설정 폼 (토글)
# ════════════════════════════════════════════════════════════════════════════
if st.session_state.show_settings:
    with st.container(border=True):
        st.subheader("🗄️ DB 접속 설정")

        cfg = st.session_state.config or {}

        fc1, fc2 = st.columns(2)
        with fc1:
            f_host   = st.text_input("Host",     value=cfg.get("host",   "192.168.246.64"))
            f_dbname = st.text_input("Database", value=cfg.get("dbname", "ysr2000"))
        with fc2:
            f_port = st.number_input("Port", value=int(cfg.get("port", 5432)),
                                     min_value=1, max_value=65535, step=1)
            f_user = st.text_input("Username", value=cfg.get("user", "edba"))

        pw_placeholder = "저장된 비밀번호 유지 (변경 시만 입력)" if cfg.get("password") else "비밀번호 입력"
        f_pw = st.text_input("Password", type="password", placeholder=pw_placeholder)

        # 비밀번호 미입력 시 기존 값 유지
        effective_pw = f_pw if f_pw else cfg.get("password", "")

        bc1, bc2 = st.columns(2)

        with bc1:
            if st.button("💾 저장", use_container_width=True, type="primary"):
                if not effective_pw:
                    st.error("비밀번호를 입력해주세요.")
                else:
                    save_config(f_host, f_port, f_dbname, f_user, effective_pw)
                    new_cfg = {
                        "host": f_host, "port": int(f_port),
                        "dbname": f_dbname, "user": f_user, "password": effective_pw,
                    }
                    st.session_state.config = new_cfg
                    # 저장 후 바로 연결 시도
                    try:
                        try_connect(f_host, f_port, f_dbname, f_user, effective_pw)
                        st.session_state.db_connected = True
                        st.session_state.show_settings = False
                    except Exception as e:
                        st.session_state.db_connected = False
                        st.error(f"저장 완료, 연결 실패: {e}")
                    st.rerun()

        with bc2:
            if st.button("🔌 연결 테스트", use_container_width=True):
                if not effective_pw:
                    st.error("비밀번호를 입력해주세요.")
                else:
                    try:
                        try_connect(f_host, f_port, f_dbname, f_user, effective_pw)
                        st.success("✅ 연결 성공!")
                    except Exception as e:
                        st.error(f"❌ 연결 실패: {e}")

st.divider()


# ════════════════════════════════════════════════════════════════════════════
#  사이드바 — Excel 업로드
# ════════════════════════════════════════════════════════════════════════════
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


# ════════════════════════════════════════════════════════════════════════════
#  메인 — Excel 미리보기 + 비교 버튼
# ════════════════════════════════════════════════════════════════════════════
if df_excel is not None:
    with st.expander(f"📊 Excel 미리보기  ({len(df_excel)}행)", expanded=False):
        st.dataframe(df_excel.head(20), use_container_width=True)
else:
    st.info("← 사이드바에서 Excel 파일을 업로드하세요.")

st.subheader("비교 버튼")

ready = df_excel is not None and st.session_state.db_connected

if df_excel is None:
    st.warning("Excel 파일을 업로드해주세요.")
elif not st.session_state.db_connected:
    st.warning("DB에 연결되지 않았습니다. ⚙️ 접속 설정을 확인하세요.")

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

# ── 버튼 클릭 처리 ────────────────────────────────────────────────────────────
if clicked_idx is not None:
    btn = BUTTONS[clicked_idx]
    cfg = st.session_state.config
    st.subheader(f"결과: {btn['label']}")

    with st.spinner("DB 쿼리 실행 중…"):
        try:
            conn = get_conn(cfg)
            df_db = pd.read_sql(btn["query"], conn)
            conn.close()
        except Exception as e:
            st.error(f"DB 오류: {e}")
            with st.expander("상세"):
                st.code(traceback.format_exc())
            st.stop()

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

    n_match    = (df_result["상태"] == "일치").sum()
    n_mismatch = (df_result["상태"] == "불일치").sum()
    n_no_db    = (df_result["상태"] == "DB 누락").sum()
    n_no_excel = (df_result["상태"] == "Excel 누락").sum()

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("🟢 일치",       n_match)
    m2.metric("🟠 불일치",     n_mismatch)
    m3.metric("🔴 DB 누락",    n_no_db)
    m4.metric("🔵 Excel 누락", n_no_excel)

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
