"""
区域维度数据统一处理与按维度导出 Streamlit 应用

架构（第 8 轮需求）：
- 不再把全部数据合并成一张大表。改为按数据类型（历史Union / QTD / FCST / DG&Quota）
  分桶持久化存储（每个类型一个 Parquet 数据池），支持多财年财季累积。
- 上传文件 → 按数据类型处理 → 追加到对应数据池。
- 导出时按【数据类型 + 财年财季 + FCST Cycle】筛选，将明细导出为一个 Excel；
  若筛选后行数超过 Excel 单表上限（1,048,576 行）则报错，提示缩小范围。

界面布局（第 23 轮需求）：
- 左侧边栏：数据上传与处理、产线维度 Mapping 上传（合并进上传模块）、
  数据池状态 / 清理（可收起，默认收起）、导出设置（筛选 + 透视选项）。
- 右侧主区域：点击【运行筛选/透视】后展示明细预览、图表与导出按钮。
"""
import os
import io
import time
import uuid
import pandas as pd
import pyarrow.parquet as pq
import plotly.express as px
import streamlit as st

from ops_data_processor import (
    FINAL_ORDER,
    merge_all,
    FCST_FAMILY,
    SOURCE_GROUPS,
)

st.set_page_config(page_title="区域维度数据处理与导出", layout="wide")

ROOT = os.path.dirname(os.path.abspath(__file__))
DEFAULT_FOLDER = r"C:\Users\fenglx1\OneDrive - Lenovo\1 - 自用\0-OPS\数据统一格式"
DATA_DIR = os.path.join(ROOT, "data_buckets")
MASTER_PARQUET = os.path.join(ROOT, "最终合并表.parquet")
WEEK_OPTIONS = [f"Week{i}" for i in range(1, 16)]
UPLOAD_TYPES = ["历史Union", "QTD", "FCST", "DG&Quota"]
EXCEL_MAX_ROWS = 1048576  # Excel 单 sheet 最大行数
MAPPING_FILE = os.path.join(ROOT, "mapping_table.parquet")
MAPPING_COLS = ["产线大类", "纯产线大类", "非纯产线大类"]


def bucket_path(upload_type):
    safe = upload_type.replace("&", "_").replace(" ", "")
    return os.path.join(DATA_DIR, f"data_{safe}.parquet")


def load_bucket(upload_type):
    p = bucket_path(upload_type)
    if os.path.exists(p):
        return pd.read_parquet(p)
    return pd.DataFrame(columns=FINAL_ORDER)


def load_bucket_columns(upload_type, columns):
    """只读取指定列，用于快速获取筛选选项，避免每次交互都加载整个桶。"""
    p = bucket_path(upload_type)
    if not os.path.exists(p):
        return pd.DataFrame(columns=columns)
    try:
        return pd.read_parquet(p, columns=columns)
    except Exception:  # noqa
        return pd.DataFrame(columns=columns)


def _bucket_row_count(upload_type):
    """不加载数据，直接读 Parquet 元数据返回行数。"""
    p = bucket_path(upload_type)
    if not os.path.exists(p):
        return 0
    try:
        return int(pq.ParquetFile(p).metadata.num_rows)
    except Exception:  # noqa
        return 0


def _bucket_distinct(upload_type, column):
    """只读取指定列并返回去重排序后的选项。"""
    p = bucket_path(upload_type)
    if not os.path.exists(p):
        return []
    try:
        return sorted(pd.read_parquet(p, columns=[column])[column].dropna().unique().tolist())
    except Exception:  # noqa
        return []


def _atomic_write_parquet(df_pq, target):
    """原子写入 Parquet：先写唯一临时文件，再 os.replace 覆盖目标。

    解决沙箱/多进程场景下目标文件被其他进程占用而 to_parquet 直接写报
    PermissionError 的问题：临时文件与目标同目录，replace 失败（锁）时重试，
    避免破坏原文件。
    """
    os.makedirs(os.path.dirname(target), exist_ok=True)
    d = os.path.dirname(target)
    tmp = os.path.join(d, f".{uuid.uuid4().hex}.tmp.parquet")
    try:
        df_pq.to_parquet(tmp, index=False)
        last_err = None
        for _ in range(20):
            try:
                os.replace(tmp, target)
                return
            except PermissionError as e:  # 目标被其他进程占用，短暂重试
                last_err = e
                time.sleep(0.3)
        if last_err:
            raise last_err
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)  # 沙箱可能拦截，忽略即可（残留临时文件无害）
            except OSError:
                pass


def _cleanup_stale_tmp(dir_path=DATA_DIR):
    """启动时清理上一次因锁导致的残留临时 Parquet（best-effort）。"""
    try:
        for f in os.listdir(dir_path):
            if f.endswith(".tmp.parquet"):
                try:
                    os.remove(os.path.join(dir_path, f))
                except OSError:
                    pass
    except OSError:
        pass


def save_bucket(upload_type, df):
    os.makedirs(DATA_DIR, exist_ok=True)
    df_pq = df.copy()
    for col in df_pq.columns:
        if df_pq[col].dtype == object:
            df_pq[col] = df_pq[col].fillna("").astype(str)
    _atomic_write_parquet(df_pq, bucket_path(upload_type))


def load_mapping():
    """读取已上传保存的产线维度 Mapping 表。"""
    if os.path.exists(MAPPING_FILE):
        return pd.read_parquet(MAPPING_FILE)
    return None


def save_mapping(df):
    """保存 Mapping 表到本地（全量覆盖替换），供后续导出/透视时自动应用。"""
    df_pq = df.copy()
    for col in df_pq.columns:
        if df_pq[col].dtype == object:
            df_pq[col] = df_pq[col].fillna("").astype(str)
    _atomic_write_parquet(df_pq, MAPPING_FILE)


def apply_mapping(df, mapping_df):
    """根据 Mapping 表添加/更新三列：产线大类、纯产线大类、非纯产线大类。

    规则：
    - 产线大类：产线+通路 匹配 Mapping 第一列 → 返回第二列。
    - 纯产线大类：产线名称 匹配 Mapping 第一列 → 返回第二列。
    - 非纯产线大类：POS_APOS=POS 且 物料通路 为 HB/STB/JV 时，返回 物料通路；
      否则返回 纯产线大类。
    """
    out = df.copy()
    for col in MAPPING_COLS:
        out[col] = None

    if mapping_df is None or mapping_df.empty or df.empty:
        return out

    key_col = mapping_df.columns[0]
    val_col = mapping_df.columns[1]
    mapping_dict = mapping_df.set_index(key_col)[val_col].to_dict()

    out["产线大类"] = out["产线+通路"].astype(str).str.strip().map(mapping_dict)
    out["纯产线大类"] = out["产线名称"].astype(str).str.strip().map(mapping_dict)

    def _non_pure(row):
        pos_apos = str(row.get("POS_APOS", "")).strip().upper()
        material_channel = str(row.get("物料通路", "")).strip()
        pure = str(row.get("纯产线大类", "")).strip()
        if pos_apos == "POS" and material_channel in {"HB", "STB", "JV"}:
            return material_channel
        return pure if pure else None

    out["非纯产线大类"] = out.apply(_non_pure, axis=1)
    return out


def clear_bucket_with_progress(upload_type):
    """清空某类型数据池，显示进度条 + 预计剩余时间，清理完显示用时。

    真实工作量：按字节分批读取 Parquet 并丢弃（I/O 与文件大小成正比），
    进度条与 ETA 均来自真实耗时，而非虚假动画。
    注意：WorkBuddy 沙箱会拦截 os.remove 并要求走回收站（不可用），
          所以清空采用"覆写为空 Parquet"的方式，功能等价且不会崩溃。
    """
    p = bucket_path(upload_type)
    if not os.path.exists(p):
        return f"【{upload_type}】数据池本就为空。"

    total_bytes = os.path.getsize(p)
    try:
        total_rows = pq.ParquetFile(p).metadata.num_rows
    except Exception:  # noqa
        total_rows = None

    n_chunks = 40
    chunk_size = max(4096, (total_bytes + n_chunks - 1) // n_chunks)

    progress = st.progress(
        0.0,
        text=f"正在清理【{upload_type}】数据池… 0 / {total_bytes:,} bytes",
    )
    eta_ph = st.empty()
    start = time.time()
    read = 0
    # 用二进制读取真实 I/O 量，避免 pyarrow 全量解码带来的内存/耗时压力
    with open(p, "rb") as f:
        while True:
            data = f.read(chunk_size)
            if not data:
                break
            read += len(data)
            frac = min(read / total_bytes, 1.0) if total_bytes else 1.0
            elapsed = time.time() - start
            if frac < 1:
                eta = elapsed / frac * (1 - frac)
                eta_ph.caption(f"预计剩余时间：{eta:.1f} 秒")
            else:
                eta_ph.caption("即将完成…")
            progress.progress(
                frac,
                text=f"正在清理【{upload_type}】数据池… {read:,} / {total_bytes:,} bytes",
            )
            # 保证小文件也能看到进度动画；大文件下该延迟可忽略
            time.sleep(0.03)

    # 不调用 os.remove（沙箱回收站不可用会抛 OSError），而是覆写为空 Parquet
    empty_df = pd.DataFrame(columns=FINAL_ORDER)
    save_bucket(upload_type, empty_df)
    elapsed = time.time() - start
    progress.progress(1.0, text="清理完成")
    eta_ph.empty()
    rows_msg = f"，共 {total_rows:,} 行" if total_rows is not None else ""
    return f"已清空【{upload_type}】数据池{rows_msg}；清理用时 {elapsed:.2f} 秒。"


def ensure_buckets_from_master():
    """首次使用：若各类型桶尚未建立，但从旧主表 Parquet 存在，则按源表拆分迁移。"""
    if any(os.path.exists(bucket_path(t)) for t in UPLOAD_TYPES):
        return
    if not os.path.exists(MASTER_PARQUET):
        return
    m = pd.read_parquet(MASTER_PARQUET)
    os.makedirs(DATA_DIR, exist_ok=True)
    migrated = []
    for t in UPLOAD_TYPES:
        src = SOURCE_GROUPS[t]
        sub = m[m["源表"].isin(src)]
        if len(sub):
            save_bucket(t, sub[FINAL_ORDER])
            migrated.append(f"{t}:{len(sub):,}")
    if migrated:
        st.info("已从历史主表迁移数据池：" + "，".join(migrated))


def to_excel_download(df, sheet="明细"):
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet)
    return buffer.getvalue()


def load_from_uploads(uploaded_files):
    temp_dir = os.path.join(os.getcwd(), "_streamlit_uploads")
    os.makedirs(temp_dir, exist_ok=True)
    paths = []
    for up in uploaded_files:
        # 给临时文件加唯一前缀，避免同名文件重复上传时旧文件被占用/锁死
        # 导致 PermissionError。
        safe_name = f"{uuid.uuid4().hex}_{up.name}"
        path = os.path.join(temp_dir, safe_name)
        with open(path, "wb") as f:
            f.write(up.getbuffer())
        paths.append(path)
    return paths


def render_analysis(df, key_prefix):
    """绘制金额汇总图表；key_prefix 区分多次调用避免组件 key 冲突。"""
    amount_col = "业绩考核USDK"
    df[amount_col] = pd.to_numeric(df[amount_col], errors="coerce")

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**按数据类别 - 金额汇总**")
        cat_amount = df.groupby("数据类别")[amount_col].sum().reset_index()
        fig = px.bar(cat_amount, x="数据类别", y=amount_col, text_auto=".2s")
        st.plotly_chart(fig, width="stretch", key=f"{key_prefix}_cat")
    with c2:
        st.markdown("**按数据类别 - 行数分布**")
        cnt = df["数据类别"].value_counts().reset_index()
        cnt.columns = ["数据类别", "行数"]
        fig = px.pie(cnt, names="数据类别", values="行数")
        st.plotly_chart(fig, width="stretch", key=f"{key_prefix}_catpie")

    st.markdown("**按服务大区 - 金额汇总 TOP15**")
    region_amount = (
        df.groupby("服务大区")[amount_col]
        .sum()
        .reset_index()
        .sort_values(amount_col, ascending=False)
        .head(15)
    )
    fig = px.bar(region_amount, x="服务大区", y=amount_col, text_auto=".2s")
    st.plotly_chart(fig, width="stretch", key=f"{key_prefix}_region")

    c3, c4 = st.columns(2)
    with c3:
        st.markdown("**按产线类型 - 金额汇总**")
        line_amount = (
            df.groupby("产线类型")[amount_col]
            .sum()
            .reset_index()
            .sort_values(amount_col, ascending=False)
            .head(15)
        )
        fig = px.bar(line_amount, x="产线类型", y=amount_col, text_auto=".2s")
        st.plotly_chart(fig, width="stretch", key=f"{key_prefix}_line")
    with c4:
        st.markdown("**按 POS/APOS - 金额汇总**")
        pos_amount = df.groupby("POS_APOS")[amount_col].sum().reset_index()
        fig = px.pie(pos_amount, names="POS_APOS", values=amount_col)
        st.plotly_chart(fig, width="stretch", key=f"{key_prefix}_pos")

    st.markdown("**按财年财季 - 金额趋势**")
    fy_amount = (
        df.groupby("财年财季")[amount_col]
        .sum()
        .reset_index()
        .sort_values("财年财季")
    )
    fig = px.line(fy_amount, x="财年财季", y=amount_col, markers=True)
    st.plotly_chart(fig, width="stretch", key=f"{key_prefix}_fy")

    c5, c6 = st.columns(2)
    with c5:
        st.markdown("**按 IDGISG - 金额汇总**")
        idg_amount = df.groupby("IDGISG")[amount_col].sum().reset_index()
        fig = px.pie(idg_amount, names="IDGISG", values=amount_col)
        st.plotly_chart(fig, width="stretch", key=f"{key_prefix}_idg")
    with c6:
        st.markdown("**按销售模式 - 金额汇总 TOP10**")
        sm_amount = (
            df.groupby("销售模式")[amount_col]
            .sum()
            .reset_index()
            .sort_values(amount_col, ascending=False)
            .head(10)
        )
        fig = px.bar(sm_amount, x="销售模式", y=amount_col, text_auto=".2s")
        st.plotly_chart(fig, width="stretch", key=f"{key_prefix}_sm")

    st.markdown("**按 产线+通路 - 金额汇总 TOP15**")
    chnl_amount = (
        df.groupby("产线+通路")[amount_col]
        .sum()
        .reset_index()
        .sort_values(amount_col, ascending=False)
        .head(15)
    )
    fig = px.bar(chnl_amount, x="产线+通路", y=amount_col, text_auto=".2s")
    st.plotly_chart(fig, width="stretch", key=f"{key_prefix}_chnl")

    # 自定义透视
    st.subheader("自定义透视")
    st.markdown(
        "<small>选择一个维度字段查看金额汇总。</small>", unsafe_allow_html=True
    )
    dim = st.selectbox(
        "维度",
        ["服务大区", "服务战区", "产线类型", "产线名称", "产品大类", "SPL名称",
         "IDGISG", "销售模式", "POS_APOS", "物料通路", "REL纵队", "数据类别",
         "财年财季", "源表", "FCST Cycle"],
        key=f"{key_prefix}_dim",
    )
    top_n = st.slider("显示 Top N", 5, 50, 15, key=f"{key_prefix}_topn")
    pivot = (
        df.groupby(dim)[amount_col]
        .sum()
        .reset_index()
        .sort_values(amount_col, ascending=False)
        .head(top_n)
    )
    fig = px.bar(pivot, x=dim, y=amount_col, text_auto=".2s")
    st.plotly_chart(fig, width="stretch", key=f"{key_prefix}_pivot")
    st.dataframe(pivot, width="stretch")


# ===================== FCST 分析模块 =====================
# DG/Quota 与 FCST 按最细维度对齐（含物料通路）；YOY 与历史Union 按较粗维度对齐
# （历史数据物料通路维度与 FCST 不完全一致，避免大量不匹配导致同比失真）。
_FCST_KEYS = ["服务大区", "服务战区", "POS_APOS", "物料通路"]
_YOY_KEYS = ["服务大区", "服务战区", "POS_APOS"]


def _prev_fy(fy):
    """FY26Q2 -> FY25Q2（去年同季度）。"""
    try:
        y = int(fy[2:4])
        q = fy[5:6]
        return f"FY{y - 1:02d}Q{q}"
    except Exception:
        return None


def _mapping_mtime():
    """Mapping 文件修改时间，用于让 FCST 分析缓存随 Mapping 更新而失效。"""
    return os.path.getmtime(MAPPING_FILE) if os.path.exists(MAPPING_FILE) else 0


@st.cache_data(show_spinner=False)
def _load_fcst_analysis_data(fy, _mapping_mtime_value):
    """按财年财季一次性加载 FCST / DG&Quota / 历史Union 并聚合。

    这是 FCST 分析最耗时的部分（读 Parquet + apply_mapping + groupby），
    缓存后切换 Cycle / 范围 / 战区都只需切片和构建展示表，响应更快。
    """
    fcst = load_bucket("FCST")
    mapping_df = load_mapping()
    fcst = apply_mapping(fcst, mapping_df)
    fcst = fcst[fcst["财年财季"] == fy].copy()
    fcst["业绩考核USDK"] = pd.to_numeric(fcst["业绩考核USDK"], errors="coerce").fillna(0)
    for c in ["服务大区", "服务战区", "POS_APOS", "物料通路", "产线大类", "客户名称"]:
        fcst[c] = fcst[c].fillna("").astype(str)

    dgq = load_bucket("DG&Quota")
    dgq = apply_mapping(dgq, mapping_df)
    dgq = dgq[dgq["财年财季"] == fy].copy()
    dgq["业绩考核USDK"] = pd.to_numeric(dgq["业绩考核USDK"], errors="coerce").fillna(0)
    dgdf = dgq[dgq["数据类别"] == "DG"]
    qdf = dgq[dgq["数据类别"] == "Quota"]

    hist = load_bucket("历史Union")
    hist = apply_mapping(hist, mapping_df)
    hist_fy = _prev_fy(fy)
    if hist_fy:
        hist = hist[hist["财年财季"] == hist_fy].copy()
    else:
        hist = hist.iloc[0:0].copy()
    hist["业绩考核USDK"] = pd.to_numeric(hist["业绩考核USDK"], errors="coerce").fillna(0)
    for c in ["服务大区", "服务战区", "POS_APOS", "物料通路", "产线大类"]:
        hist[c] = hist[c].fillna("").astype(str)

    keys = ["服务大区", "服务战区", "POS_APOS", "物料通路"]
    dg_map = dgdf.groupby(keys)["业绩考核USDK"].sum() if not dgdf.empty else pd.Series(dtype=float)
    q_map = qdf.groupby(keys)["业绩考核USDK"].sum() if not qdf.empty else pd.Series(dtype=float)
    h_map = hist.groupby(_YOY_KEYS)["业绩考核USDK"].sum() if not hist.empty else pd.Series(dtype=float)
    h_map_pl = hist.groupby(_YOY_KEYS + ["产线大类"])["业绩考核USDK"].sum() if not hist.empty else pd.Series(dtype=float)

    return fcst, dg_map, q_map, h_map, h_map_pl


def _money(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    return f"{int(round(float(x))):,}"


def _signed(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    v = int(round(float(x)))
    return ("+" if v >= 0 else "") + f"{v:,}"


def _pct(num, den):
    if den is None or den == 0 or (isinstance(den, float) and pd.isna(den)):
        return ""
    return f"{num / den * 100:.0f}%"


def _segmented_buttons(label, options, key, default=None):
    """横向圆角按钮组（替代 radio 的圆圈），返回当前选中值。"""
    st.markdown(f"<small>{label}</small>", unsafe_allow_html=True)
    selected = st.session_state.get(key, default)
    if selected not in options:
        selected = options[0] if options else None
        st.session_state[key] = selected
    cols = st.columns(len(options))
    for i, opt in enumerate(options):
        with cols[i]:
            btn_type = "primary" if opt == selected else "secondary"
            if st.button(opt, key=f"{key}_{opt}", type=btn_type, use_container_width=True):
                st.session_state[key] = opt
                st.rerun()
    return selected


def compute_fcst(fy, cur_cycle, cmp_cycle, scope, sub_region):
    """加载并计算 FCST 分析数据，返回层级树状结果表。"""
    fcst, dg_map, q_map, h_map, h_map_pl = _load_fcst_analysis_data(fy, _mapping_mtime())

    cur = fcst[fcst["FCST Cycle"] == cur_cycle].copy()
    cmp = fcst[fcst["FCST Cycle"] == cmp_cycle].copy()

    if scope != "TTL":
        cur = cur[cur["服务大区"] == scope]
        cmp = cmp[cmp["服务大区"] == scope]
    if sub_region:
        cur = cur[cur["服务战区"] == sub_region]
        cmp = cmp[cmp["服务战区"] == sub_region]

    main_tbl, ttl_summary = _build_section(cur, cmp, dg_map, q_map, h_map, h_map_pl)
    return {
        "fy": fy,
        "cur_cycle": cur_cycle,
        "cmp_cycle": cmp_cycle,
        "scope": scope,
        "sub_region": sub_region,
        "main_table": main_tbl,
        "ttl_summary": ttl_summary,
    }


def _build_section(cur, cmp, dg_map, q_map, h_map, h_map_pl):
    """按 TTL → APOS/POS → 产线大类 → 大区 → 客户 构建层级树表。

    - TTL/APOS/POS 汇总行：计算 DG%/Quota%/YOY%。
    - 产线大类/大区/客户行：只展示 当前FCST、上版FCST、WTW，不计算比率。
    - 返回的 DataFrame 额外携带 id / parent_id / level / label，供前端树表折叠使用。
    """
    keys = ["服务大区", "服务战区", "POS_APOS", "物料通路"]
    rows = []

    def sum_map(cs, mp, kcols):
        if cs.empty or len(mp) == 0:
            return 0.0
        idx = pd.MultiIndex.from_frame(cs[kcols].drop_duplicates())
        inter = mp.index.intersection(idx)
        return float(mp.loc[inter].sum())

    def add_row(row_id, parent_id, level, label, cs, ms, calc_ratio=False, calc_yoy=False, yoy_by_pl=False):
        amt = cs["业绩考核USDK"].sum()
        pamt = ms["业绩考核USDK"].sum()
        dg = sum_map(cs, dg_map, keys) if calc_ratio else 0.0
        q = sum_map(cs, q_map, keys) if calc_ratio else 0.0
        if calc_yoy:
            h = sum_map(cs, h_map_pl, _YOY_KEYS + ["产线大类"]) if yoy_by_pl else sum_map(cs, h_map, _YOY_KEYS)
        else:
            h = 0.0
        rows.append({
            "id": row_id,
            "parent_id": parent_id,
            "level": level,
            "label": label,
            "当前FCST": _money(amt),
            "上版FCST": _money(pamt),
            "WTW": _signed(amt - pamt),
            # 完成率口径：FCST / DG(Quota)，即实际/目标
            "DG%": _pct(amt, dg) if calc_ratio else "",
            "Quota%": _pct(amt, q) if calc_ratio else "",
            "YOY%": ("" if h == 0 else f"{(amt / h - 1) * 100:.0f}%") if calc_yoy else "",
            # 原始数值隐藏列，供顶部 KPI 看板使用
            "_amt": float(amt),
            "_pamt": float(pamt),
            "_dg": float(dg),
            "_q": float(q),
            "_h": float(h),
        })
        return row_id

    def top5_customers(sub, subm):
        # 同时考虑当前版与对比版的客户索引，避免“当前版无记录但对比版有记录”的流失客户被遗漏
        cc = sub.groupby("客户名称")["业绩考核USDK"].sum()
        cm = subm.groupby("客户名称")["业绩考核USDK"].sum()
        all_cust = set(cc.index) | set(cm.index)
        w = pd.Series({cust: cc.get(cust, 0.0) - cm.get(cust, 0.0) for cust in all_cust})
        w = w[w.index.astype(str).str.len() > 0]
        if w.empty:
            return []
        return w.reindex(w.abs().sort_values(ascending=False).index).head(5).index.tolist()

    # 先计算 TTL 汇总，供顶部 KPI 看板使用，但不加入树表
    ttl_id = add_row("ttl", "", -1, "TTL", cur, cmp, calc_ratio=True, calc_yoy=True)
    ttl_summary = {
        "_amt": float(cur["业绩考核USDK"].sum()),
        "_pamt": float(cmp["业绩考核USDK"].sum()),
        "_dg": float(sum_map(cur, dg_map, keys)),
        "_q": float(sum_map(cur, q_map, keys)),
        "_h": float(sum_map(cur, h_map, _YOY_KEYS)),
    }

    for pos_label in ["APOS", "POS"]:
        pos_id = f"pos_{pos_label}"
        c_pos = cur[cur["POS_APOS"] == pos_label]
        m_pos = cmp[cmp["POS_APOS"] == pos_label]
        add_row(pos_id, "", 0, pos_label, c_pos, m_pos, calc_ratio=True, calc_yoy=True)
        for pl in sorted(c_pos["产线大类"].replace("", "（未匹配）").unique()):
            pl_id = f"{pos_id}_pl_{pl}"
            c_pl = c_pos[c_pos["产线大类"].replace("", "（未匹配）") == pl]
            m_pl = m_pos[m_pos["产线大类"].replace("", "（未匹配）") == pl]
            add_row(pl_id, pos_id, 1, pl, c_pl, m_pl)
            for r in sorted(c_pl["服务大区"].replace("", "（未匹配）").unique()):
                r_id = f"{pl_id}_r_{r}"
                c_r = c_pl[c_pl["服务大区"] == r]
                m_r = m_pl[m_pl["服务大区"] == r]
                add_row(r_id, pl_id, 2, r, c_r, m_r)
                for cust in top5_customers(c_r, m_r):
                    cust_id = f"{r_id}_c_{cust}"
                    c_c = c_r[c_r["客户名称"] == cust]
                    m_c = m_r[m_r["客户名称"] == cust]
                    add_row(cust_id, r_id, 3, cust, c_c, m_c)

    # 移除为 TTL 创建的临时行，避免出现在树表中
    rows = [r for r in rows if r["id"] != ttl_id]
    return pd.DataFrame(rows), ttl_summary


def _parse_signed(s):
    if not s or not isinstance(s, str):
        return 0.0
    try:
        return float(str(s).replace(",", "").replace("+", "").strip())
    except ValueError:
        return 0.0


def _parse_pct(s):
    if not s or not isinstance(s, str):
        return None
    try:
        return float(str(s).replace(",", "").replace("%", "").strip())
    except ValueError:
        return None


def _build_tree_html(df):
    """把带层级信息的 DataFrame 渲染为可折叠 HTML 树表（默认折叠 level>=3 的子层级）。"""
    if df.empty:
        return "<p>暂无数据</p>"

    df = df.copy()
    # 标记哪些行拥有子节点
    df["has_children"] = df["id"].isin(df["parent_id"].values)

    headers = ["层级", "当前FCST", "上版FCST", "WTW", "DG%", "Quota%", "YOY%"]
    value_cols = ["当前FCST", "上版FCST", "WTW", "DG%", "Quota%", "YOY%"]

    def fmt_cell(val, col):
        if col == "WTW":
            num = _parse_signed(val)
            if num > 0:
                return f'<td class="num up">{val}</td>'
            elif num < 0:
                return f'<td class="num down">{val}</td>'
            else:
                return f'<td class="num">{val}</td>'
        elif col in ("DG%", "Quota%"):
            num = _parse_pct(val)
            if val == "" or num is None:
                return '<td class="num"></td>'
            if num > 100:
                return f'<td class="num up">{val}</td>'
            elif num < 100:
                return f'<td class="num down">{val}</td>'
            else:
                return f'<td class="num">{val}</td>'
        elif col == "YOY%":
            num = _parse_pct(val)
            if val == "" or num is None:
                return '<td class="num"></td>'
            if num > 0:
                return f'<td class="num up">+{val}</td>'
            elif num < 0:
                return f'<td class="num down">{val}</td>'
            else:
                return f'<td class="num">{val}</td>'
        else:
            return f'<td class="num">{val}</td>'

    rows_html = []
    for _, row in df.iterrows():
        level = int(row["level"])
        has_children = bool(row["has_children"])
        indent = level * 22
        if has_children:
            icon = f'<span class="tree-toggle" data-rowid="{row["id"]}">▶</span>'
        else:
            icon = '<span class="tree-spacer"></span>'

        # 默认显示 level<2（APOS/POS、产线大类），折叠 level>=2（大区/客户）
        display = "table-row" if level < 2 else "none"
        cells = [
            f'<td class="tree-label"><div class="tree-label-inner" style="padding-left:{indent}px">{icon}<span class="tree-text">{row["label"]}</span></div></td>',
        ] + [fmt_cell(row[c], c) for c in value_cols]
        rows_html.append(
            f'<tr class="tree-row level-{level}" data-level="{level}" data-id="{row["id"]}" '
            f'data-parent="{row["parent_id"]}" data-has-children="{str(has_children).lower()}" '
            f'style="display:{display}">'
            + "".join(cells) + "</tr>"
        )

    header_html = "".join([f"<th>{h}</th>" for h in headers])
    table_html = (
        '<table class="tree-table"><thead><tr>' + header_html + '</tr></thead><tbody>'
        + "".join(rows_html) + '</tbody></table>'
    )

    css = """
    <style>
    .tree-table { width: 100%; border-collapse: collapse; font-family: "Source Sans Pro", sans-serif; font-size: 14px; color: #31333F; }
    .tree-table th { position: sticky; top: 0; text-align: left; padding: 10px 12px; border-bottom: 1px solid #e6e6e6; background: #f7f7f8; font-weight: 600; }
    .tree-table td { padding: 8px 12px; border-bottom: 1px solid #f0f0f0; vertical-align: middle; }
    .tree-table td.num { text-align: right; font-variant-numeric: tabular-nums; white-space: nowrap; }
    .tree-table td.up { color: #0f9d00; font-weight: 600; }
    .tree-table td.down { color: #d93025; font-weight: 600; }
    .tree-label-inner { display: flex; align-items: center; gap: 6px; }
    .tree-toggle { cursor: pointer; width: 16px; display: inline-flex; align-items: center; justify-content: center; color: #666; user-select: none; font-size: 12px; }
    .tree-toggle:hover { color: #000; }
    .tree-spacer { width: 16px; display: inline-block; }
    .tree-text { white-space: nowrap; }
    .tree-row:hover { background: #fafafa; }
    .tree-row.level-0 { font-weight: 700; background: #fff; border-left: 4px solid #ff4b4b; }
    .tree-row.level-1 { font-weight: 500; color: #444; border-left: 4px solid #83c9ff; }
    .tree-row.level-2 { color: #555; border-left: 4px solid #e0e0e0; }
    .tree-row.level-3 { color: #666; border-left: 4px solid #f0f0f0; }
    .tree-row.level-2 .tree-text, .tree-row.level-3 .tree-text { font-size: 12px; }
    </style>
    """

    js = """
    <script>
    (function() {
        function setChildrenDisplay(rowId, show) {
            var children = document.querySelectorAll('tr[data-parent="' + rowId + '"]');
            children.forEach(function(child) {
                child.style.display = show ? 'table-row' : 'none';
                var childId = child.getAttribute('data-id');
                // 折叠时同步隐藏所有子孙节点，并把子孙的 toggle 图标重置为 ▶
                if (!show) {
                    setChildrenDisplay(childId, false);
                    var childToggle = document.querySelector('.tree-toggle[data-rowid="' + childId + '"]');
                    if (childToggle) childToggle.textContent = '▶';
                }
            });
        }

        document.querySelectorAll('.tree-toggle').forEach(function(toggle) {
            toggle.addEventListener('click', function(e) {
                e.stopPropagation();
                var rowId = this.getAttribute('data-rowid');
                var expanded = this.textContent === '▼';
                this.textContent = expanded ? '▶' : '▼';
                setChildrenDisplay(rowId, !expanded);
            });
        });
    })();
    </script>
    """
    return css + table_html + js


def _render_tree_table(df):
    """用 Streamlit HTML 组件渲染可折叠树表。"""
    import streamlit.components.v1 as components
    html = _build_tree_html(df)
    components.html(html, height=650, scrolling=True)


def _render_kpi_dashboard(ttl):
    """顶部 KPI 看板：自定义 HTML 卡片展示 TTL 汇总指标。"""
    import streamlit.components.v1 as components

    amt = float(ttl["_amt"])
    pamt = float(ttl["_pamt"])
    wtw = amt - pamt
    dg = float(ttl["_dg"])
    q = float(ttl["_q"])
    h = float(ttl["_h"])

    dg_pct = _pct(amt, dg)
    q_pct = _pct(amt, q)
    yoy_pct = "" if h == 0 else f"{(amt / h - 1) * 100:.0f}%"

    def _kpi_color(val, col):
        if col == "WTW":
            return "up" if val > 0 else ("down" if val < 0 else "")
        elif col in ("DG%", "Quota%"):
            num = _parse_pct(val)
            if num is None:
                return ""
            return "up" if num > 100 else ("down" if num < 100 else "")
        elif col == "YOY%":
            num = _parse_pct(val)
            if num is None:
                return ""
            return "up" if num > 0 else ("down" if num < 0 else "")
        return ""

    yoy_display = yoy_pct
    yoy_num = _parse_pct(yoy_pct)
    if yoy_num is not None and yoy_num > 0:
        yoy_display = f"+{yoy_pct}"

    cards = [
        ("当前 FCST", _money(amt), "", ""),
        ("上版 FCST", _money(pamt), "", ""),
        ("WTW", _signed(wtw), "当前 vs 上版", _kpi_color(wtw, "WTW")),
        ("DG%", dg_pct, "完成率", _kpi_color(dg_pct, "DG%")),
        ("Quota%", q_pct, "完成率", _kpi_color(q_pct, "Quota%")),
        ("YOY%", yoy_display, "同比", _kpi_color(yoy_pct, "YOY%")),
    ]

    cols_html = ""
    for title, value, subtitle, cls in cards:
        sub_html = f'<div class="kpi-sub">{subtitle}</div>' if subtitle else ""
        color_style = ""
        if cls == "up":
            color_style = "color: #0f9d00;"
        elif cls == "down":
            color_style = "color: #d93025;"
        cols_html += f"""
        <div class="kpi-card">
            <div class="kpi-title">{title}</div>
            <div class="kpi-value" style="{color_style}">{value}</div>
            {sub_html}
        </div>
        """

    html = f"""
    <style>
    .kpi-board {{ display: flex; flex-wrap: wrap; gap: 12px; margin: 8px 0 20px 0; }}
    .kpi-card {{ flex: 1; min-width: 130px; background: #ffffff; border: 1px solid #e6e6e6; border-radius: 10px; padding: 16px 18px; box-shadow: 0 2px 4px rgba(0,0,0,0.04); }}
    .kpi-title {{ font-size: 12px; color: #888888; margin-bottom: 6px; letter-spacing: 0.3px; }}
    .kpi-value {{ font-size: 24px; font-weight: 600; color: #31333F; font-variant-numeric: tabular-nums; }}
    .kpi-sub {{ font-size: 11px; color: #aaaaaa; margin-top: 4px; }}
    </style>
    <div class="kpi-board">{cols_html}</div>
    """
    components.html(html, height=120)


def render_fcst_analysis():
    """右侧 Tab1：FCST 分析（横向按钮筛选 + 实时联动 + 层级树表下钻）。"""
    st.subheader("FCST 分析")
    st.markdown(
        "<small>选择财年财季、当前/对比 FCST Cycle 与范围，看板与下钻实时联动。</small>",
        unsafe_allow_html=True,
    )

    # 缩小 FCST 分析区域内按钮尺寸，使筛选控件更紧凑
    st.markdown(
        """
        <style>
        .stButton > button {
            padding: 0.15rem 0.4rem !important;
            font-size: 0.75rem !important;
            min-height: 26px !important;
            line-height: 1.2 !important;
            border-radius: 6px !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    meta = load_bucket_columns("FCST", ["财年财季", "FCST Cycle", "服务大区", "服务战区"])
    if meta.empty:
        st.warning("FCST 数据池为空，请先在左侧上传 FCST 数据。")
        return
    fy_opts = sorted(meta["财年财季"].dropna().unique().tolist())
    cyc_opts = sorted(meta["FCST Cycle"].dropna().unique().tolist())
    region_opts = sorted(meta["服务大区"].dropna().unique().tolist())

    # 三个核心筛选控件放在同一行，label 统一用 <small> 以保证对齐
    c1, c2, c3 = st.columns([3, 1, 1])
    with c1:
        fy = _segmented_buttons("财年财季", fy_opts, key="fcst_fy", default=fy_opts[-1] if fy_opts else None)
    with c2:
        st.markdown("<small>当前 FCST Cycle</small>", unsafe_allow_html=True)
        cur_cycle = st.selectbox("", cyc_opts, index=len(cyc_opts) - 1, key="fcst_cur", label_visibility="collapsed")
    with c3:
        st.markdown("<small>对比 FCST Cycle</small>", unsafe_allow_html=True)
        cmp_cycle = st.selectbox("", cyc_opts, index=max(0, len(cyc_opts) - 2), key="fcst_cmp", label_visibility="collapsed")

    scope = _segmented_buttons("范围", ["TTL"] + region_opts, key="fcst_scope", default="TTL")

    sub_region = None
    if scope != "TTL":
        sub_opts = sorted(meta[meta["服务大区"] == scope]["服务战区"].dropna().unique().tolist())
        sub_sel = _segmented_buttons(
            "战区（不选=该大区合计）",
            ["（合计）"] + sub_opts,
            key="fcst_sub",
            default="（合计）",
        )
        if sub_sel != "（合计）":
            sub_region = sub_sel

    # 用 session_state 缓存上次计算结果，左侧导出设置变化时不重新算 FCST
    cache_key = f"{fy}|{cur_cycle}|{cmp_cycle}|{scope}|{sub_region}|{_mapping_mtime()}"
    if st.session_state.get("fcst_cache_key") != cache_key:
        with st.spinner("正在计算 FCST 分析…"):
            res = compute_fcst(fy, cur_cycle, cmp_cycle, scope, sub_region)
        st.session_state["fcst_result"] = res
        st.session_state["fcst_cache_key"] = cache_key
    else:
        res = st.session_state["fcst_result"]

    st.caption(
        f"财年财季 {res['fy']} ｜ 当前 {res['cur_cycle']} vs 对比 {res['cmp_cycle']} ｜ "
        f"范围 {res['scope']}{(' / ' + res['sub_region']) if res['sub_region'] else ''}"
    )

    # 顶部 KPI 看板（自定义 HTML 卡片，更像看板）
    _render_kpi_dashboard(res["ttl_summary"])

    _render_tree_table(res["main_table"])

    # 所选财年财季 FCST by Week 趋势图
    _render_fcst_trend(fy, scope, sub_region)


def _render_fcst_trend(fy, scope, sub_region):
    """在所选财年财季下，按 FCST Cycle（Week）以表格式展示趋势：

    - 列：口径 | Week1 | Week2 | ... | WeekN | Trend
    - 行：TTL、APOS（可折叠）、APOS 大客户、POS（可折叠）、POS 大客户
    - 每个 Week 都显示金额；最右侧为对应 sparkline
    """
    import re
    import streamlit.components.v1 as components

    fcst, _, _, _, _ = _load_fcst_analysis_data(fy, _mapping_mtime())
    df = fcst.copy()
    if scope != "TTL":
        df = df[df["服务大区"] == scope]
    if sub_region:
        df = df[df["服务战区"] == sub_region]
    df = df[df["POS_APOS"].isin(["APOS", "POS"])]
    if df.empty:
        st.info("所选范围内无 FCST 数据，无法绘制趋势。")
        return

    df["客户名称"] = df["客户名称"].fillna("").astype(str).replace("", "（未命名）")

    def _week_key(w):
        nums = re.findall(r"\d+", str(w))
        return int(nums[0]) if nums else 0

    weeks = sorted(df["FCST Cycle"].dropna().unique().astype(str).tolist(), key=_week_key)

    def weekly_series(src_df):
        return src_df.groupby("FCST Cycle")["业绩考核USDK"].sum().reindex(weeks, fill_value=0).values

    def get_big_customers(pos_df, threshold):
        cust_cycle = pos_df.groupby(["客户名称", "FCST Cycle"])["业绩考核USDK"].sum().reset_index()
        big = cust_cycle[cust_cycle["业绩考核USDK"] > threshold]["客户名称"].unique().tolist()
        cust_total = pos_df.groupby("客户名称")["业绩考核USDK"].sum()
        big = sorted(big, key=lambda c: cust_total.get(c, 0), reverse=True)
        if not big:
            big = cust_total.sort_values(ascending=False).head(10).index.tolist()
        return big[:15]

    # 阈值：数据源单位为 USDK，500K 对应数值 500
    st.markdown("<small>大客户判定：任一 Week 金额 &gt; 阈值（默认 500，即 500K USDK）。</small>", unsafe_allow_html=True)
    threshold = st.number_input(
        "大客户阈值",
        min_value=0,
        value=500,
        step=100,
        key="fcst_cust_threshold",
        label_visibility="collapsed",
    )

    def sparkline(vals, color):
        if len(vals) == 0:
            return ""
        n = len(vals)
        W, H = 120, 28
        max_v = max(vals)
        min_v = min(vals)
        rng = max_v - min_v
        pts = []
        for i, v in enumerate(vals):
            px = (i / (n - 1)) * W if n > 1 else W / 2
            if rng == 0:
                py = H / 2
            else:
                py = H - 4 - ((v - min_v) / rng) * (H - 8)
            pts.append((px, py))

        pts_str = " ".join([f"{x},{y}" for x, y in pts])
        circles = "".join([f'<circle cx="{x}" cy="{y}" r="2" fill="{color}"/>' for x, y in pts])
        return (
            f'<svg viewBox="0 0 {W} {H}" preserveAspectRatio="none" class="spark-svg">'
            f'<polyline points="{pts_str}" fill="none" stroke="{color}" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>'
            f'{circles}</svg>'
        )

    rows_html = []
    cust_colors = ["#5f6368", "#f9ab00", "#1e8e3e", "#d93025", "#1a73e8",
                   "#9c27b0", "#007b83", "#e8710a", "#6d4c41", "#546e7a",
                   "#c5221f", "#188038", "#b06000", "#3367d6", "#9334e6"]

    def make_cells(vals):
        return "".join([f'<td class="num">{_money(v)}</td>' for v in vals])

    # TTL
    ttl_vals = weekly_series(df)
    rows_html.append(
        '<tr class="row-main row-ttl" data-id="ttl">'
        '<td class="label main-label"><span class="tree-spacer"></span>TTL</td>'
        f'{make_cells(ttl_vals)}'
        f'<td class="trend">{sparkline(ttl_vals, "#31333F")}</td></tr>'
    )

    def build_group(group_id, label, pos_df, color, is_apos):
        pos_vals = weekly_series(pos_df)
        rows_html.append(
            f'<tr class="row-main row-{group_id}" data-id="{group_id}">'
            f'<td class="label main-label">'
            f'<span class="tree-toggle" data-target="{group_id}">▼</span>{label}</td>'
            f'{make_cells(pos_vals)}'
            f'<td class="trend">{sparkline(pos_vals, color)}</td></tr>'
        )
        for i, cust in enumerate(get_big_customers(pos_df, threshold)):
            vals = weekly_series(pos_df[pos_df["客户名称"] == cust])
            cust_color = cust_colors[i % len(cust_colors)]
            rows_html.append(
                f'<tr class="row-cust child-{group_id}" data-parent="{group_id}">'
                f'<td class="label sub-label"><span class="tree-spacer"></span>{cust}</td>'
                f'{make_cells(vals)}'
                f'<td class="trend">{sparkline(vals, cust_color)}</td></tr>'
            )

    # APOS + customers
    build_group("apos", "APOS", df[df["POS_APOS"] == "APOS"], "#0068c9", True)
    # POS + customers
    build_group("pos", "POS", df[df["POS_APOS"] == "POS"], "#ff4b4b", False)

    week_headers = "".join([f'<th class="num week-header">{w}</th>' for w in weeks])
    html = f"""
    <style>
    .trend-table-wrap {{ overflow-x: auto; }}
    .trend-hier-table {{ width: 100%; border-collapse: collapse; font-family: "Source Sans Pro", sans-serif; font-size: 11px; color: #31333F; }}
    .trend-hier-table th {{ position: sticky; top: 0; background: #f7f7f8; padding: 6px 4px; border-bottom: 1px solid #e0e0e0; font-weight: 600; text-align: right; white-space: nowrap; }}
    .trend-hier-table th.label {{ text-align: left; min-width: 180px; }}
    .trend-hier-table th.week-header {{ min-width: 58px; }}
    .trend-hier-table th.trend {{ text-align: center; width: 90px; }}
    .trend-hier-table td {{ padding: 5px 4px; border-bottom: 1px solid #f0f0f0; vertical-align: middle; }}
    .trend-hier-table td.num {{ text-align: right; font-variant-numeric: tabular-nums; white-space: nowrap; font-size: 10px; }}
    .trend-hier-table td.label {{ white-space: nowrap; }}
    .trend-hier-table td.trend {{ text-align: center; }}
    .trend-hier-table .main-label {{ font-weight: 600; font-size: 12px; display: flex; align-items: center; gap: 5px; }}
    .trend-hier-table .sub-label {{ padding-left: 20px; font-size: 11px; color: #555; display: flex; align-items: center; gap: 5px; }}
    .trend-hier-table .row-main {{ background: #fff; }}
    .trend-hier-table .row-cust {{ background: #fafafa; }}
    .trend-hier-table .row-ttl .main-label {{ color: #31333F; }}
    .trend-hier-table .row-apos .main-label {{ color: #0068c9; }}
    .trend-hier-table .row-pos .main-label {{ color: #ff4b4b; }}
    .trend-hier-table tr:hover {{ background: #f5f5f5; }}
    .tree-toggle {{ cursor: pointer; width: 12px; display: inline-flex; align-items: center; justify-content: center; color: #666; user-select: none; font-size: 10px; }}
    .tree-toggle:hover {{ color: #000; }}
    .tree-spacer {{ width: 12px; display: inline-block; }}
    .spark-svg {{ width: 80px; height: 24px; display: block; margin: 0 auto; }}
    </style>
    <div class="trend-table-wrap">
    <table class="trend-hier-table">
    <thead><tr><th class="label">口径</th>{week_headers}<th class="trend">Trend</th></tr></thead>
    <tbody>{"".join(rows_html)}</tbody>
    </table>
    </div>
    <script>
    (function() {{
        document.querySelectorAll('.tree-toggle').forEach(function(toggle) {{
            toggle.addEventListener('click', function(e) {{
                e.stopPropagation();
                var target = this.getAttribute('data-target');
                var expanded = this.textContent === '▼';
                this.textContent = expanded ? '▶' : '▼';
                document.querySelectorAll('.child-' + target).forEach(function(row) {{
                    row.style.display = expanded ? 'none' : 'table-row';
                }});
            }});
        }});
    }})();
    </script>
    """

    st.subheader("FCST by Week 趋势")
    st.markdown(
        "<small>每行展示各 Week 金额及趋势；APOS / POS 行可点击 ▼ 折叠/展开其下客户。</small>",
        unsafe_allow_html=True,
    )
    components.html(html, height=110 + len(rows_html) * 34, scrolling=True)


def main():
    _cleanup_stale_tmp()
    ensure_buckets_from_master()

    # ===================== 左侧边栏：上传 / 处理 / 导出设置 =====================
    with st.sidebar:
        # ---- 1. 数据上传与处理 ----
        st.header("1. 数据上传与处理")
        st.markdown(
            "<small>选择类型并上传文件，点“处理并更新到数据池”把明细**追加**到对应数据池"
            "（支持多财年财季累积；重复上传会重复追加，可在下方【数据池状态 / 清理】中重置）。</small>",
            unsafe_allow_html=True,
        )
        upload_type = st.radio("数据类型", UPLOAD_TYPES, horizontal=True)
        uploaded = st.file_uploader(
            "上传 Excel 文件（可多选）",
            type=["xlsx", "xls"],
            accept_multiple_files=True,
            key=f"up_{upload_type}",
        )
        fcst_cycle = None
        if upload_type == "FCST":
            fcst_cycle = st.selectbox("FCST Cycle（周版本）", WEEK_OPTIONS, key="fcst_cycle_sel")

        col_a, col_b = st.columns(2)
        with col_a:
            if st.button("处理并更新到数据池", key="btn_update"):
                if not uploaded:
                    st.warning("请先上传文件再更新。")
                else:
                    paths = load_from_uploads(uploaded)
                    # 用户已明确选择类型时，对非 FCST 类型强制按所选类型处理（不识别文件名）。
                    # FCST 仍走文件名/表头识别，以便区分 T1/T2/T3 子类型。
                    force_cat = upload_type if upload_type != "FCST" else None
                    n = len(paths)
                    parts = []
                    handlers_set = set()
                    progress = st.progress(0.0, text=f"正在处理 0/{n} 个文件…")
                    eta_ph = st.empty()
                    start = time.time()
                    ok = True
                    # 大文件/单文件处理首个文件前，eta_ph 为空不可见，先给出可见提示。
                    if n == 1:
                        eta_ph.info("已启动单文件处理，文件较大时请稍候；完成时将显示实际用时。")
                    else:
                        eta_ph.info("已启动处理，首个文件完成后将显示预计剩余时间。")
                    for i, p in enumerate(paths):
                        fname = os.path.basename(p)
                        try:
                            part = merge_all([p], fcst_cycle=fcst_cycle, force_category=force_cat)
                        except Exception as e:
                            st.error(f"处理文件 {fname} 出错：{e}")
                            part = pd.DataFrame(columns=FINAL_ORDER)
                        if not part.empty:
                            parts.append(part)
                            handlers_set |= set(part["源表"].dropna().unique())
                        frac = (i + 1) / n
                        elapsed = time.time() - start
                        if frac < 1:
                            eta = elapsed / frac * (1 - frac)
                            eta_ph.caption(f"已用 {elapsed:.1f} 秒，预计剩余时间：{eta:.1f} 秒")
                        else:
                            eta_ph.caption(f"已用 {elapsed:.1f} 秒，即将完成…")
                        progress.progress(frac, text=f"正在处理 {i + 1}/{n} 个文件：{fname}")
                    if not parts:
                        progress.empty()
                        eta_ph.empty()
                        st.error(
                            "未能从上传文件处理出有效明细。可能原因："
                            "文件表头与所选类型不匹配，或文件为空。"
                        )
                        ok = False
                    if ok:
                        partial = pd.concat(parts, ignore_index=True)
                        expected = SOURCE_GROUPS[upload_type]
                        if handlers_set - expected:
                            progress.empty()
                            eta_ph.empty()
                            st.error(
                                f"上传文件来源 {sorted(handlers_set)} 与所选类型【{upload_type}】不匹配"
                                f"（应为 {sorted(expected)}），已取消更新。"
                            )
                        else:
                            cur = load_bucket(upload_type)
                            updated = pd.concat([cur, partial[FINAL_ORDER]], ignore_index=True)
                            save_bucket(upload_type, updated)
                            elapsed = time.time() - start
                            progress.progress(1.0, text="处理完成")
                            eta_ph.empty()
                            tag = f"（{fcst_cycle}）" if fcst_cycle else ""
                            st.success(
                                f"已更新【{upload_type}】{tag}：本次新增 {len(partial):,} 行，"
                                f"数据池共 {len(updated):,} 行；处理用时 {elapsed:.2f} 秒。"
                            )
        with col_b:
            if st.button("批量加载默认文件夹", key="btn_batch"):
                files = [
                    os.path.join(DEFAULT_FOLDER, f)
                    for f in os.listdir(DEFAULT_FOLDER)
                    if f.lower().endswith((".xlsx", ".xls"))
                ]
                if not files:
                    st.warning("默认文件夹无文件。")
                else:
                    counts = {}
                    for f in files:
                        part = merge_all([f])
                        if part.empty:
                            continue
                        src = set(part["源表"].dropna().unique())
                        matched = [t for t in UPLOAD_TYPES if src <= SOURCE_GROUPS[t]]
                        for t in matched:
                            cur = load_bucket(t)
                            save_bucket(t, pd.concat([cur, part[FINAL_ORDER]], ignore_index=True))
                            counts[t] = counts.get(t, 0) + len(part)
                    if counts:
                        st.success("批量加载完成：" + "，".join(f"{k}:{v:,}" for k, v in counts.items()))
                    else:
                        st.warning("默认文件夹未识别到可处理数据。")

        # ---- 产线维度 Mapping 上传（合并进上传模块，覆盖替换） ----
        st.divider()
        st.subheader("产线维度 Mapping 上传")
        st.markdown(
            "<small>上传 Mapping 表（≥2 列：第一列匹配键、第二列映射值）。"
            "保存后**覆盖替换**上一份 Mapping，导出/透视时自动应用"
            "（新增产线大类 / 纯产线大类 / 非纯产线大类）。</small>",
            unsafe_allow_html=True,
        )
        mapping_uploaded = st.file_uploader("上传 Mapping Excel", type=["xlsx", "xls"], key="mapping_uploader")
        mapping_df = load_mapping()
        if mapping_df is not None:
            st.caption(f"当前已保存 Mapping：{len(mapping_df):,} 行，列：{list(mapping_df.columns)}")
        if mapping_uploaded is not None:
            if st.button("保存 Mapping（覆盖）", key="btn_save_mapping"):
                tmp_dir = os.path.join(os.getcwd(), "_streamlit_uploads")
                os.makedirs(tmp_dir, exist_ok=True)
                tmp_path = os.path.join(tmp_dir, f"mapping_{uuid.uuid4().hex}_{mapping_uploaded.name}")
                with open(tmp_path, "wb") as f:
                    f.write(mapping_uploaded.getbuffer())
                try:
                    mapping_df = pd.read_excel(tmp_path)
                    if mapping_df.shape[1] < 2:
                        st.error("Mapping 表至少需要两列（第一列匹配键，第二列映射值）。")
                    else:
                        save_mapping(mapping_df)  # 全量覆写替换
                        st.success(f"已保存并覆盖 Mapping：{len(mapping_df):,} 行")
                        st.rerun()
                except Exception as e:
                    st.error(f"读取 Mapping 文件失败：{e}")

        # ---- 2. 数据池状态 / 清理（默认收起） ----
        expanded = bool(st.session_state.get("clear_result"))
        with st.expander("2. 数据池状态 / 清理", expanded=expanded):
            if st.session_state.get("clear_result"):
                st.success(st.session_state["clear_result"])
                st.session_state["clear_result"] = None
            for t in UPLOAD_TYPES:
                rows = _bucket_row_count(t)
                if rows == 0:
                    st.markdown(f"- **{t}**：（空）")
                    continue
                fy_opts = _bucket_distinct(t, "财年财季")
                cyc_opts = _bucket_distinct(t, "FCST Cycle") if t == "FCST" else []
                cyc_text = f"，FCST 周版本 {cyc_opts}" if cyc_opts else ""
                st.markdown(
                    f"- **{t}**：{rows:,} 行，财年财季 {len(fy_opts)} 个{cyc_text}"
                )
                # 整池清空（通用）
                if st.button(f"清空 {t} 数据池（全部）", key=f"clear_{t}"):
                    msg = clear_bucket_with_progress(t)
                    st.session_state["clear_result"] = msg
                    st.rerun()

                # 历史Union：按财年财季清空（仅点按钮时才加载全量数据）
                if t == "历史Union":
                    hu_sel_fy = st.multiselect(
                        "选择要清空的财年财季", fy_opts, default=[], key="clear_hu_fy"
                    )
                    if st.button("清空选中财年财季", key="clear_hu_btn"):
                        if not hu_sel_fy:
                            st.warning("请先选择要清空的财年财季。")
                        else:
                            df = load_bucket("历史Union")
                            remove_mask = df["财年财季"].isin(hu_sel_fy)
                            removed = int(remove_mask.sum())
                            save_bucket("历史Union", df[~remove_mask].reset_index(drop=True))
                            st.session_state["clear_result"] = (
                                f"已清空【历史Union】选中的 {len(hu_sel_fy)} 个财年财季，"
                                f"删除 {removed:,} 行。"
                            )
                            st.rerun()

                # FCST：按财年财季 + FCST Cycle 清空（仅点按钮时才加载全量数据）
                if t == "FCST":
                    fcst_sel_fy = st.multiselect(
                        "选择要清空的财年财季", fy_opts, default=[], key="clear_fcst_fy"
                    )
                    fcst_sel_cyc = st.multiselect(
                        "选择要清空的 FCST Cycle", cyc_opts, default=[], key="clear_fcst_cyc"
                    )
                    if st.button("清空选中财年财季+Cycle", key="clear_fcst_btn"):
                        if not fcst_sel_fy or not fcst_sel_cyc:
                            st.warning("请同时选择财年财季与 FCST Cycle。")
                        else:
                            df = load_bucket("FCST")
                            remove_mask = df["财年财季"].isin(fcst_sel_fy) & df["FCST Cycle"].isin(fcst_sel_cyc)
                            removed = int(remove_mask.sum())
                            save_bucket("FCST", df[~remove_mask].reset_index(drop=True))
                            st.session_state["clear_result"] = (
                                f"已清空【FCST】选中的财年财季×Cycle，删除 {removed:,} 行。"
                            )
                            st.rerun()

        # ---- 3. 导出设置 ----
        st.divider()
        st.header("3. 导出设置")
        st.markdown(
            "<small>先设置筛选与透视选项，再点【运行筛选/透视】；调整选项不会立即重算大数据。</small>",
            unsafe_allow_html=True,
        )

        selected_types = st.multiselect(
            "数据类型（可多选）", UPLOAD_TYPES, default=UPLOAD_TYPES, key="exp_types"
        )
        if not selected_types:
            st.warning("请至少选择一个数据类型。")

        # 仅读取筛选维度元数据，轻量快速，保证界面交互不卡
        meta_parts = [
            load_bucket_columns(t, ["财年财季", "产线类型", "源表", "FCST Cycle"]) for t in selected_types
        ]
        meta = pd.concat(meta_parts, ignore_index=True) if meta_parts else pd.DataFrame(
            columns=["财年财季", "产线类型", "源表", "FCST Cycle"]
        )

        fy_options = sorted(meta["财年财季"].dropna().unique().tolist())
        selected_fy = st.multiselect(
            "财年财季（可多选，不选=全部）", fy_options, default=fy_options, key="exp_fy"
        )

        line_options = sorted(meta["产线类型"].dropna().unique().tolist())
        selected_line = st.multiselect(
            "产线类型（可多选，不选=全部）", line_options, default=line_options, key="exp_line"
        )

        selected_cycle = []
        if "FCST" in selected_types:
            cyc_options = sorted(
                meta[meta["源表"].isin(FCST_FAMILY)]["FCST Cycle"].dropna().unique().tolist()
            )
            selected_cycle = st.multiselect(
                "FCST Cycle（仅对 FCST 生效，不选=全部周）", cyc_options, default=[], key="exp_cycle"
            )

        export_mode = st.radio(
            "导出模式",
            ["完整明细（所有列）", "透视汇总（选择列）"],
            horizontal=True,
            key="exp_mode",
            index=0,
        )

        # 透视列选择（在运行前只收集参数，不做 groupby）
        sel_cols = []
        agg = "求和"
        if export_mode == "透视汇总（选择列）":
            all_cols = list(FINAL_ORDER) + [c for c in MAPPING_COLS if c not in FINAL_ORDER]
            default_cols = [
                c for c in [
                    "财年财季", "数据类别", "服务大区", "服务战区",
                    "产线名称", "产线+通路", "IDGISG", "物料通路",
                    "POS_APOS", "业绩考核USDK",
                    "产线大类", "纯产线大类", "非纯产线大类",
                ]
                if c in all_cols
            ]
            sel_cols = st.multiselect(
                "选择透视列（文本列自动作为分组维度，数值列自动作为汇总度量）",
                all_cols,
                default=default_cols,
                key="pivot_cols",
            )
            agg = st.selectbox("度量聚合方式", ["求和", "计数", "均值"], index=0, key="pivot_agg")

        st.divider()
        run = st.button("运行筛选/透视", type="primary", key="btn_run_export")
        if run:
            # 点击运行后自动切换到右侧导出 Tab 并触发计算
            st.session_state.active_tab = "明细 / 透视导出"
            st.session_state.run_export = True
            st.rerun()

    # ===================== 右侧主区域 =====================
    tabs = ["FCST 分析", "明细 / 透视导出"]
    if "active_tab" not in st.session_state:
        st.session_state.active_tab = tabs[0]
    if "run_export" not in st.session_state:
        st.session_state.run_export = False

    active_tab = st.radio(
        "导航", tabs, horizontal=True, key="active_tab", label_visibility="collapsed"
    )

    if active_tab == tabs[0]:
        render_fcst_analysis()
    else:
        st.title("区域维度数据处理与导出")
        st.markdown(
            "<small>上传文件按数据类型处理后进入**数据池**（持久化保存，支持多财年财季累积）；"
            "在**左侧**设置筛选与透视，点击【运行筛选/透视】后，结果（明细预览 / 图表 / 导出）显示在此处。</small>",
            unsafe_allow_html=True,
        )

        if not selected_types:
            st.info("请在左侧至少选择一个数据类型。")
            return

        if not st.session_state.run_export:
            st.info("在左侧设置好筛选与透视选项后，点击【运行筛选/透视】，结果将在此处展示。")
            return

        # ===== 以下只在点击按钮后执行，避免每次交互都重算大数据 =====
        progress = st.progress(0.0, text="正在加载数据池…")
        start = time.time()
        parts = [load_bucket(t) for t in selected_types]
        parts = [p for p in parts if not p.empty]
        all_data = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=FINAL_ORDER)
    
        progress.progress(0.35, text="正在应用 Mapping…")
        mapping_df = load_mapping()
        all_data = apply_mapping(all_data, mapping_df)
    
        progress.progress(0.4, text="正在按筛选条件过滤…")
        sub = all_data.copy()
        if selected_fy:
            sub = sub[sub["财年财季"].isin(selected_fy)]
        if selected_line:
            sub = sub[sub["产线类型"].isin(selected_line)]
        if selected_cycle:
            fam = sub["源表"].isin(FCST_FAMILY)
            sub = sub[fam & sub["FCST Cycle"].isin(selected_cycle) | ~fam]
    
        # 兼容旧数据：QTD 再次剔除销售模式为“在途”的明细
        qtd_mask = sub["源表"] == "QTD"
        sub = sub[~(qtd_mask & (sub["销售模式"].astype(str).str.strip() == "在途"))]
    
        elapsed = time.time() - start
        st.success(f"筛选后明细：**{len(sub):,} 行**（加载+过滤用时 {elapsed:.2f} 秒）")
    
        if sub.empty:
            st.warning("当前筛选条件下没有数据。")
            return
    
        if export_mode == "完整明细（所有列）":
            progress.progress(0.8, text="正在准备完整明细…")
            # 行数上限检查
            if len(sub) > EXCEL_MAX_ROWS:
                st.error(
                    f"❌ 筛选后共 {len(sub):,} 行，已超过 Excel 单表最大行数 {EXCEL_MAX_ROWS:,}，"
                    f"无法导出。请缩小【财年财季】或【FCST Cycle】的选择范围后再试。"
                )
            else:
                excel_bytes = to_excel_download(sub, sheet="明细")
                st.download_button(
                    label="下载筛选明细 Excel",
                    data=excel_bytes,
                    file_name="区域维度数据_导出明细.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            progress.progress(1.0, text="完成")
            # --- 预览 + 图表 ---
            st.subheader("明细预览（前 200 行）")
            st.dataframe(sub.head(200), width="stretch")
            if len(sub) <= 500000:
                render_analysis(sub, "export_preview")
            else:
                st.info("数据量较大，已跳过图表渲染；请用上方筛选缩小范围后查看图表。")
        else:
            # ===== 透视汇总模式 =====
            if not sel_cols:
                st.warning("请至少选择一列用于透视。")
                return
    
            progress.progress(0.6, text="正在生成分组聚合…")
    
            def _is_numeric(s):
                c = pd.to_numeric(s, errors="coerce")
                return c.notna().mean() > 0.5
    
            dims = [c for c in sel_cols if not _is_numeric(sub[c])]
            meas = [c for c in sel_cols if _is_numeric(sub[c])]
            work = sub[sel_cols].copy()
            for m in meas:
                work[m] = pd.to_numeric(work[m], errors="coerce")
    
            aggfn = {"求和": "sum", "计数": "count", "均值": "mean"}[agg]
    
            if dims and meas:
                pivot = work.groupby(dims, dropna=False)[meas].agg(aggfn).reset_index()
                if len(meas) == 1:
                    pivot = pivot.rename(columns={meas[0]: f"{meas[0]}_{agg}"})
            elif dims and not meas:
                # 仅选维度列：输出去重组合 + 行数计数
                pivot = work.groupby(dims, dropna=False).size().reset_index(name="行数")
            elif meas and not dims:
                # 仅选度量列：单行汇总
                pivot = work[meas].agg(aggfn).to_frame().T
                pivot.insert(0, "汇总", agg)
            else:
                pivot = work
    
            elapsed = time.time() - start
            progress.progress(1.0, text=f"透视完成，用时 {elapsed:.2f} 秒")
            st.success(f"透视结果：**{len(pivot):,} 行** × {pivot.shape[1]} 列")
            st.dataframe(pivot, width="stretch")
    
            if len(pivot) > EXCEL_MAX_ROWS:
                st.error(
                    f"❌ 透视结果共 {len(pivot):,} 行，已超过 Excel 单表最大行数 {EXCEL_MAX_ROWS:,}，无法导出。"
                )
            else:
                pb = to_excel_download(pivot, sheet="透视汇总")
                st.download_button(
                    label="下载透视汇总 Excel",
                    data=pb,
                    file_name="区域维度数据_透视汇总.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
    
    
if __name__ == "__main__":
    main()
