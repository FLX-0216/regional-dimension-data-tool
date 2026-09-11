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

# Streamlit 在 ScriptRunner 线程执行脚本；pandas 3 + pyarrow 25 在首次于非主线程
# 导入 pyarrow 后再由其他线程做 Arrow 分配时可能触发 mimalloc SIGSEGV。
# 提前强制使用 system 内存池，可规避该崩溃（apache/arrow#50471）。
os.environ.setdefault("ARROW_DEFAULT_MEMORY_POOL", "system")

import io
import re
import time
import uuid
from datetime import datetime
import pandas as pd
import pyarrow.parquet as pq
import plotly.express as px
import streamlit as st

from ops_data_processor import (
    FINAL_ORDER,
    merge_all,
    FCST_FAMILY,
    SOURCE_GROUPS,
    add_core_memoline,
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
MAPPING_COLS = ["产线大类", "纯产线大类", "非纯产线大类", "展示顺序"]


def _pos_display_label(pos):
    """FCST 分析展示层把 APOS/POS 替换为 Solutions/Services（数据源字段不变）。"""
    if pos == "APOS":
        return "Solutions"
    if pos == "POS":
        return "Services"
    return pos


def _is_dark_theme():
    """检测 Streamlit 当前主题（用户可在应用内切换黑色/深色）。

    components.html 渲染的是隔离 iframe，不会继承 Streamlit 的主题，
    因此不能仅靠 CSS @media (prefers-color-scheme) —— 当 OS 是浅色、
    用户在 Streamlit 里切到黑色时后者不生效。这里用 st.get_option 读取
    运行时的 theme.base，作为显式 class 注入到 iframe 内容里。
    """
    try:
        base = str(st.get_option("theme.base")).lower()
        return base == "dark"
    except Exception:  # noqa
        return False


def _theme_cls():
    return "theme-dark" if _is_dark_theme() else "theme-light"


_THEME_RUNTIME_JS = """
<script>
(function() {
    function detect() {
        try {
            var doc = window.parent.document;
            var app = doc.querySelector('.stApp') || doc.body;
            if (!app) return;
            var bg = getComputedStyle(app).backgroundColor || '';
            var m = bg.match(/[\\d.]+/g);
            if (!m || m.length < 3) return;
            var lum = 0.299 * parseFloat(m[0]) + 0.587 * parseFloat(m[1]) + 0.114 * parseFloat(m[2]);
            var el = document.documentElement;
            if (lum < 128) { el.classList.add('theme-dark'); el.classList.remove('theme-light'); }
            else { el.classList.add('theme-light'); el.classList.remove('theme-dark'); }
            // 同步所有静态主题包装 div（Python 渲染的 theme-light/theme-dark 类不会随运行时切换自动更新）
            var dark = el.classList.contains('theme-dark');
            document.querySelectorAll('.theme-light, .theme-dark').forEach(function(d) {
                d.classList.toggle('theme-dark', dark);
                d.classList.toggle('theme-light', !dark);
            });
        } catch (e) { /* 跨域时静默失败 */ }
    }
    detect();
    setTimeout(detect, 60);
    setTimeout(detect, 200);
    // 持续轮询：用户在 light/dark 间切换时（Streamlit 复用 iframe 不重载组件），也能跟随切换
    setInterval(detect, 800);
})();
</script>
"""


def _esc_html(s):
    """HTML 转义，避免维度值中的特殊字符破坏表格。"""
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def bucket_path(upload_type):
    safe = upload_type.replace("&", "_").replace(" ", "")
    return os.path.join(DATA_DIR, f"data_{safe}.parquet")


def _bucket_mtime(upload_type):
    """数据桶文件 mtime，用于让缓存随数据上传/清理自动失效。"""
    p = bucket_path(upload_type)
    return os.path.getmtime(p) if os.path.exists(p) else 0


def _invalidate_fcst_cache():
    """数据桶或 Mapping 发生任何写入后调用，强制 FCST 分析相关缓存失效。

    作用：左侧数据源（上传 / 删除指定 Cycle / 清空数据池 / 重传）变化后，
    右侧 FCST 分析（KPI 看板、层级树表、by Week 趋势）立即反映最新数据，
    不再出现“旧周次残留（如删掉的 Week2-5 还在）”或“重传后不更新”的问题。

    同时清掉两层缓存：
    - session_state 中上次计算结果的缓存（key 含桶 mtime）
    - @st.cache_data 缓存的底层聚合数据（_load_fcst_analysis_data）
    """
    try:
        st.session_state.pop("fcst_cache_key", None)
    except Exception:  # noqa
        pass
    try:
        st.session_state.pop("fcst_result", None)
    except Exception:  # noqa
        pass
    try:
        _load_fcst_analysis_data.clear()
    except Exception:  # noqa
        pass


def week_sort_key(w):
    """将 'Week10' 解析为可比较的整数 10，用于自然升序排序。"""
    nums = re.findall(r"\d+", str(w))
    return int(nums[0]) if nums else 0


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
    # 任何数据桶写入都让 FCST 分析缓存失效，保证右侧实时刷新
    _invalidate_fcst_cache()


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
    # Mapping 写入同样影响 FCST 分析的 DG%/Quota%/YOY% 计算，需失效缓存
    _invalidate_fcst_cache()


def apply_mapping(df, mapping_df):
    """根据 Mapping 表添加/更新列：产线大类、纯产线大类、非纯产线大类、展示顺序。

    规则：
    - 产线大类：产线+通路 匹配 Mapping 第一列 → 返回第二列。
    - 纯产线大类：产线名称 匹配 Mapping 第一列 → 返回第二列。
    - 非纯产线大类：POS_APOS=POS 且 物料通路 为 HB/STB/JV 时，返回 物料通路；
      否则返回 纯产线大类。
    - 展示顺序：Mapping 含第三列时，按第一列映射到数值顺序，用于 FCST 分析
      中产线大类升序展示；无该列时返回 NaN，后续按名称兜底排序。
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

    # 展示顺序：兼容旧 Mapping（无该列）和新 Mapping（第三列为展示顺序）
    if "展示顺序" in mapping_df.columns:
        order_col = "展示顺序"
    elif len(mapping_df.columns) >= 3:
        order_col = mapping_df.columns[2]
    else:
        order_col = None
    if order_col:
        order_dict = mapping_df.set_index(key_col)[order_col].to_dict()
        out["展示顺序"] = pd.to_numeric(
            out["产线+通路"].astype(str).str.strip().map(order_dict),
            errors="coerce",
        )
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
def _load_fcst_analysis_data(fy, _mapping_mtime_value, _fcst_mt, _dgq_mt, _hist_mt):
    """按财年财季一次性加载 FCST / DG&Quota / 历史Union 并聚合。

    这是 FCST 分析最耗时的部分（读 Parquet + apply_mapping + groupby），
    缓存后切换 Cycle / 范围 / 战区都只需切片和构建展示表，响应更快。

    缓存 key 包含 Mapping 与三个数据桶的 mtime：任何一次上传/清理都会
    改写对应 parquet，使 mtime 变化，从而自动失效缓存，避免拿到旧数据
    （例如新上传的 Week 显示为 0）。
    """
    fcst = load_bucket("FCST")
    mapping_df = load_mapping()
    fcst = apply_mapping(fcst, mapping_df)
    # 需求 3：确保 Core/Memoline 列存在（兼容旧桶未含该列的情况）
    if "Core/Memoline" not in fcst.columns:
        fcst = add_core_memoline(fcst)
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
    fcst, dg_map, q_map, h_map, h_map_pl = _load_fcst_analysis_data(
        fy, _mapping_mtime(), _bucket_mtime("FCST"), _bucket_mtime("DG&Quota"), _bucket_mtime("历史Union")
    )

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
    """按 TTL → Solutions/Services → 产线大类 → 大区 → 客户 构建层级树表。

    - TTL/Solutions/Services 汇总行：计算 DG%/Quota%/YOY%。
    - 产线大类/大区/客户行：只展示 当前FCST、上版FCST、WTW，不计算比率。
    - 产线大类按 Mapping 中的「展示顺序」升序排列（无顺序时按名称兜底）。
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
        add_row(pos_id, "", 0, _pos_display_label(pos_label), c_pos, m_pos, calc_ratio=True, calc_yoy=True)
        # 子行 unique 用 (当前 ∪ 对比) 并集，保证"对比版有但当前版没了"的
        # 分类（如被砍掉的产线大类、流失的大区）也能渲染出来，做到
        # 父行金额 = 所有子行金额之和，所有 WTW 差异都看得见。
        # 同时 fillna("（未匹配）") + replace("", "（未匹配）") 统一 NaN/空串，
        # 避免未匹配行被静默漏掉导致父子对不上。
        c_pls = c_pos["产线大类"].fillna("（未匹配）").replace("", "（未匹配）")
        m_pls = m_pos["产线大类"].fillna("（未匹配）").replace("", "（未匹配）")
        # 产线大类按 Mapping 展示顺序升序；未匹配放到最后
        all_pls = list(set(c_pls) | set(m_pls))
        if all_pls:
            c_order = (
                c_pos.groupby("产线大类")["展示顺序"].min()
                if "展示顺序" in c_pos.columns
                else pd.Series(dtype=float)
            )
            m_order = (
                m_pos.groupby("产线大类")["展示顺序"].min()
                if "展示顺序" in m_pos.columns
                else pd.Series(dtype=float)
            )
            def _pl_sort_key(pl):
                if pl == "（未匹配）":
                    return (999999, pl)
                o = min(
                    c_order.get(pl, float("nan")) if isinstance(c_order, pd.Series) else float("nan"),
                    m_order.get(pl, float("nan")) if isinstance(m_order, pd.Series) else float("nan"),
                )
                if pd.isna(o):
                    return (999998, pl)
                return (int(o), pl)
            all_pls = sorted(all_pls, key=_pl_sort_key)
        for pl in all_pls:
            pl_id = f"{pos_id}_pl_{pl}"
            c_pl = c_pos[c_pls == pl]
            m_pl = m_pos[m_pls == pl]
            add_row(pl_id, pos_id, 1, pl, c_pl, m_pl)
            c_rs = c_pl["服务大区"].fillna("（未匹配）").replace("", "（未匹配）")
            m_rs = m_pl["服务大区"].fillna("（未匹配）").replace("", "（未匹配）")
            for r in sorted(set(c_rs) | set(m_rs)):
                r_id = f"{pl_id}_r_{r}"
                c_r = c_pl[c_rs == r]
                m_r = m_pl[m_rs == r]
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
        f'<div class="{_theme_cls()}">'
        '<table class="tree-table"><thead><tr>' + header_html + '</tr></thead><tbody>'
        + "".join(rows_html) + '</tbody></table>'
        '</div>'
    )

    css = """
    <style>
    .tree-table { width: 100%; table-layout: fixed; border-collapse: collapse; font-family: "Source Sans Pro", sans-serif; font-size: 14px; color: #31333F; }
    .tree-table * { box-sizing: border-box; }
    .tree-table th, .tree-table td { padding: 8px 10px; border-bottom: 1px solid #e6e6e6; vertical-align: middle; }
    .tree-table th { position: sticky; top: 0; text-align: left; background: #f7f7f8; font-weight: 600; border-bottom: 2px solid #cfcfcf; z-index: 2; }
    .tree-table td.num { text-align: right; font-variant-numeric: tabular-nums; white-space: nowrap; }
    /* 数值列（当前FCST/上版FCST/WTW/DG%/Quota%/YOY%）表头与数据同向右对齐，避免串位 */
    .tree-table th:nth-child(n+2) { text-align: right; }
    .tree-table td.up { color: #0f9d00; font-weight: 600; }
    .tree-table td.down { color: #d93025; font-weight: 600; }
    .tree-table th:nth-child(1), .tree-table td:nth-child(1) { width: 32%; min-width: 260px; }
    .tree-table th:nth-child(n+2), .tree-table td:nth-child(n+2) { width: 11.333%; }
    .tree-label-inner { display: flex; align-items: center; gap: 6px; }
    .tree-toggle { cursor: pointer; width: 16px; display: inline-flex; align-items: center; justify-content: center; color: #666; user-select: none; font-size: 12px; }
    .tree-toggle:hover { color: #000; }
    .tree-spacer { width: 16px; display: inline-block; }
    .tree-text { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .tree-row:hover { background: #f2f4f8; }
    .tree-row.level-0 { font-weight: 700; background: #fff; }
    .tree-row.level-0 td:first-child { box-shadow: inset 4px 0 #ff4b4b; }
    .tree-row.level-1 { font-weight: 600; color: #333; }
    .tree-row.level-1 td:first-child { box-shadow: inset 4px 0 #83c9ff; }
    .tree-row.level-2 { color: #444; }
    .tree-row.level-2 td:first-child { box-shadow: inset 4px 0 #cfcfcf; }
    .tree-row.level-3 { color: #555; }
    .tree-row.level-3 td:first-child { box-shadow: inset 4px 0 #eaeaea; }
    .tree-row.level-2 .tree-text, .tree-row.level-3 .tree-text { font-size: 12px; }
    /* 深色模式（显式 class：Streamlit 应用内切黑色主题时也能生效，不依赖 OS prefers-color-scheme） */
    .theme-dark .tree-table { color: #f5f5f5; background: #0e1117; }
    .theme-dark .tree-table th { background: #262730; color: #f5f5f5; border-bottom-color: #48484f; }
    .theme-dark .tree-table td { border-bottom-color: #36363f; }
    .theme-dark .tree-row.level-0 { background: #171922; }
    .theme-dark .tree-row.level-1 { color: #ffffff; }
    .theme-dark .tree-row.level-2 { color: #ededed; }
    .theme-dark .tree-row.level-3 { color: #d6d6d6; }
    .theme-dark .tree-toggle { color: #c4c4c4; }
    .theme-dark .tree-toggle:hover { color: #ffffff; }
    .theme-dark .tree-row:hover { background: #262732; }
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
                // 展开/收起后重算 iframe 高度，避免残留空白或内容被裁切
                setTimeout(adjustIframeHeight, 0);
                setTimeout(adjustIframeHeight, 60);
            });
        });

        function adjustIframeHeight() {
            var wrap = document.querySelector('.tree-table-wrap') || document.querySelector('.tree-table');
            if (!wrap) return;
            var newHeight = wrap.offsetHeight + 8;
            var frame = window.frameElement;
            if (frame) { frame.style.height = newHeight + 'px'; return; }
            try {
                var iframes = window.parent.document.querySelectorAll('iframe');
                for (var i = 0; i < iframes.length; i++) {
                    if (iframes[i].contentWindow === window) {
                        iframes[i].style.height = newHeight + 'px';
                        return;
                    }
                }
            } catch (err) { /* 跨源时静默失败 */ }
        }
        adjustIframeHeight();
        setTimeout(adjustIframeHeight, 50);
        setTimeout(adjustIframeHeight, 150);
    })();
    </script>
    """
    return css + table_html + js + _THEME_RUNTIME_JS


def _render_tree_table(df):
    """用 Streamlit HTML 组件渲染可折叠树表（高度随可见行数自适应，展开/收起由 JS 动态调整）。"""
    import streamlit.components.v1 as components
    html = _build_tree_html(df)
    # 初始可见行：level<2（TTL/APOS/POS、产线大类）；JS 加载后按实际内容精确调整
    try:
        visible_rows = int((pd.to_numeric(df["level"], errors="coerce") < 2).sum())
    except Exception:  # noqa
        visible_rows = len(df)
    visible_rows = max(1, visible_rows)
    components.html(html, height=46 + visible_rows * 36 + 10, scrolling=False)


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
    /* 深色模式（由运行时 JS 检测父页面背景后自动切换） */
    .theme-dark .kpi-card {{ background: #1f212b; border-color: #36363f; box-shadow: none; }}
    .theme-dark .kpi-title {{ color: #9aa0a6; }}
    .theme-dark .kpi-value {{ color: #f5f5f5; }}
    .theme-dark .kpi-sub {{ color: #80868b; }}
    </style>
    <div class="kpi-board">{cols_html}</div>
    {_THEME_RUNTIME_JS}
    """
    components.html(html, height=120)


def _render_fcst_controls_inline():
    """在吸顶容器内渲染 FCST 维度/范围控件（财年、当前/对比 Cycle、范围、战区）。
    返回选中的 (fy, cur_cycle, cmp_cycle, scope, sub_region)；若数据池为空，返回 None。
    极紧凑单行布局：5 个 selectbox 横向排开，无标签。"""
    meta = load_bucket_columns("FCST", ["财年财季", "FCST Cycle", "服务大区", "服务战区"])
    if meta.empty:
        st.warning("FCST 数据池为空，请先在左侧上传 FCST 数据。")
        return None
    fy_opts = sorted(meta["财年财季"].dropna().unique().tolist())
    cyc_opts = sorted(meta["FCST Cycle"].dropna().unique().tolist(), key=week_sort_key)
    region_opts = sorted(meta["服务大区"].dropna().unique().tolist())
    all_scope = ["TTL"] + region_opts

    c1, c2, c3, c4, c5 = st.columns([2, 2, 2, 2, 2])
    with c1:
        fy = st.selectbox("财年", fy_opts, index=len(fy_opts) - 1, key="fcst_fy", label_visibility="collapsed")
    with c2:
        cur_cycle = st.selectbox("当前 Cycle", cyc_opts, index=len(cyc_opts) - 1, key="fcst_cur", label_visibility="collapsed")
    with c3:
        cmp_cycle = st.selectbox("对比 Cycle", cyc_opts, index=max(0, len(cyc_opts) - 2), key="fcst_cmp", label_visibility="collapsed")
    with c4:
        scope = st.selectbox("范围", all_scope, index=0, key="fcst_scope", label_visibility="collapsed")
    with c5:
        if scope != "TTL":
            sub_opts = sorted(meta[meta["服务大区"] == scope]["服务战区"].dropna().unique().tolist())
            sub_sel = st.selectbox("战区", ["（合计）"] + sub_opts, index=0, key="fcst_sub", label_visibility="collapsed")
            sub_region = None if sub_sel == "（合计）" else sub_sel
        else:
            sub_region = None
            st.markdown(
                "<span style='font-size:0.5rem;color:#aaa;'>范围=TTL 时不选战区</span>",
                unsafe_allow_html=True,
            )

    return fy, cur_cycle, cmp_cycle, scope, sub_region


def _render_fcst_modules(fy, cur_cycle, cmp_cycle, scope, sub_region):
    """FCST 分析三大模块：差异分析、by Week 趋势对比、Core MIX 分析。
    通过顶部固定容器中的 fcst_module tab 切换，仅渲染当前选中的模块，
    避免模块标题随滚动被覆盖，也避免展开客户明细后与其他模块位置重叠。
    """
    fcst_module = st.session_state.get("fcst_module", "差异分析")

    st.caption(
        f"财年财季 {fy} ｜ 当前 {cur_cycle} vs 对比 {cmp_cycle} ｜ "
        f"范围 {scope}{(' / ' + sub_region) if sub_region else ''}"
    )

    if fcst_module == "差异分析":
        # 用 session_state 缓存上次计算结果，左侧导出设置变化时不重新算 FCST
        # 注意：cache_key 必须包含三个数据桶的 mtime，否则上传/清理数据后
        # session 缓存仍会返回旧的（=0 的）结果，表现为"选了 Week10 还是 0"。
        cache_key = (
            f"{fy}|{cur_cycle}|{cmp_cycle}|{scope}|{sub_region}"
            f"|{_mapping_mtime()}"
            f"|{_bucket_mtime('FCST')}|{_bucket_mtime('DG&Quota')}|{_bucket_mtime('历史Union')}"
        )
        res = st.session_state.get("fcst_result")
        if st.session_state.get("fcst_cache_key") != cache_key or res is None:
            with st.spinner("正在计算 FCST 分析…"):
                res = compute_fcst(fy, cur_cycle, cmp_cycle, scope, sub_region)
            st.session_state["fcst_result"] = res
            st.session_state["fcst_cache_key"] = cache_key
        with st.container(border=True):
            st.markdown('<div id="fcst-module-marker" style="display:none;"></div>', unsafe_allow_html=True)
            _render_kpi_dashboard(res["ttl_summary"])
            _render_tree_table(res["main_table"])

    elif fcst_module == "FCST by Week 趋势":
        with st.container(border=True):
            st.markdown('<div id="fcst-trend-marker" style="display:none;"></div>', unsafe_allow_html=True)
            _render_fcst_trend(fy, scope, sub_region)

    elif fcst_module == "Core MIX 分析":
        with st.container(border=True):
            st.markdown('<div id="fcst-coremix-marker" style="display:none;"></div>', unsafe_allow_html=True)
            _render_core_mix(fy, scope, sub_region, cur_cycle)
            # by Week Core MIX：按 FCST Cycle 展示每个周 Core/Memoline 金额及 Core MIX
            st.markdown(
                "<div style='margin-top:0.5rem;padding-top:0.4rem;border-top:1px dashed #ccc;'>"
                "<b style='font-size:0.85rem;'>by Week Core MIX</b></div>",
                unsafe_allow_html=True,
            )
            _render_core_mix_by_week(fy, scope, sub_region, cur_cycle)


def _render_fcst_trend(fy, scope, sub_region):
    """在所选财年财季下，按 FCST Cycle（Week）以表格式展示趋势：

    - 列：口径 | Week1 | Week2 | ... | WeekN | Trend
    - 行：TTL、Solutions（可折叠）、Services（可折叠）及各自大客户
    - 每个 Week 都显示金额；最右侧为对应 sparkline
    - 默认折叠大客户明细，避免收起后下方大片留白
    """
    import re
    import streamlit.components.v1 as components

    fcst, _, _, _, _ = _load_fcst_analysis_data(
        fy, _mapping_mtime(), _bucket_mtime("FCST"), _bucket_mtime("DG&Quota"), _bucket_mtime("历史Union")
    )
    df = fcst.copy()
    if scope != "TTL":
        df = df[df["服务大区"] == scope]
    if sub_region:
        df = df[df["服务战区"] == sub_region]
    df = df[df["POS_APOS"].isin(["APOS", "POS"])]
    if df.empty:
        st.info("所选范围内无 FCST 数据，无法绘制趋势。")
        return

    # 仅 strip，保留空客户名（HB/JV 等数据源无客户的行仍计入父行总额，
    # 但大客户明细中会被剔除，不展示空名行）
    df["客户名称"] = df["客户名称"].fillna("").astype(str).str.strip()

    def _week_key(w):
        nums = re.findall(r"\d+", str(w))
        return int(nums[0]) if nums else 0

    weeks = sorted(df["FCST Cycle"].dropna().unique().astype(str).tolist(), key=_week_key)

    def weekly_series(src_df):
        return src_df.groupby("FCST Cycle")["业绩考核USDK"].sum().reindex(weeks, fill_value=0).values

    def get_big_customers(pos_df, threshold):
        # 剔除客户名称为空的行（如 HB/JV 等数据源无客户的行），仅影响大客户明细展示；
        # 这些金额已计入父行 TTL/Solutions/Services 总额，不受剔除影响。
        pos_df = pos_df[pos_df["客户名称"].astype(str).str.strip() != ""]
        if pos_df.empty:
            return []
        cust_cycle = pos_df.groupby(["客户名称", "FCST Cycle"])["业绩考核USDK"].sum().reset_index()
        # 判定标准：任一 Week 该客户合计金额 > 阈值（默认 500 = 500K USDK）即列为大客户
        big = cust_cycle[cust_cycle["业绩考核USDK"] > threshold]["客户名称"].unique().tolist()
        cust_total = pos_df.groupby("客户名称")["业绩考核USDK"].sum()
        # 按总合计金额降序排列，便于优先展示头部客户
        big = sorted(big, key=lambda c: cust_total.get(c, 0), reverse=True)
        if not big:
            # 无客户达标时，回退展示金额最高的前 10 个客户
            big = cust_total.sort_values(ascending=False).head(10).index.tolist()
        # 注意：不再截断前 15 名，保证满足阈值的大客户完整列出
        return big

    c1, c2, c3, c4 = st.columns([3, 2, 2, 1])
    with c1:
        st.markdown(
            "<small>每行展示各 Week 金额及趋势；用右侧开关控制 Solutions / Services 大客户明细。</small>",
            unsafe_allow_html=True,
        )
    with c2:
        show_solutions_cust = st.toggle("Solutions 客户", value=False, key="trend_show_apos_cust")
    with c3:
        show_services_cust = st.toggle("Services 客户", value=False, key="trend_show_pos_cust")
    with c4:
        # 阈值：数据源单位为 USDK，1000K 对应数值 1000
        threshold = st.number_input(
            "大客户阈值",
            min_value=0,
            value=1000,
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

    def build_group(group_id, label, pos_df, color, expanded):
        pos_vals = weekly_series(pos_df)
        toggle_icon = "▼" if expanded else "▶"
        rows_html.append(
            f'<tr class="row-main row-{group_id}" data-id="{group_id}">'
            f'<td class="label main-label">'
            f'<span class="tree-toggle" data-target="{group_id}">{toggle_icon}</span>{label}</td>'
            f'{make_cells(pos_vals)}'
            f'<td class="trend">{sparkline(pos_vals, color)}</td></tr>'
        )
        if not expanded:
            return
        for i, cust in enumerate(get_big_customers(pos_df, threshold)):
            vals = weekly_series(pos_df[pos_df["客户名称"] == cust])
            cust_color = cust_colors[i % len(cust_colors)]
            rows_html.append(
                f'<tr class="row-cust child-{group_id}" data-parent="{group_id}">'
                f'<td class="label sub-label"><span class="tree-spacer"></span>{cust}</td>'
                f'{make_cells(vals)}'
                f'<td class="trend">{sparkline(vals, cust_color)}</td></tr>'
            )

    # Solutions + customers
    build_group("apos", "Solutions", df[df["POS_APOS"] == "APOS"], "#0068c9", show_solutions_cust)
    # Services + customers
    build_group("pos", "Services", df[df["POS_APOS"] == "POS"], "#ff4b4b", show_services_cust)

    week_headers = "".join([f'<th class="num week-header">{w}</th>' for w in weeks])
    html = f"""
    <style>
    .trend-table-wrap {{ overflow-x: auto; }}
    .trend-hier-table {{ width: 100%; table-layout: fixed; border-collapse: collapse; font-family: "Source Sans Pro", sans-serif; font-size: 11px; color: #31333F; }}
    .trend-hier-table * {{ box-sizing: border-box; }}
    .trend-hier-table th, .trend-hier-table td {{ padding: 5px 6px; border-bottom: 1px solid #f0f0f0; vertical-align: middle; }}
    .trend-hier-table th {{ position: sticky; top: 0; background: #f7f7f8; font-weight: 600; white-space: nowrap; }}
    .trend-hier-table th:nth-child(1) {{ text-align: left; width: 220px; min-width: 220px; }}
    .trend-hier-table th:nth-child(n+2) {{ text-align: right; }}
    .trend-hier-table th:nth-child(n+2):not(:last-child) {{ width: calc((100% - 310px) / {len(weeks)}); min-width: 58px; }}
    .trend-hier-table th:last-child {{ width: 90px; text-align: center; }}
    .trend-hier-table td.num {{ text-align: right; font-variant-numeric: tabular-nums; white-space: nowrap; font-size: 10px; }}
    .trend-hier-table td.label {{ white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
    .trend-hier-table td.trend {{ text-align: center; }}
    /* 注意：td 不能用 display:flex，否则单元格高度脱离表格行高导致横线窜位 */
    .trend-hier-table .main-label {{ font-weight: 600; font-size: 12px; }}
    .trend-hier-table .sub-label {{ padding-left: 20px; font-size: 11px; color: #555; }}
    .trend-hier-table .tree-toggle {{ margin-right: 5px; }}
    .trend-hier-table .tree-spacer {{ display: inline-block; }}
    .trend-hier-table .row-main {{ background: #fff; }}
    .trend-hier-table .row-cust {{ background: #fafafa; }}
    .trend-hier-table .row-ttl .main-label {{ color: #31333F; }}
    .trend-hier-table .row-apos .main-label {{ color: #0068c9; }}
    .trend-hier-table .row-pos .main-label {{ color: #ff4b4b; }}
    .trend-hier-table tr:hover {{ background: #f5f5f5; }}
    .tree-toggle {{ cursor: default; width: 12px; display: inline-flex; align-items: center; justify-content: center; color: #666; user-select: none; font-size: 10px; }}
    .tree-spacer {{ width: 12px; display: inline-block; }}
    .spark-svg {{ width: 80px; height: 24px; display: block; margin: 0 auto; }}
    /* 深色模式（显式 class：Streamlit 应用内切黑色主题时也能生效，不依赖 OS prefers-color-scheme） */
    .theme-dark .trend-hier-table {{ color: #f5f5f5; }}
    .theme-dark .trend-hier-table th {{ background: #262730; border-bottom-color: #48484f; color: #f5f5f5; }}
    .theme-dark .trend-hier-table td {{ border-bottom-color: #36363f; }}
    .theme-dark .trend-hier-table .row-main {{ background: #171922; }}
    .theme-dark .trend-hier-table .row-cust {{ background: #1f212b; }}
    .theme-dark .trend-hier-table .row-ttl .main-label {{ color: #ffffff; }}
    .theme-dark .trend-hier-table .sub-label {{ color: #dcdcdc; }}
    .theme-dark .trend-hier-table .row-apos .main-label {{ color: #6ab7ff; }}
    .theme-dark .trend-hier-table .row-pos .main-label {{ color: #ff7a7a; }}
    .theme-dark .tree-toggle {{ color: #c4c4c4; }}
    .theme-dark .trend-hier-table tr:hover {{ background: #262732; }}
    /* dark 主题下 TTL 行 sparkline 由深灰改为浅色，避免看不清 */
    .theme-dark .trend-hier-table .row-ttl .spark-svg polyline {{ stroke: #f5f5f5; }}
    .theme-dark .trend-hier-table .row-ttl .spark-svg circle {{ fill: #f5f5f5; }}
    </style>
    <div class="trend-table-wrap {_theme_cls()}">
    <table class="trend-hier-table">
    <thead><tr><th class="label">口径</th>{week_headers}<th class="trend">Trend</th></tr></thead>
    <tbody>{"".join(rows_html)}</tbody>
    </table>
    </div>
    <script>
    (function() {{
        function adjustIframeHeight() {{
            var wrap = document.querySelector('.trend-table-wrap');
            if (!wrap) return;
            var newHeight = wrap.offsetHeight + 6;
            // 优先用 frameElement（srcdoc 同源 iframe），失败则回退到 parent 中查找
            var frame = window.frameElement;
            if (frame) {{
                frame.style.height = newHeight + 'px';
                return;
            }}
            try {{
                var iframes = window.parent.document.querySelectorAll('iframe');
                for (var i = 0; i < iframes.length; i++) {{
                    if (iframes[i].contentWindow === window) {{
                        iframes[i].style.height = newHeight + 'px';
                        return;
                    }}
                }}
            }} catch (e) {{ /* 跨源时静默失败 */ }}
        }}
        // 初始及布局稳定后多次重算高度，确保 iframe 与内容完全贴合、无留白
        adjustIframeHeight();
        setTimeout(adjustIframeHeight, 50);
        setTimeout(adjustIframeHeight, 150);
        setTimeout(adjustIframeHeight, 300);
    }})();
    </script>
    {_THEME_RUNTIME_JS}
    """

    # 初始高度按实际渲染行数估算；JS 在加载后会再次精确调整，避免展开/收起后残留空白。
    base_height = 80
    row_height = 28
    components.html(html, height=base_height + len(rows_html) * row_height, scrolling=False)


def _load_core_mix_base(fy, fcst_mt, dgq_mt, hist_mt):
    """Core MIX 基础数据加载（按数据桶 mtime 缓存）：
    - concat FCST + 历史Union
    - 统一补算 Core/Memoline
    - 过滤 FY + 仅 APOS/POS
    返回已处理好的 DataFrame。
    """
    fcst, _, _, _, _ = _load_fcst_analysis_data(fy, _mapping_mtime(), fcst_mt, dgq_mt, hist_mt)
    parts = [fcst]
    try:
        hist = load_bucket("历史Union")
        if not hist.empty:
            for col in ("财年财季", "服务大区", "POS_APOS", "物料通路", "业绩考核USDK", "FCST Cycle"):
                if col not in hist.columns:
                    hist[col] = None
            parts.append(hist)
    except Exception:
        pass
    df = pd.concat(parts, ignore_index=True, sort=False)
    df = add_core_memoline(df)
    if "财年财季" in df.columns and fy:
        df = df[df["财年财季"] == fy]
    if "POS_APOS" in df.columns:
        df = df[df["POS_APOS"].isin(["APOS", "POS"])]
    if "业绩考核USDK" in df.columns:
        df["业绩考核USDK"] = pd.to_numeric(df["业绩考核USDK"], errors="coerce").fillna(0)
    if "Core/Memoline" in df.columns:
        df["_core_amt"] = df["业绩考核USDK"].where(df["Core/Memoline"] == "Core", 0.0)
        df["_memo_amt"] = df["业绩考核USDK"].where(df["Core/Memoline"] == "Memoline", 0.0)
    return df


def _render_core_mix(fy, scope, sub_region, cur_cycle):
    """Core MIX 棋盘格分析：Core 金额 ÷ (Core + Memoline) 金额。

    - 纵向/横向维度可交互选择（大区、区域、纯产线大类、产线名称、产品大类、SPL）。
    - 数据范围：聚合所选 FY + 范围内【全部周 + FCST/ACT(历史Union)】的 Core/Memoline 金额，
      确保新增的 Core/Memoline 逻辑对所有明细数据生效（不局限于当前 Cycle）。
    - 行：纵向维度值按 Core MIX 降序；超过总体 Core MIX 的维度下方用绿色虚线分隔。
    - 列：横向维度值 + 汇总 Core MIX 列。
    - 单元格颜色：> 汇总深绿+加粗白字，≤ 汇总浅绿+普通深色字。
    """
    import streamlit.components.v1 as components

    df = _load_core_mix_base(
        fy, _bucket_mtime("FCST"), _bucket_mtime("DG&Quota"), _bucket_mtime("历史Union")
    )
    if scope != "TTL" and "服务大区" in df.columns:
        df = df[df["服务大区"] == scope]
    if sub_region and "服务战区" in df.columns:
        df = df[df["服务战区"] == sub_region]
    # 棋盘格只展示选中的当前 Cycle（顶部"当前 FCST Cycle"选哪个就显示哪个）
    if "FCST Cycle" in df.columns and cur_cycle:
        df = df[df["FCST Cycle"] == cur_cycle]
    if df.empty:
        st.info("所选 Cycle/范围内无有效 Core/Memoline 数据。")
        return

    st.markdown(
        "<small>Core MIX = Core 金额 ÷ (Core + Memoline) 金额；选择纵向/横向维度，生成可交互棋盘格。</small>",
        unsafe_allow_html=True,
    )

    DIM_LABELS = {
        "服务大区": "大区",
        "服务战区": "区域",
        "纯产线大类": "纯产线大类",
        "产线名称": "产线名称",
        "产品大类": "产品大类",
        "SPL名称": "SPL",
    }
    DIM_KEYS = list(DIM_LABELS.keys())

    # 读取当前选择；若两边相同则自动切换，避免 groupby 重复列报错
    v_default = st.session_state.get("core_mix_vertical", "服务大区")
    h_default = st.session_state.get("core_mix_horizontal", "产线名称")
    if h_default == v_default:
        h_default = next((k for k in DIM_KEYS if k != v_default), "产线名称")

    c1, c2 = st.columns(2)
    with c1:
        v_options = [k for k in DIM_KEYS if k != h_default]
        vertical_dim = st.selectbox(
            "纵向维度",
            v_options,
            format_func=lambda x: DIM_LABELS[x],
            index=v_options.index(v_default) if v_default in v_options else 0,
            key="core_mix_vertical",
        )
    with c2:
        h_options = [k for k in DIM_KEYS if k != vertical_dim]
        horizontal_dim = st.selectbox(
            "横向维度",
            h_options,
            format_func=lambda x: DIM_LABELS[x],
            index=h_options.index(h_default) if h_default in h_options else 0,
            key="core_mix_horizontal",
        )

    # 数据范围：聚合所选范围内【全部周 + FCST/ACT】的 Core/Memoline（不局限于当前 Cycle）
    # 仅 Core/Memoline 有值的行参与计算（直接复用缓存的 _core_amt / _memo_amt）
    valid = df[df["Core/Memoline"].isin(["Core", "Memoline"])].copy()
    valid["_core"] = valid["_core_amt"]
    valid["_memo"] = valid["_memo_amt"]

    def _mix(core, memo):
        s = core + memo
        return (core / s * 100) if s > 0 else None

    total_core = float(valid["_core"].sum())
    total_memo = float(valid["_memo"].sum())
    total_mix = _mix(total_core, total_memo) or 0.0

    # 横向维度列（按自身 Core MIX 降序）
    h_grp = valid.groupby(horizontal_dim, dropna=False).agg(_core=("_core", "sum"), _memo=("_memo", "sum")).reset_index()
    h_grp["mix"] = h_grp.apply(lambda r: _mix(r["_core"], r["_memo"]), axis=1)
    h_grp = h_grp[h_grp["_core"] + h_grp["_memo"] > 0].sort_values("mix", ascending=False, na_position="last").reset_index(drop=True)

    # 纵向维度行（按自身 Core MIX 降序）
    v_grp = valid.groupby(vertical_dim, dropna=False).agg(_core=("_core", "sum"), _memo=("_memo", "sum")).reset_index()
    v_grp["mix"] = v_grp.apply(lambda r: _mix(r["_core"], r["_memo"]), axis=1)
    v_grp = v_grp[v_grp["_core"] + v_grp["_memo"] > 0].sort_values("mix", ascending=False, na_position="last").reset_index(drop=True)

    # 计算每个纵向×横向交叉的 Core MIX
    cross = valid.groupby([vertical_dim, horizontal_dim], dropna=False).agg(_core=("_core", "sum"), _memo=("_memo", "sum")).reset_index()
    cross["mix"] = cross.apply(lambda r: _mix(r["_core"], r["_memo"]), axis=1)
    cross_idx = cross.set_index([vertical_dim, horizontal_dim])["mix"].to_dict()

    m1, m2, m3 = st.columns(3)
    m1.metric("总体 Core MIX", f"{total_mix:.0f}%")
    m2.metric("Core 金额合计", f"{int(round(total_core)):,}")
    m3.metric("Memoline 金额合计", f"{int(round(total_memo)):,}")

    if v_grp.empty or h_grp.empty:
        st.info("所选维度下无有效 Core/Memoline 数据。")
        return

    def _mix_color(pct, cutoff):
        """Core MIX 颜色：以 cutoff（总 Core MIX）为分界，> cutoff 用深绿+加粗白色文字（明显突出），
        < cutoff 用浅绿+普通深色文字；None 用灰底灰字。"""
        if pct is None or pd.isna(pct):
            return ("#f5f5f5", "#999999", False)
        p = max(0.0, min(100.0, float(pct)))
        is_above = p > cutoff
        if is_above:
            # 深绿系 + 加粗白色：更醒目地表示超过汇总
            t = (p - cutoff) / max(100.0 - cutoff, 1.0) if cutoff < 100 else p / 100.0
            t = max(0.0, min(1.0, t))
            # 起点偏深绿 #1a6b1a，饱和度随 t 略增
            r = int(26 + (1 - t) * 10)
            g = int(107 + (1 - t) * 15)
            b = int(26 + (1 - t) * 10)
            bg = f"#{r:02x}{g:02x}{b:02x}"
            fg = "#ffffff"
            return (bg, fg, True)
        else:
            # 浅绿系 + 普通深色文字
            t = p / max(cutoff, 1.0) if cutoff > 0 else p / 100.0
            t = max(0.0, min(1.0, t))
            r = int(245 - t * 35)
            g = int(250 - t * 20)
            b = int(245 - t * 35)
            bg = f"#{r:02x}{g:02x}{b:02x}"
            fg = "#1a1a1a"
            return (bg, fg, False)

    def _fmt_pct(pct):
        return "—" if pct is None or pd.isna(pct) else f"{int(round(pct))}%"

    # 汇总行（置顶）：汇总列=总体，横向列=各横向维度自身 mix
    # 汇总行首列显示"汇总"标签（与汇总列标题对应）
    summary_cells = ['<td class="dim-label summary-label-cell"><b>汇总</b></td>']
    bg, fg, bold = _mix_color(total_mix, total_mix)
    fw = "bold" if bold else "normal"
    summary_cells.append(f'<td class="mix-cell mix-summary" style="background:{bg};color:{fg};font-weight:{fw}"><b>{_fmt_pct(total_mix)}</b></td>')
    for _, h in h_grp.iterrows():
        bg, fg, bold = _mix_color(h["mix"], total_mix)
        fw = "bold" if bold else "normal"
        summary_cells.append(f'<td class="mix-cell" style="background:{bg};color:{fg};font-weight:{fw}">{_fmt_pct(h["mix"])}</td>')

    row_html_list = []
    # 绿色虚线分隔位置：最后一个 mix > total_mix 的纵向维度之后
    cutoff_idx = -1
    for i, row in v_grp.iterrows():
        if (row["mix"] or 0) > total_mix:
            cutoff_idx = i

    n_cols = len(h_grp) + 2
    for i, row in v_grp.iterrows():
        v_val = row[vertical_dim]
        cells = [f'<td class="dim-label">{_esc_html(str(v_val))}</td>']
        # 汇总列 = 纵向维度自身 mix
        bg, fg, bold = _mix_color(row["mix"], total_mix)
        fw = "bold" if bold else "normal"
        cls = "mix-cell mix-summary mix-above" if bold else "mix-cell mix-summary mix-below"
        cells.append(f'<td class="{cls}" style="background:{bg};color:{fg};font-weight:{fw}">{_fmt_pct(row["mix"])}</td>')
        for _, h in h_grp.iterrows():
            h_val = h[horizontal_dim]
            mix = cross_idx.get((v_val, h_val), None)
            bg, fg, bold = _mix_color(mix, total_mix)
            fw = "bold" if bold else "normal"
            cls = "mix-cell mix-above" if bold else "mix-cell mix-below"
            cells.append(f'<td class="{cls}" style="background:{bg};color:{fg};font-weight:{fw}">{_fmt_pct(mix)}</td>')
        row_html_list.append("<tr>" + "".join(cells) + "</tr>")
        if i == cutoff_idx:
            row_html_list.append(
                f'<tr class="sep-row"><td colspan="{n_cols}"></td></tr>'
            )

    # 表头：汇总列标题不带 Core MIX（汇总行已显示 Core MIX%）
    h_header_cells = [f'<th class="dim-label">{DIM_LABELS[vertical_dim]}</th>', '<th class="mix-header mix-summary">汇总</th>']
    for _, h in h_grp.iterrows():
        h_val = h[horizontal_dim]
        mix = h["mix"]
        h_header_cells.append(f'<th class="mix-header">{_esc_html(str(h_val))}<br><span class="h-mix">{_fmt_pct(mix)}</span></th>')

    css = """
    <style>
    .core-mix-wrap { overflow: auto; max-height: 480px; }
    .core-mix-table { width: 100%; table-layout: fixed; border-collapse: separate; border-spacing: 0; font-family: "Source Sans Pro", sans-serif; font-size: 11px; color: #31333F; }
    .core-mix-table * { box-sizing: border-box; }
    .core-mix-table th, .core-mix-table td { padding: 3px 5px; border-bottom: 1px solid #e0e0e0; border-right: 1px solid #e0e0e0; vertical-align: middle; text-align: center; }
    .core-mix-table th:last-child, .core-mix-table td:last-child { border-right: none; }
    /* 表头吸顶 */
    .core-mix-table thead th { position: sticky; top: 0; background: #f7f7f8; font-weight: 600; z-index: 30; }
    .core-mix-table thead th.dim-label { z-index: 40; }
    .core-mix-table thead th.mix-summary { z-index: 40; }
    /* 汇总行吸顶（紧跟表头） */
    .core-mix-table tbody tr.summary-row td { position: sticky; top: 40px; background: #ffffff; z-index: 25; }
    .core-mix-table tbody tr.summary-row td.dim-label { background: #fafafa; z-index: 35; }
    .core-mix-table tbody tr.summary-row td.mix-summary { background: #ffffff; z-index: 35; }
    /* 首列（纵向维度）+ 第二列（汇总 Core MIX）左吸 */
    .core-mix-table td.dim-label, .core-mix-table th.dim-label { text-align: left; width: 130px; min-width: 130px; background: #fafafa; font-weight: 500; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; position: sticky; left: 0; z-index: 20; }
    .core-mix-table th.dim-label { background: #f7f7f8; }
    .core-mix-table td.mix-summary, .core-mix-table th.mix-summary { width: 80px; min-width: 80px; position: sticky; left: 130px; z-index: 20; background: inherit; }
    .core-mix-table th.mix-header:not(.mix-summary), .core-mix-table td.mix-cell:not(.mix-summary) { width: 80px; min-width: 80px; }
    .core-mix-table td.mix-cell { font-variant-numeric: tabular-nums; white-space: nowrap; }
    .core-mix-table .h-mix { font-size: 10px; color: #666; font-weight: 400; }
    .core-mix-table tr:hover td.mix-cell { filter: brightness(0.95); }
    .core-mix-table tr.sep-row td { border-top: 2px dashed #0f9d00; padding: 0; height: 0; background: transparent; }
    /* 深色模式（显式 class：Streamlit 应用内切黑色主题时也能生效，不依赖 OS prefers-color-scheme） */
    .theme-dark .core-mix-table { color: #f5f5f5; background: #0e1117; }
    .theme-dark .core-mix-table th { background: #262730; color: #f5f5f5; border-bottom-color: #48484f; border-right-color: #48484f; }
    .theme-dark .core-mix-table td { border-bottom-color: #36363f; border-right-color: #36363f; }
    .theme-dark .core-mix-table td.dim-label { background: #1b1d26; color: #f5f5f5; font-weight: 600; }
    .theme-dark .core-mix-table th.dim-label { background: #262730; color: #f5f5f5; }
    .theme-dark .core-mix-table td.mix-summary { background: #0e1117; color: #f5f5f5; }
    .theme-dark .core-mix-table tbody tr.summary-row td { background: #0e1117; }
    .theme-dark .core-mix-table tbody tr.summary-row td.dim-label { background: #1b1d26; }
    .theme-dark .core-mix-table .h-mix { color: #d0d0d0; }
    </style>
    """

    html = (
        css
        + '<div class="core-mix-wrap ' + _theme_cls() + '"><table class="core-mix-table"><thead><tr>'
        + "".join(h_header_cells)
        + "</tr></thead><tbody>"
        + '<tr class="summary-row">' + "".join(summary_cells) + "</tr>"
        + "".join(row_html_list)
        + "</tbody></table></div>"
        + _THEME_RUNTIME_JS
    )

    # 限高 + 内部滚动（表头与汇总行已 sticky 固定），避免 SPL/产线等长列表把页面撑得过长
    components.html(
        html,
        height=min(90 + (len(v_grp) + 2) * 26, 560),
        scrolling=False,
    )


def _render_core_mix_by_week(fy, scope, sub_region, cur_cycle=None):
    """by Week Core MIX 分析（跟随 Core MIX 棋盘格的纵向维度）：
    - 第一列：上方纵向维度所选维度的值（如 大区/区域/产线名称...）
    - 列：各 Week（FCST Cycle）的 Core MIX%
    - 最后一列：by week Trend 折线 sparkline（类似 by Week 趋势表的结构）
    - 首行：汇总（整体每周 Core MIX + trend）
    - 数据范围：所选 FY + scope/sub_region + 全部周（不按 cur_cycle 过滤）
    - 配色：以【当前 FCST Cycle 列的值】为基准——大于基准的单元格按绿色由深到浅渐变
      （越高于基准越深），小于基准的按橙色由浅到深渐变（越低于基准越深）。
    """
    import re as _re
    import streamlit.components.v1 as components

    df = _load_core_mix_base(
        fy, _bucket_mtime("FCST"), _bucket_mtime("DG&Quota"), _bucket_mtime("历史Union")
    )
    if scope != "TTL" and "服务大区" in df.columns:
        df = df[df["服务大区"] == scope]
    if sub_region and "服务战区" in df.columns:
        df = df[df["服务战区"] == sub_region]
    if df.empty or "FCST Cycle" not in df.columns:
        st.info("所选范围内无 by Week Core MIX 数据。")
        return

    # 跟随 Core MIX 棋盘格的纵向维度
    vertical_dim = st.session_state.get("core_mix_vertical", "服务大区")
    if vertical_dim not in df.columns:
        st.info(f"纵向维度 {vertical_dim} 不在数据中。")
        return

    st.markdown(
        f"<small style='color:#666;'>按 FCST Cycle 展示每个 Week 在【<b>{_esc_html(vertical_dim)}</b>】维度下的 Core MIX%；"
        "配色以【汇总行当前 Cycle 值】为基准（该单元格绿字加粗、背景随主题反色）："
        "≥基准=绿色渐变（值越大越深，白字加粗），&lt;基准=橙色渐变（值越小越深，黑字）；"
        "最后一列=各维度值的 by week Trend。</small>",
        unsafe_allow_html=True,
    )

    valid = df[df["Core/Memoline"].isin(["Core", "Memoline"])].copy()

    def _wk_key(w):
        nums = _re.findall(r"\d+", str(w))
        return int(nums[0]) if nums else 0

    def _mix(c, m):
        s = c + m
        return (c / s * 100) if s > 0 else None

    # 各周（自然序号排序）
    weeks = sorted(
        valid["FCST Cycle"].dropna().unique().astype(str).tolist(), key=_wk_key
    )
    if not weeks:
        st.info("所选范围内无 by Week Core MIX 数据。")
        return

    # 纵向维度各行（按自身整体 Core MIX 降序）
    v_grp = (
        valid.groupby(vertical_dim, dropna=False)
        .agg(_core=("_core_amt", "sum"), _memo=("_memo_amt", "sum"))
        .reset_index()
    )
    v_grp["mix"] = v_grp.apply(lambda r: _mix(r["_core"], r["_memo"]), axis=1)
    v_grp = v_grp[v_grp["_core"] + v_grp["_memo"] > 0].sort_values(
        "mix", ascending=False, na_position="last"
    ).reset_index(drop=True)

    # 维度值 × 周 交叉 Core MIX
    cross = (
        valid.groupby([vertical_dim, "FCST Cycle"], dropna=False)
        .agg(_core=("_core_amt", "sum"), _memo=("_memo_amt", "sum"))
        .reset_index()
    )
    cross["mix"] = cross.apply(lambda r: _mix(r["_core"], r["_memo"]), axis=1)
    cross_idx = cross.set_index([vertical_dim, "FCST Cycle"])["mix"].to_dict()

    # 每周整体 Core MIX（汇总行）
    week_mix = (
        valid.groupby("FCST Cycle")
        .agg(_core=("_core_amt", "sum"), _memo=("_memo_amt", "sum"))
        .reset_index()
    )
    week_mix["mix"] = week_mix.apply(lambda r: _mix(r["_core"], r["_memo"]), axis=1)
    week_mix_idx = week_mix.set_index("FCST Cycle")["mix"].to_dict()

    total_core = float(valid["_core_amt"].sum())
    total_memo = float(valid["_memo_amt"].sum())
    total_mix = _mix(total_core, total_memo)

    def _fmt_pct(x):
        return "—" if x is None or pd.isna(x) else f"{int(round(x))}%"

    # —— 配色基准 = 汇总行【当前 FCST Cycle】列的值（如 Week11 汇总 55%）——
    # 该基准单元格本身：文字绿色加粗，背景 dark=白色 / light=黑色（CSS 随主题切换）；
    # 其余所有单元格（含汇总行其他周、各维度行全部周）：
    #   >= 基准 → 绿色渐变（值越大越深），文字白色加粗；
    #   <  基准 → 橙色渐变（值越小越深），文字黑色不加粗。
    summary_vals = [week_mix_idx.get(w) for w in weeks]
    summary_ref = week_mix_idx.get(cur_cycle) if cur_cycle else None
    try:
        ref_col_idx = weeks.index(cur_cycle) if cur_cycle else None
    except ValueError:
        ref_col_idx = None

    row_data = []  # (label, vals)
    for _, r in v_grp.iterrows():
        v_val = r[vertical_dim]
        row_data.append((v_val, [cross_idx.get((v_val, w)) for w in weeks]))

    all_vals = [
        v
        for vals in [summary_vals] + [rd[1] for rd in row_data]
        for v in vals
        if v is not None and not pd.isna(v)
    ]
    if summary_ref is not None and not pd.isna(summary_ref) and all_vals:
        max_excess = max([v - summary_ref for v in all_vals if v >= summary_ref] or [0.0])
        max_deficit = max([summary_ref - v for v in all_vals if v < summary_ref] or [0.0])
    else:
        max_excess = max_deficit = 0.0

    def _blend(c1, c2, t):
        return "#%02x%02x%02x" % tuple(int(a + (b - a) * t) for a, b in zip(c1, c2))

    def _mix_bg_fg(pct):
        """ >= 基准绿渐变（值越大越深，白字加粗）；< 基准橙渐变（值越小越深，黑字不加粗）。"""
        if pct is None or pd.isna(pct):
            return ("transparent", "#999999", False)
        if summary_ref is None or pd.isna(summary_ref):
            return ("transparent", "inherit", False)
        if pct >= summary_ref:
            t = (pct - summary_ref) / max_excess if max_excess > 0 else 0.0
            return (_blend((200, 230, 201), (27, 94, 32), t), "#ffffff", True)   # 值越大越深
        t = (summary_ref - pct) / max_deficit if max_deficit > 0 else 0.0
        return (_blend((255, 224, 178), (230, 81, 0), t), "#1a1a1a", False)      # 值越小越深

    def _spark_line(vals, color):
        """按周走势折线 sparkline（与 by Week 趋势表同款风格）。"""
        pts = [v for v in vals if v is not None and not pd.isna(v)]
        if len(pts) < 2:
            return ""
        W, H = 110, 26
        max_v = max(pts)
        min_v = min(pts)
        rng = max_v - min_v
        coords = []
        for i, v in enumerate(vals):
            if v is None or pd.isna(v):
                coords.append(None)
                continue
            px = (i / (len(vals) - 1)) * (W - 4) + 2 if len(vals) > 1 else W / 2
            py = H / 2 if rng == 0 else H - 4 - ((v - min_v) / rng) * (H - 8)
            coords.append((px, py))
        # 先过滤掉缺周（None）坐标再解包，否则稀疏维度（如 产品大类/SPL）会 TypeError
        valid_coords = [c for c in coords if c is not None]
        seg_pts = " ".join([f"{x:.1f},{y:.1f}" for x, y in valid_coords])
        circles = "".join(
            [f'<circle cx="{x:.1f}" cy="{y:.1f}" r="1.8" fill="{color}"/>' for x, y in valid_coords]
        )
        return (
            f'<svg viewBox="0 0 {W} {H}" preserveAspectRatio="none" class="cmbw-spark">'
            f'<polyline points="{seg_pts}" fill="none" stroke="{color}" stroke-width="1.5" '
            f'stroke-linecap="round" stroke-linejoin="round"/>{circles}</svg>'
        )

    # 表头：维度名 | Week... | Trend
    header_cells = [f'<th class="lbl">{_esc_html(vertical_dim)}</th>']
    header_cells += [f'<th class="num">{_esc_html(w)}</th>' for w in weeks]
    header_cells.append('<th class="num trendcol">Trend</th>')

    rows_html = []

    def _render_row(label, vals, is_summary=False):
        cells = [
            '<td class="lbl">'
            + ("<b>" + _esc_html(str(label)) + "</b>" if is_summary else _esc_html(str(label)))
            + "</td>"
        ]
        for i, v in enumerate(vals):
            if is_summary and ref_col_idx is not None and i == ref_col_idx and summary_ref is not None and not pd.isna(summary_ref):
                # 基准单元格：绿字加粗，背景随主题（dark=白/light=黑，由 CSS 控制）
                cells.append(f'<td class="num ref-cell">{_fmt_pct(v)}</td>')
                continue
            bg, fg, bold = _mix_bg_fg(v)
            fw = "bold" if bold else "normal"
            if is_summary and bg == "transparent":
                # 汇总行无数据单元格不写内联背景，交给 CSS（吸顶时不透底，且深浅主题都正确）
                cells.append(f'<td class="num" style="color:{fg};">{_fmt_pct(v)}</td>')
                continue
            cells.append(
                f'<td class="num" style="background:{bg};color:{fg};font-weight:{fw};">{_fmt_pct(v)}</td>'
            )
        spark_color = "#31333F" if is_summary else "#888888"
        rows_html.append(
            '<tr class="' + ("summary-row" if is_summary else "data-row") + '">'
            + "".join(cells)
            + f'<td class="num trendcol">{_spark_line(vals, spark_color)}</td></tr>'
        )

    # 汇总行（置顶；当前 Cycle 单元格为配色基准）
    _render_row("汇总", summary_vals, is_summary=True)
    # 各维度值行
    for label, vals in row_data:
        _render_row(label, vals)

    n_week_cols = len(weeks)
    colgroup = (
        '<colgroup><col style="width:150px;">'
        + "".join(['<col style="width:64px;">'] * n_week_cols)
        + '<col style="width:120px;"></colgroup>'
    )

    html = f"""
    <style>
    .cmbw-wrap {{ overflow-x: auto; max-height: 420px; }}
    .cmbw-table {{ width: 100%; border-collapse: collapse; font-family: "Source Sans Pro", sans-serif; font-size: 11px; color: #31333F; }}
    .cmbw-table * {{ box-sizing: border-box; }}
    .cmbw-table th, .cmbw-table td {{ padding: 3px 5px; border-bottom: 1px solid #e0e0e0; border-right: 1px solid #e0e0e0; vertical-align: middle; }}
    .cmbw-table th:last-child, .cmbw-table td:last-child {{ border-right: none; }}
    .cmbw-table thead th {{ position: sticky; top: 0; height: 26px; background: #f7f7f8; font-weight: 600; text-align: center; white-space: nowrap; z-index: 5; }}
    .cmbw-table thead th.lbl {{ text-align: left; }}
    /* 汇总行吸顶（紧贴表头下方），滚动时不被滚没 */
    .cmbw-table tbody tr.summary-row td {{ background: #fbfbfc; font-weight: 600; position: sticky; top: 26px; z-index: 4; }}
    .cmbw-table tbody tr.summary-row td.num {{ font-weight: 600; }}
    .cmbw-table td.num {{ text-align: center; font-variant-numeric: tabular-nums; white-space: nowrap; font-size: 10px; }}
    .cmbw-table td.lbl {{ text-align: left; font-weight: 500; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
    .cmbw-table td.lbl.dim-current {{ color: #b45309; }}
    .cmbw-table .trendcol {{ padding: 1px 4px; }}
    .cmbw-spark {{ width: 105px; height: 24px; display: block; margin: 0 auto; }}
    .theme-dark .cmbw-table {{ color: #f5f5f5; background: #0e1117; }}
    .theme-dark .cmbw-table thead th {{ background: #262730; color: #f5f5f5; border-bottom-color: #48484f; border-right-color: #48484f; }}
    .theme-dark .cmbw-table td {{ border-bottom-color: #36363f; border-right-color: #36363f; }}
    .theme-dark .cmbw-table td.lbl {{ background: #1b1d26; color: #f5f5f5; }}
    .theme-dark .cmbw-table tbody tr.summary-row td {{ background: #1f212b; }}
    /* 基准单元格（汇总行当前 Cycle）：绿字加粗加大字号；背景 dark=白 / light=黑，随主题切换 */
    .cmbw-table td.ref-cell {{ color: #00b050; font-weight: 700; font-size: 13px; }}
    .theme-light .cmbw-table td.ref-cell {{ background: #000000; }}
    .theme-dark .cmbw-table td.ref-cell {{ background: #ffffff; }}
    /* dark 主题下汇总行 sparkline 由深灰改为浅色，避免看不清 */
    .theme-dark .cmbw-table tr.summary-row .cmbw-spark polyline {{ stroke: #f5f5f5; }}
    .theme-dark .cmbw-table tr.summary-row .cmbw-spark circle {{ fill: #f5f5f5; }}
    </style>
    <div class="cmbw-wrap {_theme_cls()}">
    <table class="cmbw-table">
    {colgroup}
    <thead><tr>{"".join(header_cells)}</tr></thead>
    <tbody>{"".join(rows_html)}</tbody>
    </table>
    </div>
    {_THEME_RUNTIME_JS}
    """
    components.html(html, height=76 + (len(v_grp) + 1) * 26 + 2, scrolling=False)


def _auto_refresh_on_data_change():
    """近实时自动刷新（零服务器模式）：每 5 秒检测各数据桶文件 mtime，
    一旦 OneDrive 把新副本同步到本机（mtime 变化），自动整页重载，
    使左侧上传/清理后右侧无需手动点刷新即可看到最新数据。
    st.fragment(run_every=...) 在 streamlit>=1.38 可用；旧版本自动降级为仅手动刷新。
    """
    cur = {t: _bucket_mtime(t) for t in UPLOAD_TYPES}
    prev = st.session_state.get("_auto_refresh_mtimes")
    if prev is None:
        st.session_state["_auto_refresh_mtimes"] = cur
        return
    if cur != prev:
        st.session_state["_auto_refresh_mtimes"] = cur
        st.rerun()
    st.caption("🔄 数据自动刷新已开启：OneDrive 同步到新数据后将自动更新本页")


# streamlit>=1.38 才有 st.fragment(run_every=...)；旧版本降级为无自动刷新（保留手动按钮）
_AUTO_REFRESH_OK = hasattr(st, "fragment")
if _AUTO_REFRESH_OK:
    _auto_refresh_on_data_change = st.fragment(run_every=5)(_auto_refresh_on_data_change)


def main():
    _cleanup_stale_tmp()
    ensure_buckets_from_master()
    # 近实时自动刷新：OneDrive 同步到新数据后自动重载（仅作检测，不阻塞主流程）
    _auto_refresh_on_data_change()

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
            st.caption(
                "「本机文件更新于」是你这台电脑上数据文件的最后修改时间；"
                "若与上传人看到的不一致，说明 OneDrive 还没把最新数据同步到你的电脑——"
                "请等待同步完成，或右键该文件夹「始终保留在此设备」。"
            )
            for t in UPLOAD_TYPES:
                rows = _bucket_row_count(t)
                mt = _bucket_mtime(t)
                mt_str = datetime.fromtimestamp(mt).strftime("%Y-%m-%d %H:%M:%S") if mt else "—"
                if rows == 0:
                    st.markdown(f"- **{t}**：（空），本机文件更新于 `{mt_str}`")
                    continue
                fy_opts = _bucket_distinct(t, "财年财季")
                cyc_opts = _bucket_distinct(t, "FCST Cycle") if t == "FCST" else []
                cyc_text = f"，FCST 周版本 {cyc_opts}" if cyc_opts else ""
                st.markdown(
                    f"- **{t}**：{rows:,} 行，财年财季 {len(fy_opts)} 个{cyc_text}，"
                    f"本机文件更新于 `{mt_str}`"
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
            # 点击运行后自动切换到右侧导出视图并触发计算
            st.session_state.main_view = "明细 / 透视导出"
            st.session_state.run_export = True
            st.rerun()

    # ===================== 右侧主区域 =====================
    if "run_export" not in st.session_state:
        st.session_state.run_export = False

    # 顶部 fixed 区：主视图切换（贴顶）+ FCST 分析模块 tab（第二行）+ FCST 维度/范围选择
    # 真正钉在视口最顶端（position: fixed），高度随内容自适应但保持紧凑。
    with st.container(border=True):
        st.markdown(
            '<div id="main-top-marker" style="display:none;"></div>',
            unsafe_allow_html=True,
        )
        # Row 1：主视图切换 | 刷新（贴容器顶端）
        _r1c1, _r1c2 = st.columns([11, 1])
        with _r1c1:
            main_view = st.radio(
                "视图",
                ["FCST 分析", "明细 / 透视导出"],
                horizontal=True,
                key="main_view",
                label_visibility="collapsed",
                index=0,
            )
        with _r1c2:
            if st.button(
                "🔄",
                key="fcst_refresh",
                use_container_width=True,
                help="左侧数据源变更后若右侧未自动刷新，点此强制刷新",
            ):
                _invalidate_fcst_cache()
                st.rerun()

        if main_view == "FCST 分析":
            # Row 2：FCST 分析三个模块 tab（放在 FCST 分析下方）
            fcst_module = st.radio(
                "FCST模块",
                ["差异分析", "FCST by Week 趋势", "Core MIX 分析"],
                horizontal=True,
                key="fcst_module",
                label_visibility="collapsed",
            )
            # Row 3：FCST 控件（5 个 selectbox 横向）+ 数据版本
            fcst_vals = _render_fcst_controls_inline()
            try:
                _fcst_mt = _bucket_mtime("FCST")
                if _fcst_mt:
                    _mt_str = datetime.fromtimestamp(_fcst_mt).strftime("%Y-%m-%d %H:%M:%S")
                    st.markdown(
                        f"<span style='font-size:0.5rem;color:#888;'>FCST 数据更新于 {_mt_str}</span>",
                        unsafe_allow_html=True,
                    )
            except Exception:  # noqa
                pass
        else:
            fcst_vals = None
            st.markdown(
                "<span style='font-size:0.55rem;color:#888;'>请在左侧设置筛选与透视，点击【运行筛选/透视】后在此查看结果。</span>",
                unsafe_allow_html=True,
            )

    # 顶部容器 fixed + 极紧凑样式：通过 #main-top-marker 定位
    # 注意：Streamlit 1.6x 的容器外层 testid 是 stLayoutWrapper（非旧版 stVerticalBlockBorderWrapper）；
    # 页面自带 60px 高的白色 stHeader（z-index 999990），fixed 条须置于其下（top:60px）否则被遮挡。
    st.markdown(
        """
        <style>
        /* 顶部容器 fixed 钉在视口顶端（stHeader 下方），高度随内容自适应但保持紧凑。
           背景跟随主题（JS 兜底会同步为父页面实际背景色），高度与 left/width 由 JS 动态设置。 */
        [data-testid="stLayoutWrapper"]:has(#main-top-marker) {
            position: fixed !important;
            top: 60px !important;
            left: 330px !important;
            right: 0 !important;
            width: auto !important;
            z-index: 999989 !important;
            height: auto !important;
            min-height: 46px !important;
            max-height: none !important;
            overflow: visible !important;
            background: var(--background-color, #ffffff) !important;
            padding: 2px 8px !important;
            margin: 0 !important;
            line-height: 1 !important;
            box-sizing: border-box !important;
            box-shadow: 0 1px 4px rgba(0,0,0,0.10) !important;
        }
        /* 防止内容被 fixed 顶部条遮挡，给主区域加 padding-top（JS 兜底会覆盖为实际高度+间距） */
        [data-testid="stMainBlockContainer"] {
            padding-top: 150px !important;
        }
        /* 极小字号与行高 */
        [data-testid="stLayoutWrapper"]:has(#main-top-marker) h3 {
            font-size: 0.7rem !important;
            margin: 0 !important;
            line-height: 1 !important;
        }
        [data-testid="stLayoutWrapper"]:has(#main-top-marker) .stMarkdown,
        [data-testid="stLayoutWrapper"]:has(#main-top-marker) .stMarkdown p,
        [data-testid="stLayoutWrapper"]:has(#main-top-marker) .stMarkdown small,
        [data-testid="stLayoutWrapper"]:has(#main-top-marker) .stMarkdown span {
            font-size: 0.55rem !important;
            margin: 0 !important;
            padding: 0 !important;
            line-height: 1.1 !important;
        }
        [data-testid="stLayoutWrapper"]:has(#main-top-marker) .stCaption {
            font-size: 0.5rem !important;
            margin: 0 !important;
        }
        /* 横向块紧贴、无间距 */
        [data-testid="stLayoutWrapper"]:has(#main-top-marker) [data-testid="stHorizontalBlock"] {
            gap: 0.15rem !important;
            margin: 0 !important;
            padding: 0 !important;
            align-items: center !important;
        }
        [data-testid="stLayoutWrapper"]:has(#main-top-marker) [data-testid="stHorizontalBlock"] > div {
            margin: 0 !important;
            padding: 0 !important;
        }
        /* 按钮极小 */
        [data-testid="stLayoutWrapper"]:has(#main-top-marker) .stButton > button {
            padding: 0rem 0.2rem !important;
            font-size: 0.55rem !important;
            min-height: 14px !important;
            line-height: 1 !important;
            border-radius: 3px !important;
        }
        /* radio 横向选项极小 */
        [data-testid="stLayoutWrapper"]:has(#main-top-marker) .stRadio {
            margin: 0 !important;
            padding: 0 !important;
        }
        [data-testid="stLayoutWrapper"]:has(#main-top-marker) .stRadio > label {
            display: none !important;
        }
        [data-testid="stLayoutWrapper"]:has(#main-top-marker) .stRadio [role="radiogroup"] {
            gap: 0 !important;
            margin: 0 !important;
        }
        [data-testid="stLayoutWrapper"]:has(#main-top-marker) .stRadio [role="radiogroup"] label {
            padding: 0 0.3rem !important;
            font-size: 0.55rem !important;
            min-height: 14px !important;
        }
        /* 下拉框极小 */
        [data-testid="stLayoutWrapper"]:has(#main-top-marker) .stSelectbox {
            margin: 0 !important;
            padding: 0 !important;
        }
        [data-testid="stLayoutWrapper"]:has(#main-top-marker) .stSelectbox [data-baseweb="select"] {
            min-height: 16px !important;
            font-size: 0.55rem !important;
        }
        [data-testid="stLayoutWrapper"]:has(#main-top-marker) .stSelectbox [data-baseweb="select"] > div {
            padding-top: 0 !important;
            padding-bottom: 0 !important;
            min-height: 16px !important;
        }
        [data-testid="stLayoutWrapper"]:has(#main-top-marker) [data-baseweb="select"] {
            border-radius: 3px !important;
        }
        /* Streamlit 1.6x 用 React-Aria ComboBox，额外压缩其高度 */
        [data-testid="stLayoutWrapper"]:has(#main-top-marker) .stSelectbox div.react-aria-ComboBox {
            min-height: 18px !important;
            height: 18px !important;
        }
        [data-testid="stLayoutWrapper"]:has(#main-top-marker) .stSelectbox .react-aria-ComboBox > div {
            min-height: 18px !important;
            height: 18px !important;
            padding: 0 4px !important;
        }
        [data-testid="stLayoutWrapper"]:has(#main-top-marker) .stSelectbox .react-aria-ComboBox input {
            min-height: 16px !important;
            height: 16px !important;
            font-size: 0.55rem !important;
            padding: 0 4px !important;
        }
        [data-testid="stLayoutWrapper"]:has(#main-top-marker) .stSelectbox .react-aria-ComboBox button {
            min-height: 16px !important;
            height: 16px !important;
            width: 16px !important;
            padding: 0 !important;
        }
        [data-testid="stLayoutWrapper"]:has(#main-top-marker) .stSelectbox .react-aria-ComboBox button svg {
            width: 11px !important;
            height: 11px !important;
        }
        /* 隐藏 warning 信息的留白 */
        [data-testid="stLayoutWrapper"]:has(#main-top-marker) .stAlert {
            padding: 0.1rem 0.3rem !important;
            font-size: 0.55rem !important;
            margin: 0 !important;
        }
        /* 每个分析模块的容器（差异分析 / by Week / Core MIX）略缩边距 */
        [data-testid="stLayoutWrapper"]:has(#fcst-module-marker),
        [data-testid="stLayoutWrapper"]:has(#fcst-trend-marker),
        [data-testid="stLayoutWrapper"]:has(#fcst-coremix-marker) {
            padding: 0.4rem 0.6rem !important;
            margin-bottom: 0.5rem !important;
        }
        [data-testid="stLayoutWrapper"]:has(#fcst-module-marker) h3,
        [data-testid="stLayoutWrapper"]:has(#fcst-trend-marker) h3,
        [data-testid="stLayoutWrapper"]:has(#fcst-coremix-marker) h3 {
            font-size: 1rem !important;
            margin: 0 0 0.25rem 0 !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # JS 兜底：直接在父文档中给顶部容器打上 inline fixed 样式。
    # 原因：部分浏览器/Streamlit DOM 下 CSS :has() 可能不匹配，或祖先元素带 transform
    # 导致 position:fixed 失效；这里用 JS 每隔一段时间强制应用，确保真正钉在视口顶端。
    import streamlit.components.v1 as _components

    _pin_js = """
    <script>
    (function() {
        var STYLE_ID = 'pin-bar-style-v3';
        function ensureStyle(doc) {
            if (doc.getElementById(STYLE_ID)) return;
            var st = doc.createElement('style');
            st.id = STYLE_ID;
            st.textContent = [
                '.fcst-pin-bar { position: fixed !important; z-index: 999989 !important; height: auto !important; min-height: 46px !important; overflow: visible !important; background: var(--background-color, #ffffff) !important; padding: 2px 8px !important; box-sizing: border-box !important; box-shadow: 0 1px 4px rgba(0,0,0,0.10) !important; line-height: 1 !important; }',
                '.fcst-pin-bar [data-testid="stVerticalBlock"] { gap: 0 !important; }',
                '.fcst-pin-bar [data-testid="stVerticalBlock"] > div { margin: 0 !important; padding: 0 !important; }',
                '.fcst-pin-bar [data-testid="stHorizontalBlock"] { gap: 4px !important; margin: 0 !important; padding: 0 !important; align-items: center !important; min-height: 0 !important; }',
                '.fcst-pin-bar [data-testid="stHorizontalBlock"] > div { margin: 0 !important; padding: 0 !important; min-height: 0 !important; }',
                '.fcst-pin-bar .stMarkdown, .fcst-pin-bar .stMarkdown p, .fcst-pin-bar .stMarkdown span, .fcst-pin-bar .stMarkdown small { font-size: 10px !important; margin: 0 !important; padding: 0 !important; line-height: 1.1 !important; }',
                '.fcst-pin-bar .stButton { margin: 0 !important; padding: 0 !important; }',
                '.fcst-pin-bar .stButton > button { padding: 0 6px !important; font-size: 10px !important; min-height: 18px !important; height: 18px !important; line-height: 1 !important; border-radius: 3px !important; }',
                /* --- selectbox：Streamlit 1.6x 用 React-Aria ComboBox 结构 --- */
                '.fcst-pin-bar .stSelectbox { margin: 0 !important; padding: 0 !important; }',
                '.fcst-pin-bar .stSelectbox > div { margin: 0 !important; min-height: 0 !important; }',
                '.fcst-pin-bar .stSelectbox div.react-aria-ComboBox { min-height: 18px !important; height: 18px !important; margin: 0 !important; }',
                '.fcst-pin-bar .stSelectbox .react-aria-ComboBox > div { min-height: 18px !important; height: 18px !important; padding: 0 4px !important; }',
                '.fcst-pin-bar .stSelectbox .react-aria-ComboBox input { min-height: 16px !important; height: 16px !important; font-size: 10px !important; padding: 0 4px !important; }',
                '.fcst-pin-bar .stSelectbox .react-aria-ComboBox button { min-height: 16px !important; height: 16px !important; width: 16px !important; padding: 0 !important; }',
                '.fcst-pin-bar .stSelectbox .react-aria-ComboBox button svg { width: 11px !important; height: 11px !important; }',
                /* --- radio 紧凑 --- */
                '.fcst-pin-bar .stRadio { margin: 0 !important; padding: 0 !important; }',
                '.fcst-pin-bar .stRadio > div { margin: 0 !important; min-height: 0 !important; }',
                '.fcst-pin-bar .stRadio [role="radiogroup"] { gap: 2px !important; margin: 0 !important; min-height: 0 !important; }',
                '.fcst-pin-bar .stRadio [role="radiogroup"] label { min-height: 17px !important; height: 17px !important; font-size: 10px !important; padding: 0 4px !important; gap: 2px !important; margin: 0 !important; }',
                '.fcst-pin-bar .stAlert { padding: 1px 4px !important; font-size: 10px !important; margin: 0 !important; min-height: 0 !important; }'
            ].join('\\n');
            doc.head.appendChild(st);
        }
        function headerH(doc) {
            var hd = doc.querySelector('[data-testid="stHeader"]');
            return hd ? hd.getBoundingClientRect().height : 60;
        }
        function sidebarW(doc) {
            var sb = doc.querySelector('[data-testid="stSidebar"]');
            return sb ? (sb.getBoundingClientRect().width || 0) : 0;
        }
        function themeBg(doc) {
            var app = doc.querySelector('.stApp') || doc.body;
            return app ? getComputedStyle(app).backgroundColor : '';
        }
        function apply() {
            try {
                var doc = window.parent.document;
                ensureStyle(doc);
                var marker = doc.getElementById('main-top-marker');
                if (!marker) return;
                var el = marker.closest('[data-testid="stLayoutWrapper"]')
                      || marker.closest('[data-testid="stVerticalBlock"]');
                if (!el) return;
                if (!el.classList.contains('fcst-pin-bar')) el.classList.add('fcst-pin-bar');
                var hh = headerH(doc);
                var sw = sidebarW(doc);
                el.style.setProperty('top', hh + 'px', 'important');
                el.style.setProperty('left', sw + 'px', 'important');
                el.style.setProperty('width', 'calc(100% - ' + sw + 'px)', 'important');
                // 背景跟随主题（dark 模式下避免白底白字）
                var bg = themeBg(doc);
                if (bg) el.style.setProperty('background-color', bg, 'important');
                // 按实际高度动态计算主内容偏移，确保内容不被遮挡
                var barH = el.offsetHeight || 46;
                var main = doc.querySelector('[data-testid="stMainBlockContainer"]')
                        || doc.querySelector('.main .block-container');
                if (main) main.style.paddingTop = (hh + barH + 10) + 'px';
            } catch (e) { /* 跨域时静默失败 */ }
        }
        apply();
        setInterval(apply, 600);
    })();
    </script>
    """
    _components.html(_pin_js, height=0)

    if main_view == "FCST 分析":
        if fcst_vals is not None:
            fy, cur_cycle, cmp_cycle, scope, sub_region = fcst_vals
            _render_fcst_modules(fy, cur_cycle, cmp_cycle, scope, sub_region)
    else:
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
        # 需求 3：导出数据补充 Core/Memoline 列（兼容旧桶）
        all_data = add_core_memoline(all_data)
    
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
