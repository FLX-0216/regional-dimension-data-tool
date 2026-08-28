"""
OPS 数据统一格式处理脚本
将多个源表按规则清洗、追加合并为统一表格。
"""
import os
import re
import shutil
import tempfile
import pandas as pd
import numpy as np

# 最终统一表头（规范 26 列）
FINAL_COLUMNS = [
    "财年财季", "数据类别", "是否业绩考核", "是否FCST",
    "服务管理省份", "服务大区", "服务战区", "产线类型", "产线名称",
    "产品大类", "SPL名称", "IDGISG", "销售模式", "POS_APOS",
    "物料通路", "REL纵队", "业绩考核USDK", "项目/商机编号",
    "客户名称", "代理名称", "项目名称", "签约主体", "IB_NEW",
    "PM/sales", "Sales", "产线+通路"
]

# 源表溯源 + FCST Cycle 版本列（追加在规范 26 列之外，用于拆分展示与版本对比）
FCST_FAMILY = {"T1 FCST SDA&STB", "T2 FCST", "T3 FCST", "T1 HB&JV"}
SOURCE_GROUPS = {
    "历史Union": {"历史Union"},
    "QTD": {"QTD"},
    "FCST": FCST_FAMILY,
    "DG&Quota": {"DG&Quota"},
}


def _build_final_order():
    """最终输出列顺序：在“是否FCST”右侧插入“FCST Cycle”，末尾追加“源表”。"""
    order = []
    for c in FINAL_COLUMNS:
        order.append(c)
        if c == "是否FCST":
            order.append("FCST Cycle")
    order.append("源表")
    return order


FINAL_ORDER = _build_final_order()

def clean_region(val):
    """清洗服务大区：仅把真正的空白置为 None。

    注意：不再按关键词过滤（如“其他/产线/行业集成”），避免把源表中
    有效的非空服务大区误清空（第 5、9 条）。真正的空白由后续全局过滤剔除（第 2 条）。
    """
    if pd.isna(val):
        return None
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none"):
        return None
    return s


def norm_channel(val):
    """渠道名称归一化（第 4 条）：Softbundle→STB，SDA-新阳光→SDA。"""
    if pd.isna(val):
        return val
    s = str(val)
    s = s.replace("SDA-新阳光", "SDA").replace("Softbundle", "STB")
    return s


def _apply_hw_chnl(out, df):
    """第 6 条：产线类型=HW 时，产线+通路 取 KAB产品组 列的值。"""
    if "KAB产品组" not in df.columns:
        return
    hw_mask = out["产线类型"].astype(str).str.strip() == "HW"
    if not hw_mask.any():
        return
    out.loc[hw_mask, "产线+通路"] = df.loc[hw_mask, "KAB产品组"].astype(object).values


def safe_get(df, col, default=None):
    """安全取列，列不存在时返回default。"""
    if col in df.columns:
        return df[col].copy()
    return pd.Series([default] * len(df), index=df.index)


def coalesce_series(*series):
    """按顺序取第一个非空值。"""
    result = series[0].copy()
    for s in series[1:]:
        result = result.fillna(s)
    return result


def compute_chnl_history(row):
    """历史Union/QTD 的产线+通路计算逻辑。"""
    pos_apos = str(row.get("POS_APOS", "")).strip().upper()
    prod_line = str(row.get("业绩产线", "")).strip()
    attrib = str(row.get("业绩归属", "")).strip()
    prod_cat = str(row.get("产品大类", "")).strip()

    if pos_apos == "POS":
        if prod_line == "T2":
            if "运维服务_DWS" in attrib:
                return "Windows"
            if "TruScale" in attrib or "PC DaaS" in attrib:
                return "External Funder"
            return attrib
        if prod_line == "T1":
            return str(row.get("物料通路", "")).strip() or prod_cat
        if prod_line == "T3":
            for k in ["T9", "X9"]:
                if k in attrib:
                    return "T9&X9"
            for k in ["B6", "X6", "T8"]:
                if k in attrib:
                    return k
            if attrib:
                return "Others"
            return str(row.get("物料通路", "")).strip() or prod_cat
    elif pos_apos == "APOS":
        if prod_line == "T2":
            return attrib
        else:
            return prod_cat
    return None


def compute_chnl_qtd(row):
    """QTD 的产线+通路计算逻辑（优先使用 T3下单通路，不存在时回退到业绩归属）。"""
    pos_apos = str(row.get("POS_APOS", "")).strip().upper()
    prod_line = str(row.get("业绩产线", "")).strip()
    t3_channel_col = "T3下单通路" if "T3下单通路" in row else None
    channel = str(row.get(t3_channel_col, row.get("业绩归属", ""))).strip() if t3_channel_col else str(row.get("业绩归属", "")).strip()

    if pos_apos == "POS":
        if prod_line == "T2":
            if "运维服务_DWS" in channel:
                return "Windows"
            if "TruScale" in channel or "PC DaaS" in channel:
                return "External Funder"
            return channel
        if prod_line == "T1":
            return str(row.get("物料通路", "")).strip() or str(row.get("产品大类", "")).strip()
        if prod_line == "T3":
            for k in ["T9", "X9"]:
                if k in channel:
                    return "T9&X9"
            for k in ["B6", "X6", "T8"]:
                if k in channel:
                    return k
            if channel:
                return "Others"
            return str(row.get("物料通路", "")).strip() or str(row.get("产品大类", "")).strip()
    elif pos_apos == "APOS":
        if prod_line == "T2":
            return channel
        else:
            return str(row.get("产品大类", "")).strip()
    return None


def compute_pos_apos_t1(row):
    """T1 FCST 的 POS_APOS：销售通路 SDA→APOS，STB→POS。"""
    chnl = str(row.get("销售通路", "")).strip().upper()
    if chnl == "SDA":
        return "APOS"
    if chnl == "STB":
        return "POS"
    return None


def compute_sales_t1(row):
    """T1 FCST 的 Sales 字段。"""
    pos_apos = str(row.get("POS_APOS", "")).strip().upper()
    if pos_apos == "POS":
        return row.get("SSG APOS OS", None)
    elif pos_apos == "APOS":
        return row.get("SSG POS OS", None)
    return None


def compute_chnl_t1(row):
    """T1 FCST 的产线+通路（第 1 条修正）：销售通路 SDA→产线名称，STB→STB。

    与物料通路使用一致的归一化（Softbundle→STB、SDA-新阳光→SDA），
    避免 销售通路=Softbundle 时物料通路已变 STB 而本列未跟上的不一致。
    """
    chnl = norm_channel(str(row.get("销售通路", "")).strip()).upper()
    if chnl == "STB":
        return "STB"
    return str(row.get("产线名称", "")).strip() or None


def compute_sales_t2(row):
    """T2 FCST 的 Sales（第 10 条）：MML-租赁=是→SSG POS OS，否则→SSG APOS OS。"""
    rent = str(row.get("MML-租赁", "")).strip()
    if rent == "是":
        return row.get("SSG POS OS", None)
    return row.get("SSG APOS OS", None)


def compute_sales_t3(row):
    """T3 FCST 的 Sales 字段。"""
    pos_apos = str(row.get("POS_APOS", "")).strip().upper()
    # 逻辑原文：POS/APOS 都返回 CoreService OS IS
    return row.get("CoreService OS IS", None)


def compute_chnl_t2(row):
    """T2 FCST 的产线+通路（第 11 条）。

    MML-租赁=是 且 新分类-区域含 运维服务_DWS → Windows；
    MML-租赁=是 且 新分类-区域含 TruScale/PC DaaS → External Funder；
    否则 → 新分类-区域。
    """
    is_rent = str(row.get("MML-租赁", "")).strip() == "是"
    region_new = str(row.get("新分类-区域", "")).strip()
    if is_rent:
        if "运维服务_DWS" in region_new:
            return "Windows"
        if "TruScale" in region_new or "PC DaaS" in region_new:
            return "External Funder"
    return region_new


def compute_pos_apos_t3(row):
    """T3 FCST 的 POS_APOS 判断。"""
    s = row.get("T3当Q服务收入KUSD去税", 0)
    h = row.get("T3当Q硬件收入KUSD去税", 0)
    try:
        s_val = float(s) if pd.notna(s) else 0
    except Exception:
        s_val = 0
    try:
        h_val = float(h) if pd.notna(h) else 0
    except Exception:
        h_val = 0
    if s_val != 0:
        return "APOS"
    if h_val != 0:
        return "POS"
    return None


def compute_amount_t3(row):
    """T3 FCST 的业绩考核USDK。"""
    pos_apos = str(row.get("POS_APOS", "")).strip().upper()
    if pos_apos == "APOS":
        return row.get("T3当Q服务收入KUSD去税", None)
    elif pos_apos == "POS":
        return row.get("T3当Q硬件收入KUSD去税", None)
    return None


def compute_chnl_t3(row):
    """T3 FCST 的产线+通路。"""
    s = row.get("T3当Q服务收入KUSD去税", 0)
    try:
        s_val = float(s) if pd.notna(s) else 0
    except Exception:
        s_val = 0
    if s_val != 0:
        return str(row.get("产线名称", "")).strip()

    h = row.get("T3当Q硬件收入KUSD去税", 0)
    try:
        h_val = float(h) if pd.notna(h) else 0
    except Exception:
        h_val = 0
    if h_val != 0:
        channel = str(row.get("T3下单通路", "")).strip()
        for k in ["T9", "X9"]:
            if k in channel:
                return "T9&X9"
        for k in ["B6", "X6", "T8"]:
            if k in channel:
                return k
        return "Others"
    return None


def compute_pos_apos_t2(row):
    """T2 FCST 的 POS_APOS。"""
    rent = str(row.get("MML-租赁", "")).strip().lower()
    if rent in ["是", "true", "yes", "1"]:
        return "POS"
    return "APOS"


def process_history_union(df):
    """处理历史Union表。"""
    out = pd.DataFrame()
    out["财年财季"] = safe_get(df, "财年财季")
    out["数据类别"] = "ACT"
    out["是否业绩考核"] = safe_get(df, "是否业绩考核")
    out["是否FCST"] = None
    out["服务管理省份"] = safe_get(df, "服务管理省份")
    out["服务大区"] = safe_get(df, "服务大区").apply(clean_region)
    out["服务战区"] = safe_get(df, "服务战区")
    out["产线类型"] = safe_get(df, "业绩产线")

    def line_name(row):
        if str(row.get("业绩产线", "")).strip() == "T2":
            return row.get("业绩归属", None)
        return row.get("产品大类", None)

    out["产线名称"] = df.apply(line_name, axis=1)
    out["产品大类"] = safe_get(df, "产品二级分类")
    out["SPL名称"] = safe_get(df, "产品小类")
    out["IDGISG"] = safe_get(df, "IDGISG")
    out["销售模式"] = safe_get(df, "销售模式")
    out["POS_APOS"] = safe_get(df, "POS_APOS")
    out["物料通路"] = safe_get(df, "物料通路")
    out["REL纵队"] = safe_get(df, "REL纵队")
    out["业绩考核USDK"] = safe_get(df, "业绩考核USDK")
    out["项目/商机编号"] = safe_get(df, "项目号")
    out["客户名称"] = safe_get(df, "客户名称")
    out["代理名称"] = safe_get(df, "代理名称")
    out["项目名称"] = safe_get(df, "项目名称")
    out["签约主体"] = safe_get(df, "签约主体")
    out["IB_NEW"] = safe_get(df, "IB_NEW")
    out["PM/sales"] = None
    out["Sales"] = safe_get(df, "Sales")
    out["产线+通路"] = df.apply(compute_chnl_history, axis=1)
    _apply_hw_chnl(out, df)
    return out


def process_qtd(df):
    """处理QTD表。"""
    out = pd.DataFrame()
    out["财年财季"] = safe_get(df, "财年财季")
    out["数据类别"] = "ACT"
    out["是否业绩考核"] = safe_get(df, "是否业绩考核")
    out["是否FCST"] = None
    out["服务管理省份"] = safe_get(df, "服务管理省份")
    out["服务大区"] = safe_get(df, "服务大区").apply(clean_region)
    out["服务战区"] = safe_get(df, "服务战区")
    out["产线类型"] = safe_get(df, "业绩产线")

    def line_name(row):
        if str(row.get("业绩产线", "")).strip() == "T2":
            return row.get("业绩归属", None)
        return row.get("产品大类", None)

    out["产线名称"] = df.apply(line_name, axis=1)
    out["产品大类"] = safe_get(df, "产品二级分类")
    out["SPL名称"] = safe_get(df, "产品小类")
    out["IDGISG"] = safe_get(df, "IDGISG")
    out["销售模式"] = safe_get(df, "销售模式")
    out["POS_APOS"] = safe_get(df, "POS_APOS")
    out["物料通路"] = safe_get(df, "物料通路")
    out["REL纵队"] = safe_get(df, "REL纵队")
    out["业绩考核USDK"] = safe_get(df, "业绩考核USDK")
    out["项目/商机编号"] = safe_get(df, "项目号")
    out["客户名称"] = safe_get(df, "客户名称")
    out["代理名称"] = safe_get(df, "代理名称")
    out["项目名称"] = safe_get(df, "项目名称")
    out["签约主体"] = safe_get(df, "签约主体")
    out["IB_NEW"] = safe_get(df, "IB_NEW")
    out["PM/sales"] = None
    out["Sales"] = safe_get(df, "Sales")
    out["产线+通路"] = df.apply(compute_chnl_qtd, axis=1)
    _apply_hw_chnl(out, df)
    return out


def process_t1_fcst(df):
    """处理T1 FCST SDA&STB表。"""
    # 只取“产线类型”=T1 的明细（用户补充修正）：该源表混有 T2/T3 行，
    # 这些不应从本表取，否则会污染 T2/T3 桶并造成 PM/sales 空白被误判为重复。
    if "产线类型" in df.columns:
        keep = df["产线类型"].astype(str).str.strip() == "T1"
        df = df[keep].copy()
        if df.empty:
            return pd.DataFrame(columns=FINAL_COLUMNS)
    out = pd.DataFrame()
    out["财年财季"] = safe_get(df, "签约财年").astype(str).str.strip() + safe_get(df, "签约财季").astype(str).str.strip()
    out["数据类别"] = "FCST"
    out["是否业绩考核"] = None
    out["是否FCST"] = safe_get(df, "FCST/商机")
    out["服务管理省份"] = safe_get(df, "SSG-管理省份")
    out["服务大区"] = safe_get(df, "SSG-大区").apply(clean_region)
    out["服务战区"] = safe_get(df, "SSG-战区")
    out["产线类型"] = safe_get(df, "产线类型")
    out["产线名称"] = safe_get(df, "产线名称")
    out["产品大类"] = safe_get(df, "产品大类")
    out["SPL名称"] = safe_get(df, "SPL名称")

    def idgisg(row):
        name = str(row.get("产线名称", "")).strip()
        return "IDG" if "IDG" in name else "ISG"

    out["IDGISG"] = df.apply(idgisg, axis=1)
    out["销售模式"] = safe_get(df, "销售模式")
    out["POS_APOS"] = df.apply(compute_pos_apos_t1, axis=1)
    out["物料通路"] = safe_get(df, "销售通路")
    out["REL纵队"] = safe_get(df, "KAB纵队")
    out["业绩考核USDK"] = safe_get(df, "当Q收入金额(K$)(去税)")
    out["项目/商机编号"] = safe_get(df, "商机编号")
    out["客户名称"] = safe_get(df, "最终客户")
    out["代理名称"] = safe_get(df, "客户/代理商名称")
    out["项目名称"] = safe_get(df, "商机名称")
    out["签约主体"] = safe_get(df, "我方签约主体")
    out["IB_NEW"] = safe_get(df, "新/续扩签")
    out["PM/sales"] = None
    out["Sales"] = df.apply(compute_sales_t1, axis=1)
    out["产线+通路"] = df.apply(compute_chnl_t1, axis=1)
    return out


def process_t2_fcst(df):
    """处理T2 FCST表。"""
    out = pd.DataFrame()
    out["财年财季"] = safe_get(df, "签约财年").astype(str).str.strip() + safe_get(df, "签约财季").astype(str).str.strip()
    out["数据类别"] = "FCST"
    out["是否业绩考核"] = None
    out["是否FCST"] = safe_get(df, "是否FCST")
    out["服务管理省份"] = safe_get(df, "服务管理省份")
    out["服务大区"] = safe_get(df, "服务大区").apply(clean_region)
    out["服务战区"] = safe_get(df, "服务战区")
    out["产线类型"] = safe_get(df, "产线类型")
    out["产线名称"] = safe_get(df, "新分类-区域")
    out["产品大类"] = None
    out["SPL名称"] = None
    out["IDGISG"] = safe_get(df, "系统IDG/ISG")
    out["销售模式"] = safe_get(df, "销售模式")
    out["POS_APOS"] = df.apply(compute_pos_apos_t2, axis=1)
    out["物料通路"] = safe_get(df, "销售通路")
    out["REL纵队"] = safe_get(df, "REL纵队")
    out["业绩考核USDK"] = safe_get(df, "系统booking(K$)")
    out["项目/商机编号"] = safe_get(df, "商机编号")
    out["客户名称"] = safe_get(df, "最终用户")
    out["代理名称"] = safe_get(df, "客户/代理商名称")
    out["项目名称"] = safe_get(df, "商机名称")
    out["签约主体"] = safe_get(df, "我方签约主体")
    out["IB_NEW"] = safe_get(df, "新/续扩签")
    out["PM/sales"] = safe_get(df, "PM/sales")
    out["Sales"] = df.apply(compute_sales_t2, axis=1)
    out["产线+通路"] = df.apply(compute_chnl_t2, axis=1)
    return out


def idgisg_t3(row):
    """T3 FCST 的 IDGISG（第 12 条）：ISG/IDG修正=中性→ISG，否则取原值。"""
    v = str(row.get("ISG/IDG修正", "")).strip()
    if v == "中性":
        return "ISG"
    return v


def chnl_t3_hardware(row):
    """T3 POS 行的产线+通路：由 T3下单通路派生。"""
    channel = str(row.get("T3下单通路", "")).strip()
    for k in ["T9", "X9"]:
        if k in channel:
            return "T9&X9"
    for k in ["B6", "X6", "T8"]:
        if k in channel:
            return k
    return "Others"


def process_t3_fcst(df):
    """处理T3 FCST表（第 12、13 条）。

    第 13 条：仅保留 服务收入KUSD去税!=0 或 硬件收入KUSD去税!=0 的明细；
    服务收入!=0 → POS_APOS=APOS，金额入业绩考核USDK；
    硬件收入!=0 → POS_APOS=POS，金额入业绩考核USDK。
    两列都不为0的同一源行展开为两行（每行都同时带有 POS_APOS 与金额，
    即“同一行、不同列”，不会拆成维度行/金额行）。
    """
    base = pd.DataFrame()
    base["财年财季"] = safe_get(df, "签约财年").astype(str).str.strip() + safe_get(df, "签约财季").astype(str).str.strip()
    base["数据类别"] = "FCST"
    base["是否业绩考核"] = None
    base["是否FCST"] = safe_get(df, "是否FCST")
    base["服务管理省份"] = safe_get(df, "服务管理省份")
    base["服务大区"] = safe_get(df, "服务大区").apply(clean_region)
    base["服务战区"] = safe_get(df, "服务战区")
    base["产线类型"] = safe_get(df, "产线类型")
    base["产线名称"] = safe_get(df, "产线名称")
    base["产品大类"] = safe_get(df, "产品大类")
    base["SPL名称"] = safe_get(df, "SPL名称")
    base["IDGISG"] = df.apply(idgisg_t3, axis=1)
    base["销售模式"] = safe_get(df, "销售模式")
    base["物料通路"] = safe_get(df, "销售通路")
    base["REL纵队"] = safe_get(df, "REL纵队")
    base["项目/商机编号"] = safe_get(df, "商机编号")
    base["客户名称"] = safe_get(df, "最终用户")
    base["代理名称"] = safe_get(df, "客户/代理商名称")
    base["项目名称"] = safe_get(df, "商机名称")
    base["签约主体"] = safe_get(df, "我方签约主体")
    base["IB_NEW"] = safe_get(df, "新/续扩签")
    base["PM/sales"] = safe_get(df, "PM/sales")
    base["Sales"] = df.apply(compute_sales_t3, axis=1)

    s = pd.to_numeric(df["T3当Q服务收入KUSD去税"], errors="coerce").fillna(0)
    h = pd.to_numeric(df["T3当Q硬件收入KUSD去税"], errors="coerce").fillna(0)
    chnl_s = df["产线名称"].astype(str).str.strip()
    chnl_h = df.apply(chnl_t3_hardware, axis=1)

    rows = []
    for i in range(len(df)):
        if s.iloc[i] != 0:
            r = base.iloc[i].copy()
            r["POS_APOS"] = "APOS"
            r["业绩考核USDK"] = s.iloc[i]
            r["产线+通路"] = chnl_s.iloc[i]
            rows.append(r)
        if h.iloc[i] != 0:
            r = base.iloc[i].copy()
            r["POS_APOS"] = "POS"
            r["业绩考核USDK"] = h.iloc[i]
            r["产线+通路"] = chnl_h.iloc[i]
            rows.append(r)

    if rows:
        out = pd.DataFrame(rows)[FINAL_COLUMNS]
    else:
        out = pd.DataFrame(columns=FINAL_COLUMNS)
    return out


def process_t1_hb_jv(df):
    """处理T1 HB&JV表。"""
    out = pd.DataFrame()
    out["财年财季"] = safe_get(df, "财年财季")
    out["数据类别"] = "FCST"
    out["是否业绩考核"] = None
    out["是否FCST"] = None
    out["服务管理省份"] = None
    out["服务大区"] = safe_get(df, "服务大区").apply(clean_region)
    out["服务战区"] = safe_get(df, "服务战区")
    out["产线类型"] = "T1"
    out["产线名称"] = safe_get(df, "产线名称")
    out["产品大类"] = None
    out["SPL名称"] = None
    out["IDGISG"] = safe_get(df, "IDGISG")
    out["销售模式"] = None
    out["POS_APOS"] = safe_get(df, "POS_APOS")
    out["物料通路"] = safe_get(df, "物料通路")
    out["REL纵队"] = None
    out["业绩考核USDK"] = safe_get(df, "业绩考核USDK")
    out["项目/商机编号"] = None
    out["客户名称"] = None
    out["代理名称"] = None
    out["项目名称"] = None
    out["签约主体"] = None
    out["IB_NEW"] = None
    out["PM/sales"] = None
    out["Sales"] = None
    out["产线+通路"] = safe_get(df, "物料通路")
    return out


def process_dg_quota(df, category=None):
    """处理DG/Quota表：宽格式转长格式。

    源表若包含“数据类别”列，则按该列真实值输出（DG 或 Quota）；
    否则回退到传入的 category 参数。
    """
    if df.empty:
        return pd.DataFrame(columns=FINAL_COLUMNS)
    required_cols = ["财年财季", "服务大区", "服务战区", "APOS", "HB", "STB", "JV", "SDA"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        label = category or "DG/Quota"
        raise ValueError(f"{label} 缺少必要列: {missing}")

    # 用源表索引作为对齐键（DG 与 Quota 可能拥有相同的大区/战区组合）
    df_idx = df.reset_index().rename(columns={"index": "_src_idx"})
    id_vars = ["_src_idx", "财年财季", "服务大区", "服务战区"]
    value_vars = ["APOS", "HB", "STB", "JV", "SDA"]
    long = df_idx.melt(id_vars=id_vars, value_vars=value_vars,
                       var_name="原始列", value_name="金额")
    # 过滤空金额
    long = long[long["金额"].notna()]
    long["POS_APOS"] = long["原始列"].apply(lambda x: "APOS" if x == "APOS" else "POS")
    long["物料通路"] = long["原始列"].apply(lambda x: "SDA" if x == "APOS" else x)

    # 保留源表的数据类别列，与 long 对齐
    if "数据类别" in df.columns:
        cat_map = df_idx.set_index("_src_idx")["数据类别"].to_dict()
        long["数据类别_源"] = long["_src_idx"].map(cat_map).fillna(category)
    else:
        long["数据类别_源"] = category

    out = pd.DataFrame()
    out["财年财季"] = long["财年财季"]
    out["数据类别"] = long["数据类别_源"]
    out["是否业绩考核"] = None
    out["是否FCST"] = None
    out["服务管理省份"] = None
    out["服务大区"] = long["服务大区"].apply(clean_region)
    out["服务战区"] = long["服务战区"]
    out["产线类型"] = None
    out["产线名称"] = None
    out["产品大类"] = None
    out["SPL名称"] = None
    out["IDGISG"] = None
    out["销售模式"] = None
    out["POS_APOS"] = long["POS_APOS"]
    out["物料通路"] = long["物料通路"]
    out["REL纵队"] = None
    out["业绩考核USDK"] = long["金额"]
    out["项目/商机编号"] = None
    out["客户名称"] = None
    out["代理名称"] = None
    out["项目名称"] = None
    out["签约主体"] = None
    out["IB_NEW"] = None
    out["PM/sales"] = None
    out["Sales"] = None
    out["产线+通路"] = None
    return out


# 文件类别识别与处理函数映射
PROCESSORS = {
    "历史Union": process_history_union,
    "QTD": process_qtd,
    "T1 FCST SDA&STB": process_t1_fcst,
    "T2 FCST": process_t2_fcst,
    "T3 FCST": process_t3_fcst,
    "T1 HB&JV": process_t1_hb_jv,
    "DG&Quota": lambda df: process_dg_quota(df, category=None),
    "DG": lambda df: process_dg_quota(df, category="DG"),
    "Quota": lambda df: process_dg_quota(df, category="Quota"),
}


def detect_fcst_category(name):
    """根据文件名模式识别 FCST 子类型（name 需为大写）。

    规则（按优先级）：
    - 含 T3 → T3 FCST
    - 含 T2 → T2 FCST
    - 含 HB 且 含 JV → T1 HB&JV
    - 含 SDA 且 含 STB → T1 FCST SDA&STB
    - 其余兜底 → T1 FCST SDA&STB
    """
    if "T3" in name:
        return "T3 FCST"
    if "T2" in name:
        return "T2 FCST"
    if "HB" in name and "JV" in name:
        return "T1 HB&JV"
    if "SDA" in name and "STB" in name:
        return "T1 FCST SDA&STB"
    # 兜底：未匹配到明确模式时按 T1 FCST SDA&STB 处理
    return "T1 FCST SDA&STB"


def detect_category(file_path_or_name):
    """根据文件名识别数据类别。"""
    name = os.path.basename(file_path_or_name).upper()
    # 去掉 upload 时加上的 uuid 前缀（形如 1a2b..._原文件名），避免前缀干扰识别
    name = re.sub(r"^[0-9A-F]{32}_", "", name)

    # DG&Quota 合并文件：文件名同时含 DG 与 QUOTA
    if "DG" in name and "QUOTA" in name:
        return "DG&Quota"
    # 显式类型优先（文件名含这些关键词时直接判定，避免被 FCST 模式误伤）
    explicit = {
        "历史Union": ["历史UNION", "历史 UNION"],
        "QTD": ["QTD"],
        "DG": ["DG"],
        "Quota": ["QUOTA"],
    }
    for cat, keywords in explicit.items():
        for kw in keywords:
            if kw in name:
                return cat
    # FCST 子类型按文件名模式识别
    return detect_fcst_category(name)


def read_first_sheet(path):
    """读取Excel第一个sheet；若文件被占用则先复制到临时副本再读。"""
    try:
        xl = pd.ExcelFile(path)
        return pd.read_excel(path, sheet_name=xl.sheet_names[0])
    except PermissionError:
        tmp_dir = tempfile.gettempdir()
        tmp_path = os.path.join(tmp_dir, f"ops_read_{os.path.basename(path)}")
        shutil.copy2(path, tmp_path)
        xl = pd.ExcelFile(tmp_path)
        return pd.read_excel(tmp_path, sheet_name=xl.sheet_names[0])


def post_process(merged):
    """合并后的统一后处理。"""
    if merged.empty:
        return merged

    # 第 2 条：剔除服务大区为空白的行
    merged = merged[merged["服务大区"].notna()].copy()
    merged["服务大区"] = merged["服务大区"].astype(str).str.strip()
    merged = merged[merged["服务大区"] != ""]

    # 第 2 条补充：所有数据类别均剔除指定服务大区
    exclude_regions = {"产线", "其他", "园区集成", "行业集成"}
    merged = merged[~merged["服务大区"].isin(exclude_regions)]
    merged = merged.reset_index(drop=True)

    # 第 1 条：ACT 仅保留“是否业绩考核”含“业绩考核”的行（剔除纯财务结算）
    act_mask = merged["数据类别"] == "ACT"
    merged = merged[~(act_mask & ~merged["是否业绩考核"].astype(str).str.contains("业绩考核", na=False))]
    merged = merged.reset_index(drop=True)

    # 第 3、7 条：FCST 剔除“是否FCST”为 否 / 商机 的行
    fcst_mask = merged["数据类别"] == "FCST"
    merged = merged[~(fcst_mask & merged["是否FCST"].astype(str).isin(["否", "商机"]))]
    merged = merged.reset_index(drop=True)

    # 第 5 条：T1/T2/T3 FCST 剔除销售模式为“在途”的明细
    fcst_mask = merged["数据类别"] == "FCST"
    fcst_t_mask = fcst_mask & merged["产线类型"].isin(["T1", "T2", "T3"])
    merged = merged[~(fcst_t_mask & (merged["销售模式"].astype(str).str.strip() == "在途"))]

    # QTD 数据不取销售模式为“在途”的明细
    qtd_mask = merged["源表"] == "QTD"
    merged = merged[~(qtd_mask & (merged["销售模式"].astype(str).str.strip() == "在途"))]

    # 第 4 条：物料通路 / 产线+通路 名称归一化
    for col in ["物料通路", "产线+通路"]:
        merged[col] = merged[col].apply(norm_channel)

    merged = merged.reset_index(drop=True)
    return merged


def merge_all(files_or_paths, fcst_cycle=None, force_category=None):
    """处理多个文件并合并。

    fcst_cycle: 当批次含 FCST 系列源表时，给这些行统一打上 FCST Cycle 版本标签
    （如 "Week3"）；非 FCST 行该列留空。全量重建时通常不传（留空）。
    force_category: 用户已指定数据类型时，强制按该类型处理所有文件，
                    不再根据文件名识别（用于 历史Union/QTD/DG&Quota 等）。
                    FCST 不建议强制，因为 FCST 内部还有 T1/T2/T3 子类型。
    """
    results = []
    for item in files_or_paths:
        if isinstance(item, tuple):
            category, df = item
        else:
            path = item
            category = force_category or detect_category(path)
            if category is None:
                print(f"跳过无法识别的文件: {path}")
                continue
            df = read_first_sheet(path)

        if category not in PROCESSORS:
            print(f"跳过未支持的数据类别: {category}")
            continue
        if df.empty:
            print(f"{category}: 空数据，跳过")
            continue
        try:
            processed = PROCESSORS[category](df)
            processed = processed[FINAL_COLUMNS].copy()
            # 源表标签：记录每行来自哪个源表，便于按类型拆分展示
            # （历史Union 与 QTD 的“数据类别”同为 ACT，必须靠源表区分）。
            processed["源表"] = category
            # FCST Cycle：仅 FCST 系列打版本标签，其余留空
            processed["FCST Cycle"] = fcst_cycle if category in FCST_FAMILY else None
            # 拼接安全网：源表读出的 StringDtype（可空字符串）列与
            # 其它 object 类型列 concat 时，不同 pandas 版本下偶发 dtype
            # 不兼容导致值被置为 NaN。统一转成 object 规避。
            # `StringDtype` 的 dtype 名在不同版本可能为 'string' 或 'str'。
            for col in processed.columns:
                dt = str(processed[col].dtype)
                if dt in ("string", "str"):
                    processed[col] = processed[col].astype(object)
            results.append(processed)
            print(f"{category}: {len(processed)} 行 (FCST Cycle={fcst_cycle if category in FCST_FAMILY else '-'})")
        except Exception as e:
            print(f"处理 {category} 出错: {e}")

    if not results:
        return pd.DataFrame(columns=FINAL_ORDER)
    merged = pd.concat(results, ignore_index=True)
    merged = post_process(merged)
    # 统一输出列顺序：是否FCST 右侧插入 FCST Cycle，末尾追加 源表
    merged = merged[FINAL_ORDER]
    return merged


def incremental_merge(master, partial, upload_type, fcst_cycle=None):
    """增量合并：用新上传的 partial 更新主表 master。

    - 历史Union / QTD / DG&Quota：替换该来源的全部旧行（整类覆盖）。
    - FCST：按 (源表∈FCST系列) 且 (FCST Cycle==fcst_cycle) 精确替换，
      其余周版本的行保留，实现“传哪周更新哪周”。
    - master 为空时直接返回 partial。
    """
    if master is None or len(master) == 0:
        return partial.copy()
    if upload_type not in SOURCE_GROUPS:
        return pd.concat([master, partial], ignore_index=True)

    src_set = SOURCE_GROUPS[upload_type]
    master = master.copy()
    if "源表" not in master.columns:
        master["源表"] = None
    if "FCST Cycle" not in master.columns:
        master["FCST Cycle"] = None

    if upload_type == "FCST":
        is_fcst = master["源表"].isin(src_set)
        is_target = master["FCST Cycle"] == fcst_cycle
        is_blank = master["FCST Cycle"].isna()
        # 替换所选周；同时清掉“未标注版本”的 FCST 基线，
        # 这样首次上传周版本不会与全量重建产生的空白基线重复。
        keep = ~(is_fcst & (is_target | is_blank))
    else:
        keep = ~master["源表"].isin(src_set)

    updated = pd.concat([master[keep], partial.reindex(columns=FINAL_ORDER)], ignore_index=True)
    return updated[FINAL_ORDER]


if __name__ == "__main__":
    data_dir = r"C:\Users\fenglx1\OneDrive - Lenovo\1 - 自用\0-OPS\数据统一格式"
    files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith((".xlsx", ".xls"))]
    print(f"发现文件: {files}")
    merged = merge_all(files)
    output_path = r"C:\Users\fenglx1\WorkBuddy\2026-08-25-14-42-27\最终合并表.xlsx"
    merged.to_excel(output_path, index=False)
    print(f"合并完成，共 {len(merged)} 行，已导出: {output_path}")
