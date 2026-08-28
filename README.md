# 区域维度数据处理与导出

基于 Streamlit 的OPS/区域维度数据处理工具，支持多类型数据上传、分桶持久化、按维度筛选导出，以及 FCST 分析看板。

## 本地运行

```bash
pip install -r requirements.txt
streamlit run app.py
```

## 部署到 Streamlit Community Cloud（推荐）

1. 把本仓库文件上传到 **GitHub 仓库**（需包含 `app.py`、`ops_data_processor.py`、`requirements.txt`、`.streamlit/config.toml`，以及数据池文件 `data_buckets/*.parquet` 和 `mapping_table.parquet`）。
2. 在 [Streamlit Community Cloud](https://streamlit.io/cloud) 登录 GitHub 账号，选择该仓库。
3. 平台自动部署，生成一个 `https://xxx.streamlit.app` 的网址。
4. 把网址发给其他人，他们打开就能用。

## 数据池文件说明

- `data_buckets/data_历史Union.parquet`
- `data_buckets/data_QTD.parquet`
- `data_buckets/data_FCST.parquet`
- `data_buckets/data_DG_Quota.parquet`
- `mapping_table.parquet`

这些是应用的核心数据文件，部署时必须一起带上，否则页面打开后数据池为空。

## 主要功能

- 上传历史Union、QTD、FCST、DG&Quota 数据并追加到数据池
- 产线维度 Mapping 上传与自动匹配
- 按多维度筛选并导出明细或透视汇总
- FCST 分析看板：TTL / APOS / POS 层级下钻、DG% / Quota% / YOY% / WTW、FCST by Week 趋势
