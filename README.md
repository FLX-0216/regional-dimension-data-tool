# 区域维度数据处理与导出

基于 Streamlit 的OPS/区域维度数据处理工具，支持多类型数据上传、分桶持久化、按维度筛选导出，以及 FCST 分析看板。

## 本地运行（命令行）

```bash
pip install -r requirements.txt
streamlit run app.py
```

## 共享文件夹 + 双击启动（推荐给团队，无需服务器/局域网）

适合分散在不同地点、不想每人搭环境的场景。原理：把整个程序文件夹放进
一个会自动同步的共享盘（OneDrive / 企业微信微盘 / 钉钉钉盘），每个人
同步到本地后双击「启动.bat」，Streamlit 就在自己电脑上运行，浏览器自动
打开 `http://localhost:8501`，互不影响。

1. 把本文件夹整体放进共享同步盘（保留 `app.py`、`ops_data_processor.py`、
   `requirements.txt`、`.streamlit/config.toml`、`启动.bat`、`使用说明.txt`，
   以及数据文件 `data_buckets/*.parquet`、`mapping_table.parquet`）。
2. 每位同事：确认本机已装 Python 3.10+ 并勾选 “Add Python to PATH”
   （详见 `使用说明.txt`）。
3. 双击「启动.bat」→ 浏览器自动打开即成功；用完关闭 “Streamlit服务”
   黑窗口退出。

> 共享盘只负责统一分发代码与数据，真正的运行在各人电脑本地，因此
> 不需要同一局域网，也不需要公网服务器。更新程序/数据时覆盖本文件夹
> 后重新启动即可。

## 部署到 Streamlit Community Cloud（可选，公网网址）

若希望给大家一个不用装 Python 的公网网址，可走 Streamlit Cloud：

1. 把本仓库文件上传到 **GitHub 仓库**（需包含 `app.py`、`ops_data_processor.py`、`requirements.txt`、`.streamlit/config.toml`，以及数据池文件 `data_buckets/*.parquet` 和 `mapping_table.parquet`）。
2. 在 [Streamlit Community Cloud](https://streamlit.io/cloud) 登录 GitHub 账号，选择该仓库。
3. **关键：在 Advanced settings（高级设置）里：**
   - 把 **Python version** 选成 **3.11**（目前 Cloud 默认 3.14 与部分依赖不兼容）。
   - 在 **Secrets / Environment variables** 里添加 `ARROW_DEFAULT_MEMORY_POOL=system`，避免 pyarrow 在 Streamlit 的脚本线程中触发 mimalloc 段错误。
4. 点 Deploy，等 1–2 分钟，生成一个 `https://xxx.streamlit.app` 的网址。
5. 把网址发给其他人，他们打开就能用。

> 说明：`runtime.txt` 在 Community Cloud 上不会被读取，Python 版本必须在网页端手动选；`requirements.txt` 仅设最低版本下限，便于在不同 Python 版本上自动解析兼容依赖。

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
