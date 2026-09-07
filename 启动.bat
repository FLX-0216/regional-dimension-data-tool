@echo off
chcp 65001 >nul
title 区域维度数据处理工具 - 启动器
cd /d "%~dp0"

REM ===== 1. 检测 Python（优先 py 启动器，其次 python / python3）=====
set "PY="
where py >nul 2>nul && set "PY=py"
if not defined PY (where python >nul 2>nul && set "PY=python")
if not defined PY (where python3 >nul 2>nul && set "PY=python3")
if not defined PY (
  echo.
  echo ============================================================
  echo   未检测到 Python，无法启动本工具。
  echo   请先安装 Python 3.10 及以上版本，安装时务必勾选
  echo   “Add Python to PATH”（添加到环境变量）。
  echo   下载地址： https://www.python.org/downloads/
  echo ============================================================
  echo.
  pause
  exit /b 1
)

REM ===== 2. 安装依赖（已安装会自动跳过，首次约 1-2 分钟）=====
echo 正在检查并安装所需依赖，请稍候……
%PY% -m pip install -r requirements.txt --quiet
if errorlevel 1 (
  echo.
  echo 依赖安装失败。请确认网络通畅，或右键本文件选择“以管理员身份运行”后重试。
  echo.
  pause
  exit /b 1
)

REM ===== 3. 启动 Streamlit 服务（独立窗口，关闭该窗口即退出）=====
echo.
echo 正在启动服务，浏览器将自动打开……
start "Streamlit服务" %PY% -m streamlit run app.py --server.headless true --browser.gatherUsageStats false --server.port 8501

REM ===== 4. 等待服务就绪后打开浏览器 =====
timeout /t 7 >nul
start "" http://localhost:8501
echo.
echo 工具已启动！若浏览器未自动弹出，请手动访问： http://localhost:8501
echo 使用完毕后，关闭标题为“Streamlit服务”的黑色窗口即可退出。
echo.
pause
