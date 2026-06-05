import os
import threading
import time
import secrets
from datetime import datetime, timedelta, timezone
from urllib.parse import quote
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Form, Depends, HTTPException, status, UploadFile, File, BackgroundTasks
from fastapi.responses import HTMLResponse, RedirectResponse, FileResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
import hashlib
import requests
import html
import concurrent.futures

# --- 缓存及并发防抖机制 ---


import database as db
import scanner
import position_query

# --- 安全：强制检查并移除固定的 SESSION_SECRET 默认值 ---
session_secret = os.environ.get("SESSION_SECRET")
if not session_secret or session_secret == "tao_pro_ultra_secret":
    raise RuntimeError(
        "CRITICAL SECURITY ERROR: SESSION_SECRET 环境变量未配置或使用了默认值！\n"
        "请在 .env 文件或系统的 systemd 服务环境变量中配置高强度的密钥。"
    )

# --- 稳定性：通过 FastAPI Lifespan 管理后台扫描线程的生命周期与单例保护 ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动前初始化
    db.init_db()
    
    # 启动扫描线程
    threading.Thread(target=scanner.start_scanner, daemon=True).start()
    
    # 自动为数据库中存在但没有缓存的钱包进行后台初始化
    def auto_init_missing_caches():
        time.sleep(5)  # 等待扫描器与 WSS 准备就绪
        try:
            conn = db.get_db()
            missing_wallets = conn.execute("""
                SELECT DISTINCT w.address 
                FROM wallets w
                WHERE w.is_active = 1
                  AND w.address NOT IN (SELECT DISTINCT address FROM wallets_cache)
            """).fetchall()
            conn.close()
            
            if missing_wallets:
                db.add_log("INFO", f"启动自检：发现 {len(missing_wallets)} 个监控钱包缺失本地持仓缓存，开始后台批量初始化...")
                for row in missing_wallets:
                    addr = row["address"]
                    position_query.initialize_wallet_cache(addr)
                    time.sleep(1)  # 稍微控制频次，避免 RPC 节点限频
                db.add_log("INFO", "启动自检：缺失的钱包持仓缓存初始化队列执行完毕。")
        except Exception as e:
            db.add_log("ERROR", f"启动自检初始化缓存失败: {str(e)}")

    threading.Thread(target=auto_init_missing_caches, daemon=True).start()
    
    yield
    
    # 停止扫描器，释放 WSS 资源
    scanner.stop_scanner()

app = FastAPI(title="TAO 生态监控系统 PRO", lifespan=lifespan)
app.add_middleware(SessionMiddleware, secret_key=session_secret)

# 自动适配模板目录：如果脚本同级目录下存在 templates 目录，则直接使用它；否则回退到父级目录下的 templates
local_templates = os.path.join(os.path.dirname(__file__), "templates")
if os.path.isdir(local_templates):
    templates = Jinja2Templates(directory=local_templates)
else:
    templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "..", "templates"))

# --- CSRF 安全防御机制 ---
def get_csrf_token(request: Request):
    if "csrf_token" not in request.session:
        request.session["csrf_token"] = secrets.token_urlsafe(32)
    return request.session["csrf_token"]

# 将 CSRF 令牌生成函数注入到 Jinja2 模板全局变量中
templates.env.globals["get_csrf_token"] = get_csrf_token

def verify_csrf(request: Request, csrf_token: str):
    session_token = request.session.get("csrf_token")
    if not session_token or csrf_token != session_token:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="CSRF Token 验证失败，请刷新页面后重试。"
        )

BJ_OFFSET_SECONDS = 8 * 60 * 60
BEIJING_TZ = timezone(timedelta(hours=8))

def to_beijing_datetime_str(value):
    if not value:
        return ""
    try:
        dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        return dt.astimezone(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return value

def with_beijing_created_at(rows):
    converted = []
    for row in rows:
        item = dict(row) if not isinstance(row, dict) else dict(row)
        item["created_at_bj"] = to_beijing_datetime_str(item.get("created_at"))
        converted.append(item)
    return converted

def get_uptime_seconds():
    if scanner.LAST_CONNECT_TIME == 0:
        return 0
    return int(time.time() - scanner.LAST_CONNECT_TIME)

def get_runtime_status():
    groups = db.get_groups()
    configured_tg = [g for g in groups if g.get("tg_token") and g.get("tg_chat_id")]
    missing_tg = [g for g in groups if not g.get("tg_token") or not g.get("tg_chat_id")]

    # 彻底弃用通过 system_logs 判定健康状态的旧逻辑，改用内存中 LAST_TG_ERROR 的实时判定

    if scanner.LAST_CONNECT_TIME > 0:
        wss_label = f"WSS 已连接 ({scanner.CURRENT_WSS_LABEL})" if scanner.CURRENT_WSS_LABEL else "WSS 已连接"
        wss_ok = True
        wss_detail = f"连接时长 {time.strftime('%H:%M:%S', time.gmtime(get_uptime_seconds()))}"
    else:
        wss_label = "WSS 未连接"
        wss_ok = False
        wss_detail = scanner.LAST_ERROR or "等待连接"

    if scanner.LAST_BLOCK_TIME > 0:
        last_scan_label = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(scanner.LAST_BLOCK_TIME))
        last_scan_detail = f"最近区块 #{scanner.LAST_BLOCK_NUMBER}"
    else:
        last_scan_label = "暂无扫描"
        last_scan_detail = "启动后收到新区块才会显示"

    # 仅使用内存中的 LAST_TG_ERROR 判定异常（与 WSS 监控状态逻辑保持一致）
    if scanner.LAST_TG_ERROR:
        tg_label = "Telegram 推送异常"
        tg_ok = False
        tg_detail = scanner.LAST_TG_ERROR
    elif missing_tg:
        tg_label = "Telegram 配置不完整"
        tg_ok = False
        tg_detail = f"{len(missing_tg)} 个分组还没填 Bot Token 或 Chat ID"
    elif configured_tg:
        tg_label = "Telegram 已配置"
        tg_ok = True
        tg_detail = f"已配置 {len(configured_tg)} 个分组"
    else:
        tg_label = "Telegram 未配置"
        tg_ok = False
        tg_detail = "还没有监控分组"

    uptime_data = db.get_uptime_data()
    uptime_rate = 0
    if uptime_data:
        uptime_rate = round((sum(uptime_data) / len(uptime_data)) * 100, 2)
    tg_delivery = db.get_notification_success_rate(24)

    return {
        "wss_ok": wss_ok,
        "wss_label": wss_label,
        "wss_detail": wss_detail,
        "last_scan_label": last_scan_label,
        "last_scan_detail": last_scan_detail,
        "tg_ok": tg_ok,
        "tg_label": tg_label,
        "tg_detail": tg_detail,
        "uptime": time.strftime('%H:%M:%S', time.gmtime(get_uptime_seconds())),
        "wss_latency_ms": scanner.LAST_WSS_LATENCY_MS if scanner.LAST_CONNECT_TIME > 0 else 0,
        "notification_count": db.get_notification_audit_count(),
        "uptime_rate": uptime_rate,
        "tg_delivery_rate": tg_delivery["rate"],
        "tg_delivery_total": tg_delivery["total"],
        "tg_delivery_sent": tg_delivery["sent_total"],
        "tg_queue_size": scanner.TG_QUEUE.qsize(),
    }

def check_login(request: Request):
    if not request.session.get("user"):
        raise HTTPException(status_code=status.HTTP_302_FOUND, headers={"Location": "/login"})
    return True

# --- 路由定义 (同步运行，避免事件循环卡死) ---

@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

@app.post("/login")
def do_login(request: Request, username: str = Form(...), password: str = Form(...)):
    # 防爆破登录锁定逻辑
    ip = request.client.host
    locked, remaining = db.is_login_locked(ip)
    if locked:
        return templates.TemplateResponse("login.html", {
            "request": request,
            "error": f"登录失败次数过多，此 IP 被限制登录。请在 {int(remaining/60) + 1} 分钟后再试。"
        })

    if db.verify_user(username, password):
        db.record_login_attempt(ip, success=True)
        request.session["user"] = username
        return RedirectResponse(url="/", status_code=status.HTTP_302_FOUND)
        
    db.record_login_attempt(ip, success=False)
    return templates.TemplateResponse("login.html", {"request": request, "error": "账号或密码错误"})

# --- 安全修复：退出登录修改为 POST ---
@app.post("/logout")
def logout(request: Request, csrf_token: str = Form(...)):
    check_login(request)
    verify_csrf(request, csrf_token)
    request.session.clear()
    return RedirectResponse(url="/login", status_code=303)

@app.get("/api/status")
def api_status(request: Request):
    check_login(request)
    return JSONResponse(get_runtime_status())

@app.get("/api/test_wss/{slot}")
def api_test_wss(request: Request, slot: str):
    check_login(request)
    if slot not in {"primary", "backup", "query", "query_backup"}:
        raise HTTPException(status_code=400, detail="invalid slot")

    if slot == "primary":
        key = "dwellir_wss"
        default = "wss://api-bittensor-mainnet.n.dwellir.com"
    elif slot == "backup":
        key = "dwellir_wss_backup"
        default = ""
    elif slot == "query":
        key = "query_wss"
        default = ""
    else:
        key = "query_wss_backup"
        default = ""

    raw_url = db.get_setting(key, default).strip()
    if not raw_url:
        raise HTTPException(status_code=400, detail="empty endpoint")

    try:
        result = scanner.test_wss_endpoint(raw_url)
        return JSONResponse({"ok": True, "slot": slot, **result})
    except Exception as e:
        return JSONResponse({"ok": False, "slot": slot, "url": raw_url, "error": str(e)}, status_code=200)

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    try:
        check_login(request)
    except:
        return RedirectResponse("/login")

    uptime_data = db.get_uptime_data()
    uptime_points = db.get_uptime_series()
    uptime_sec = get_uptime_seconds()
    uptime_str = time.strftime('%H:%M:%S', time.gmtime(uptime_sec))
    return templates.TemplateResponse("index.html", {
        "request": request,
        "page": "dashboard",
        "uptime": uptime_str,
        "uptime_history": uptime_data,
        "uptime_points": uptime_points,
        "status": get_runtime_status()
    })

@app.get("/logs", response_class=HTMLResponse)
def logs_page(request: Request):
    check_login(request)
    conn = db.get_db()
    logs = conn.execute("SELECT * FROM system_logs ORDER BY created_at DESC LIMIT 100").fetchall()
    conn.close()
    logs = with_beijing_created_at(logs)
    uptime_sec = get_uptime_seconds()
    uptime_str = time.strftime('%H:%M:%S', time.gmtime(uptime_sec))
    return templates.TemplateResponse("index.html", {
        "request": request,
        "page": "logs",
        "logs": logs,
        "uptime": uptime_str,
        "status": get_runtime_status()
    })

@app.get("/audit", response_class=HTMLResponse)
def audit_page(request: Request):
    check_login(request)
    audit_logs = with_beijing_created_at(db.get_notification_audit_logs(100))
    uptime_sec = get_uptime_seconds()
    uptime_str = time.strftime('%H:%M:%S', time.gmtime(uptime_sec))
    return templates.TemplateResponse("index.html", {
        "request": request,
        "page": "audit",
        "audit_logs": audit_logs,
        "uptime": uptime_str,
        "status": get_runtime_status()
    })

@app.get("/monitoring", response_class=HTMLResponse)
def monitoring_page(request: Request):
    check_login(request)
    open_group = request.query_params.get("open_group", "")
    groups = db.get_groups()
    for g in groups:
        g['wallets'] = db.get_wallets_by_group(g['id'])

    uptime_sec = get_uptime_seconds()
    uptime_str = time.strftime('%H:%M:%S', time.gmtime(uptime_sec))
    return templates.TemplateResponse("index.html", {
        "request": request,
        "page": "monitoring",
        "groups": groups,
        "open_group": open_group,
        "uptime": uptime_str,
        "status": get_runtime_status()
    })

@app.get("/settings", response_class=HTMLResponse)
def settings_page(request: Request):
    check_login(request)
    settings = {
        "dwellir_wss": db.get_setting("dwellir_wss", "wss://api-bittensor-mainnet.n.dwellir.com"),
        "dwellir_wss_backup": db.get_setting("dwellir_wss_backup", ""),
        "query_wss": db.get_setting("query_wss", ""),
        "query_wss_backup": db.get_setting("query_wss_backup", ""),
        "wss_load_balance": db.get_setting("wss_load_balance", "0"),
        "tg_throttle_ms": db.get_setting("tg_throttle_ms", "500"),
        "public_url": db.get_setting("public_url", ""),
        "cache_threshold_tao": db.get_setting("cache_threshold_tao", "60.0"),
    }
    cache_stats = db.get_cache_stats()
    uptime_sec = get_uptime_seconds()
    uptime_str = time.strftime('%H:%M:%S', time.gmtime(uptime_sec))
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "page": "settings",
            "settings": settings,
            "cache_stats": cache_stats,
            "uptime": uptime_str,
            "status": get_runtime_status()
        }
    )

@app.post("/group/add")
def add_group(request: Request, name: str = Form(...), type: str = Form(...), csrf_token: str = Form(...)):
    check_login(request)
    verify_csrf(request, csrf_token)
    db.execute_write("INSERT INTO monitor_groups (name, type) VALUES (?, ?)", (name, type))
    return RedirectResponse("/monitoring", status_code=303)

@app.post("/group/update/{id}")
def update_group(
    request: Request,
    id: int,
    tg_token: str = Form(""),
    tg_chat_id: str = Form(""),
    tg_token_backup: str = Form(""),
    tg_chat_id_backup: str = Form(""),
    split_stake_bots: str = Form("0"),
    threshold_tao: float = Form(5.0),
    csrf_token: str = Form(...),
):
    check_login(request)
    verify_csrf(request, csrf_token)
    db.execute_write(
        """
        UPDATE monitor_groups
        SET tg_token=?, tg_chat_id=?, tg_token_backup=?, tg_chat_id_backup=?, split_stake_bots=?, threshold_tao=?
        WHERE id=?
        """,
        (
            tg_token,
            tg_chat_id,
            tg_token_backup,
            tg_chat_id_backup,
            1 if split_stake_bots == "1" else 0,
            threshold_tao,
            id,
        )
    )
    # 修改配置成功后，立刻重置内存中旧的 TG 报错状态，确保主页卡片状态实时刷新
    scanner.LAST_TG_ERROR = ""
    return RedirectResponse(f"/monitoring?open_group={id}", status_code=303)

@app.post("/group/rename/{id}")
def rename_group(request: Request, id: int, name: str = Form(...), csrf_token: str = Form(...)):
    check_login(request)
    verify_csrf(request, csrf_token)
    db.execute_write("UPDATE monitor_groups SET name=? WHERE id=?", (name.strip(), id))
    return RedirectResponse(f"/monitoring?open_group={id}", status_code=303)

@app.post("/group/delete/{id}")
def delete_group(request: Request, id: int, csrf_token: str = Form(...)):
    check_login(request)
    verify_csrf(request, csrf_token)
    def operation(conn):
        conn.execute("DELETE FROM monitor_groups WHERE id=?", (id,))
        conn.execute("DELETE FROM wallets_cache WHERE address NOT IN (SELECT address FROM wallets)")
    db.execute_write_returning(operation)
    return RedirectResponse("/monitoring", status_code=303)

@app.post("/wallet/add")
def add_wallet(request: Request, group_id: int = Form(...), address: str = Form(...), alias: str = Form(...), csrf_token: str = Form(...)):
    check_login(request)
    verify_csrf(request, csrf_token)
    addr_clean = address.strip()
    db.execute_write("INSERT INTO wallets (group_id, address, alias) VALUES (?, ?, ?)", (group_id, addr_clean, alias.strip()))
    # 在后台线程中立即为该监控钱包初始化拉取并建立持仓缓存
    threading.Thread(target=position_query.initialize_wallet_cache, args=(addr_clean,), daemon=True).start()
    return RedirectResponse(f"/monitoring?open_group={group_id}", status_code=303)

@app.post("/wallet/toggle/{id}")
def toggle_wallet(request: Request, id: int, state: int = Form(...), csrf_token: str = Form(...)):
    check_login(request)
    verify_csrf(request, csrf_token)
    def operation(conn):
        wallet = conn.execute("SELECT group_id FROM wallets WHERE id=?", (id,)).fetchone()
        conn.execute("UPDATE wallets SET is_active=? WHERE id=?", (state, id))
        return wallet["group_id"] if wallet else ""

    group_id = db.execute_write_returning(operation)
    return RedirectResponse(f"/monitoring?open_group={group_id}", status_code=303)

@app.post("/wallet/delete/{id}")
def del_wallet(request: Request, id: int, csrf_token: str = Form(...)):
    check_login(request)
    verify_csrf(request, csrf_token)
    def operation(conn):
        wallet = conn.execute("SELECT group_id, address FROM wallets WHERE id=?", (id,)).fetchone()
        if wallet:
            addr = wallet["address"]
            conn.execute("DELETE FROM wallets WHERE id=?", (id,))
            conn.execute("DELETE FROM wallets_cache WHERE address = ? AND NOT EXISTS (SELECT 1 FROM wallets WHERE address = ?)", (addr, addr))
            return wallet["group_id"]
        return ""

    group_id = db.execute_write_returning(operation)
    return RedirectResponse(f"/monitoring?open_group={group_id}", status_code=303)

@app.post("/save_settings")
def save_sys_settings(
    request: Request, 
    dwellir_wss: str = Form(...), 
    dwellir_wss_backup: str = Form(""), 
    query_wss: str = Form(""), 
    query_wss_backup: str = Form(""), 
    wss_load_balance: str = Form("0"), 
    tg_throttle_ms: str = Form(...),
    cache_threshold_tao: str = Form("60.0"),
    csrf_token: str = Form(...)
):
    check_login(request)
    verify_csrf(request, csrf_token)
    
    db.set_setting("dwellir_wss", dwellir_wss.strip())
    db.set_setting("dwellir_wss_backup", dwellir_wss_backup.strip())
    db.set_setting("query_wss", query_wss.strip())
    db.set_setting("query_wss_backup", query_wss_backup.strip())
    db.set_setting("wss_load_balance", "1" if wss_load_balance == "1" else "0")
    db.set_setting("tg_throttle_ms", tg_throttle_ms)
    db.set_setting("cache_threshold_tao", cache_threshold_tao.strip())
    
    with position_query.QUERY_SUBSTRATE_LOCK:
        if position_query.QUERY_SUBSTRATE:
            try:
                position_query.QUERY_SUBSTRATE.close()
            except Exception:
                pass
            position_query.QUERY_SUBSTRATE = None

    return RedirectResponse("/settings", status_code=303)

@app.post("/test_tg/{group_id}")
def test_tg(request: Request, group_id: int, csrf_token: str = Form(...)):
    check_login(request)
    verify_csrf(request, csrf_token)
    success, error = scanner.send_telegram_msg_to_group_now(group_id, "🔔 <b>测试通知</b>\n该分组机器人配置正确！", allow_retry=False)
    if success:
        return RedirectResponse(f"/monitoring?open_group={group_id}&msg=Success", status_code=303)
    error_text = str(error or "Unknown error").replace("\n", " ")[:160]
    safe_error = quote(f"Failed: {error_text}")
    return RedirectResponse(f"/monitoring?open_group={group_id}&msg={safe_error}", status_code=303)

@app.get("/backup")
def backup(request: Request):
    check_login(request)
    return FileResponse(db.DB_PATH, filename="tao_pro_backup.db")

def build_wallet_txt(groups):
    lines = []
    for group in groups:
        lines.append(f"# {group['name']}|{group['type']}")
        for wallet in group.get("wallets", []):
            lines.append(f"{wallet['address']}|{wallet['alias']}")
        lines.append("")
    return "\n".join(lines).strip() + "\n"

def parse_wallet_txt(text):
    parsed = []
    current_group = None
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("# "):
            payload = line[2:]
            if "|" not in payload:
                raise ValueError(f"分组头格式错误: {line}")
            name, group_type = [part.strip() for part in payload.split("|", 1)]
            if group_type not in {"whale", "wallet"}:
                raise ValueError(f"分组类型错误: {group_type}")
            current_group = {"name": name, "type": group_type, "wallets": []}
            parsed.append(current_group)
            continue
        if current_group is None:
            raise ValueError("钱包数据前缺少分组头")
        if "|" not in line:
            raise ValueError(f"钱包行格式错误: {line}")
        address, alias = [part.strip() for part in line.split("|", 1)]
        if not address or not alias:
            raise ValueError(f"钱包行不能为空: {line}")
        current_group["wallets"].append({"address": address, "alias": alias})
    return parsed

@app.get("/export_wallets")
def export_wallets(request: Request):
    check_login(request)
    groups = db.get_groups()
    for group in groups:
        group["wallets"] = db.get_wallets_by_group(group["id"])
    content = build_wallet_txt(groups)
    export_path = os.path.join(os.path.dirname(__file__), "wallet_groups_export.txt")
    with open(export_path, "w", encoding="utf-8", newline="\n") as f:
        f.write(content)
    return FileResponse(export_path, filename="wallet_groups_export.txt", media_type="text/plain")

@app.get("/wallet_template")
def wallet_template(request: Request):
    check_login(request)
    template = "# 默认巨鲸组|whale\n5ABC...地址1|备注1\n5DEF...地址2|备注2\n\n# 默认钱包组|wallet\n5XYZ...地址3|备注3\n"
    template_path = os.path.join(os.path.dirname(__file__), "wallet_groups_template.txt")
    with open(template_path, "w", encoding="utf-8", newline="\n") as f:
        f.write(template)
    return FileResponse(template_path, filename="wallet_groups_template.txt", media_type="text/plain")

@app.post("/import_wallets")
def import_wallets(request: Request, file: UploadFile = File(...), csrf_token: str = Form(...)):
    check_login(request)
    verify_csrf(request, csrf_token)
    
    raw = file.file.read()
    text = raw.decode("utf-8")
    parsed_groups = parse_wallet_txt(text)

    addresses_to_init = set()

    def operation(conn):
        for parsed_group in parsed_groups:
            existing_group = conn.execute(
                "SELECT * FROM monitor_groups WHERE name = ? AND type = ?",
                (parsed_group["name"], parsed_group["type"])
            ).fetchone()
            if existing_group:
                group_id = existing_group["id"]
            else:
                conn.execute(
                    "INSERT INTO monitor_groups (name, type) VALUES (?, ?)",
                    (parsed_group["name"], parsed_group["type"])
                )
                group_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

            for wallet in parsed_group["wallets"]:
                addr = wallet["address"].strip()
                addresses_to_init.add(addr)
                existing_wallet = conn.execute(
                    "SELECT id FROM wallets WHERE group_id = ? AND address = ?",
                    (group_id, addr)
                ).fetchone()
                if existing_wallet:
                    conn.execute(
                        "UPDATE wallets SET alias = ? WHERE id = ?",
                        (wallet["alias"], existing_wallet["id"])
                    )
                else:
                    conn.execute(
                        "INSERT INTO wallets (group_id, address, alias) VALUES (?, ?, ?)",
                        (group_id, addr, wallet["alias"])
                    )

    db.execute_write_returning(operation)
    
    # 后台线程异步批量初始化这些导入的钱包缓存
    for addr in addresses_to_init:
        threading.Thread(target=position_query.initialize_wallet_cache, args=(addr,), daemon=True).start()

    return RedirectResponse("/monitoring", status_code=303)

def extract_numeric_value(obj):
    """提取 Substrate 节点返回数据中各种嵌套类型的数值 (兼容 dict/SafeFloat/U64/ScaleObj 等)"""
    if obj is None:
        return 0.0
    if isinstance(obj, (int, float)):
        return float(obj)
    if isinstance(obj, dict):
        if 'mantissa' in obj and 'exponent' in obj:
            try:
                return float(obj['mantissa']) * (10.0 ** float(obj['exponent']))
            except Exception:
                return 0.0
        if 'bits' in obj:
            return float(obj['bits'])
        if 'value' in obj:
            return extract_numeric_value(obj['value'])
    if hasattr(obj, 'value'):
        return extract_numeric_value(obj.value)
    return 0.0

@app.post("/api/clear_non_monitored_cache")
def clear_non_monitored_cache(request: Request, csrf_token: str = Form(...)):
    check_login(request)
    verify_csrf(request, csrf_token)
    def operation(conn):
        conn.execute("DELETE FROM wallets_cache WHERE address NOT IN (SELECT address FROM wallets)")
    db.execute_write_returning(operation)
    db.vacuum_db()
    return {"ok": True}

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
