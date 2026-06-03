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

# 全局查询线程池，避免高频点击时重复新建线程池的开销
QUERY_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=5)

import database as db
import scanner

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
    
    # 启动后异步自动注册 Webhook
    register_all_webhooks()
    
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
    if slot not in {"primary", "backup", "query"}:
        raise HTTPException(status_code=400, detail="invalid slot")

    if slot == "primary":
        key = "dwellir_wss"
        default = "wss://api-bittensor-mainnet.n.dwellir.com"
    elif slot == "backup":
        key = "dwellir_wss_backup"
        default = ""
    else:
        key = "query_wss"
        default = "wss://api-bittensor-mainnet.n.dwellir.com"

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
        "wss_load_balance": db.get_setting("wss_load_balance", "0"),
        "tg_throttle_ms": db.get_setting("tg_throttle_ms", "500"),
        "public_url": db.get_setting("public_url", ""),
        "query_wss": db.get_setting("query_wss", ""),
    }
    uptime_sec = get_uptime_seconds()
    uptime_str = time.strftime('%H:%M:%S', time.gmtime(uptime_sec))
    return templates.TemplateResponse("index.html", {"request": request, "page": "settings", "settings": settings, "uptime": uptime_str, "status": get_runtime_status()})

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
    register_all_webhooks()
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
    db.execute_write("DELETE FROM monitor_groups WHERE id=?", (id,))
    return RedirectResponse("/monitoring", status_code=303)

@app.post("/wallet/add")
def add_wallet(request: Request, group_id: int = Form(...), address: str = Form(...), alias: str = Form(...), csrf_token: str = Form(...)):
    check_login(request)
    verify_csrf(request, csrf_token)
    db.execute_write("INSERT INTO wallets (group_id, address, alias) VALUES (?, ?, ?)", (group_id, address.strip(), alias.strip()))
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
        wallet = conn.execute("SELECT group_id FROM wallets WHERE id=?", (id,)).fetchone()
        conn.execute("DELETE FROM wallets WHERE id=?", (id,))
        return wallet["group_id"] if wallet else ""

    group_id = db.execute_write_returning(operation)
    return RedirectResponse(f"/monitoring?open_group={group_id}", status_code=303)

@app.post("/save_settings")
def save_sys_settings(
    request: Request, 
    dwellir_wss: str = Form(...), 
    dwellir_wss_backup: str = Form(""), 
    wss_load_balance: str = Form("0"), 
    tg_throttle_ms: str = Form(...),
    public_url: str = Form(""),
    query_wss: str = Form(""),
    csrf_token: str = Form(...)
):
    check_login(request)
    verify_csrf(request, csrf_token)
    
    public_url_clean = public_url.strip()
    if public_url_clean and not public_url_clean.lower().startswith("https://"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="公网基础 URL 必须以 https:// 开头，Telegram Webhook 仅支持 HTTPS 地址。"
        )
        
    db.set_setting("dwellir_wss", dwellir_wss.strip())
    db.set_setting("dwellir_wss_backup", dwellir_wss_backup.strip())
    db.set_setting("wss_load_balance", "1" if wss_load_balance == "1" else "0")
    db.set_setting("tg_throttle_ms", tg_throttle_ms)
    db.set_setting("public_url", public_url_clean)
    db.set_setting("query_wss", query_wss.strip())
    register_all_webhooks()
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
    
    # 性能优化：直接使用同步读取，避免事件循环空转
    raw = file.file.read()
    text = raw.decode("utf-8")
    parsed_groups = parse_wallet_txt(text)

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
                existing_wallet = conn.execute(
                    "SELECT id FROM wallets WHERE group_id = ? AND address = ?",
                    (group_id, wallet["address"])
                ).fetchone()
                if existing_wallet:
                    conn.execute(
                        "UPDATE wallets SET alias = ? WHERE id = ?",
                        (wallet["alias"], existing_wallet["id"])
                    )
                else:
                    conn.execute(
                        "INSERT INTO wallets (group_id, address, alias) VALUES (?, ?, ?)",
                        (group_id, wallet["address"], wallet["alias"])
                    )

    db.execute_write_returning(operation)
    return RedirectResponse("/monitoring", status_code=303)

# --- Telegram Webhook Helper Functions ---

def extract_numeric_value(obj):
    """提取 Substrate 节点返回数据中各种嵌套类型的数值 (兼容 dict/U64/ScaleObj 等)"""
    if obj is None:
        return 0.0
    if isinstance(obj, (int, float)):
        return float(obj)
    if isinstance(obj, dict):
        if 'bits' in obj:
            return float(obj['bits'])
        if 'value' in obj:
            return extract_numeric_value(obj['value'])
    if hasattr(obj, 'value'):
        return extract_numeric_value(obj.value)
    return 0.0

def register_webhook(bot_token, public_url):
    if not bot_token or not public_url:
        return
    token_hash = hashlib.md5(bot_token.encode("utf-8")).hexdigest()
    # 生成安全的 Webhook 验证 Secret Token
    secret_token = hashlib.sha256((bot_token + session_secret).encode("utf-8")).hexdigest()
    url = public_url.strip().rstrip('/')
    webhook_url = f"{url}/tg/webhook/{token_hash}"
    api_url = f"https://api.telegram.org/bot{bot_token}/setWebhook"
    try:
        res = requests.post(api_url, json={"url": webhook_url, "secret_token": secret_token}, timeout=5)
        if res.ok:
            db.add_log("INFO", f"TG 机器人 Webhook 注册成功: {token_hash[:8]}...")
        else:
            db.add_log("ERROR", f"TG 机器人 Webhook 注册失败 ({res.status_code}): {res.text[:200]}")
    except Exception as e:
        db.add_log("ERROR", f"TG 机器人 Webhook 注册发生异常: {str(e)}")

def register_all_webhooks():
    public_url = db.get_setting("public_url", "").strip()
    if not public_url:
        db.add_log("INFO", "未配置公网 URL，跳过 TG Webhook 注册")
        return
    
    tokens = set()
    groups = db.get_groups()
    for g in groups:
        if g.get("tg_token"):
            tokens.add(g["tg_token"].strip())
        if g.get("tg_token_backup"):
            tokens.add(g["tg_token_backup"].strip())
            
    for token in tokens:
        threading.Thread(target=register_webhook, args=(token, public_url), daemon=True).start()

def edit_message_text(bot_token, chat_id, message_id, original_text, append_text, reply_markup=None):
    # 定义切割标识符，避免多次重复点击时追加多个旧仓位或报错区块
    markers = [
        "\n\n💰 <b>当前操作者仓位</b>",
        "\n\n💰 <b>剩余可用:</b>",
        "\n\n💰 剩余可用:",
        "\n\n❌ 当前仓位查询超时",
        "\n\n❌ 查询超时",
        "\n\n❌ 节点"
    ]
    for marker in markers:
        if marker in original_text:
            original_text = original_text.split(marker)[0]
        
    new_text = original_text + append_text
    url = f"https://api.telegram.org/bot{bot_token}/editMessageText"
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": new_text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
        
    try:
        requests.post(url, json=payload, timeout=5)
    except Exception as e:
        db.add_log("ERROR", f"编辑 TG 消息失败: {str(e)}")

def _query_blockchain_data(dwellir_wss, address, netuid):
    from substrateinterface import SubstrateInterface
    substrate = None
    try:
        substrate = SubstrateInterface(url=dwellir_wss, ws_options={"timeout": 5})
        
        # A. 查询可用 TAO 余额
        account_info = substrate.query(
            module="System",
            storage_function="Account",
            params=[address]
        )
        free_tao = 0.0
        if account_info:
            val_dict = account_info.value if hasattr(account_info, "value") else account_info
            if isinstance(val_dict, dict):
                free_tao = float(val_dict.get("data", {}).get("free", 0)) / 1e9
            
        # B. 聚合当前冷钱包在此子网上的 Alpha 质押余额（采用 O(1) 按 Key 精确点对点查询，代替 query_map 扫链）
        alpha_stake = 0.0
        try:
            # 查出该 coldkey 关联的所有 active staking hotkeys
            hotkeys_obj = substrate.query("SubtensorModule", "StakingHotkeys", [address])
            hotkeys = hotkeys_obj.value if hasattr(hotkeys_obj, 'value') else hotkeys_obj
            
            if isinstance(hotkeys, list) and len(hotkeys) > 0:
                for hk in hotkeys:
                    hk_str = hk[0] if isinstance(hk, (list, tuple)) else hk
                    
                    val = 0.0
                    # 依次查找新旧版本 Alpha 存储
                    for storage_name in ("Alpha", "AlphaV2"):
                        try:
                            alpha_obj = substrate.query("SubtensorModule", storage_name, [hk_str, address, int(netuid)])
                            if alpha_obj is not None:
                                val = extract_numeric_value(alpha_obj)
                                break
                        except Exception:
                            continue
                            
                    # 备用回退到旧版 Stake 字段
                    if val == 0.0:
                        try:
                            stake_obj = substrate.query("SubtensorModule", "Stake", [hk_str, address])
                            if stake_obj is not None:
                                val = extract_numeric_value(stake_obj)
                        except Exception:
                            pass
                            
                    alpha_stake += val / 1e9
        except Exception as e:
            db.add_log("ERROR", f"按 Key 点对点查询 Alpha 余额时出错: {str(e)}")
            
        # C. 查询子网池以估算 Alpha 价值的 TAO 数量（修复表名：SubnetTA -> SubnetTAO）
        equivalent_tao = None
        price = None
        try:
            tao_pool_obj = substrate.query("SubtensorModule", "SubnetTAO", [int(netuid)])
            alpha_pool_obj = substrate.query("SubtensorModule", "SubnetAlphaIn", [int(netuid)])
            
            tao_pool = extract_numeric_value(tao_pool_obj)
            alpha_pool = extract_numeric_value(alpha_pool_obj)
            
            if alpha_pool > 0:
                price = tao_pool / alpha_pool
                equivalent_tao = alpha_stake * price
        except Exception as e:
            db.add_log("ERROR", f"估算 Alpha 折算 TAO 时出错: {str(e)}")
            
        return free_tao, alpha_stake, equivalent_tao, price
    finally:
        if substrate:
            try:
                substrate.close()
            except Exception:
                pass

def handle_tg_callback(bot_token, callback_query):
    callback_id = callback_query.get("id")
    callback_data = callback_query.get("data", "")
    message = callback_query.get("message")
    if not message:
        return
        
    chat_id = message["chat"]["id"]
    message_id = message["message_id"]
    
    # 1. 立即回复 Telegram 消除转圈
    ans_url = f"https://api.telegram.org/bot{bot_token}/answerCallbackQuery"
    try:
        requests.post(ans_url, json={"callback_query_id": callback_id}, timeout=5)
    except Exception:
        pass
        
    # 2. 解析 callback_data: qb:{netuid}:{address}
    parts = callback_data.split(":")
    if len(parts) < 3:
        return
    netuid = parts[1]
    address = parts[2]
    
    # 为了避免 message.text 丢失 HTML 格式标签，优先从审计日志中查找原始 HTML 消息
    original_text = ""
    try:
        conn = db.get_db()
        row = conn.execute(
            "SELECT message FROM notification_audit_logs "
            "WHERE (address = ? OR from_address = ? OR to_address = ?) AND netuid = ? "
            "ORDER BY created_at DESC LIMIT 1",
            (address, address, address, str(netuid))
        ).fetchone()
        conn.close()
        if row and row["message"]:
            original_text = row["message"]
    except Exception as e:
        db.add_log("WARN", f"从审计日志获取 HTML 原始消息出错: {str(e)}")
        
    if not original_text:
        # 降级备用：使用 Telegram 附带的纯文本并进行 HTML 转义以防止标签解析报错
        original_text = html.escape(message.get("text") or "")
    
    # 3. 建立独立、隔离的链上连接（不共享扫描连接，设置 10 秒超时保护）
    dwellir_wss = db.get_setting("query_wss", "").strip()
    if not dwellir_wss:
        dwellir_wss = db.get_setting("dwellir_wss", "wss://api-bittensor-mainnet.n.dwellir.com").strip()
    if not dwellir_wss:
        dwellir_wss = "wss://api-bittensor-mainnet.n.dwellir.com"
        
    start_time = time.perf_counter()
    try:
        # 复用全局线程池 QUERY_EXECUTOR 提交，防止重复创建线程池开销
        future = QUERY_EXECUTOR.submit(_query_blockchain_data, dwellir_wss, address, netuid)
        free_tao, alpha_stake, equivalent_tao, price = future.result(timeout=10.0)
        
        duration_ms = int((time.perf_counter() - start_time) * 1000)
        db.add_log("INFO", f"仓位查询完成: 用时 {duration_ms}ms, 目标: {address}, 子网: {netuid}")
        
        balance_info = (
            f"\n\n💰 <b>当前操作者仓位</b>\n"
            f"剩余可用: <code>{free_tao:.4f} T</code>\n"
            f"SN{netuid} 总 Alpha: <code>{alpha_stake:.4f}</code>"
        )
        if price is not None:
            balance_info += f"\n当前 Alpha 价格: <code>1 Alpha ≈ {price:.4f} T</code>"
        if equivalent_tao is not None:
            balance_info += f"\n折合: <code>≈ {equivalent_tao:.4f} T</code>"
        
        edit_message_text(bot_token, chat_id, message_id, original_text, balance_info, message.get("reply_markup"))
        
    except concurrent.futures.TimeoutError:
        db.add_log("ERROR", "执行 Webhook 链上余额查询超时 (10秒)")
        edit_message_text(bot_token, chat_id, message_id, original_text, "\n\n❌ 当前仓位查询超时，请稍后再试", message.get("reply_markup"))
    except Exception as e:
        db.add_log("ERROR", f"执行 Webhook 链上余额查询时发生异常: {str(e)}")
        edit_message_text(bot_token, chat_id, message_id, original_text, "\n\n❌ 当前仓位查询超时，请稍后再试", message.get("reply_markup"))

@app.post("/tg/webhook/{token_md5}")
async def tg_webhook(token_md5: str, request: Request, background_tasks: BackgroundTasks):
    try:
        payload = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "Invalid JSON"}, status_code=400)
    
    callback_query = payload.get("callback_query")
    if not callback_query:
        return JSONResponse({"ok": True})
        
    callback_data = callback_query.get("data", "")
    if not callback_data.startswith("qb:"):
        return JSONResponse({"ok": True})
        
    # 查找 bot_token 鉴权
    groups = db.get_groups()
    bot_token = None
    for g in groups:
        for key in ("tg_token", "tg_token_backup"):
            val = g.get(key)
            if val and hashlib.md5(val.strip().encode("utf-8")).hexdigest() == token_md5:
                bot_token = val.strip()
                break
        if bot_token:
            break
            
    if not bot_token:
        return JSONResponse({"ok": False, "error": "Unauthorized bot token"}, status_code=401)
        
    # 安全防护：校验 Telegram secret_token 报头，确保请求来自于真实 Telegram API
    received_secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
    expected_secret = hashlib.sha256((bot_token + session_secret).encode("utf-8")).hexdigest()
    if received_secret != expected_secret:
        db.add_log("WARN", f"TG Webhook 鉴权失败: Secret Token 不匹配")
        return JSONResponse({"ok": False, "error": "Unauthorized secret token"}, status_code=403)
        
    background_tasks.add_task(handle_tg_callback, bot_token, callback_query)
    return JSONResponse({"ok": True})

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
