import sqlite3
import os
import time
import threading
from passlib.context import CryptContext

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
# 自动适配：若在 proposed 子目录下测试，数据库放置在父级目录；否则放置在同级目录下
if os.path.basename(os.path.dirname(__file__)) == "proposed":
    DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data.db')
else:
    DB_PATH = os.path.join(os.path.dirname(__file__), 'data.db')
DB_TIMEOUT_SECONDS = 30
DB_BUSY_TIMEOUT_MS = 30000
DB_RETRY_DELAYS = (0.2, 0.5, 1.0, 2.0)

# --- 内存缓存系统与线程锁 ---
_GROUPS_CACHE = None
_WALLETS_CACHE = {}  # group_id -> wallets 列表
_cache_lock = threading.RLock()  # 读写锁保护，保证多线程安全

def clear_cache():
    global _GROUPS_CACHE, _WALLETS_CACHE
    with _cache_lock:
        _GROUPS_CACHE = None
        _WALLETS_CACHE = {}

def get_db():
    resolved_path = os.path.abspath(DB_PATH)
    conn = sqlite3.connect(resolved_path, timeout=DB_TIMEOUT_SECONDS)
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout={DB_BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def _is_locked_error(error):
    return "database is locked" in str(error).lower() or "database table is locked" in str(error).lower()

def _run_write(operation):
    # 移除了在此处无条件清除缓存的代码，避免高频日志/审计写入将缓存优化彻底打掉。
    last_error = None
    for attempt, delay in enumerate((0, *DB_RETRY_DELAYS)):
        if delay:
            time.sleep(delay)
        conn = get_db()
        try:
            result = operation(conn)
            conn.commit()
            return result
        except sqlite3.OperationalError as e:
            conn.rollback()
            last_error = e
            if not _is_locked_error(e) or attempt >= len(DB_RETRY_DELAYS):
                raise
        finally:
            conn.close()
    raise last_error

def execute_write(sql, params=()):
    def operation(conn):
        conn.execute(sql, params)

    _run_write(operation)
    # 仅在实际的分组/钱包/配置被修改时才清除配置缓存
    clear_cache()

def execute_write_returning(operation):
    res = _run_write(operation)
    # 仅在实际的配置写入被调用时清除缓存
    clear_cache()
    return res

def init_db():
    conn = get_db()
    c = conn.cursor()
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    # 1. 监控分组表
    c.execute('''
        CREATE TABLE IF NOT EXISTS monitor_groups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            type TEXT NOT NULL,
            tg_token TEXT,
            tg_chat_id TEXT,
            tg_token_backup TEXT,
            tg_chat_id_backup TEXT,
            split_stake_bots BOOLEAN DEFAULT 0,
            threshold_tao REAL DEFAULT 5.0,
            is_active BOOLEAN DEFAULT 1
        )
    ''')

    # 2. 钱包表
    c.execute('''
        CREATE TABLE IF NOT EXISTS wallets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id INTEGER,
            address TEXT NOT NULL,
            alias TEXT NOT NULL,
            is_active BOOLEAN DEFAULT 1,
            FOREIGN KEY (group_id) REFERENCES monitor_groups(id) ON DELETE CASCADE
        )
    ''')

    # 3. 运行日志表
    c.execute('''
        CREATE TABLE IF NOT EXISTS system_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            level TEXT,
            message TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # 4. 可用性看板数据
    c.execute('''
        CREATE TABLE IF NOT EXISTS uptime_history (
            timestamp INTEGER PRIMARY KEY,
            status INTEGER
        )
    ''')

    # 5. 系统设置
    c.execute('''CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)''')

    # 6. 用户表
    c.execute('''CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password_hash TEXT)''')

    # 7. 通知审计日志
    c.execute('''
        CREATE TABLE IF NOT EXISTS notification_audit_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id INTEGER,
            group_name TEXT,
            group_type TEXT,
            action TEXT,
            address TEXT,
            from_address TEXT,
            to_address TEXT,
            alias TEXT,
            amount REAL,
            unit TEXT,
            netuid TEXT,
            detail TEXT,
            threshold_amount REAL,
            received_tao REAL,
            tx_ref TEXT,
            tx_hash TEXT,
            message TEXT,
            send_status TEXT DEFAULT 'queued',
            error_message TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # 8. 登录尝试记录表 (防爆破)
    c.execute('''
        CREATE TABLE IF NOT EXISTS login_attempts (
            ip TEXT PRIMARY KEY,
            attempts INTEGER DEFAULT 0,
            last_attempt INTEGER NOT NULL
        )
    ''')

    # --- 性能优化：创建常用索引 ---
    c.execute("CREATE INDEX IF NOT EXISTS idx_wallets_group_addr ON wallets(group_id, address)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_wallets_address ON wallets(address)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_system_logs_created ON system_logs(created_at)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_audit_logs_created ON notification_audit_logs(created_at)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_audit_logs_status_created ON notification_audit_logs(send_status, created_at)")

    # 默认创建一个巨鲸监控组和钱包监控组
    c.execute("SELECT count(*) FROM monitor_groups")
    if c.fetchone()[0] == 0:
        c.execute("INSERT INTO monitor_groups (name, type, threshold_tao) VALUES ('默认巨鲸组', 'whale', 5.0)")
        c.execute("INSERT INTO monitor_groups (name, type) VALUES ('默认钱包组', 'wallet')")

    existing_columns = {row["name"] for row in conn.execute("PRAGMA table_info(monitor_groups)").fetchall()}
    if "tg_token_backup" not in existing_columns:
        conn.execute("ALTER TABLE monitor_groups ADD COLUMN tg_token_backup TEXT")
    if "tg_chat_id_backup" not in existing_columns:
        conn.execute("ALTER TABLE monitor_groups ADD COLUMN tg_chat_id_backup TEXT")
    if "split_stake_bots" not in existing_columns:
        conn.execute("ALTER TABLE monitor_groups ADD COLUMN split_stake_bots BOOLEAN DEFAULT 0")

    conn.commit()
    conn.close()

# --- 日志与监控逻辑 ---

def add_log(level, message):
    def operation(conn):
        conn.execute("INSERT INTO system_logs (level, message) VALUES (?, ?)", (level, message))

    _run_write(operation)

def record_uptime(status):
    def operation(conn):
        now_min = int(time.time() / 60) * 60
        conn.execute("INSERT OR REPLACE INTO uptime_history (timestamp, status) VALUES (?, ?)", (now_min, status))
        conn.execute("DELETE FROM uptime_history WHERE timestamp < ?", (now_min - 86400,))

    _run_write(operation)

def get_uptime_data():
    conn = get_db()
    c = conn.cursor()
    now_min = int(time.time() / 60) * 60
    start_min = now_min - 86400
    c.execute("SELECT timestamp, status FROM uptime_history WHERE timestamp >= ? ORDER BY timestamp ASC", (start_min,))
    rows = {r['timestamp']: r['status'] for r in c.fetchall()}

    data = []
    for t in range(start_min, now_min + 60, 60):
        data.append(rows.get(t, 0))
    conn.close()
    return data

def get_uptime_series():
    conn = get_db()
    c = conn.cursor()
    now_min = int(time.time() / 60) * 60
    start_min = now_min - 86400
    c.execute("SELECT timestamp, status FROM uptime_history WHERE timestamp >= ? ORDER BY timestamp ASC", (start_min,))
    rows = {r['timestamp']: r['status'] for r in c.fetchall()}

    points = []
    for t in range(start_min, now_min + 60, 60):
        points.append({"timestamp": t, "status": rows.get(t, 0)})
    conn.close()
    return points

# --- 基础配置管理 ---

def create_admin_user(username, plain_password):
    hashed_pwd = pwd_context.hash(plain_password)

    def operation(conn):
        conn.execute("INSERT OR REPLACE INTO users (username, password_hash) VALUES (?, ?)", (username, hashed_pwd))

    _run_write(operation)
    clear_cache()

def verify_user(username, plain_password):
    conn = get_db()
    row = conn.execute("SELECT password_hash FROM users WHERE username = ?", (username,)).fetchone()
    conn.close()
    return pwd_context.verify(plain_password, row['password_hash']) if row else False

# --- 登录防爆破逻辑 ---

def record_login_attempt(ip, success):
    def operation(conn):
        now = int(time.time())
        if success:
            conn.execute("DELETE FROM login_attempts WHERE ip = ?", (ip,))
        else:
            row = conn.execute("SELECT attempts FROM login_attempts WHERE ip = ?", (ip,)).fetchone()
            attempts = row["attempts"] + 1 if row else 1
            conn.execute("INSERT OR REPLACE INTO login_attempts (ip, attempts, last_attempt) VALUES (?, ?, ?)", (ip, attempts, now))
    _run_write(operation)

def is_login_locked(ip):
    conn = get_db()
    row = conn.execute("SELECT attempts, last_attempt FROM login_attempts WHERE ip = ?", (ip,)).fetchone()
    conn.close()
    if not row:
        return False, 0
    attempts = row["attempts"]
    last_attempt = row["last_attempt"]
    lock_duration = 900
    now = int(time.time())
    if attempts >= 5 and (now - last_attempt) < lock_duration:
        remaining = lock_duration - (now - last_attempt)
        return True, remaining
    if (now - last_attempt) >= lock_duration:
        def operation(conn):
            conn.execute("DELETE FROM login_attempts WHERE ip = ?", (ip,))
        _run_write(operation)
    return False, 0

def get_setting(key, default=""):
    conn = get_db()
    row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    conn.close()
    return row['value'] if row else default

def set_setting(key, value):
    def operation(conn):
        conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, str(value)))

    _run_write(operation)
    clear_cache()

# --- 读操作缓存与锁（返回深层拷贝以防调用方修改污染缓存） ---

def get_groups():
    global _GROUPS_CACHE
    with _cache_lock:
        if _GROUPS_CACHE is not None:
            return [dict(g) for g in _GROUPS_CACHE]
        conn = get_db()
        rows = conn.execute("SELECT * FROM monitor_groups").fetchall()
        conn.close()
        _GROUPS_CACHE = [dict(r) for r in rows]
        return [dict(g) for g in _GROUPS_CACHE]

def get_wallets_by_group(group_id):
    global _WALLETS_CACHE
    with _cache_lock:
        if group_id in _WALLETS_CACHE:
            return [dict(w) for w in _WALLETS_CACHE[group_id]]
        conn = get_db()
        rows = conn.execute("SELECT * FROM wallets WHERE group_id = ?", (group_id,)).fetchall()
        conn.close()
        _WALLETS_CACHE[group_id] = [dict(r) for r in rows]
        return [dict(w) for w in _WALLETS_CACHE[group_id]]

def get_wallet_by_group_and_address(group_id, address):
    conn = get_db()
    row = conn.execute(
        "SELECT * FROM wallets WHERE group_id = ? AND address = ?",
        (group_id, address)
    ).fetchone()
    conn.close()
    return dict(row) if row else None

def add_notification_audit_log(entry):
    def operation(conn):
        conn.execute(
            '''
            INSERT INTO notification_audit_logs (
                group_id, group_name, group_type, action, address, from_address, to_address,
                alias, amount, unit, netuid, detail, threshold_amount, received_tao,
                tx_ref, tx_hash, message, send_status, error_message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                entry.get("group_id"),
                entry.get("group_name"),
                entry.get("group_type"),
                entry.get("action"),
                entry.get("address"),
                entry.get("from_address"),
                entry.get("to_address"),
                entry.get("alias"),
                entry.get("amount"),
                entry.get("unit"),
                entry.get("netuid"),
                entry.get("detail"),
                entry.get("threshold_amount"),
                entry.get("received_tao"),
                entry.get("tx_ref"),
                entry.get("tx_hash"),
                entry.get("message"),
                entry.get("send_status", "queued"),
                entry.get("error_message"),
            )
        )
        return conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    return _run_write(operation)

def update_notification_audit_log(audit_id, send_status, error_message=""):
    def operation(conn):
        conn.execute(
            "UPDATE notification_audit_logs SET send_status = ?, error_message = ? WHERE id = ?",
            (send_status, error_message, audit_id)
        )

    _run_write(operation)

def get_notification_audit_logs(limit=100):
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM notification_audit_logs ORDER BY created_at DESC, id DESC LIMIT ?",
        (limit,)
    ).fetchall()
    conn.close()
    return rows

def get_notification_audit_count():
    conn = get_db()
    row = conn.execute("SELECT COUNT(*) AS total FROM notification_audit_logs").fetchone()
    conn.close()
    return row["total"] if row else 0

def get_notification_success_rate(hours=24):
    conn = get_db()
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN send_status = 'sent' THEN 1 ELSE 0 END) AS sent_total
        FROM notification_audit_logs
        WHERE created_at >= datetime('now', ?)
        """,
        (f"-{hours} hours",)
    ).fetchone()
    conn.close()

    total = row["total"] if row and row["total"] is not None else 0
    sent_total = row["sent_total"] if row and row["sent_total"] is not None else 0
    rate = round((sent_total / total) * 100, 2) if total else 0
    return {
        "total": total,
        "sent_total": sent_total,
        "rate": rate,
    }
