import sqlite3
import os
import time
from passlib.context import CryptContext

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
DB_PATH = os.path.join(os.path.dirname(__file__), 'data.db')

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    c = conn.cursor()

    # 1. 监控分组表 (每个组可以有不同的机器人和阈值)
    c.execute('''
        CREATE TABLE IF NOT EXISTS monitor_groups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            type TEXT NOT NULL, -- 'whale' 或 'wallet'
            tg_token TEXT,
            tg_chat_id TEXT,
            tg_token_backup TEXT,
            tg_chat_id_backup TEXT,
            split_stake_bots BOOLEAN DEFAULT 0,
            threshold_tao REAL DEFAULT 5.0,
            is_active BOOLEAN DEFAULT 1
        )
    ''')

    # 2. 钱包表 (关联到分组)
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
            level TEXT, -- INFO, ERROR, WARN
            message TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # 4. 可用性看板数据 (记录每分钟状态)
    c.execute('''
        CREATE TABLE IF NOT EXISTS uptime_history (
            timestamp INTEGER PRIMARY KEY,
            status INTEGER -- 1 为正常, 0 为异常
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

    # 默认创建一个巨鲸监控组和钱包监控组 (如果不存在)
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
    conn = get_db()
    conn.execute("INSERT INTO system_logs (level, message) VALUES (?, ?)", (level, message))
    # 自动清理24小时前的日志
    conn.execute("DELETE FROM system_logs WHERE created_at < datetime('now', '-1 day')")
    conn.commit()
    conn.close()

def record_uptime(status):
    conn = get_db()
    now_min = int(time.time() / 60) * 60
    conn.execute("INSERT OR REPLACE INTO uptime_history (timestamp, status) VALUES (?, ?)", (now_min, status))
    # 只保留最近24小时的数据 (1440分钟)
    conn.execute("DELETE FROM uptime_history WHERE timestamp < ?", (now_min - 86400,))
    conn.commit()
    conn.close()

def get_uptime_data():
    conn = get_db()
    c = conn.cursor()
    # 获取最近24小时的1440个数据点，如果没有则补0
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

# --- 基础配置管理 (原有逻辑适配) ---

def create_admin_user(username, plain_password):
    conn = get_db()
    hashed_pwd = pwd_context.hash(plain_password)
    conn.execute("INSERT OR REPLACE INTO users (username, password_hash) VALUES (?, ?)", (username, hashed_pwd))
    conn.commit()
    conn.close()

def verify_user(username, plain_password):
    conn = get_db()
    row = conn.execute("SELECT password_hash FROM users WHERE username = ?", (username,)).fetchone()
    conn.close()
    return pwd_context.verify(plain_password, row['password_hash']) if row else False

def get_setting(key, default=""):
    conn = get_db()
    row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    conn.close()
    return row['value'] if row else default

def set_setting(key, value):
    conn = get_db()
    conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, str(value)))
    conn.commit()
    conn.close()

def get_groups():
    conn = get_db()
    rows = conn.execute("SELECT * FROM monitor_groups").fetchall()
    conn.close()
    return [dict(r) for r in rows]

def get_wallets_by_group(group_id):
    conn = get_db()
    rows = conn.execute("SELECT * FROM wallets WHERE group_id = ?", (group_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_wallet_by_group_and_address(group_id, address):
    conn = get_db()
    row = conn.execute(
        "SELECT * FROM wallets WHERE group_id = ? AND address = ?",
        (group_id, address)
    ).fetchone()
    conn.close()
    return dict(row) if row else None

def add_notification_audit_log(entry):
    conn = get_db()
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
    audit_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit()
    conn.close()
    return audit_id

def update_notification_audit_log(audit_id, send_status, error_message=""):
    conn = get_db()
    conn.execute(
        "UPDATE notification_audit_logs SET send_status = ?, error_message = ? WHERE id = ?",
        (send_status, error_message, audit_id)
    )
    conn.commit()
    conn.close()

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
