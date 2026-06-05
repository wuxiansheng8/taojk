import html
import queue
import threading
import time
import os
from urllib.parse import urlparse, urlunparse
import requests
from substrateinterface import SubstrateInterface
import database as db
import concurrent.futures
from datetime import datetime, timezone
import position_query

SCANNER_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=5)
QUERY_LOCKS = {}
QUERY_LOCKS_LOCK = threading.Lock()

def get_query_lock(address, netuid):
    key = (address, netuid)
    with QUERY_LOCKS_LOCK:
        if key not in QUERY_LOCKS:
            QUERY_LOCKS[key] = threading.Lock()
        return QUERY_LOCKS[key]

def edit_message_text(bot_token, chat_id, message_id, original_text, append_text, reply_markup=None):
    markers = [
        "\n\n💰 <b>当前钱包仓位</b>",
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

def edit_message_reply_markup(bot_token, chat_id, message_id, reply_markup):
    url = f"https://api.telegram.org/bot{bot_token}/editMessageReplyMarkup"
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "reply_markup": reply_markup
    }
    try:
        requests.post(url, json=payload, timeout=5)
    except Exception as e:
        db.add_log("WARN", f"更新按钮状态失败: {str(e)}")

def background_query_and_update(audit_id, address, netuid, original_msg, bot_token, chat_id, reply_markup):
    lock = get_query_lock(address, netuid)
    with lock:
        cached = db.get_wallet_cache(address, netuid)
        if cached and cached.get("dirty") == 0:
            try:
                dt = datetime.strptime(cached["updated_at"], "%Y-%m-%d %H:%M:%S")
                if (datetime.now(timezone.utc).replace(tzinfo=None) - dt).total_seconds() < 3.0:
                    balance_info = position_query.format_balance_info(
                        netuid, cached["free_tao"], cached["alpha_stake"], cached["equivalent_tao"], cached["price"]
                    )
                    _edit_tg_msg_now(audit_id, original_msg, balance_info, bot_token, chat_id, reply_markup)
                    return
            except Exception:
                pass

        try:
            free_tao, alpha_stake, equivalent_tao, price = position_query._query_blockchain_data(address, netuid)
            db.update_wallet_cache(address, netuid, free_tao, alpha_stake, equivalent_tao, price)
            balance_info = position_query.format_balance_info(netuid, free_tao, alpha_stake, equivalent_tao, price)
        except Exception as e:
            db.add_log("ERROR", f"后台余额查询失败: {str(e)}")
            return

        _edit_tg_msg_now(audit_id, original_msg, balance_info, bot_token, chat_id, reply_markup)

def _edit_tg_msg_now(audit_id, original_msg, balance_info, bot_token, chat_id, reply_markup):
    message_id = None
    for _ in range(50):
        conn = db.get_db()
        row = conn.execute("SELECT message_id, send_status FROM notification_audit_logs WHERE id = ?", (audit_id,)).fetchone()
        conn.close()
        if row and row["message_id"]:
            message_id = row["message_id"]
            break
        if row and row["send_status"] == "failed":
            return
        time.sleep(0.2)
        
    if not message_id:
        db.add_log("WARN", f"后台更新消息失败: 未能在限时内获取到 audit_id={audit_id} 的 message_id")
        return
        
    edit_message_text(bot_token, chat_id, message_id, original_msg, balance_info, reply_markup)


RAO_DECIMALS = 1e9

TRANSFER_CALLS = {
    "transfer",
    "transfer_keep_alive",
    "transfer_allow_death",
}

BATCH_CALLS = {
    ("Utility", "batch"),
    ("Utility", "batch_all"),
    ("Utility", "force_batch"),
}

TEXT = {
    "transfer": "普通转账",
    "add_stake": "加仓",
    "remove_stake": "减仓",
    "move_stake": "换仓",
}

# --- 限制队列边界与运行控制变量 ---
TG_QUEUE = queue.Queue(maxsize=1000)
IS_RUNNING = False
ACTIVE_SUBSTRATE = None  # 全局活跃连接实例，用于优雅停机

LAST_SEND_TIME = 0
LAST_CONNECT_TIME = 0
LAST_BLOCK_TIME = 0
LAST_BLOCK_NUMBER = 0
LAST_ERROR = ""
CURRENT_WSS_LABEL = ""
LAST_WSS_LATENCY_MS = 0
WSS_INDEX = 0
LAST_TG_SUCCESS_TIME = 0
LAST_TG_ERROR = ""
CONSECUTIVE_HANDLER_ERRORS = 0  # 追踪处理连续错误的次数

def mask_chat_id(cid):
    cid_str = str(cid or "")
    if not cid_str:
        return ""
    if len(cid_str) > 8:
        return f"{cid_str[:4]}***{cid_str[-4:]}"
    return cid_str

# --- 跨平台文件排他锁实现 (扫描进程单例保护) ---
_lock_file = None

def acquire_lock():
    global _lock_file
    lock_path = os.path.join(os.path.dirname(db.DB_PATH), "scanner.lock")
    try:
        # Unix/Linux 平台
        import fcntl
        _lock_file = open(lock_path, "w")
        fcntl.flock(_lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except (ImportError, IOError):
        try:
            # Windows 平台
            import msvcrt
            _lock_file = open(lock_path, "w")
            msvcrt.locking(_lock_file.fileno(), msvcrt.LK_NBLCK, 1)
            return True
        except (ImportError, IOError):
            # 抢占锁失败或平台不支持
            if _lock_file:
                try:
                    _lock_file.close()
                except Exception:
                    pass
                _lock_file = None
            return False

def release_lock():
    global _lock_file
    if _lock_file:
        try:
            _lock_file.close()
        except Exception:
            pass
        _lock_file = None

def get_wss_targets():
    primary = db.get_setting("dwellir_wss", "wss://api-bittensor-mainnet.n.dwellir.com").strip()
    backup = db.get_setting("dwellir_wss_backup", "").strip()
    load_balance = db.get_setting("wss_load_balance", "0") == "1"
    targets = []

    if primary:
        targets.append(("主接口", primary))
    if backup and backup != primary:
        targets.append(("备用接口", backup))
    if not targets:
        targets.append(("主接口", "wss://api-bittensor-mainnet.n.dwellir.com"))

    return targets, load_balance

def pick_wss_target():
    targets, load_balance = get_wss_targets()
    if not targets:
        return "主接口", "wss://api-bittensor-mainnet.n.dwellir.com", False
    
    target = targets[WSS_INDEX % len(targets)]
    return target[0], target[1], load_balance

def normalize_wss_url(url):
    url = (url or "").strip()
    if not url:
        return "wss://api-bittensor-mainnet.n.dwellir.com"

    parsed = urlparse(url)
    if parsed.scheme == "https":
        return urlunparse(parsed._replace(scheme="wss"))
    if parsed.scheme == "http":
        return urlunparse(parsed._replace(scheme="ws"))
    return url

def test_wss_endpoint(url):
    normalized_url = normalize_wss_url(url)
    start = time.perf_counter()
    substrate = SubstrateInterface(url=normalized_url)
    substrate.get_chain_head()
    latency_ms = int((time.perf_counter() - start) * 1000)
    return {"url": normalized_url, "latency_ms": latency_ms}

def raw_value(value):
    return getattr(value, "value", value)

def decoded_value(value):
    if hasattr(value, "decode"):
        try:
            return value.decode()
        except Exception:
            pass
    return raw_value(value)

def object_attr(value, name, default=None):
    if isinstance(value, dict):
        return value.get(name, default)
    return getattr(value, name, default)

def format_tao(rao_amount):
    return float(rao_amount) / RAO_DECIMALS

def to_float_tao(rao_amount):
    if rao_amount is None:
        return None
    return format_tao(rao_amount)

def safe_text(value):
    if value is None:
        return ""
    return html.escape(str(value))

def short_address(address):
    address = str(address or "")
    if len(address) <= 12:
        return address
    return f"{address[:4]}...{address[-4:]}"

def extrinsic_url(tx_ref):
    if not tx_ref:
        return ""
    return f"https://taostats.io/extrinsic/{tx_ref}"

def subnet_url(netuid):
    if netuid is None:
        return ""
    if "->" in str(netuid):
        return ""
    return f"https://taostats.io/subnets/{netuid}"

def profile_url(address):
    if not address:
        return ""
    return f"https://backprop.finance/dtao/profile/{address}"

def build_inline_keyboard(tx_ref=None, netuid=None, address=None):
    buttons = []
    profile_link = profile_url(address)

    if profile_link:
        buttons.append({"text": "💰 查看当前钱包地址", "url": profile_link})

    if not buttons:
        return None

    return {"inline_keyboard": [buttons]}

def normalize_address(value):
    value = raw_value(value)
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        for key in ("Id", "id", "Address20", "Address32", "address"):
            if key in value:
                return normalize_address(value[key])
    return str(value) if value is not None else ""

def is_root_netuid(netuid):
    return str(netuid) == "0"

def format_netuid(value):
    return "未提供" if value is None or value == "" else str(value)

def call_arg_map(call):
    call = raw_value(call) or {}
    return {arg.get("name"): arg.get("value") for arg in call.get("call_args", [])}

def call_arg_values(call):
    call = raw_value(call) or {}
    return [arg.get("value") for arg in call.get("call_args", [])]

def get_call_arg(call, names, index=None):
    args_by_name = call_arg_map(call)
    for name in names:
        if name in args_by_name:
            return args_by_name[name]

    values = call_arg_values(call)
    if index is not None and len(values) > index:
        return values[index]
    return None

def get_last_call_arg(call, names):
    value = get_call_arg(call, names)
    if value is not None:
        return value

    values = call_arg_values(call)
    return values[-1] if values else None

def phase_extrinsic_index(phase):
    phase = raw_value(phase)
    if isinstance(phase, dict):
        value = phase.get("ApplyExtrinsic")
        if value is None:
            value = phase.get("apply_extrinsic")
        return int(value) if value is not None else None
    if isinstance(phase, str):
        if phase.startswith("ApplyExtrinsic(") and phase.endswith(")"):
            return int(phase[len("ApplyExtrinsic("):-1])
        if phase.isdigit():
            return int(phase)
    return None

def event_record_extrinsic_index(record):
    phase = object_attr(record, "phase")
    index = phase_extrinsic_index(phase)
    if index is not None:
        return index

    phase = raw_value(phase)
    if phase == "ApplyExtrinsic":
        index = object_attr(record, "extrinsic_idx")
        if index is None:
            index = object_attr(record, "extrinsic_index")
        return int(index) if index is not None else None

    return None

def event_name(event):
    raw_event = event
    event = raw_value(event) or {}
    if isinstance(event, dict):
        module = event.get("module_id") or event.get("module")
        name = event.get("event_id") or event.get("event")
        return module, name

    module = object_attr(raw_event, "module_id") or object_attr(raw_event, "module")
    name = object_attr(raw_event, "event_id") or object_attr(raw_event, "event")
    if module or name:
        return module, name

    text = str(raw_event)
    if "System" in text and "ExtrinsicSuccess" in text:
        return "System", "ExtrinsicSuccess"
    return None, None

def event_attributes(event):
    event = raw_value(event) or {}
    if isinstance(event, dict):
        attrs = event.get("attributes")
        if attrs is None:
            attrs = event.get("params")
        return attrs or []
    return object_attr(event, "attributes", []) or object_attr(event, "params", []) or []

def attr_value(attrs, names, index=None):
    if isinstance(attrs, dict):
        for name in names:
            if name in attrs:
                return attrs[name]
        return None

    if isinstance(attrs, (list, tuple)):
        for item in attrs:
            item = raw_value(item)
            if isinstance(item, dict):
                name = item.get("name")
                if name in names:
                    return item.get("value")

        if index is not None and len(attrs) > index:
            item = raw_value(attrs[index])
            if isinstance(item, dict) and "value" in item:
                return item.get("value")
            return item

    return None

def values_equal(left, right):
    return str(left) == str(right)

def events_by_extrinsic_index(substrate, block_hash):
    grouped = {}
    for event_record in substrate.get_events(block_hash=block_hash):
        record = decoded_value(event_record) or {}
        index = event_record_extrinsic_index(record)
        if index is None:
            continue
        grouped.setdefault(index, []).append(record)
    return grouped

def successful_extrinsic_indices(grouped_events):
    success_indices = set()
    for index, records in grouped_events.items():
        for record in records:
            module, name = event_name(object_attr(record, "event"))
            if module == "System" and name == "ExtrinsicSuccess":
                success_indices.add(index)
                break
    return success_indices

def tao_received_by_account(events, account):
    total_rao = 0
    for record in events:
        event = object_attr(record, "event")
        module, name = event_name(event)
        if module != "Balances" or name not in {"Deposit", "Endowed"}:
            continue

        attrs = event_attributes(event)
        who = normalize_address(attr_value(attrs, ("who", "account", "AccountId"), 0))
        if who != account:
            continue

        amount = attr_value(attrs, ("amount", "free_balance"), 1)
        if amount is not None:
            total_rao += int(amount)

    return to_float_tao(total_rao) if total_rao else None

def fee_paid_by_account(events, account):
    total_rao = 0
    for record in events:
        event = object_attr(record, "event")
        module, name = event_name(event)
        if module != "TransactionPayment" or name != "TransactionFeePaid":
            continue

        attrs = event_attributes(event)
        who = normalize_address(attr_value(attrs, ("who", "account"), 0))
        if who != account:
            continue

        amount = attr_value(attrs, ("actual_fee", "fee"), 1)
        if amount is not None:
            total_rao += int(amount)

    return to_float_tao(total_rao) if total_rao else None

def subtensor_event_attrs(events, event_id, account=None, hotkey=None, origin_netuid=None, destination_netuid=None):
    for record in events:
        event = object_attr(record, "event")
        module, name = event_name(event)
        if module != "SubtensorModule" or name != event_id:
            continue

        attrs = event_attributes(event)
        if account is not None and normalize_address(attr_value(attrs, ("coldkey", "account"), 0)) != account:
            continue
        if hotkey is not None and normalize_address(attr_value(attrs, ("hotkey",), 1)) != hotkey:
            continue
        if origin_netuid is not None and not values_equal(attr_value(attrs, ("origin_netuid",), 2), origin_netuid):
            continue
        if destination_netuid is not None and not values_equal(attr_value(attrs, ("destination_netuid",), 3), destination_netuid):
            continue
        return attrs
    return None

def stake_swapped_tao(events, account, hotkey, origin_netuid, destination_netuid):
    attrs = subtensor_event_attrs(
        events,
        "StakeSwapped",
        account=account,
        hotkey=hotkey,
        origin_netuid=origin_netuid,
        destination_netuid=destination_netuid,
    )
    if attrs is None:
        return None
    return to_float_tao(attr_value(attrs, ("tao_amount", "amount"), 4))

def stake_removed_amounts(events, account, hotkey, netuid):
    attrs = None
    for record in events:
        event = object_attr(record, "event")
        module, name = event_name(event)
        if module != "SubtensorModule" or name != "StakeRemoved":
            continue

        candidate = event_attributes(event)
        if normalize_address(attr_value(candidate, ("coldkey", "account"), 0)) != account:
            continue
        if normalize_address(attr_value(candidate, ("hotkey",), 1)) != hotkey:
            continue
        if not values_equal(attr_value(candidate, ("netuid",), 4), netuid):
            continue
        attrs = candidate
        break

    if attrs is None:
        return None, None

    tao = to_float_tao(attr_value(attrs, ("tao_amount", "amount"), 2))
    alpha = to_float_tao(attr_value(attrs, ("alpha_amount",), 3))
    return tao, alpha

def stake_removed_entries(events, account, hotkey):
    entries = []
    for record in events:
        event = object_attr(record, "event")
        module, name = event_name(event)
        if module != "SubtensorModule" or name != "StakeRemoved":
            continue

        attrs = event_attributes(event)
        if normalize_address(attr_value(attrs, ("coldkey", "account"), 0)) != account:
            continue
        if normalize_address(attr_value(attrs, ("hotkey",), 1)) != hotkey:
            continue

        entries.append({
            "tao": to_float_tao(attr_value(attrs, ("tao_amount", "amount"), 2)),
            "alpha": to_float_tao(attr_value(attrs, ("alpha_amount",), 3)),
            "netuid": attr_value(attrs, ("netuid",), 4),
        })
    return entries

def stake_added_amounts(events, account, hotkey, netuid):
    for record in events:
        event = object_attr(record, "event")
        module, name = event_name(event)
        if module != "SubtensorModule" or name != "StakeAdded":
            continue

        attrs = event_attributes(event)
        if normalize_address(attr_value(attrs, ("coldkey", "account"), 0)) != account:
            continue
        if normalize_address(attr_value(attrs, ("hotkey",), 1)) != hotkey:
            continue
        if not values_equal(attr_value(attrs, ("netuid",), 4), netuid):
            continue

        tao = to_float_tao(attr_value(attrs, ("tao_amount", "amount"), 2))
        alpha = to_float_tao(attr_value(attrs, ("alpha_amount",), 3))
        return tao, alpha
    return None, None

def stake_transferred_entry(events):
    for record in events:
        event = object_attr(record, "event")
        module, name = event_name(event)
        if module != "SubtensorModule" or name != "StakeTransferred":
            continue

        attrs = event_attributes(event)
        return {
            "from_account": normalize_address(attr_value(attrs, ("coldkey_from", "from", "account_from"), 0)),
            "to_account": normalize_address(attr_value(attrs, ("coldkey_to", "to", "account_to"), 1)),
            "hotkey": normalize_address(attr_value(attrs, ("hotkey",), 2)),
            "origin_netuid": attr_value(attrs, ("origin_netuid",), 3),
            "destination_netuid": attr_value(attrs, ("destination_netuid",), 4),
            "tao_amount": to_float_tao(attr_value(attrs, ("tao_amount", "amount"), 5)),
        }
    return None

def first_stake_removed_entry(events):
    for record in events:
        event = object_attr(record, "event")
        module, name = event_name(event)
        if module != "SubtensorModule" or name != "StakeRemoved":
            continue
        attrs = event_attributes(event)
        return {
            "account": normalize_address(attr_value(attrs, ("coldkey", "account"), 0)) if attr_value(attrs, ("coldkey", "account"), 0) else None,
            "hotkey": normalize_address(attr_value(attrs, ("hotkey",), 1)),
            "tao_amount": to_float_tao(attr_value(attrs, ("tao_amount", "amount"), 2)),
            "alpha_amount": to_float_tao(attr_value(attrs, ("alpha_amount",), 3)),
            "netuid": attr_value(attrs, ("netuid",), 4),
        }
    return None

def first_stake_added_entry(events):
    for record in events:
        event = object_attr(record, "event")
        module, name = event_name(event)
        if module != "SubtensorModule" or name != "StakeAdded":
            continue
        attrs = event_attributes(event)
        return {
            "account": normalize_address(attr_value(attrs, ("coldkey", "account"), 0)) if attr_value(attrs, ("coldkey", "account"), 0) else None,
            "hotkey": normalize_address(attr_value(attrs, ("hotkey",), 1)),
            "tao_amount": to_float_tao(attr_value(attrs, ("tao_amount", "amount"), 2)),
            "alpha_amount": to_float_tao(attr_value(attrs, ("alpha_amount",), 3)),
            "netuid": attr_value(attrs, ("netuid",), 4),
        }
    return None

def alert_from_evm_stake_transfer(events, tx_ref=None, tx_hash=None):
    entry = stake_transferred_entry(events)
    if entry:
        origin_netuid = entry["origin_netuid"]
        destination_netuid = entry["destination_netuid"]
        hotkey = entry["hotkey"]
        tao_amount = entry["tao_amount"]

        if not is_root_netuid(origin_netuid) and is_root_netuid(destination_netuid):
            removed_tao, alpha_amount = stake_removed_amounts(events, entry["from_account"], hotkey, origin_netuid)
            if removed_tao is not None:
                tao_amount = removed_tao
            check_and_alert(
                TEXT["remove_stake"],
                entry["from_account"],
                alpha_amount or 0,
                unit="Alpha",
                netuid=origin_netuid,
                detail=hotkey,
                threshold_amount=tao_amount,
                received_tao=tao_amount,
                tx_ref=tx_ref,
                tx_hash=tx_hash,
            )
            return True

        if is_root_netuid(origin_netuid) and not is_root_netuid(destination_netuid):
            added_tao, added_alpha = stake_added_amounts(events, entry["to_account"], hotkey, destination_netuid)
            if added_tao is not None:
                tao_amount = added_tao
            check_and_alert(
                TEXT["add_stake"],
                entry["to_account"],
                added_tao or 0,
                unit="TAO",
                netuid=destination_netuid,
                detail=hotkey,
                threshold_amount=tao_amount,
                tx_ref=tx_ref,
                tx_hash=tx_hash,
                alpha_amount=added_alpha,
            )
            return True

    removed = first_stake_removed_entry(events)
    added = first_stake_added_entry(events)
    if not removed or not added:
        return False

    if is_root_netuid(removed["netuid"]) and not is_root_netuid(added["netuid"]):
        check_and_alert(
            TEXT["add_stake"],
            added["account"],
            added["tao_amount"] or 0,
            unit="TAO",
            netuid=added["netuid"],
            detail=added["hotkey"],
            threshold_amount=added["tao_amount"],
            tx_ref=tx_ref,
            tx_hash=tx_hash,
            alpha_amount=added.get("alpha_amount"),
        )
        return True

    if not is_root_netuid(removed["netuid"]) and is_root_netuid(added["netuid"]):
        check_and_alert(
            TEXT["remove_stake"],
            removed["account"],
            removed["alpha_amount"] or 0,
            unit="Alpha",
            netuid=removed["netuid"],
            detail=removed["hotkey"],
            threshold_amount=removed["tao_amount"],
            received_tao=removed["tao_amount"],
            tx_ref=tx_ref,
            tx_hash=tx_hash,
        )
        return True

    return False

def proxy_call_succeeded(events):
    saw_proxy = False
    for record in events:
        event = object_attr(record, "event")
        module, name = event_name(event)
        if module != "Proxy" or name != "ProxyExecuted":
            continue

        saw_proxy = True
        attrs = event_attributes(event)
        result = attr_value(attrs, ("result",), 0)
        if isinstance(result, dict) and "Err" in result:
            return False

    return True if saw_proxy else None

def has_subtensor_alertable_event(events):
    for record in events:
        event = object_attr(record, "event")
        module, name = event_name(event)
        if module != "SubtensorModule":
            continue
        if name in {"StakeAdded", "StakeRemoved", "StakeSwapped", "StakeTransferred"}:
            return True
    return False

def send_telegram_msg_to_group(group_id, msg, audit_id=None, reply_markup=None, bot_token=None, chat_id=None):
    try:
        TG_QUEUE.put({
            "group_id": group_id,
            "msg": msg,
            "audit_id": audit_id,
            "reply_markup": reply_markup,
            "bot_token": bot_token,
            "chat_id": chat_id,
        }, block=False)
        return True
    except queue.Full:
        db.add_log("ERROR", f"TG 消息队列堆积已满，丢弃该消息。分组 ID: {group_id}")
        if audit_id:
            db.update_notification_audit_log(audit_id, "failed", "消息队列溢出丢弃")
        return False

def _safe_tg_throttle_ms():
    raw_value = db.get_setting("tg_throttle_ms", "500")
    try:
        return max(0, int(raw_value))
    except (TypeError, ValueError):
        db.add_log("WARN", f"TG 推送间隔配置无效({raw_value})，已临时使用 500ms")
        return 500

def _safe_retry_after(value):
    try:
        return max(1, int(float(value)))
    except (TypeError, ValueError):
        return 5

def _load_tg_target(group_id, bot_token=None, chat_id=None):
    conn = db.get_db()
    group_row = conn.execute(
        "SELECT name, tg_token, tg_chat_id, tg_token_backup, tg_chat_id_backup, split_stake_bots FROM monitor_groups WHERE id = ?",
        (group_id,)
    ).fetchone()
    conn.close()

    if not group_row:
        return None, None, None, f"监控分组不存在: {group_id}"

    # 转换为标准字典，保障 get() 调用的安全性
    group = dict(group_row)
    bot_token = bot_token or group["tg_token"]
    chat_id = chat_id or group["tg_chat_id"]
    if not bot_token or not chat_id:
        return group, bot_token, chat_id, "Bot Token 或 Chat ID 为空"

    return group, bot_token, chat_id, ""

def _update_migrated_chat_id(group_id, group, old_chat_id, migrated_chat_id):
    conn = db.get_db()
    if str(old_chat_id) == str(group["tg_chat_id"]):
        conn.execute("UPDATE monitor_groups SET tg_chat_id = ? WHERE id = ?", (str(migrated_chat_id), group_id))
    elif group["tg_chat_id_backup"] and str(old_chat_id) == str(group["tg_chat_id_backup"]):
        conn.execute("UPDATE monitor_groups SET tg_chat_id_backup = ? WHERE id = ?", (str(migrated_chat_id), group_id))
    conn.commit()
    conn.close()

def send_telegram_msg_to_group_now(group_id, msg, audit_id=None, reply_markup=None, bot_token=None, chat_id=None, allow_retry=True):
    global LAST_SEND_TIME, LAST_TG_SUCCESS_TIME, LAST_TG_ERROR

    # _load_tg_target 返回的 bot_token 和 chat_id 已经是兜底解析后的实际值
    group, bot_token, chat_id, error = _load_tg_target(group_id, bot_token, chat_id)
    
    # 优雅获取分组名称并带兜底
    group_name = (group.get("name") if group else "") or f"分组-{group_id}"
    
    # 动态匹配主/备机器人
    bot_slot = "主机器人"
    if group and bot_token and bot_token == group.get("tg_token_backup"):
        bot_slot = "热备机器人"
        
    masked_cid = mask_chat_id(chat_id)
    bot_info = f"分组 [{group_name}] / {bot_slot} / Chat ID: {masked_cid}"

    if error:
        LAST_TG_ERROR = f"{bot_info} {error}"
        if audit_id:
            db.update_notification_audit_log(audit_id, "failed", error)
        db.add_log("ERROR", f"TG 发送失败，{bot_info}: {error}")
        return False, error

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": msg,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup

    try:
        res = requests.post(url, json=payload, timeout=10)
        LAST_SEND_TIME = time.time()
    except Exception as e:
        error = str(e)
        LAST_TG_ERROR = f"{bot_info} 网络异常: {error}"
        if audit_id:
            db.update_notification_audit_log(audit_id, "failed", error[:500])
        db.add_log("ERROR", f"TG 网络异常，{bot_info}: {error}")
        return False, error

    if res.status_code == 429:
        try:
            retry_after = _safe_retry_after(res.json().get("parameters", {}).get("retry_after", 5))
        except Exception:
            retry_after = 5
        error = f"429 retry_after={retry_after}"
        LAST_TG_ERROR = f"{bot_info} {error}"
        if audit_id:
            db.update_notification_audit_log(audit_id, "retrying" if allow_retry else "failed", error)
        db.add_log("WARN", f"TG 频率限制，{bot_info}，需要等待 {retry_after} 秒")
        return False, error

    if not res.ok:
        migrate_to_chat_id = None
        try:
            migrate_to_chat_id = res.json().get("parameters", {}).get("migrate_to_chat_id")
        except Exception:
            migrate_to_chat_id = None

        if migrate_to_chat_id and allow_retry:
            _update_migrated_chat_id(group_id, group, chat_id, migrate_to_chat_id)
            if audit_id:
                db.update_notification_audit_log(audit_id, "retrying", f"migrate_to_chat_id={migrate_to_chat_id}")
            db.add_log("WARN", f"TG 群升级为超级群，{bot_info} 自动切换 Chat ID -> {migrate_to_chat_id}")
            return send_telegram_msg_to_group_now(
                group_id,
                msg,
                audit_id=audit_id,
                reply_markup=reply_markup,
                bot_token=bot_token,
                chat_id=str(migrate_to_chat_id),
                allow_retry=False,
            )

        error = res.text[:500]
        LAST_TG_ERROR = f"{bot_info} 发送失败: {error}"
        if audit_id:
            db.update_notification_audit_log(audit_id, "failed", error)
        db.add_log("ERROR", f"TG 发送失败，{bot_info}: {error}")
        return False, error

    msg_id = None
    try:
        msg_id = res.json().get("result", {}).get("message_id")
    except Exception:
        pass

    LAST_TG_ERROR = ""
    LAST_TG_SUCCESS_TIME = time.time()
    if audit_id:
        db.update_notification_audit_log(audit_id, "sent", "", message_id=msg_id)
    return True, ""

def _process_tg_queue_item(item):
    global LAST_SEND_TIME
    max_retries = 3
    attempt = 0

    group_id = item["group_id"]
    conn = db.get_db()
    group_row = conn.execute(
        "SELECT name, tg_token, tg_chat_id, tg_token_backup FROM monitor_groups WHERE id = ?",
        (group_id,)
    ).fetchone()
    conn.close()
    
    # 转换为标准字典
    group_dict = dict(group_row) if group_row else {}
    group_name = group_dict.get("name") or f"分组-{group_id}"
    
    item_bot_token = item.get("bot_token")
    item_chat_id = item.get("chat_id")
    
    bot_slot = "主机器人"
    if item_bot_token and item_bot_token == group_dict.get("tg_token_backup"):
        bot_slot = "热备机器人"
        
    masked_cid = mask_chat_id(item_chat_id or group_dict.get("tg_chat_id", ""))
    bot_info = f"分组 [{group_name}] / {bot_slot} / Chat ID: {masked_cid}"

    # 原地通过循环方式发送同一条消息，防止 429 报错重回队尾时发生时序错乱
    while attempt < max_retries:
        attempt += 1
        throttle_ms = _safe_tg_throttle_ms()
        elapsed = (time.time() - LAST_SEND_TIME) * 1000
        if elapsed < throttle_ms:
            time.sleep((throttle_ms - elapsed) / 1000)

        ok, error = send_telegram_msg_to_group_now(
            item["group_id"],
            item["msg"],
            audit_id=item.get("audit_id"),
            reply_markup=item.get("reply_markup"),
            bot_token=item.get("bot_token"),
            chat_id=item.get("chat_id"),
        )
        
        if ok:
            return True
            
        if error.startswith("429 retry_after="):
            retry_after = _safe_retry_after(error.split("=", 1)[1] or 5)
            # 指数级退避重试，限制最大等待时间
            sleep_sec = min(30, retry_after * attempt)
            db.add_log("WARN", f"TG 推送频率限流，{bot_info}，原地等待 {sleep_sec} 秒后进行第 {attempt} 次重试...")
            time.sleep(sleep_sec)
            continue
        else:
            # 其他网络或机器人配置报错，退出重试
            break

    # 超出重试次数
    audit_id = item.get("audit_id")
    if audit_id:
        db.update_notification_audit_log(audit_id, "failed", f"发送重试超出上限: {LAST_TG_ERROR or '网络故障'}")
    return False

def tg_worker():
    global LAST_TG_ERROR
    while IS_RUNNING:
        try:
            item = TG_QUEUE.get(timeout=1.0)
        except queue.Empty:
            continue

        try:
            _process_tg_queue_item(item)
        except Exception as e:
            error = str(e)
            LAST_TG_ERROR = error
            audit_id = item.get("audit_id") if isinstance(item, dict) else None
            if audit_id:
                db.update_notification_audit_log(audit_id, "failed", error[:500])
            group_id = item.get("group_id") if isinstance(item, dict) else None
            group_name = "未知分组"
            if group_id:
                conn = db.get_db()
                g_row = conn.execute("SELECT name FROM monitor_groups WHERE id = ?", (group_id,)).fetchone()
                conn.close()
                if g_row:
                    group_name = g_row["name"]
            
            error_msg_log = f"TG Worker 异常，分组 [{group_name}]: {error}"
            db.add_log("ERROR", error_msg_log)
        finally:
            TG_QUEUE.task_done()

def build_alert_message(
    title,
    action,
    amount,
    unit="TAO",
    address=None,
    from_address=None,
    to_address=None,
    netuid=None,
    detail="",
    received_tao=None,
    tx_ref=None,
    tx_hash=None,
    alias_address=None,
    alias_from=None,
    alias_to=None,
    group_type=None,
    alpha_amount=None,
):
    if action == TEXT["transfer"]:
        if group_type == "wallet":
            msg = f"<b>{safe_text(action)}</b>\n"
            msg += f"{amount:.4f} {unit}\n"
            alias = alias_from or alias_to or ""
            if alias:
                msg += f"🏷️ <b>备注：{safe_text(alias)}</b>\n"
            msg += f"From: <code>{safe_text(from_address)}</code>\n"
            msg += f"To: <code>{safe_text(to_address)}</code>\n"
            return msg
        else:
            msg = f"<b>{safe_text(title)}</b>\n"
            msg += f"<b>{safe_text(action)}</b>\n"
            msg += f"{amount:.4f} {unit}\n"
            msg += f"From: <code>{safe_text(from_address)}</code>\n"
            msg += f"To: <code>{safe_text(to_address)}</code>\n"
            return msg

    icon = "🟢" if action == TEXT["add_stake"] else "🔴"
    if action == TEXT["move_stake"]:
        icon = "🟡"

    swap_detail = ""
    if action in {TEXT["add_stake"], TEXT["remove_stake"], TEXT["move_stake"]}:
        alpha_val = None
        tao_val = None
        if action == TEXT["add_stake"]:
            tao_val = amount
            alpha_val = alpha_amount
        else:
            alpha_val = amount
            tao_val = received_tao

        if alpha_val is not None and tao_val is not None and alpha_val > 0:
            price = tao_val / alpha_val
            if action == TEXT["move_stake"]:
                target_netuid = netuid
                if netuid and "->" in str(netuid):
                    target_netuid = str(netuid).split("->")[-1]
                swap_detail = f"<code>{alpha_val:.2f}α ⇄ {tao_val:.3f}𝞃</code>\n折算(SN{safe_text(format_netuid(target_netuid))})≈ <code>{price:.6f}𝞃</code>\n"
            else:
                swap_detail = f"<code>{alpha_val:.2f}α ⇄ {tao_val:.3f}𝞃</code>\nalpha(SN{safe_text(format_netuid(netuid))})≈ <code>{price:.6f}𝞃</code>\n"

    # 钱包监控组
    if group_type == "wallet":
        msg = f"{icon} {safe_text(action)} SN{safe_text(format_netuid(netuid))}\n"
        if swap_detail:
            msg += swap_detail
        elif detail:
            msg += f"🔥 热钱包: <code>{safe_text(short_address(detail))}</code>\n"

        msg += "\n"

        if alias_address:
            msg += f"🏷️ <b>备注：{safe_text(alias_address)}</b>\n"

        msg += f"👤 钱包地址: <code>{safe_text(short_address(address))}</code>\n"
        return msg

    # 巨鲸组
    msg = f"{icon} {safe_text(action)} SN{safe_text(format_netuid(netuid))}\n"
    if swap_detail:
        msg += swap_detail
    elif detail:
        msg += f"🔥 热钱包: <code>{safe_text(short_address(detail))}</code>\n"

    msg += f"\n👤 钱包地址: <code>{safe_text(short_address(address))}</code>\n"

    return msg

def check_and_alert(
    action,
    address,
    amount,
    unit="TAO",
    from_address=None,
    to_address=None,
    netuid=None,
    detail="",
    threshold_amount=None,
    received_tao=None,
    fee_tao=None,
    tx_ref=None,
    tx_hash=None,
    alpha_amount=None,
):
    groups = db.get_groups()
    matched_groups = []

    # 1. 匹配订阅分组，计算匹配状态
    for group in groups:
        if not group.get("is_active"):
            continue

        wallets = db.get_wallets_by_group(group["id"])
        active_wallets = {w["address"]: w["alias"] for w in wallets if w["is_active"]}

        alias = ""
        is_monitored = False
        for candidate in (address, from_address, to_address):
            if candidate in active_wallets:
                alias = active_wallets[candidate]
                is_monitored = True
                break

        amount_for_threshold = threshold_amount or amount
        threshold_matched = amount_for_threshold is not None and amount_for_threshold >= float(group.get("threshold_tao") or 5.0)

        if group["type"] == "wallet":
            if not is_monitored:
                continue
            title = f"🎯 监控钱包 [{alias}]"
        elif group["type"] == "whale":
            if not threshold_matched:
                continue
            title = f"🐋 巨鲸异动 ({group['name']})"
        else:
            continue

        matched_groups.append({
            "group": group,
            "title": title,
            "alias": alias,
            "active_wallets": active_wallets
        })

    # 2. 换仓或加减仓时，确定查询/缓存子网号 (110->15 换仓取目的子网 15)
    lookup_netuid = int(str(netuid).split("->")[-1]) if netuid and "->" in str(netuid) else (int(netuid) if netuid is not None else None)
    
    # 3. 无论金额大小，只要已有本地缓存记录，执行 Delta 滑移更新缓存
    has_cache = False
    if address and lookup_netuid is not None:
        has_cache = (db.get_wallet_cache(address, lookup_netuid) is not None)
        
    if has_cache:
        db.update_wallet_cache_delta(address, lookup_netuid, action, amount, alpha_amount=alpha_amount, received_tao=received_tao)

    # 4. 如果未匹配到任何需要播报的分组，静默退出
    if not matched_groups:
        return

    # 5. 命中推送，读取更新后的缓存
    cached_info = None
    if has_cache:
        cached_info = db.get_wallet_cache(address, lookup_netuid)

    for item in matched_groups:
        group = item["group"]
        title = item["title"]
        alias = item["alias"]
        active_wallets = item["active_wallets"]

        alias_address = active_wallets.get(address) if address else None
        alias_from = active_wallets.get(from_address) if from_address else None
        alias_to = active_wallets.get(to_address) if to_address else None

        msg = build_alert_message(
            title,
            action,
            amount,
            unit=unit,
            address=address,
            from_address=from_address,
            to_address=to_address,
            netuid=netuid,
            detail=detail,
            received_tao=received_tao,
            tx_ref=tx_ref,
            tx_hash=tx_hash,
            alias_address=alias_address,
            alias_from=alias_from,
            alias_to=alias_to,
            group_type=group.get("type"),
            alpha_amount=alpha_amount,
        )

        # 有缓存：直接拼装 Delta 增量值秒发
        if cached_info:
            free_tao = cached_info["free_tao"] or 0.0
            alpha_stake = cached_info["alpha_stake"] or 0.0
            equivalent_tao = cached_info["equivalent_tao"] or 0.0
            
            balance_info = (
                f"\n\n💰 <b>当前钱包仓位</b>\n"
                f"剩余可用: <code>{free_tao:.4f} T</code>\n"
            )
            if equivalent_tao > 0:
                balance_info += f"SN{lookup_netuid} 总 Alpha: <code>{alpha_stake:.4f}</code> ≈ <code>{equivalent_tao:.4f} T</code>"
            else:
                balance_info += f"SN{lookup_netuid} 总 Alpha: <code>{alpha_stake:.4f}</code>"
                
            msg += balance_info

        # 构造 Inline 按钮
        reply_markup = None
        if action != TEXT["transfer"]:
            reply_markup = build_inline_keyboard(tx_ref=tx_ref, netuid=netuid, address=address)
            
        send_with_backup = (
            group.get("split_stake_bots")
            and action == TEXT["remove_stake"]
            and group.get("tg_token_backup")
            and group.get("tg_chat_id_backup")
        )
        bot_token = group.get("tg_token_backup") if send_with_backup else group.get("tg_token")
        chat_id = group.get("tg_chat_id_backup") if send_with_backup else group.get("tg_chat_id")
        
        audit_id = db.add_notification_audit_log({
            "group_id": group["id"],
            "group_name": group["name"],
            "group_type": group["type"],
            "action": action,
            "address": address,
            "from_address": from_address,
            "to_address": to_address,
            "alias": alias,
            "amount": amount,
            "unit": unit,
            "netuid": str(netuid) if netuid is not None else "",
            "detail": detail,
            "threshold_amount": threshold_amount,
            "received_tao": received_tao,
            "tx_ref": tx_ref,
            "tx_hash": tx_hash,
            "message": msg,
            "send_status": "queued",
            "error_message": "",
        })
        
        # 秒级秒发 (不阻塞主扫描线程)
        send_telegram_msg_to_group(
            group["id"],
            msg,
            audit_id=audit_id,
            reply_markup=reply_markup,
            bot_token=bot_token,
            chat_id=chat_id,
        )
        
        # 读取配置的本地缓存写入阈值
        try:
            cache_threshold = float(db.get_setting("cache_threshold_tao", "60.0"))
        except Exception:
            cache_threshold = 60.0
            
        amount_for_cache = threshold_amount or amount
        # 只要是监控的钱包分组，或者金额达到/超过缓存阈值，就提交异步任务进行链上查仓以创建/校准缓存，并纠正 TG 消息
        should_query = (group.get("type") == "wallet") or (amount_for_cache >= cache_threshold)
        
        if lookup_netuid is not None and should_query:
            SCANNER_EXECUTOR.submit(
                background_query_and_update,
                audit_id=audit_id,
                address=address,
                netuid=lookup_netuid,
                original_msg=msg,
                bot_token=bot_token,
                chat_id=chat_id,
                reply_markup=reply_markup,
            )

def alert_from_stake_events(events, tx_ref=None, tx_hash=None):
    for record in events:
        event = object_attr(record, "event")
        module, name = event_name(event)
        if module != "SubtensorModule":
            continue

        attrs = event_attributes(event)

        if name == "StakeRemoved":
            account = normalize_address(attr_value(attrs, ("coldkey", "account"), 0))
            hotkey = normalize_address(attr_value(attrs, ("hotkey",), 1))
            received_tao = to_float_tao(attr_value(attrs, ("tao_amount", "amount"), 2))
            alpha_amount = to_float_tao(attr_value(attrs, ("alpha_amount",), 3)) or 0
            netuid = attr_value(attrs, ("netuid",), 4)
            if is_root_netuid(netuid):
                continue
            check_and_alert(
                TEXT["remove_stake"],
                account,
                alpha_amount,
                unit="Alpha",
                netuid=netuid,
                detail=hotkey,
                threshold_amount=received_tao,
                received_tao=received_tao,
                tx_ref=tx_ref,
                tx_hash=tx_hash,
            )

        elif name == "StakeAdded":
            account = normalize_address(attr_value(attrs, ("coldkey", "account"), 0))
            hotkey = normalize_address(attr_value(attrs, ("hotkey",), 1))
            tao_amount = to_float_tao(attr_value(attrs, ("tao_amount", "amount"), 2))
            alpha_amount = to_float_tao(attr_value(attrs, ("alpha_amount",), 3)) or 0
            netuid = attr_value(attrs, ("netuid",), 4)
            if is_root_netuid(netuid) or tao_amount is None:
                continue
            check_and_alert(
                TEXT["add_stake"],
                account,
                tao_amount,
                unit="TAO",
                netuid=netuid,
                detail=hotkey,
                threshold_amount=tao_amount,
                tx_ref=tx_ref,
                tx_hash=tx_hash,
                alpha_amount=alpha_amount,
            )

def handle_call(call, signer, events=None, tx_ref=None, tx_hash=None):
    events = events or []
    call = raw_value(call) or {}
    call_module = call.get("call_module")
    call_function = call.get("call_function")

    if (call_module, call_function) in BATCH_CALLS:
        nested_calls = get_call_arg(call, ("calls",), 0) or []
        for nested_call in nested_calls:
            handle_call(nested_call, signer, events=events, tx_ref=tx_ref, tx_hash=tx_hash)
        return

    if call_module == "Proxy" and call_function == "proxy":
        if proxy_call_succeeded(events) is False:
            return
        real = normalize_address(get_call_arg(call, ("real",), 0))
        nested_call = get_call_arg(call, ("call",), 2)
        handle_call(nested_call, real or signer, events=events, tx_ref=tx_ref, tx_hash=tx_hash)
        return

    if call_module == "Balances" and call_function in TRANSFER_CALLS:
        return

    if call_module == "SubtensorModule" and call_function in {"add_stake", "add_stake_limit"}:
        hotkey = normalize_address(get_call_arg(call, ("hotkey", "hotkey_ss58"), 0))
        netuid = get_call_arg(call, ("netuid",), 1)
        if is_root_netuid(netuid):
            return
        event_tao, event_alpha = stake_added_amounts(events, signer, hotkey, netuid)
        if event_tao is None:
            db.add_log("WARN", f"跳过无 StakeAdded 成功事件的加仓交易: {tx_ref or 'unknown'}")
            return
        amount_tao = event_tao
        check_and_alert(
            TEXT["add_stake"],
            signer,
            amount_tao,
            unit="TAO",
            netuid=netuid,
            detail=hotkey,
            threshold_amount=amount_tao,
            tx_ref=tx_ref,
            tx_hash=tx_hash,
            alpha_amount=event_alpha,
        )
        return

    if call_module == "SubtensorModule" and call_function in {"remove_stake", "remove_stake_limit"}:
        hotkey = normalize_address(get_call_arg(call, ("hotkey", "hotkey_ss58"), 0))
        netuid = get_call_arg(call, ("netuid",), 1)
        if is_root_netuid(netuid):
            return
        amount_rao = get_last_call_arg(call, ("amount_unstaked", "amount"))
        amount = format_tao(amount_rao) if amount_rao is not None else 0
        received_tao, _ = stake_removed_amounts(events, signer, hotkey, netuid)
        if received_tao is None:
            received_tao = tao_received_by_account(events, signer)
        fee_tao = fee_paid_by_account(events, signer)
        check_and_alert(
            TEXT["remove_stake"],
            signer,
            amount,
            unit="Alpha",
            netuid=netuid,
            detail=hotkey,
            threshold_amount=received_tao,
            received_tao=received_tao,
            fee_tao=fee_tao,
            tx_ref=tx_ref,
            tx_hash=tx_hash,
        )
        return

    if call_module == "SubtensorModule" and call_function in {"remove_stake_full", "remove_stake_full_limit"}:
        hotkey = normalize_address(get_call_arg(call, ("hotkey", "hotkey_ss58"), 0))
        netuid = get_call_arg(call, ("netuid",), 1)
        if is_root_netuid(netuid):
            return
        received_tao, alpha_amount = stake_removed_amounts(events, signer, hotkey, netuid)
        check_and_alert(
            TEXT["remove_stake"],
            signer,
            alpha_amount or 0,
            unit="Alpha",
            netuid=netuid,
            detail=hotkey,
            threshold_amount=received_tao,
            received_tao=received_tao,
            tx_ref=tx_ref,
            tx_hash=tx_hash,
        )
        return

    if call_module == "SubtensorModule" and call_function == "unstake_all":
        hotkey = normalize_address(get_call_arg(call, ("hotkey", "hotkey_ss58"), 0))
        for entry in stake_removed_entries(events, signer, hotkey):
            if is_root_netuid(entry["netuid"]):
                continue
            check_and_alert(
                TEXT["remove_stake"],
                signer,
                entry["alpha"] or 0,
                unit="Alpha",
                netuid=entry["netuid"],
                detail=hotkey,
                threshold_amount=entry["tao"],
                received_tao=entry["tao"],
                tx_ref=tx_ref,
                tx_hash=tx_hash,
            )
        return

    if call_module == "SubtensorModule" and call_function in {"swap_stake", "swap_stake_limit"}:
        hotkey = normalize_address(get_call_arg(call, ("hotkey", "hotkey_ss58"), 0))
        origin_netuid = get_call_arg(call, ("origin_netuid",), 1)
        destination_netuid = get_call_arg(call, ("destination_netuid",), 2)

        if not is_root_netuid(origin_netuid) and is_root_netuid(destination_netuid):
            amount_rao = get_call_arg(call, ("alpha_amount", "amount"), 3)
            alpha_amount = format_tao(amount_rao)
            received_tao = stake_swapped_tao(events, signer, hotkey, origin_netuid, destination_netuid)
            check_and_alert(
                TEXT["remove_stake"],
                signer,
                alpha_amount,
                unit="Alpha",
                netuid=origin_netuid,
                detail=hotkey,
                threshold_amount=received_tao,
                received_tao=received_tao,
                tx_ref=tx_ref,
                tx_hash=tx_hash,
            )
            return

        if not is_root_netuid(origin_netuid) and not is_root_netuid(destination_netuid):
            amount_rao = get_call_arg(call, ("alpha_amount", "amount"), 3)
            alpha_amount = format_tao(amount_rao)
            tao_value = stake_swapped_tao(events, signer, hotkey, origin_netuid, destination_netuid)
            check_and_alert(
                TEXT["move_stake"],
                signer,
                alpha_amount,
                unit="Alpha",
                netuid=f"{origin_netuid}->{destination_netuid}",
                detail=hotkey,
                threshold_amount=tao_value,
                received_tao=tao_value,
                tx_ref=tx_ref,
                tx_hash=tx_hash,
            )
            return

        if is_root_netuid(origin_netuid) and not is_root_netuid(destination_netuid):
            amount_rao = get_call_arg(call, ("tao_amount", "amount"), 3)
            amount_tao = format_tao(amount_rao) if amount_rao is not None else 0
            added_tao, added_alpha = stake_added_amounts(events, signer, hotkey, destination_netuid)
            if added_tao is not None:
                amount_tao = added_tao
            check_and_alert(
                TEXT["add_stake"],
                signer,
                amount_tao,
                unit="TAO",
                netuid=destination_netuid,
                detail=hotkey,
                threshold_amount=amount_tao,
                tx_ref=tx_ref,
                tx_hash=tx_hash,
                alpha_amount=added_alpha,
            )

def process_block(substrate, block_hash, block, block_number=None):
    try:
        grouped_events = events_by_extrinsic_index(substrate, block_hash)
        success_indices = successful_extrinsic_indices(grouped_events)
        if not success_indices:
            return

        for extrinsic_index, extrinsic in enumerate(block["extrinsics"]):
            if extrinsic_index not in success_indices:
                continue

            extrinsic_value = raw_value(extrinsic) or {}
            call = extrinsic_value.get("call", {})
            tx_ref = f"{block_number}-{extrinsic_index:04d}" if block_number is not None else None
            tx_hash = extrinsic_value.get("extrinsic_hash")
            events = grouped_events.get(extrinsic_index, [])
            signer = normalize_address(extrinsic_value.get("address"))

            if call.get("call_module") == "Ethereum":
                if has_subtensor_alertable_event(events):
                    if alert_from_evm_stake_transfer(events, tx_ref=tx_ref, tx_hash=tx_hash):
                        continue
                    alert_from_stake_events(events, tx_ref=tx_ref, tx_hash=tx_hash)
                    continue
                continue

            if not signer:
                alert_from_stake_events(events, tx_ref=tx_ref, tx_hash=tx_hash)
                continue

            handle_call(call, signer, events=events, tx_ref=tx_ref, tx_hash=tx_hash)
    except Exception as e:
        db.add_log("ERROR", f"区块 #{block_number or 'unknown'} 解析失败: {str(e)}")

def uptime_heartbeat():
    cleanup_counter = 0
    while IS_RUNNING:
        db.record_uptime(1 if LAST_CONNECT_TIME > 0 else 0)
        
        # 每隔 60 分钟清理一次系统日志，解决每次 add_log 触发全表删除造成锁冲突的问题
        cleanup_counter += 1
        if cleanup_counter >= 60:
            cleanup_counter = 0
            try:
                conn = db.get_db()
                conn.execute("DELETE FROM system_logs WHERE created_at < datetime('now', '-1 day')")
                conn.commit()
                conn.close()
            except Exception as e:
                db.add_log("WARN", f"定时自动清理过期日志失败: {str(e)}")
                
        time.sleep(60)

def start_scanner():
    global LAST_CONNECT_TIME, LAST_BLOCK_TIME, LAST_BLOCK_NUMBER, LAST_ERROR, CURRENT_WSS_LABEL, LAST_WSS_LATENCY_MS, IS_RUNNING, CONSECUTIVE_HANDLER_ERRORS, ACTIVE_SUBSTRATE, WSS_INDEX
    if IS_RUNNING:
        db.add_log("WARN", "监控服务已经在运行中，请勿重复启动。")
        return
        
    # 获得文件锁保障分布式多 worker 下只有一个进程能拉起扫描线程
    if not acquire_lock():
        db.add_log("WARN", "未获得 scanner.lock 排他锁，当前另有一个扫描器实例在运行。跳过本次线程拉起。")
        return

    IS_RUNNING = True
    db.add_log("INFO", "启动监控服务 Pro...")
    
    # 启动后台处理进程
    threading.Thread(target=tg_worker, daemon=True).start()
    threading.Thread(target=uptime_heartbeat, daemon=True).start()

    while IS_RUNNING:
        try:
            # 负载均衡开关在此优雅重构，解析出 load_balance 指示
            wss_label, wss_url, load_balance = pick_wss_target()
            normalized_url = normalize_wss_url(wss_url)
            CURRENT_WSS_LABEL = wss_label
            
            # 使用局部变量保存当前连接以隔离作用域，避免并发旧 handler 被污染
            substrate = SubstrateInterface(url=normalized_url)
            ACTIVE_SUBSTRATE = substrate
            LAST_CONNECT_TIME = time.time()
            LAST_ERROR = ""
            CONSECUTIVE_HANDLER_ERRORS = 0
            db.add_log("INFO", f"WSS 连接成功: {wss_label} ({normalized_url})")

            # 主备 failover 策略和负载均衡调度在 except 异常处理中统一管理

            def handler(obj, update_nr, subscription_id):
                global LAST_BLOCK_TIME, LAST_BLOCK_NUMBER, LAST_WSS_LATENCY_MS, CONSECUTIVE_HANDLER_ERRORS
                try:
                    block_num = obj["header"]["number"]
                    request_start = time.perf_counter()
                    # 使用闭包内引用的 local substrate，防止被并发重连的 ACTIVE_SUBSTRATE 污染
                    block_hash = substrate.get_block_hash(block_num)
                    block = substrate.get_block(block_hash=block_hash)
                    grouped_probe_start = time.perf_counter()
                    process_block(substrate, block_hash, block, block_number=block_num)
                    LAST_WSS_LATENCY_MS = int((grouped_probe_start - request_start) * 1000)
                    LAST_BLOCK_NUMBER = block_num
                    LAST_BLOCK_TIME = time.time()
                    CONSECUTIVE_HANDLER_ERRORS = 0
                except Exception as ex:
                    # 鲁棒处理：解析出错时记录日志，不终止 WebSocket 订阅线程
                    CONSECUTIVE_HANDLER_ERRORS += 1
                    err_msg = f"区块 #{obj.get('header', {}).get('number', 'unknown')} 处理异常 (累计 {CONSECUTIVE_HANDLER_ERRORS} 次): {str(ex)}"
                    db.add_log("ERROR", err_msg)
                    
                    # 超过 5 次连续出错自动关闭抛出，由外层 reconnect 重连
                    if CONSECUTIVE_HANDLER_ERRORS >= 5:
                        raise RuntimeError("连续发生 5 次区块解析失败，主动重连") from ex

            substrate.subscribe_block_headers(handler)
        except Exception as e:
            # 捕获连接异常并保存状态
            was_connected = (LAST_CONNECT_TIME > 0)
            LAST_CONNECT_TIME = 0
            LAST_ERROR = str(e)
            db.add_log("ERROR", f"{CURRENT_WSS_LABEL or 'WSS'} 连接断开: {LAST_ERROR}")
            
            # 主备 failover 与负载均衡调度策略
            if was_connected:
                # 之前连接是成功的，代表本次是正常运行中断线
                # 负载均衡模式下轮换下一个节点；主备模式下，重连时优先尝试主节点（重置为 0）
                if load_balance:
                    WSS_INDEX += 1
                else:
                    WSS_INDEX = 0
            else:
                # 之前尝试连接就失败了，当前节点不可用，轮换下一个节点尝试
                WSS_INDEX += 1
            
            if not IS_RUNNING:
                break
                
            time.sleep(10)

def stop_scanner():
    global IS_RUNNING, LAST_CONNECT_TIME, ACTIVE_SUBSTRATE
    IS_RUNNING = False
    LAST_CONNECT_TIME = 0
    if ACTIVE_SUBSTRATE:
        try:
            # 显式关闭连接，强迫 subscribe_block_headers() 底层 Websocket 抛出异常并立刻跳出阻塞，实现优雅退市
            ACTIVE_SUBSTRATE.close()
        except Exception:
            pass
        ACTIVE_SUBSTRATE = None
    release_lock()
    db.add_log("INFO", "监控服务已停止。")
