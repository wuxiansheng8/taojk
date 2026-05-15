import html
import queue
import threading
import time
from urllib.parse import urlparse, urlunparse

import requests
from substrateinterface import SubstrateInterface

import database as db

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

TG_QUEUE = queue.Queue()
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
    global WSS_INDEX
    targets, load_balance = get_wss_targets()

    if load_balance and len(targets) > 1:
        target = targets[WSS_INDEX % len(targets)]
    else:
        target = targets[WSS_INDEX % len(targets)]

    WSS_INDEX += 1
    return target


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

    subnet_link = subnet_url(netuid)
    profile_link = profile_url(address)

    if subnet_link:
        buttons.append({"text": "查看子网详情", "url": subnet_link})
    if profile_link:
        buttons.append({"text": "查看操作者钱包", "url": profile_link})

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
            "account": normalize_address(attr_value(attrs, ("coldkey", "account"), 0)),
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
            "account": normalize_address(attr_value(attrs, ("coldkey", "account"), 0)),
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
            added_tao, _ = stake_added_amounts(events, entry["to_account"], hotkey, destination_netuid)
            if added_tao is not None:
                tao_amount = added_tao
            check_and_alert(
                TEXT["add_stake"],
                entry["to_account"],
                tao_amount or 0,
                unit="TAO",
                netuid=destination_netuid,
                detail=hotkey,
                threshold_amount=tao_amount,
                tx_ref=tx_ref,
                tx_hash=tx_hash,
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
    TG_QUEUE.put({
        "group_id": group_id,
        "msg": msg,
        "audit_id": audit_id,
        "reply_markup": reply_markup,
        "bot_token": bot_token,
        "chat_id": chat_id,
    })
    return True


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
    group = conn.execute(
        "SELECT tg_token, tg_chat_id, tg_token_backup, tg_chat_id_backup, split_stake_bots FROM monitor_groups WHERE id = ?",
        (group_id,)
    ).fetchone()
    conn.close()

    if not group:
        return None, None, None, f"监控分组不存在: {group_id}"

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

    group, bot_token, chat_id, error = _load_tg_target(group_id, bot_token, chat_id)
    if error:
        LAST_TG_ERROR = error
        if audit_id:
            db.update_notification_audit_log(audit_id, "failed", error)
        db.add_log("ERROR", f"TG 发送失败: {error}")
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
        LAST_TG_ERROR = error
        if audit_id:
            db.update_notification_audit_log(audit_id, "failed", error[:500])
        db.add_log("ERROR", f"TG 网络异常: {error}")
        return False, error

    if res.status_code == 429:
        try:
            retry_after = _safe_retry_after(res.json().get("parameters", {}).get("retry_after", 5))
        except Exception:
            retry_after = 5
        error = f"429 retry_after={retry_after}"
        LAST_TG_ERROR = error
        if audit_id:
            db.update_notification_audit_log(audit_id, "retrying" if allow_retry else "failed", error)
        db.add_log("WARN", f"TG 频率限制，等待 {retry_after} 秒")
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
            db.add_log("WARN", f"TG 群升级为超级群，自动切换 Chat ID -> {migrate_to_chat_id}")
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
        LAST_TG_ERROR = error
        if audit_id:
            db.update_notification_audit_log(audit_id, "failed", error)
        db.add_log("ERROR", f"TG 发送失败: {error}")
        return False, error

    LAST_TG_ERROR = ""
    LAST_TG_SUCCESS_TIME = time.time()
    if audit_id:
        db.update_notification_audit_log(audit_id, "sent", "")
    return True, ""


def _process_tg_queue_item(item):
    global LAST_SEND_TIME

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
    if not ok and error.startswith("429 retry_after="):
        retry_after = _safe_retry_after(error.split("=", 1)[1] or 5)
        time.sleep(retry_after)
        TG_QUEUE.put(item)


def tg_worker():
    global LAST_TG_ERROR
    while True:
        item = TG_QUEUE.get()
        try:
            _process_tg_queue_item(item)
        except Exception as e:
            error = str(e)
            LAST_TG_ERROR = error
            audit_id = item.get("audit_id") if isinstance(item, dict) else None
            if audit_id:
                db.update_notification_audit_log(audit_id, "failed", error[:500])
            db.add_log("ERROR", f"TG Worker 异常: {error}")
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
):
    if action == TEXT["transfer"]:
        msg = f"<b>{safe_text(title)}</b>\n"
        msg += f"<b>{safe_text(action)}</b>\n"
        msg += f"{amount:.4f} {unit}\n"
        msg += f"From: <code>{safe_text(from_address)}</code>\n"
        msg += f"To: <code>{safe_text(to_address)}</code>\n"
        return msg

    icon = "🟢" if action == TEXT["add_stake"] else "🔴"
    if action == TEXT["move_stake"]:
        icon = "🟡"

    msg = f"{icon} <b>{safe_text(action)} SN{safe_text(format_netuid(netuid))}</b>\n"
    if received_tao is not None:
        msg += f"{icon} Swap {received_tao:.4f} TAO\n\n"
    else:
        msg += f"{icon} Swap {amount:.4f} {unit}\n\n"

    msg += f"🏠 操作者: <code>{safe_text(short_address(address))}</code>\n"
    if detail:
        msg += f"🔥 热钱包: <code>{safe_text(short_address(detail))}</code>\n"
    msg += f"\n📍 子网{safe_text(format_netuid(netuid))} "

    if tx_ref:
        detail_url = f"https://taostats.io/extrinsic/{tx_ref}"
        msg += f"🔎 区块: <code>{safe_text(tx_ref)}</code>\n"

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
):
    groups = db.get_groups()

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

        amount_for_threshold = None if unit == "Alpha" and threshold_amount is None else amount
        if threshold_amount is not None:
            amount_for_threshold = threshold_amount
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
        )
        reply_markup = None
        if action != TEXT["transfer"]:
            reply_markup = build_inline_keyboard(tx_ref=tx_ref, netuid=netuid, address=address)
        send_with_backup = (
            group.get("split_stake_bots")
            and action == TEXT["remove_stake"]
            and group.get("tg_token_backup")
            and group.get("tg_chat_id_backup")
        )
        bot_token = group.get("tg_token")
        chat_id = group.get("tg_chat_id")
        if send_with_backup:
            bot_token = group.get("tg_token_backup")
            chat_id = group.get("tg_chat_id_backup")
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
        send_telegram_msg_to_group(
            group["id"],
            msg,
            audit_id=audit_id,
            reply_markup=reply_markup,
            bot_token=bot_token,
            chat_id=chat_id,
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
        # a532519 版本：普通钱包转账提醒刻意关闭，只保留 stake 相关提醒。
        return

    if call_module == "SubtensorModule" and call_function in {"add_stake", "add_stake_limit"}:
        hotkey = normalize_address(get_call_arg(call, ("hotkey", "hotkey_ss58"), 0))
        netuid = get_call_arg(call, ("netuid",), 1)
        if is_root_netuid(netuid):
            return
        event_tao, _ = stake_added_amounts(events, signer, hotkey, netuid)
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
            amount_tao = format_tao(amount_rao)
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
                db.add_log("INFO", f"跳过无价格影响的 EVM 交易: {tx_ref or extrinsic_index}")
                continue

            if not signer:
                alert_from_stake_events(events, tx_ref=tx_ref, tx_hash=tx_hash)
                continue

            handle_call(call, signer, events=events, tx_ref=tx_ref, tx_hash=tx_hash)
    except Exception as e:
        db.add_log("ERROR", f"区块解析失败: {str(e)}")


def uptime_heartbeat():
    while True:
        db.record_uptime(1 if LAST_CONNECT_TIME > 0 else 0)
        time.sleep(60)


def start_scanner():
    global LAST_CONNECT_TIME, LAST_BLOCK_TIME, LAST_BLOCK_NUMBER, LAST_ERROR, CURRENT_WSS_LABEL, LAST_WSS_LATENCY_MS
    db.add_log("INFO", "启动监控服务 Pro...")
    threading.Thread(target=tg_worker, daemon=True).start()
    threading.Thread(target=uptime_heartbeat, daemon=True).start()

    while True:
        try:
            wss_label, wss_url = pick_wss_target()
            CURRENT_WSS_LABEL = wss_label
            substrate = SubstrateInterface(url=wss_url)
            LAST_CONNECT_TIME = time.time()
            LAST_ERROR = ""
            db.add_log("INFO", f"WSS 连接成功: {wss_label}")

            def handler(obj, update_nr, subscription_id):
                global LAST_BLOCK_TIME, LAST_BLOCK_NUMBER, LAST_WSS_LATENCY_MS
                block_num = obj["header"]["number"]
                request_start = time.perf_counter()
                block_hash = substrate.get_block_hash(block_num)
                block = substrate.get_block(block_hash=block_hash)
                grouped_probe_start = time.perf_counter()
                process_block(substrate, block_hash, block, block_number=block_num)
                LAST_WSS_LATENCY_MS = int((grouped_probe_start - request_start) * 1000)
                LAST_BLOCK_NUMBER = block_num
                LAST_BLOCK_TIME = time.time()

            substrate.subscribe_block_headers(handler)
        except Exception as e:
            LAST_CONNECT_TIME = 0
            LAST_ERROR = str(e)
            db.add_log("ERROR", f"{CURRENT_WSS_LABEL or 'WSS'} 连接断开: {LAST_ERROR}")
            time.sleep(10)
