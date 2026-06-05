import threading
import time
from substrateinterface import SubstrateInterface
from substrateinterface.storage import StorageKey
from scalecodec.base import ScaleBytes
import database as db

QUERY_SUBSTRATE = None
QUERY_SUBSTRATE_LOCK = threading.Lock()
QUERY_IO_LOCK = threading.Lock()
QUERY_HEARTBEAT_STARTED = False

def query_heartbeat_loop():
    while True:
        time.sleep(30)
        if QUERY_SUBSTRATE is None:
            continue
        try:
            with QUERY_IO_LOCK:
                if QUERY_SUBSTRATE is None:
                    continue
                QUERY_SUBSTRATE.get_chain_head()
        except Exception as e:
            with QUERY_SUBSTRATE_LOCK:
                if QUERY_SUBSTRATE is not None:
                    db.add_log("WARN", f"常驻查询连接心跳检测失败: {str(e)}")
                    try:
                        QUERY_SUBSTRATE.close()
                    except:
                        pass
                    QUERY_SUBSTRATE = None

def start_query_heartbeat():
    global QUERY_HEARTBEAT_STARTED
    if not QUERY_HEARTBEAT_STARTED:
        with QUERY_SUBSTRATE_LOCK:
            if not QUERY_HEARTBEAT_STARTED:
                t = threading.Thread(target=query_heartbeat_loop, daemon=True)
                t.start()
                QUERY_HEARTBEAT_STARTED = True
                db.add_log("INFO", "常驻查询连接心跳守护线程已启动。")


def extract_numeric_value(obj):
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

def get_query_wss_targets():
    query_primary = db.get_setting("query_wss", "").strip()
    query_backup = db.get_setting("query_wss_backup", "").strip()
    
    targets = []
    if query_primary:
        targets.append(query_primary)
    if query_backup:
        targets.append(query_backup)
        
    # 如果用户没有配置任何专属查询接口，兜底使用系统的主监听接口以保证基本运行
    if not targets:
        main_primary = db.get_setting("dwellir_wss", "wss://api-bittensor-mainnet.n.dwellir.com").strip()
        targets.append(main_primary)
        
    seen = set()
    unique_targets = []
    for t in targets:
        if t not in seen:
            unique_targets.append(t)
            seen.add(t)
    return unique_targets

def get_query_substrate(dwellir_wss=None):
    global QUERY_SUBSTRATE
    targets = get_query_wss_targets()
    if not targets:
        targets = ["wss://api-bittensor-mainnet.n.dwellir.com"]
        
    if QUERY_SUBSTRATE is None:
        with QUERY_SUBSTRATE_LOCK:
            if QUERY_SUBSTRATE is None:
                for url in targets:
                    try:
                        db.add_log("INFO", f"正在尝试初始化常驻余额查询连接: {url}")
                        new_sub = SubstrateInterface(url=url, ws_options={"timeout": 5})
                        new_sub.get_chain_head()
                        QUERY_SUBSTRATE = new_sub
                        db.add_log("INFO", f"已成功初始化常驻余额查询长连接: {url}")
                        start_query_heartbeat()
                        break
                    except Exception as e:
                        db.add_log("WARN", f"尝试使用 {url} 初始化常驻余额查询长连接失败: {str(e)}")
                        
    if QUERY_SUBSTRATE:
        try:
            if not hasattr(QUERY_SUBSTRATE, "websocket") or not QUERY_SUBSTRATE.websocket or not QUERY_SUBSTRATE.websocket.connected:
                raise Exception("Websocket connection is disconnected")
        except Exception:
            with QUERY_SUBSTRATE_LOCK:
                db.add_log("INFO", "检测到常驻查询连接断开，正在尝试重建...")
                try:
                    try: QUERY_SUBSTRATE.close()
                    except: pass
                except: pass
                QUERY_SUBSTRATE = None
                
                for url in targets:
                    try:
                        db.add_log("INFO", f"正在尝试重建常驻余额查询连接: {url}")
                        new_sub = SubstrateInterface(url=url, ws_options={"timeout": 5})
                        new_sub.get_chain_head()
                        QUERY_SUBSTRATE = new_sub
                        db.add_log("INFO", f"重建常驻余额查询长连接成功: {url}")
                        start_query_heartbeat()
                        break
                    except Exception as e:
                        db.add_log("WARN", f"尝试使用 {url} 重建常驻余额查询长连接失败: {str(e)}")
                          
    return QUERY_SUBSTRATE

def _query_blockchain_data_with_substrate(substrate, address, netuid, free_tao=None, hotkeys=None):
    def get_storage_value_type(pallet, function):
        if not substrate.metadata:
            return None
        try:
            metadata_pallet = substrate.metadata.get_metadata_pallet(pallet)
            if metadata_pallet:
                storage_item = metadata_pallet.get_storage_function(function)
                if storage_item:
                    return storage_item.get_value_type_string()
        except Exception as e:
            db.add_log("WARN", f"从元数据获取 {pallet}.{function} 类型失败: {str(e)}")
        return None

    # 出现类型解码异常时，attempt=0 会触发 init_runtime 并自动重试自愈
    for attempt in range(2):
        try:
            # 1. 批量合并查询可用余额 & StakingHotkeys (合并为 1 个 WSS/RPC 请求)
            initial_keys = []
            key_mapping = {}
            
            account_type = get_storage_value_type("System", "Account")
            if not account_type:
                with QUERY_IO_LOCK:
                    substrate.init_runtime()
                account_type = get_storage_value_type("System", "Account")
                
            if free_tao is None:
                account_key = StorageKey.create_from_storage_function(
                    "System", "Account", [address],
                    runtime_config=substrate.runtime_config,
                    metadata=substrate.metadata
                ).to_hex()
                initial_keys.append(account_key)
                key_mapping[account_key] = ("free_tao", account_type)
                
            hotkeys_type = get_storage_value_type("SubtensorModule", "StakingHotkeys")
            if hotkeys is None and hotkeys_type:
                hotkeys_key = StorageKey.create_from_storage_function(
                    "SubtensorModule", "StakingHotkeys", [address],
                    runtime_config=substrate.runtime_config,
                    metadata=substrate.metadata
                ).to_hex()
                initial_keys.append(hotkeys_key)
                key_mapping[hotkeys_key] = ("hotkeys", hotkeys_type)
                
            if initial_keys:
                with QUERY_IO_LOCK:
                    response = substrate.rpc_request("state_queryStorageAt", [initial_keys])
                if isinstance(response, dict) and "result" in response:
                    response = response["result"]
                if isinstance(response, list) and len(response) > 0:
                    changes = response[0].get("changes", [])
                    for k_hex, v_hex in changes:
                        if v_hex and v_hex != "0x":
                            name, t_str = key_mapping.get(k_hex, (None, None))
                            if name and t_str:
                                try:
                                    scale_bytes = ScaleBytes(v_hex)
                                    obj = substrate.runtime_config.create_scale_object(
                                        type_string=t_str,
                                        data=scale_bytes,
                                        metadata=substrate.metadata
                                    )
                                    obj.decode()
                                    val = obj.value
                                    if name == "free_tao" and isinstance(val, dict):
                                        free_tao = float(val.get("data", {}).get("free", 0)) / 1e9
                                    elif name == "hotkeys":
                                        hotkeys = val
                                except Exception as decode_err:
                                    db.add_log("ERROR", f"解析初始键 {name} 数据失败: {str(decode_err)}")
                                    
            if free_tao is None:
                free_tao = 0.0
            if hotkeys is None:
                hotkeys = []

            # 2. 批量合并查询持仓 & 价格池 (合并为 1 个 WSS/RPC 请求)
            storage_keys = []
            stake_key_mapping = {}
            
            alpha_v2_type = get_storage_value_type("SubtensorModule", "AlphaV2")
            total_shares_v2_type = get_storage_value_type("SubtensorModule", "TotalHotkeySharesV2")
            total_alpha_type = get_storage_value_type("SubtensorModule", "TotalHotkeyAlpha")
            tao_pool_type = get_storage_value_type("SubtensorModule", "SubnetTAO")
            alpha_pool_type = get_storage_value_type("SubtensorModule", "SubnetAlphaIn")
            
            # 价格池 Key
            if tao_pool_type:
                try:
                    key_tao_pool = StorageKey.create_from_storage_function(
                        "SubtensorModule", "SubnetTAO", [int(netuid)],
                        runtime_config=substrate.runtime_config,
                        metadata=substrate.metadata
                    ).to_hex()
                    storage_keys.append(key_tao_pool)
                    stake_key_mapping[key_tao_pool] = ("SubnetTAO", tao_pool_type, None)
                except Exception as key_err:
                    db.add_log("WARN", f"生成 SubnetTAO 键失败 (netuid: {netuid}): {str(key_err)}")
                
            if alpha_pool_type:
                try:
                    key_alpha_pool = StorageKey.create_from_storage_function(
                        "SubtensorModule", "SubnetAlphaIn", [int(netuid)],
                        runtime_config=substrate.runtime_config,
                        metadata=substrate.metadata
                    ).to_hex()
                    storage_keys.append(key_alpha_pool)
                    stake_key_mapping[key_alpha_pool] = ("SubnetAlphaIn", alpha_pool_type, None)
                except Exception as key_err:
                    db.add_log("WARN", f"生成 SubnetAlphaIn 键失败 (netuid: {netuid}): {str(key_err)}")

            # Hotkeys 质押 Key
            if isinstance(hotkeys, list) and len(hotkeys) > 0:
                for hk in hotkeys:
                    hk_str = hk[0] if isinstance(hk, (list, tuple)) else hk
                    if not isinstance(hk_str, str):
                        continue
                    
                    if alpha_v2_type:
                        try:
                            k_alphav2 = StorageKey.create_from_storage_function(
                                "SubtensorModule", "AlphaV2", [hk_str, address, int(netuid)],
                                runtime_config=substrate.runtime_config,
                                metadata=substrate.metadata
                            ).to_hex()
                            storage_keys.append(k_alphav2)
                            stake_key_mapping[k_alphav2] = ("AlphaV2", alpha_v2_type, hk_str)
                        except Exception as key_err:
                            db.add_log("WARN", f"生成 AlphaV2 键失败 (hotkey: {hk_str}): {str(key_err)}")

                    if total_shares_v2_type:
                        try:
                            k_shares_v2 = StorageKey.create_from_storage_function(
                                "SubtensorModule", "TotalHotkeySharesV2", [hk_str, int(netuid)],
                                runtime_config=substrate.runtime_config,
                                metadata=substrate.metadata
                            ).to_hex()
                            storage_keys.append(k_shares_v2)
                            stake_key_mapping[k_shares_v2] = ("TotalSharesV2", total_shares_v2_type, hk_str)
                        except Exception as key_err:
                            db.add_log("WARN", f"生成 TotalSharesV2 键失败 (hotkey: {hk_str}): {str(key_err)}")

                    if total_alpha_type:
                        try:
                            k_tot_alpha = StorageKey.create_from_storage_function(
                                "SubtensorModule", "TotalHotkeyAlpha", [hk_str, int(netuid)],
                                runtime_config=substrate.runtime_config,
                                metadata=substrate.metadata
                            ).to_hex()
                            storage_keys.append(k_tot_alpha)
                            stake_key_mapping[k_tot_alpha] = ("TotalAlpha", total_alpha_type, hk_str)
                        except Exception as key_err:
                            db.add_log("WARN", f"生成 TotalAlpha 键失败 (hotkey: {hk_str}): {str(key_err)}")
            
            # 3. 发起分片批量 RPC 请求获取持仓和池状态（每组最多 100 个 keys）
            alpha_stake = 0.0
            tao_pool = 0.0
            alpha_pool = 0.0
            
            decoded = {}
            if storage_keys:
                chunk_size = 100
                for i in range(0, len(storage_keys), chunk_size):
                    chunk = storage_keys[i:i + chunk_size]
                    with QUERY_IO_LOCK:
                        res = substrate.rpc_request("state_queryStorageAt", [chunk])
                    if isinstance(res, dict) and "result" in res:
                        res = res["result"]
                    if isinstance(res, list) and len(res) > 0:
                        for k_hex, v_hex in res[0].get("changes", []):
                            if v_hex and v_hex != "0x":
                                meta = stake_key_mapping.get(k_hex)
                                if meta:
                                    name, t_str, hk_str = meta
                                    try:
                                        obj = substrate.runtime_config.create_scale_object(t_str, ScaleBytes(v_hex), metadata=substrate.metadata)
                                        obj.decode()
                                        decoded[(name, hk_str)] = extract_numeric_value(obj)
                                    except Exception as decode_err:
                                        db.add_log("ERROR", f"解析 {name} 数据失败 (hotkey: {hk_str}, type: {t_str}): {str(decode_err)}")

            alpha_stake = 0.0
            for hk in hotkeys:
                hk_str = hk[0] if isinstance(hk, (list, tuple)) else hk
                if not isinstance(hk_str, str): continue
                shares = decoded.get(("AlphaV2", hk_str), 0.0)
                tot_shares = decoded.get(("TotalSharesV2", hk_str), 0.0)
                tot_alpha = decoded.get(("TotalAlpha", hk_str), 0.0)
                if tot_shares > 0:
                    alpha_stake += (shares / tot_shares) * tot_alpha
            alpha_stake /= 1e9

            tao_pool = decoded.get(("SubnetTAO", None), 0.0)
            alpha_pool = decoded.get(("SubnetAlphaIn", None), 0.0)
            price = (tao_pool / alpha_pool) if alpha_pool > 0 else None
            equivalent_tao = (alpha_stake * price) if price is not None else None
            return free_tao, alpha_stake, equivalent_tao, price
        except Exception as e:
            if attempt == 0:
                with QUERY_IO_LOCK:
                    substrate.metadata = None
                    substrate.init_runtime()
                continue
            raise e

def _query_blockchain_data(address, netuid):
    substrate = get_query_substrate()
    is_temp = False
    if not substrate:
        targets = get_query_wss_targets()
        for url in targets:
            try:
                substrate = SubstrateInterface(url=url, ws_options={"timeout": 5})
                is_temp = True
                break
            except Exception as e:
                db.add_log("WARN", f"余额查询临时降级建连失败 ({url}): {str(e)}")
        if not substrate:
            db.add_log("ERROR", "余额查询所有候选接口临时降级建连全部失败。")
            raise RuntimeError("All connection attempts failed")
    try:
        # QUERY_IO_LOCK 会细粒度地在 _query_blockchain_data_with_substrate 内的 rpc 级别上上锁，此处无需大粒度上锁
        return _query_blockchain_data_with_substrate(substrate, address, netuid)
    finally:
        if is_temp and substrate:
            try: substrate.close()
            except: pass

def initialize_wallet_cache(address):
    db.add_log("INFO", f"后台开始为钱包初始化本地持仓缓存: {address}")
    try:
        substrate = get_query_substrate()
        is_temp = False
        if not substrate:
            targets = get_query_wss_targets()
            for url in targets:
                try:
                    substrate = SubstrateInterface(url=url, ws_options={"timeout": 5})
                    is_temp = True
                    break
                except Exception as e:
                    db.add_log("WARN", f"缓存初始化临时降级建连失败 ({url}): {str(e)}")
            if not substrate:
                db.add_log("ERROR", "缓存初始化所有候选接口临时降级建连全部失败。")
                return
        try:
            def get_storage_value_type(pallet, function):
                if not substrate.metadata:
                    return None
                try:
                    metadata_pallet = substrate.metadata.get_metadata_pallet(pallet)
                    if metadata_pallet:
                        storage_item = metadata_pallet.get_storage_function(function)
                        if storage_item: return storage_item.get_value_type_string()
                except Exception as e:
                    db.add_log("WARN", f"缓存初始化获取 {pallet}.{function} 类型失败: {str(e)}")
                return None

            initial_keys = []
            key_mapping = {}
            account_type = get_storage_value_type("System", "Account")
            if not account_type:
                with QUERY_IO_LOCK:
                    substrate.init_runtime()
                account_type = get_storage_value_type("System", "Account")
            account_key = StorageKey.create_from_storage_function("System", "Account", [address], runtime_config=substrate.runtime_config, metadata=substrate.metadata).to_hex()
            initial_keys.append(account_key); key_mapping[account_key] = ("free_tao", account_type)
            hotkeys_type = get_storage_value_type("SubtensorModule", "StakingHotkeys")
            if hotkeys_type:
                hotkeys_key = StorageKey.create_from_storage_function("SubtensorModule", "StakingHotkeys", [address], runtime_config=substrate.runtime_config, metadata=substrate.metadata).to_hex()
                initial_keys.append(hotkeys_key); key_mapping[hotkeys_key] = ("hotkeys", hotkeys_type)
            free_tao = 0.0; hotkeys = []
            if initial_keys:
                with QUERY_IO_LOCK:
                    response = substrate.rpc_request("state_queryStorageAt", [initial_keys])
                if isinstance(response, dict) and "result" in response: response = response["result"]
                if isinstance(response, list) and len(response) > 0:
                    for k_hex, v_hex in response[0].get("changes", []):
                        if v_hex and v_hex != "0x":
                            name, t_str = key_mapping.get(k_hex, (None, None))
                            if name and t_str:
                                try:
                                    obj = substrate.runtime_config.create_scale_object(t_str, ScaleBytes(v_hex), metadata=substrate.metadata)
                                    obj.decode(); val = obj.value
                                    if name == "free_tao" and isinstance(val, dict): free_tao = float(val.get("data", {}).get("free", 0)) / 1e9
                                    elif name == "hotkeys": hotkeys = val
                                except Exception as decode_err:
                                    db.add_log("ERROR", f"解析初始键 {name} 数据失败: {str(decode_err)}")
            if not isinstance(hotkeys, list) or len(hotkeys) == 0:
                db.update_wallet_cache(address, 1, free_tao, 0.0, 0.0, 0.0); return
            total_networks = 45
            total_networks_type = get_storage_value_type("SubtensorModule", "TotalNetworks")
            if total_networks_type:
                try:
                    k_total_nets = StorageKey.create_from_storage_function("SubtensorModule", "TotalNetworks", [], runtime_config=substrate.runtime_config, metadata=substrate.metadata).to_hex()
                    with QUERY_IO_LOCK:
                        res_total = substrate.rpc_request("state_getStorage", [k_total_nets])
                    if res_total and res_total.get("result"):
                        obj = substrate.runtime_config.create_scale_object(total_networks_type, ScaleBytes(res_total["result"]), metadata=substrate.metadata)
                        obj.decode(); total_networks = int(obj.value)
                except Exception as e:
                    db.add_log("WARN", f"查询子网总数失败，将默认使用 {total_networks} 个子网: {str(e)}")
            active_netuids = list(range(total_networks))
            alpha_v2_type = get_storage_value_type("SubtensorModule", "AlphaV2")
            if not alpha_v2_type:
                db.update_wallet_cache(address, 1, free_tao, 0.0, 0.0, 0.0)
                db.add_log("ERROR", f"未能在元数据中找到 AlphaV2 类型，无法初始化缓存。")
                return
            storage_keys = []
            key_mapping = {}
            for hk in hotkeys:
                hk_str = hk[0] if isinstance(hk, (list, tuple)) else hk
                if not isinstance(hk_str, str): continue
                for netuid in active_netuids:
                    try:
                        k_alphav2 = StorageKey.create_from_storage_function("SubtensorModule", "AlphaV2", [hk_str, address, int(netuid)], runtime_config=substrate.runtime_config, metadata=substrate.metadata).to_hex()
                        storage_keys.append(k_alphav2); key_mapping[k_alphav2] = (hk_str, int(netuid))
                    except Exception as e:
                        db.add_log("WARN", f"生成 AlphaV2 键失败 (hotkey: {hk_str}, netuid: {netuid}): {str(e)}")
            active_netuids_with_stake = set()
            if storage_keys:
                chunk_size = 100
                for i in range(0, len(storage_keys), chunk_size):
                    chunk = storage_keys[i:i + chunk_size]
                    with QUERY_IO_LOCK:
                        res = substrate.rpc_request("state_queryStorageAt", [chunk])
                    if isinstance(res, dict) and "result" in res: res = res["result"]
                    if isinstance(res, list) and len(res) > 0:
                        for k_hex, v_hex in res[0].get("changes", []):
                            if v_hex and v_hex != "0x":
                                hk_str, netuid = key_mapping.get(k_hex, (None, None))
                                if hk_str is not None and netuid is not None:
                                    try:
                                        obj = substrate.runtime_config.create_scale_object(alpha_v2_type, ScaleBytes(v_hex), metadata=substrate.metadata)
                                        obj.decode(); val = extract_numeric_value(obj)
                                        if val > 0: active_netuids_with_stake.add(netuid)
                                    except Exception as decode_err:
                                        db.add_log("ERROR", f"解析 AlphaV2 数据失败 (hotkey: {hk_str}, netuid: {netuid}): {str(decode_err)}")
            if not active_netuids_with_stake:
                db.update_wallet_cache(address, 1, free_tao, 0.0, 0.0, 0.0); return
            batch_keys = []
            batch_mapping = {}
            total_shares_v2_type = get_storage_value_type("SubtensorModule", "TotalHotkeySharesV2")
            total_alpha_type = get_storage_value_type("SubtensorModule", "TotalHotkeyAlpha")
            tao_pool_type = get_storage_value_type("SubtensorModule", "SubnetTAO")
            alpha_pool_type = get_storage_value_type("SubtensorModule", "SubnetAlphaIn")
            for netuid in active_netuids_with_stake:
                if tao_pool_type:
                    try:
                        k = StorageKey.create_from_storage_function("SubtensorModule", "SubnetTAO", [int(netuid)], runtime_config=substrate.runtime_config, metadata=substrate.metadata).to_hex()
                        batch_keys.append(k); batch_mapping[k] = (netuid, "SubnetTAO", tao_pool_type, None)
                    except Exception as e:
                        db.add_log("WARN", f"生成 SubnetTAO 键失败 (netuid: {netuid}): {str(e)}")
                if alpha_pool_type:
                    try:
                        k = StorageKey.create_from_storage_function("SubtensorModule", "SubnetAlphaIn", [int(netuid)], runtime_config=substrate.runtime_config, metadata=substrate.metadata).to_hex()
                        batch_keys.append(k); batch_mapping[k] = (netuid, "SubnetAlphaIn", alpha_pool_type, None)
                    except Exception as e:
                        db.add_log("WARN", f"生成 SubnetAlphaIn 键失败 (netuid: {netuid}): {str(e)}")
                for hk in hotkeys:
                    hk_str = hk[0] if isinstance(hk, (list, tuple)) else hk
                    if not isinstance(hk_str, str): continue
                    if alpha_v2_type:
                        try:
                            k = StorageKey.create_from_storage_function("SubtensorModule", "AlphaV2", [hk_str, address, int(netuid)], runtime_config=substrate.runtime_config, metadata=substrate.metadata).to_hex()
                            batch_keys.append(k); batch_mapping[k] = (netuid, "AlphaV2", alpha_v2_type, hk_str)
                        except Exception as e:
                            db.add_log("WARN", f"生成 AlphaV2 键失败 (hotkey: {hk_str}, netuid: {netuid}): {str(e)}")
                    if total_shares_v2_type:
                        try:
                            k = StorageKey.create_from_storage_function("SubtensorModule", "TotalHotkeySharesV2", [hk_str, int(netuid)], runtime_config=substrate.runtime_config, metadata=substrate.metadata).to_hex()
                            batch_keys.append(k); batch_mapping[k] = (netuid, "TotalSharesV2", total_shares_v2_type, hk_str)
                        except Exception as e:
                            db.add_log("WARN", f"生成 TotalHotkeySharesV2 键失败 (hotkey: {hk_str}, netuid: {netuid}): {str(e)}")
                    if total_alpha_type:
                        try:
                            k = StorageKey.create_from_storage_function("SubtensorModule", "TotalHotkeyAlpha", [hk_str, int(netuid)], runtime_config=substrate.runtime_config, metadata=substrate.metadata).to_hex()
                            batch_keys.append(k); batch_mapping[k] = (netuid, "TotalAlpha", total_alpha_type, hk_str)
                        except Exception as e:
                            db.add_log("WARN", f"生成 TotalHotkeyAlpha 键失败 (hotkey: {hk_str}, netuid: {netuid}): {str(e)}")
            decoded_values = {}
            if batch_keys:
                for i in range(0, len(batch_keys), 100):
                    chunk = batch_keys[i:i + 100]
                    with QUERY_IO_LOCK:
                        res = substrate.rpc_request("state_queryStorageAt", [chunk])
                    if isinstance(res, dict) and "result" in res: res = res["result"]
                    if isinstance(res, list) and len(res) > 0:
                        for k_hex, v_hex in res[0].get("changes", []):
                            if v_hex and v_hex != "0x":
                                netuid, name, t_str, hk_str = batch_mapping.get(k_hex, (None, None, None, None))
                                if netuid is not None:
                                    try:
                                        obj = substrate.runtime_config.create_scale_object(t_str, ScaleBytes(v_hex), metadata=substrate.metadata)
                                        obj.decode(); decoded_values[(netuid, name, hk_str)] = extract_numeric_value(obj)
                                    except Exception as decode_err:
                                        db.add_log("ERROR", f"解析子网 {netuid} 的 {name} 数据失败 (hotkey: {hk_str}, type: {t_str}): {str(decode_err)}")
            for netuid in active_netuids_with_stake:
                tao_pool = decoded_values.get((netuid, "SubnetTAO", None), 0.0)
                alpha_pool = decoded_values.get((netuid, "SubnetAlphaIn", None), 0.0)
                alpha_stake = 0.0
                for hk in hotkeys:
                    hk_str = hk[0] if isinstance(hk, (list, tuple)) else hk
                    if not isinstance(hk_str, str): continue
                    val_v2 = decoded_values.get((netuid, "AlphaV2", hk_str), 0.0)
                    tot_shares = decoded_values.get((netuid, "TotalSharesV2", hk_str), 0.0)
                    tot_alpha = decoded_values.get((netuid, "TotalAlpha", hk_str), 0.0)
                    if tot_shares > 0: alpha_stake += (val_v2 / tot_shares) * tot_alpha
                alpha_stake /= 1e9
                price = (tao_pool / alpha_pool) if alpha_pool > 0 else None
                db.update_wallet_cache(address, netuid, free_tao, alpha_stake, (alpha_stake * price) if price else None, price)
        finally:
            if is_temp and substrate:
                try: substrate.close()
                except: pass
    except Exception as e:
        db.add_log("ERROR", f"后台初始化钱包 {address} 缓存总逻辑失败: {str(e)}")

def format_balance_info(netuid, free_tao, alpha_stake, equivalent_tao, price):
    balance_info = (
        f"\n\n💰 <b>当前钱包仓位</b>\n"
        f"剩余可用: <code>{free_tao:.4f} T</code>\n"
    )
    if equivalent_tao is not None:
        balance_info += f"SN{netuid} 总 Alpha: <code>{alpha_stake:.4f}</code> ≈ <code>{equivalent_tao:.4f} T</code>"
    else:
        balance_info += f"SN{netuid} 总 Alpha: <code>{alpha_stake:.4f}</code>"
    return balance_info
