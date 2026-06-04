import threading
import time
from substrateinterface import SubstrateInterface
from substrateinterface.storage import StorageKey
from scalecodec.base import ScaleBytes
import database as db

QUERY_SUBSTRATE = None
QUERY_SUBSTRATE_LOCK = threading.Lock()
QUERY_IO_LOCK = threading.Lock()

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

def get_query_substrate(dwellir_wss):
    global QUERY_SUBSTRATE
    if QUERY_SUBSTRATE is None:
        with QUERY_SUBSTRATE_LOCK:
            if QUERY_SUBSTRATE is None:
                try:
                    db.add_log("INFO", f"正在初始化独立的常驻余额查询 WSS 连接: {dwellir_wss}")
                    new_sub = SubstrateInterface(url=dwellir_wss, ws_options={"timeout": 5})
                    new_sub.get_chain_head()
                    QUERY_SUBSTRATE = new_sub
                    db.add_log("INFO", "独立的常驻余额查询 WSS 连接就绪。")
                except Exception as e:
                    db.add_log("ERROR", f"初始化独立的常驻余额查询 WSS 连接失败: {str(e)}")
                    return None
                    
    if QUERY_SUBSTRATE:
        try:
            if not hasattr(QUERY_SUBSTRATE, "websocket") or not QUERY_SUBSTRATE.websocket or not QUERY_SUBSTRATE.websocket.connected:
                raise Exception("Websocket connection is disconnected")
        except Exception:
            with QUERY_SUBSTRATE_LOCK:
                db.add_log("INFO", "检测到常驻查询连接断开，正在尝试重建...")
                try:
                    try:
                        QUERY_SUBSTRATE.close()
                    except Exception:
                        pass
                    QUERY_SUBSTRATE = None
                    new_sub = SubstrateInterface(url=dwellir_wss, ws_options={"timeout": 5})
                    new_sub.get_chain_head()
                    QUERY_SUBSTRATE = new_sub
                    db.add_log("INFO", "重建独立的常驻余额查询 WSS 连接成功。")
                except Exception as e:
                    db.add_log("ERROR", f"重建独立的常驻余额查询 WSS 连接失败: {str(e)}")
                    QUERY_SUBSTRATE = None
                    
    return QUERY_SUBSTRATE

def _query_blockchain_data(dwellir_wss, address, netuid):
    substrate = get_query_substrate(dwellir_wss)
    is_temp = False
    
    if not substrate:
        try:
            substrate = SubstrateInterface(url=dwellir_wss, ws_options={"timeout": 5})
            is_temp = True
        except Exception as e:
            db.add_log("ERROR", f"余额查询临时降级建连失败: {str(e)}")
            raise e
            
    try:
        def get_storage_value_type(pallet, function):
            try:
                metadata_pallet = substrate.metadata.get_metadata_pallet(pallet)
                if metadata_pallet:
                    storage_item = metadata_pallet.get_storage_function(function)
                    if storage_item:
                        return storage_item.get_value_type_string()
            except Exception:
                pass
            return None

        # 出现类型解码异常时，attempt=0 会触发 init_runtime 并自动重试自愈
        for attempt in range(2):
            try:
                with QUERY_IO_LOCK:
                    # A. 查询可用 TAO 余额 (System.Account)
                    account_type = get_storage_value_type("System", "Account")
                    if not account_type:
                        substrate.init_runtime()
                        account_type = get_storage_value_type("System", "Account")
                        
                    account_key = StorageKey.create_from_storage_function(
                        "System", "Account", [address],
                        runtime_config=substrate.runtime_config,
                        metadata=substrate.metadata
                    ).to_hex()
                    
                    res_account = substrate.rpc_request("state_getStorage", [account_key])
                    free_tao = 0.0
                    if res_account and res_account.get("result") and account_type:
                        scale_bytes = ScaleBytes(res_account["result"])
                        obj = substrate.runtime_config.create_scale_object(
                            type_string=account_type,
                            data=scale_bytes,
                            metadata=substrate.metadata
                        )
                        obj.decode()
                        val_dict = obj.value
                        if isinstance(val_dict, dict):
                            free_tao = float(val_dict.get("data", {}).get("free", 0)) / 1e9
                    
                    # B. 查询 StakingHotkeys
                    hotkeys_type = get_storage_value_type("SubtensorModule", "StakingHotkeys")
                    hotkeys_key = StorageKey.create_from_storage_function(
                        "SubtensorModule", "StakingHotkeys", [address],
                        runtime_config=substrate.runtime_config,
                        metadata=substrate.metadata
                    ).to_hex()
                    
                    res_hotkeys = substrate.rpc_request("state_getStorage", [hotkeys_key])
                    hotkeys = []
                    if res_hotkeys and res_hotkeys.get("result") and hotkeys_type:
                        scale_bytes = ScaleBytes(res_hotkeys["result"])
                        obj = substrate.runtime_config.create_scale_object(
                            type_string=hotkeys_type,
                            data=scale_bytes,
                            metadata=substrate.metadata
                        )
                        obj.decode()
                        hotkeys = obj.value

                    # C. 聚合当前冷钱包在此子网上的 Alpha 质押余额
                    alpha_stake = 0.0
                    if isinstance(hotkeys, list) and len(hotkeys) > 0:
                        storage_keys = []
                        key_mapping = {}
                        
                        alpha_type = get_storage_value_type("SubtensorModule", "Alpha")
                        alpha_v2_type = get_storage_value_type("SubtensorModule", "AlphaV2")
                        stake_type = get_storage_value_type("SubtensorModule", "Stake")
                        total_shares_type = get_storage_value_type("SubtensorModule", "TotalHotkeyShares")
                        total_shares_v2_type = get_storage_value_type("SubtensorModule", "TotalHotkeySharesV2")
                        total_alpha_type = get_storage_value_type("SubtensorModule", "TotalHotkeyAlpha")
                        
                        for hk in hotkeys:
                            hk_str = hk[0] if isinstance(hk, (list, tuple)) else hk
                            if not isinstance(hk_str, str):
                                continue
                            
                            if alpha_type:
                                try:
                                    k_alpha = StorageKey.create_from_storage_function(
                                        "SubtensorModule", "Alpha", [hk_str, address, int(netuid)],
                                        runtime_config=substrate.runtime_config,
                                        metadata=substrate.metadata
                                    ).to_hex()
                                    storage_keys.append(k_alpha)
                                    key_mapping[k_alpha] = (hk_str, "Alpha", alpha_type)
                                except Exception:
                                    pass
                                    
                            if alpha_v2_type:
                                try:
                                    k_alphav2 = StorageKey.create_from_storage_function(
                                        "SubtensorModule", "AlphaV2", [hk_str, address, int(netuid)],
                                        runtime_config=substrate.runtime_config,
                                        metadata=substrate.metadata
                                    ).to_hex()
                                    storage_keys.append(k_alphav2)
                                    key_mapping[k_alphav2] = (hk_str, "AlphaV2", alpha_v2_type)
                                except Exception:
                                    pass

                            if total_shares_type:
                                try:
                                    k_shares = StorageKey.create_from_storage_function(
                                        "SubtensorModule", "TotalHotkeyShares", [hk_str, int(netuid)],
                                        runtime_config=substrate.runtime_config,
                                        metadata=substrate.metadata
                                    ).to_hex()
                                    storage_keys.append(k_shares)
                                    key_mapping[k_shares] = (hk_str, "TotalShares", total_shares_type)
                                except Exception:
                                    pass

                            if total_shares_v2_type:
                                try:
                                    k_shares_v2 = StorageKey.create_from_storage_function(
                                        "SubtensorModule", "TotalHotkeySharesV2", [hk_str, int(netuid)],
                                        runtime_config=substrate.runtime_config,
                                        metadata=substrate.metadata
                                    ).to_hex()
                                    storage_keys.append(k_shares_v2)
                                    key_mapping[k_shares_v2] = (hk_str, "TotalSharesV2", total_shares_v2_type)
                                except Exception:
                                    pass

                            if total_alpha_type:
                                try:
                                    k_tot_alpha = StorageKey.create_from_storage_function(
                                        "SubtensorModule", "TotalHotkeyAlpha", [hk_str, int(netuid)],
                                        runtime_config=substrate.runtime_config,
                                        metadata=substrate.metadata
                                    ).to_hex()
                                    storage_keys.append(k_tot_alpha)
                                    key_mapping[k_tot_alpha] = (hk_str, "TotalAlpha", total_alpha_type)
                                except Exception:
                                    pass
                                    
                            if stake_type:
                                try:
                                    k_stake = StorageKey.create_from_storage_function(
                                        "SubtensorModule", "Stake", [hk_str, address],
                                        runtime_config=substrate.runtime_config,
                                        metadata=substrate.metadata
                                    ).to_hex()
                                    storage_keys.append(k_stake)
                                    key_mapping[k_stake] = (hk_str, "Stake", stake_type)
                                except Exception:
                                    pass
                        
                        chunk_size = 20
                        chunks = [storage_keys[i:i + chunk_size] for i in range(0, len(storage_keys), chunk_size)]
                        
                        hotkey_alpha_v2 = {}
                        hotkey_alpha = {}
                        hotkey_stake = {}
                        hotkey_total_shares = {}
                        hotkey_total_shares_v2 = {}
                        hotkey_total_alpha = {}

                        for chunk in chunks:
                            response = substrate.rpc_request("state_queryStorageAt", [chunk])
                            if isinstance(response, dict) and "result" in response:
                                response = response["result"]
                            if isinstance(response, list) and len(response) > 0:
                                changes = response[0].get("changes", [])
                                for k_hex, v_hex in changes:
                                    if v_hex and v_hex != "0x":
                                        hk_str, storage_name, t_str = key_mapping.get(k_hex, (None, None, None))
                                        if hk_str and storage_name and t_str:
                                            val = 0.0
                                            try:
                                                scale_bytes = ScaleBytes(v_hex)
                                                obj = substrate.runtime_config.create_scale_object(
                                                    type_string=t_str,
                                                    data=scale_bytes,
                                                    metadata=substrate.metadata
                                                )
                                                obj.decode()
                                                val = extract_numeric_value(obj)
                                            except Exception:
                                                pass
                                            
                                            if storage_name == "AlphaV2":
                                                hotkey_alpha_v2[hk_str] = val
                                            elif storage_name == "Alpha":
                                                hotkey_alpha[hk_str] = val
                                            elif storage_name == "Stake":
                                                hotkey_stake[hk_str] = val
                                            elif storage_name == "TotalShares":
                                                hotkey_total_shares[hk_str] = val
                                            elif storage_name == "TotalSharesV2":
                                                hotkey_total_shares_v2[hk_str] = val
                                            elif storage_name == "TotalAlpha":
                                                hotkey_total_alpha[hk_str] = val

                        # 份额折算公式
                        for hk in hotkeys:
                            hk_str = hk[0] if isinstance(hk, (list, tuple)) else hk
                            if not isinstance(hk_str, str):
                                continue
                            
                            val_v2_shares = hotkey_alpha_v2.get(hk_str, 0.0)
                            val_v1_shares = hotkey_alpha.get(hk_str, 0.0)
                            val_stake = hotkey_stake.get(hk_str, 0.0)
                            
                            tot_shares_v2 = hotkey_total_shares_v2.get(hk_str, 0.0)
                            tot_shares_v1 = hotkey_total_shares.get(hk_str, 0.0)
                            tot_alpha = hotkey_total_alpha.get(hk_str, 0.0)
                            
                            val_v2 = 0.0
                            if tot_shares_v2 > 0:
                                val_v2 = (val_v2_shares / tot_shares_v2) * tot_alpha
                                
                            val_v1 = 0.0
                            if tot_shares_v1 > 0:
                                val_v1 = (val_v1_shares / tot_shares_v1) * tot_alpha
                            
                            selected = val_v2 if val_v2 > 0 else val_v1
                            if selected <= 0:
                                selected = val_stake
                            
                            alpha_stake += selected / 1e9

                    # D. 估算折算 TAO 金额
                    equivalent_tao = None
                    price = None
                    tao_pool_type = get_storage_value_type("SubtensorModule", "SubnetTAO")
                    alpha_pool_type = get_storage_value_type("SubtensorModule", "SubnetAlphaIn")
                    
                    key_tao_pool = StorageKey.create_from_storage_function(
                        "SubtensorModule", "SubnetTAO", [int(netuid)],
                        runtime_config=substrate.runtime_config,
                        metadata=substrate.metadata
                    ).to_hex()
                    
                    key_alpha_pool = StorageKey.create_from_storage_function(
                        "SubtensorModule", "SubnetAlphaIn", [int(netuid)],
                        runtime_config=substrate.runtime_config,
                        metadata=substrate.metadata
                    ).to_hex()
                    
                    response = substrate.rpc_request("state_queryStorageAt", [[key_tao_pool, key_alpha_pool]])
                    if isinstance(response, dict) and "result" in response:
                        response = response["result"]
                    
                    tao_pool = 0.0
                    alpha_pool = 0.0
                    if isinstance(response, list) and len(response) > 0:
                        changes = response[0].get("changes", [])
                        for k_hex, v_hex in changes:
                            if v_hex and v_hex != "0x":
                                if k_hex == key_tao_pool and tao_pool_type:
                                    scale_bytes = ScaleBytes(v_hex)
                                    obj = substrate.runtime_config.create_scale_object(
                                        type_string=tao_pool_type,
                                        data=scale_bytes,
                                        metadata=substrate.metadata
                                    )
                                    obj.decode()
                                    tao_pool = float(obj.value)
                                elif k_hex == key_alpha_pool and alpha_pool_type:
                                    scale_bytes = ScaleBytes(v_hex)
                                    obj = substrate.runtime_config.create_scale_object(
                                        type_string=alpha_pool_type,
                                        data=scale_bytes,
                                        metadata=substrate.metadata
                                    )
                                    obj.decode()
                                    alpha_pool = float(obj.value)
                                    
                    if alpha_pool > 0:
                        price = tao_pool / alpha_pool
                        equivalent_tao = alpha_stake * price
                        
                    return free_tao, alpha_stake, equivalent_tao, price
            except Exception as query_err:
                if attempt == 0:
                    db.add_log("WARN", f"RPC 类型解析报错，尝试自愈刷新 metadata 重试: {str(query_err)}")
                    try:
                        substrate.metadata = None
                        substrate.init_runtime()
                        continue
                    except Exception:
                        pass
                raise query_err
    finally:
        if is_temp and substrate:
            try:
                substrate.close()
            except Exception:
                pass

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
