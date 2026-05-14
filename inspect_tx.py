from pprint import pprint
import sys

from substrateinterface import SubstrateInterface

from database import get_setting
from scanner import (
    decoded_value,
    event_attributes,
    event_name,
    event_record_extrinsic_index,
    normalize_wss_url,
    raw_value,
)


def parse_tx_ref(tx_ref):
    tx_ref = tx_ref.strip()
    if "-" not in tx_ref:
        raise ValueError("Expected format like 8119800-9 or 8119800-0009")
    block_number, extrinsic_index = tx_ref.split("-", 1)
    return int(block_number), int(extrinsic_index)


def main():
    if len(sys.argv) != 2:
        print("Usage: python3 inspect_tx.py 8119800-9")
        raise SystemExit(1)

    block_number, extrinsic_index = parse_tx_ref(sys.argv[1])
    url = normalize_wss_url(
        get_setting("dwellir_wss", "wss://api-bittensor-mainnet.n.dwellir.com")
    )

    substrate = SubstrateInterface(url=url)
    block_hash = substrate.get_block_hash(block_number)
    block = substrate.get_block(block_hash=block_hash)
    events = substrate.get_events(block_hash=block_hash)

    print("block:", block_number)
    print("block_hash:", block_hash)
    print("extrinsics:", len(block["extrinsics"]))

    extrinsic = block["extrinsics"][extrinsic_index]
    extrinsic_value = raw_value(extrinsic)

    print("\n--- signer ---")
    print(extrinsic_value.get("address"))

    print("\n--- call ---")
    pprint(extrinsic_value.get("call"))

    print("\n--- full extrinsic ---")
    pprint(extrinsic_value)

    print("\n--- events for this extrinsic ---")
    for i, event in enumerate(events):
        record = decoded_value(event)
        idx = event_record_extrinsic_index(record)
        if idx != extrinsic_index:
            continue

        decoded_event = record.get("event") if isinstance(record, dict) else None
        print("\nEVENT", i)
        print("name:", event_name(decoded_event))
        print("attrs:")
        pprint(event_attributes(decoded_event))
        print("full:")
        pprint(record)


if __name__ == "__main__":
    main()
