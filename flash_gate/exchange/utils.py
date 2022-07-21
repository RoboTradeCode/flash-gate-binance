from typing import Optional


def filter_dict(mapping: dict, keys: list[str]) -> dict:
    return {key: mapping.get(key) for key in keys}


def get_timestamp_in_us(ccxt_structure: dict) -> Optional[int]:
    if timestamp_in_ms := ccxt_structure.get("timestamp"):
        return timestamp_in_ms * 1000
