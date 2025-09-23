import logging
import requests
from datetime import datetime, timezone
from typing import Any, Optional, List

from config import MARKETS_LIMIT, NONE_POLICY

log = logging.getLogger(__name__)

UPBIT_MARKET_ALL = "https://api.upbit.com/v1/market/all"
UPBIT_TICKER = "https://api.upbit.com/v1/ticker?markets={markets}"


def now_utc():
    return datetime.now(timezone.utc)


def _to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        s = v.strip()
        if s == "":
            return None
        try:
            return float(s)
        except Exception:
            return None
    return None


def get_coin_list() -> List[str]:
    res = requests.get(UPBIT_MARKET_ALL, timeout=5)
    res.raise_for_status()
    data = res.json()
    markets = [c["market"] for c in data if c.get("market", "").startswith("KRW-")]
    return markets[: max(1, MARKETS_LIMIT)]


def get_ticker_rows(markets: List[str]) -> List[dict]:
    url = UPBIT_TICKER.format(markets=",".join(markets))
    res = requests.get(url, timeout=5)
    res.raise_for_status()
    payload = res.json()

    ts = now_utc()
    rows = []
    skipped = 0

    if not isinstance(payload, list):
        log.warning(f"Unexpected ticker payload: {payload}")
        return rows

    for c in payload:
        # 각 필드별 파싱 및 None 체크
        def safe_float(key):
            try:
                val = _to_float(c.get(key))
                if val is None and NONE_POLICY == "skip":
                    raise ValueError
                return val
            except Exception:
                if NONE_POLICY == "skip":
                    raise
                return None

        try:
            row = {
                "event_timestamp": ts,
                "market": c.get("market"),
                "trade_date": c.get("trade_date"),
                "trade_time": c.get("trade_time"),
                "trade_date_kst": c.get("trade_date_kst"),
                "trade_time_kst": c.get("trade_time_kst"),
                "trade_timestamp": c.get("trade_timestamp"),
                "opening_price": safe_float("opening_price"),
                "high_price": safe_float("high_price"),
                "low_price": safe_float("low_price"),
                "trade_price": safe_float("trade_price"),
                "prev_closing_price": safe_float("prev_closing_price"),
                "change": c.get("change"),
                "change_price": safe_float("change_price"),
                "change_rate": safe_float("change_rate"),
                "signed_change_price": safe_float("signed_change_price"),
                "signed_change_rate": safe_float("signed_change_rate"),
                "trade_volume": safe_float("trade_volume"),
                "acc_trade_price": safe_float("acc_trade_price"),
                "acc_trade_price_24h": safe_float("acc_trade_price_24h"),
                "acc_trade_volume": safe_float("acc_trade_volume"),
                "acc_trade_volume_24h": safe_float("acc_trade_volume_24h"),
                "highest_52_week_price": safe_float("highest_52_week_price"),
                "highest_52_week_date": c.get("highest_52_week_date"),
                "lowest_52_week_price": safe_float("lowest_52_week_price"),
                "lowest_52_week_date": c.get("lowest_52_week_date"),
                "timestamp": c.get("timestamp"),
            }
        except Exception:
            skipped += 1
            log.debug(
                f"skip row (field missing or parse error) market={c.get('market')}"
            )
            continue

        rows.append(row)

    if skipped:
        log.info(f"ticker rows skipped due to None trade_price: {skipped}")

    return rows
