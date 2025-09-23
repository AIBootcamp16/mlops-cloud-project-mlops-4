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
        tp = _to_float(c.get("trade_price"))
        cr = _to_float(c.get("signed_change_rate"))
        vol = _to_float(c.get("acc_trade_volume"))

        if tp is None:
            if NONE_POLICY == "skip":
                skipped += 1
                log.debug(f"skip row (trade_price is None) market={c.get('market')}")
                continue
            # NONE_POLICY == "none": 그대로 None 저장

        row = {
            "event_timestamp": ts,
            "market": c.get("market"),
            "trade_price": tp,
            "change_rate": cr,
            "acc_trade_volume": vol,
        }
        rows.append(row)

    if skipped:
        log.info(f"ticker rows skipped due to None trade_price: {skipped}")

    return rows
