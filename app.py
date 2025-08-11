# app.py
import os
from fastapi import FastAPI, HTTPException, Depends, Query
from pydantic import BaseModel, Field
from typing import List, Optional

from config import *
from token_client import TokenClient
from ps_client import TokenIndexer

app = FastAPI(title="ERC20 helper (Polygon)")

cli = TokenClient(RPC_URL, TOKEN_ADDRESS, ERC20_ABI)

def get_indexer():
    idx = TokenIndexer(cli, DB_PATH)
    try:
        yield idx
    finally:
        idx.close()

def bool_arg(v: Optional[str], default: bool) -> bool:
    if v is None:
        return default
    return str(v).lower() in ("1", "true", "yes", "y", "on")


class BalanceBatchBody(BaseModel):
    addresses: List[str] = Field(..., min_items=1)
    human: Optional[bool] = False


class BootstrapBody(BaseModel):
    api_key: Optional[str] = None
    start: Optional[int] = START_BLOCK
    offset: Optional[int] = 2000
    sleep: Optional[float] = 0.25


class IndexBody(BaseModel):
    start: Optional[int] = None
    batch: Optional[int] = BATCH_SIZE
    conf: Optional[int] = CONFIRMATIONS


@app.get("/health")
def health():
    try:
        _ = cli.get_token_info()
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# GET /get_balance?address=0x...&human=1
@app.get("/get_balance")
def get_balance(
    address: str = Query(..., description="0x-адрес"),
    human: Optional[str] = Query(None, description="1/true — формат с символом токена"),):
    try:
        val = cli.get_balance(address, bool_arg(human, True))
        return {"balance": val}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# POST /get_balance_batch  {"addresses":[...], "human":false}
@app.post("/get_balance_batch")
def get_balance_batch(body: BalanceBatchBody):
    try:
        if body.human:
            out = [cli.get_balance(a, True) for a in body.addresses]
        else:
            out = cli.get_balance_batch(body.addresses)
        return {"balances": out}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/get_token_info")
def get_token_info():
    try:
        return cli.get_token_info()
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# POST /bootstrap  {"api_key":"...", "start": 42812490, "offset":2000, "sleep":0.25}
@app.post("/bootstrap")
def bootstrap(body: BootstrapBody, idx: TokenIndexer = Depends(get_indexer)):
    api_key = body.api_key or os.getenv("POLYGONSCAN_API_KEY") or POLYGONSCAN_API_KEY
    if not api_key:
        raise HTTPException(status_code=400, detail="api_key is required")
    try:
        idx.first_from_polygonscan(
            api_key=api_key,
            start_block=body.start if body.start is not None else START_BLOCK,
            sleep_s=body.sleep or 0.25,
            offset=body.offset or 2000,
        )
        last = idx._get_last_block()
        return {"ok": True, "last_scanned_block": last}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# POST /index {"start": null, "batch": 2000, "conf": 20}
@app.post("/index")
def index(body: IndexBody, idx: TokenIndexer = Depends(get_indexer)):
    try:
        idx.index_transfers(
            start_block=body.start,
            batch_size=body.batch or BATCH_SIZE,
            confirmations=body.conf or CONFIRMATIONS,
        )
        last = idx._get_last_block()
        return {"ok": True, "last_scanned_block": last}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# GET /get_top?n=10&human=1&update=rpc|scan&api_key=...
@app.get("/get_top")
def get_top(
    n: int = Query(10, ge=1),
    update: str = Query("rpc", pattern="^(rpc|scan)$"),
    api_key: Optional[str] = Query(None),
    idx: TokenIndexer = Depends(get_indexer),
):
    try:
        if update == "scan":
            key = api_key or os.getenv("POLYGONSCAN_API_KEY") or POLYGONSCAN_API_KEY
            if not key:
                raise HTTPException(status_code=400, detail="api_key required for update=scan")
            rows = idx.get_top(n, api_key=key, parse_type="scan")
        else:
            rows = idx.get_top(n, parse_type="RPC")


        out = [{"address": a, "balance": b} for a, b in rows]

        return {"top": out}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# GET /get_top_with_transactions?n=10&human=1&update=rpc|scan&api_key=...
@app.get("/get_top_with_transactions")
def get_top_with_transactions(
    n: int = Query(10, ge=1),
    update: str = Query("rpc", pattern="^(rpc|scan)$"),
    api_key: Optional[str] = Query(None),
    idx: TokenIndexer = Depends(get_indexer),
):
    try:
        if update == "scan":
            key = api_key or os.getenv("POLYGONSCAN_API_KEY") or POLYGONSCAN_API_KEY
            if not key:
                raise HTTPException(status_code=400, detail="api_key required for update=scan")
            rows = idx.get_top_with_transactions(n, parse_type="scan", api_key=key)
        else:
            rows = idx.get_top_with_transactions(n, parse_type="RPC")

        dec, sym = cli.decimals, cli.symbol
        out = [{"address": a, "balance": b, "symbol": sym, "last_tx": ts}
               for a, b, ts in rows]

        return {"top": out}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8080"))
    uvicorn.run("app:app", host="127.0.0.1", port=port, reload=False)
