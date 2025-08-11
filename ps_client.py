from web3 import Web3
import sqlite3
from config import *
import time
from datetime import datetime, timezone
import requests
from web3.exceptions import Web3RPCError


class TokenIndexer:
    def __init__(self, client, db_path):
        self.client = client
        self.w3 = client.w3
        self.token = client.contract
        self.token_addr = client.address
        self.decimals = client.decimals
        self.symbol = client.symbol

        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self._create_tables()

        self.transfer_event = self.token.events.Transfer()
        self.transfer_sig = self.w3.keccak(text="Transfer(address,address,uint256)")
        self._block_ts_cache = {}


    def _create_tables(self):
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS holders (
                address TEXT PRIMARY KEY,
                balance TEXT NOT NULL,
                last_tx_block INTEGER,
                last_tx_ts INTEGER
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS events (
                event_id TEXT PRIMARY KEY,   -- f"{txHash}:{logIndex}"
                block_number INTEGER,
                tx_hash TEXT,
                log_index INTEGER,
                ts INTEGER
            );
        """)
        self.conn.commit()

    def _get_last_block(self):
        row = self.conn.execute("SELECT value FROM meta WHERE key='last_scanned_block'").fetchone()
        return int(row[0]) if row else None

    def _set_last_block(self, block):
        self.conn.execute(
            "INSERT INTO meta(key,value) VALUES('last_scanned_block', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (str(block),)
        )
        self.conn.commit()


    def _block_time(self, block_number):
        ts = self._block_ts_cache.get(block_number)
        if ts is not None:
            return ts
        blk = self.w3.eth.get_block(block_number)
        ts = int(blk.timestamp)
        self._block_ts_cache[block_number] = ts
        return ts

    def _apply_transfer(self, from_addr, to_addr, value_raw, block_number, ts, tx_hash, log_index):
        ev_id = f"{tx_hash}:{log_index}"
        cur = self.conn.cursor()

        if cur.execute("SELECT 1 FROM events WHERE event_id=?", (ev_id,)).fetchone():
            return

        def add(addr, delta):
            if addr.lower() == ZERO.lower():
                return
            row = cur.execute("SELECT balance FROM holders WHERE address=?", (addr,)).fetchone()
            old = int(row[0]) if row else 0
            new = old + delta
            if new < 0:
                new = 0
            if row:
                cur.execute(
                    "UPDATE holders SET balance=?, last_tx_block=?, last_tx_ts=? WHERE address=?",
                    (str(new), block_number, ts, addr)
                )
            else:
                cur.execute(
                    "INSERT INTO holders(address, balance, last_tx_block, last_tx_ts) VALUES (?, ?, ?, ?)",
                    (addr, str(new), block_number, ts)
                )

        add(Web3.to_checksum_address(from_addr), -int(value_raw))
        add(Web3.to_checksum_address(to_addr),   int(value_raw))

        cur.execute(
            "INSERT INTO events(event_id, block_number, tx_hash, log_index, ts) VALUES (?, ?, ?, ?, ?)",
            (ev_id, block_number, tx_hash, log_index, ts)
        )


    def first_from_polygonscan(self, api_key, start_block = START_BLOCK, sleep_s=0.25, offset=2000):
        assert 1 <= offset <= 2000, "ставим offset <= 2000, чтобы уложиться в лимит page*offset<=10000"

        base_url = "https://api.etherscan.io/v2/api"
        chain_params = {"chainid": 137}
        head = self.w3.eth.block_number
        safe_head = max(0, head - CONFIRMATIONS)
        print(f"[bootstrap] token={self.token_addr} safe_head={safe_head}")

        sess = requests.Session()

        def _pick(d, keys, default=None):
            for k in keys:
                if k in d and d[k] not in (None, "", "null"):
                    return d[k]
            return default

        total = 0
        cur_start = max(0, int(start_block))

        while True:
            if cur_start > safe_head:
                break

            page = 1
            last_blk_in_window = cur_start
            while page <= 5:  # 5 * 2000 = 10000 → не пробиваем лимит окна
                params = {
                    "module": "account",
                    "action": "tokentx",
                    "contractaddress": self.token_addr,
                    "startblock": cur_start,
                    "endblock": safe_head,
                    "sort": "asc",
                    "page": page,
                    "offset": offset,
                    "apikey": api_key,
                }
                params.update(chain_params)

                resp = sess.get(base_url, params=params, timeout=30)
                resp.raise_for_status()
                payload = resp.json()

                status = payload.get("status")
                message = payload.get("message")
                result = payload.get("result")

                if isinstance(result, dict):
                    rows = result.get("transactions") or result.get("events") or result.get("records") or []
                else:
                    rows = result if isinstance(result, list) else []

                if status == "0" and (message or "").lower().startswith("no"):
                    print(f"[bootstrap] window empty (page={page}), stop window")
                    break

                if status == "0" and "window" in (message or "").lower():
                    print(f"[bootstrap] window limit hit on page={page}, shift window")
                    break

                if status not in ("1", 1) and not rows:
                    print(f"[bootstrap] warn status={status} message={message}, rows={len(rows)}")

                if not rows:
                    print(f"[bootstrap] empty rows (page={page}), stop window")
                    break

                applied = 0
                for it in rows:
                    try:
                        blk = int(_pick(it, ["blockNumber", "block_number", "block_num"]))
                        if blk > safe_head:
                            continue
                        last_blk_in_window = max(last_blk_in_window, blk)

                        txh = _pick(it, ["hash", "tx_hash", "transactionHash"])
                        if txh and not str(txh).startswith("0x"):
                            txh = "0x" + str(txh).lower()

                        li_val = _pick(it, ["logIndex", "log_index", "logindex"])
                        if li_val is None:
                            li_val = _pick(it, ["transactionIndex", "transaction_index"], 0)
                        li = int(li_val)

                        from_a = _pick(it, ["from", "from_address"])
                        to_a = _pick(it, ["to", "to_address"])
                        val = int(_pick(it, ["value", "token_value", "amount", "raw_amount"], "0"))
                        ts = int(_pick(it, ["timeStamp", "timestamp", "block_timestamp"], "0"))

                        if not (txh and from_a and to_a):
                            continue

                        self._apply_transfer(from_a, to_a, val, blk, ts, txh, li)
                        applied += 1
                        total += 1
                    except Exception as e:
                        print(f"[bootstrap] skip item due to: {e}")

                self.conn.commit()
                print(f"[bootstrap] page={page} rows={len(rows)} applied={applied} total={total}")

                if len(rows) < offset:
                    break

                page += 1
                time.sleep(sleep_s)

            if last_blk_in_window >= cur_start:
                cur_start = last_blk_in_window + 1
            else:
                cur_start += 1

        self._set_last_block(safe_head)

    def index_transfers(self, start_block=None, batch_size=BATCH_SIZE, confirmations=CONFIRMATIONS):
        head = self.w3.eth.block_number
        safe_head = max(0, head - confirmations)

        last = self._get_last_block()
        if last is None:
            if start_block is None:
                raise RuntimeError("БД пустая, укажи стартовый блок.")
            current = start_block
        else:
            current = last + 1

        if current > safe_head:
            print("[index] актуально: новых подтверждённых блоков нет")
            return

        print(f"[index] {current} → {safe_head} (batch={batch_size})")

        while current <= safe_head:
            try_span = min(batch_size, safe_head - current + 1)

            while True:
                to_block = current + try_span - 1
                try:
                    logs = self.w3.eth.get_logs({
                        "fromBlock": current,
                        "toBlock": to_block,
                        "address": self.token_addr,
                        "topics": [self.transfer_sig],
                    })
                    break
                except Exception as e:
                    msg = str(e).lower()
                    if ("range is too large" in msg or
                            "block range" in msg or
                            "timeout" in msg or
                            "limit" in msg or
                            isinstance(e, Web3RPCError) and getattr(e, "code", None) in (-32062, -32005)):
                        if try_span <= 1:
                            raise
                        try_span = max(1, try_span // 2)
                        time.sleep(0.1)
                        continue
                    else:
                        raise

            if logs:
                for lg in logs:
                    ev = self.transfer_event.process_log(lg)
                    _from = Web3.to_checksum_address(ev["args"]["from"])
                    _to = Web3.to_checksum_address(ev["args"]["to"])
                    _val = int(ev["args"]["value"])


                    blk = int(lg["blockNumber"])
                    ts = self._block_time(blk)
                    txh = Web3.to_hex(lg["transactionHash"])
                    li = int(lg["logIndex"])

                    self._apply_transfer(_from, _to, _val, blk, ts, txh, li)

                self.conn.commit()

            self._set_last_block(to_block)
            pct = 100.0 * (to_block - current + 1) / max(1, (safe_head - current + 1))
            print(f"⬆ [{current}..{to_block}] готово {pct:.1f}%")
            current = to_block + 1

        print(f"[index] last_scanned_block={safe_head}")


    def get_top(self, n, api_key=None, parse_type = 'RPC'):
        last = self._get_last_block()
        if parse_type == 'RPC':
            self.index_transfers()
        elif parse_type == 'scan':
            if last is None:
                raise RuntimeError(
                    "Необходимо сделать первичный вызов bootstrap или rpc вызов")
            if not api_key:
                raise RuntimeError("нужен api_key")
            self.first_from_polygonscan(api_key=api_key, start_block=last + 1)

        rows = self.conn.execute("""
            SELECT address, balance
            FROM (
                SELECT address, balance
                FROM holders
                WHERE balance != '0'
                ORDER BY LENGTH(balance) DESC, balance DESC
                LIMIT ?
            ) t
            ORDER BY LENGTH(balance) ASC, balance ASC;
        """, (n,)).fetchall()
        return [(addr, int(bal) / float(10 ** self.decimals)) for addr, bal in rows]


    def get_top_with_transactions(self, n, parse_type='RPC',  api_key=None):
        last = self._get_last_block()
        if parse_type == 'RPC':
            self.index_transfers(start_block=last)
        elif parse_type == 'scan':
            if last is None:
                raise RuntimeError(
                    "Необходимо сделать первичный вызов bootstrap или rpc вызов")
            if not api_key:
                raise RuntimeError("нужен api_key")
            self.first_from_polygonscan(api_key=api_key, start_block=last + 1)
        rows = self.conn.execute("""
            SELECT address, balance, last_tx_ts
            FROM (
                SELECT address, balance, last_tx_ts
                FROM holders
                WHERE balance != '0'
                ORDER BY LENGTH(balance) DESC, balance DESC
                LIMIT ?
            ) t
            ORDER BY LENGTH(balance) ASC, balance ASC;
        """, (n,)).fetchall()
        out = []
        for addr, bal, ts in rows:
            ts_iso = datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
            out.append((addr, int(bal) / float(10 ** self.decimals), ts_iso))
        return out

    def close(self):
        self.conn.close()

