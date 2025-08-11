from decimal import Decimal
from web3.middleware.proof_of_authority import ExtraDataToPOAMiddleware
from web3 import Web3

class TokenClient:
    def __init__(self, url, token_address, abi):
        self.w3 = Web3(Web3.HTTPProvider(url))
        self.w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

        if not self.w3.is_connected():
            raise ConnectionError("Ошибка доступа к RPC")

        self.contract = self.w3.eth.contract(
            address=self.w3.to_checksum_address(token_address),
            abi=abi
        )
        self.address = self._to_checksum(token_address)
        self.decimals = self.contract.functions.decimals().call()
        self.symbol = self.contract.functions.symbol().call()

    def _to_checksum(self, addr):
        try:
            return self.w3.to_checksum_address(addr)
        except Exception:
            raise ValueError(f"Некорректный адрес: {addr}")

    def get_balance(self, address, with_token):
        raw = self.contract.functions.balanceOf(
            self.w3.to_checksum_address(address)
        ).call()
        if not with_token:
            return raw
        human = Decimal(raw) / Decimal(10 ** self.decimals)
        return f"{human.normalize()} {self.symbol}"

    def get_balance_batch(self, addresses):
        return [self.get_balance(a, False) for a in addresses]

    def get_token_info(self):
        info = {
            "address": self.address,
            "symbol": self.symbol,
            "decimals": self.decimals,
        }
        try:
            total_supply = int(self.contract.functions.totalSupply().call())
            info["totalSupply_raw"] = str(total_supply)
            info["totalSupply_human"] = str(Decimal(total_supply) / Decimal(10 ** self.decimals))
        except Exception:
            pass
        try:
            name = self.contract.functions.name().call()
            info["name"] = name
        except Exception:
            pass
        return info