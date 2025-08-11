POLYGONSCAN_API_KEY = "ВАШ ETHERSCAN API КЛЮЧ"


RPC_URL = "https://polygon-rpc.com"
TOKEN_ADDRESS = "0x1a9b54a3075119f1546c52ca0940551a6ce5d2d0"
START_BLOCK = 42812490
ZERO = "0x0000000000000000000000000000000000000000"
BATCH_SIZE = 2000
CONFIRMATIONS = 20
POLYGON_CHAIN_ID = 137
DB_PATH = "state.db"


ERC20_ABI = [
    {"constant": True, "inputs": [{"name": "_owner","type":"address"}],
     "name":"balanceOf","outputs":[{"name":"balance","type":"uint256"}],
     "type":"function"},
    {"constant": True, "inputs": [], "name":"decimals",
     "outputs":[{"name":"","type":"uint8"}], "type":"function"},
    {"constant": True, "inputs": [], "name":"symbol",
     "outputs":[{"name":"","type":"string"}], "type":"function"},
    {"constant": True, "inputs": [], "name":"name",
     "outputs":[{"name":"","type":"string"}], "type":"function"},
    {"constant": True, "inputs": [], "name":"totalSupply",
     "outputs":[{"name":"","type":"uint256"}], "type":"function"},
    {
      "anonymous": False,
      "inputs": [
        {"indexed": True,  "name": "from",  "type": "address"},
        {"indexed": True,  "name": "to",    "type": "address"},
        {"indexed": False, "name": "value", "type": "uint256"}
      ],
      "name": "Transfer",
      "type": "event"
    }
]
