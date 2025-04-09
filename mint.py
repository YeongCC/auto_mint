import os
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from web3 import Web3
from decimal import Decimal

# 载入环境变量
load_dotenv()

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# PostgreSQL 配置
DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
}

# Blockchain配置（假设你有这些信息）
WEB3_PROVIDER_RPC = 'https://opbnb-testnet-rpc.bnbchain.org'  # 根据实际修改
web3 = Web3(Web3.HTTPProvider(WEB3_PROVIDER_RPC))

# 假设合约ABI和地址你已经有了
XP_TOKEN_CONTRACT_ADDRESS = '0xYourContractAddress'
XP_OWNER_PRIVATE_KEY = '0xYourPrivateKey'
XP_TOKEN_ABI = [{
    "inputs": [{"internalType": "address", "name": "to", "type": "address"}, {"internalType": "uint256", "name": "amount", "type": "uint256"}],
    "name": "mint",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
}]
contract = web3.eth.contract(address=XP_TOKEN_CONTRACT_ADDRESS, abi=XP_TOKEN_ABI)
owner_address = web3.eth.account.from_key(XP_OWNER_PRIVATE_KEY).address

# 数据库查询用户
def fetch_users():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("""
        SELECT up.id, up.xp_points, w.wallet_address, auth_user.username
        FROM api_userprofile up
        JOIN api_wallet w ON up.id = w.user_id
        JOIN auth_user ON up.user_id = auth_user.id
    """)
    users = cursor.fetchall()
    cursor.close()
    conn.close()
    return users

# Mint XP Token 的方法
def mint_xp(wallet_address: str, amount: float):
    nonce = web3.eth.get_transaction_count(owner_address)
    tx = contract.functions.mint(wallet_address, web3.to_wei(amount, "ether")).build_transaction({
        'chainId': web3.eth.chain_id,
        'gas': 200000,
        'gasPrice': web3.eth.gas_price,
        'nonce': nonce
    })
    signed_tx = web3.eth.account.sign_transaction(tx, private_key=XP_OWNER_PRIVATE_KEY)
    tx_hash = web3.eth.send_raw_transaction(signed_tx.rawTransaction)
    receipt = web3.eth.wait_for_transaction_receipt(tx_hash)
    return tx_hash.hex(), receipt.status

# 主程序逻辑
def main():
    users = fetch_users()
    for user in users:
        username = user['username']
        xp_points = user['xp_points']
        wallet_address = user['wallet_address']

        if xp_points <= 0:
            logging.info(f"⏩ Skipping {username}, no XP to mint.")
            continue

        try:
            tx_hash, status = mint_xp(wallet_address, float(xp_points))
            if status == 1:
                logging.info(f"✅ Successfully minted {xp_points} XP for {username}. TX: {tx_hash}")
            else:
                logging.warning(f"⚠️ Mint transaction failed for {username}. TX: {tx_hash}")

        except Exception as e:
            logging.error(f"🔥 Error minting XP for {username}: {e}")

# 启动脚本
if __name__ == '__main__':
    main()
