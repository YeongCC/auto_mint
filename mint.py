import os
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from web3 import Web3
from decimal import Decimal
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "123123"
POSTGRES_DB = "maigabt"
POSTGRES_HOST = "localhost"
POSTGRES_PORT = "5432"

WEB3_RPC = "https://opbnb-testnet-rpc.bnbchain.org"
XP_TOKEN_CONTRACT_ADDRESS = "0x183738640c37341fdf9b27902473cfd85d853a93"
XP_OWNER_PRIVATE_KEY = "0x7525908b9b8b5e16e64f0ed67c903e87bd6850dbe0c186db5340d47a4a32d49a"
CHAIN_ID = 5611

XP_TOKEN_ABI = [
    {
        "inputs": [
            {"internalType": "address", "name": "to", "type": "address"},
            {"internalType": "uint256", "name": "amount", "type": "uint256"},
        ],
        "name": "mint",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    }
]

logging.basicConfig(level=logging.INFO)

def get_users():
    conn = psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    )
    cursor = conn.cursor()
    cursor.execute("""
        SELECT u.username, up.id, up.xp_points, w.wallet_address, w.id AS wallet_id
        FROM user_userprofile up
        JOIN user_user u ON up.user_id = u.id
        JOIN user_wallet w ON w.user_id = up.id
        WHERE up.xp_points > 0
    """)
    users = cursor.fetchall()
    cursor.close()
    conn.close()
    return users

def mint_xp(wallet_address, amount, nonce, web3, contract):
    tx = contract.functions.mint(
        wallet_address,
        web3.to_wei(amount, 'ether')
    ).build_transaction({
        "chainId": CHAIN_ID,
        "gas": 200000,
        "gasPrice": web3.eth.gas_price,
        "nonce": nonce,
    })

    signed_tx = web3.eth.account.sign_transaction(tx, private_key=XP_OWNER_PRIVATE_KEY)
    tx_hash = web3.eth.send_raw_transaction(signed_tx.raw_transaction)
    receipt = web3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)

    if receipt.status == 1:
        return tx_hash.hex()
    else:
        return None


def record_transaction(wallet_id, tx_hash, user_profile_id, amount, token, chain_id, status="success", retry_count=0):
    conn = psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    )
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO user_transaction 
            (wallet_id, tx_hash, user_id, amount, token, chain_id, status, retry_count, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        wallet_id,
        tx_hash,
        user_profile_id,
        Decimal(amount),
        token,
        chain_id,
        status,
        retry_count,
        datetime.now(),
        datetime.now(),
    ))

    conn.commit()
    cursor.close()
    conn.close()
   
def has_pending_transaction(profile_id, current_xp):
    conn = psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    )
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COALESCE(SUM(amount), 0)
        FROM user_transaction
        WHERE user_id = %s AND token = 'XP'
    """, (profile_id,))
    total_minted = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    return Decimal(total_minted) >= Decimal(current_xp)    
    
def run():
    logging.info("🚀 Minting XP tokens...")
    users = get_users()
    if not users:
        logging.info("❌ No users with XP found.")
        return

    web3 = Web3(Web3.HTTPProvider(WEB3_RPC))
    owner = web3.eth.account.from_key(XP_OWNER_PRIVATE_KEY)
    nonce = web3.eth.get_transaction_count(owner.address)
    contract = web3.eth.contract(address=web3.to_checksum_address(XP_TOKEN_CONTRACT_ADDRESS), abi=XP_TOKEN_ABI)

    for username, profile_id, xp, wallet_address, wallet_id in users:
        try:
            if has_pending_transaction(profile_id, xp):
                logging.info(f"⏭️ Skipping {username}: XP already minted or equal")
                continue
        
            xp_to_mint = Decimal(xp - 1)
            logging.info(f"🔄 Minting {xp_to_mint} XP for {username} → {wallet_address}")
            tx_hash = mint_xp(wallet_address, Decimal(xp_to_mint), nonce, web3, contract)
            if tx_hash:
                record_transaction(
                    wallet_id=wallet_id,
                    tx_hash=tx_hash,
                    user_profile_id=profile_id,
                    amount=xp_to_mint,
                    token="XP",
                    chain_id=CHAIN_ID,
                    status="success",
                    retry_count=0
                )
                logging.info(f"✅ Minted {xp_to_mint} XP → TX: {tx_hash}")
            else:
                logging.error(f"❌ TX failed for {username}")
        except Exception as e:
            logging.error(f"🔥 Error minting for {username}: {e}")
        nonce += 1


if __name__ == "__main__":
    run()