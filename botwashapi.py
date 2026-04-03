import sys
import asyncio
import os
import random
import requests
import logging
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv
import pytz

load_dotenv()
logging.basicConfig(level=logging.INFO, stream=sys.stdout)

BOT_API_TOKEN = os.getenv("BOT_API_TOKEN")
BOT_SERVER_URL = os.getenv("BOT_SERVER_URL", "http://localhost:8010")

TOKENS = [
    {"symbol": "bitcoin", "coin_id": "BTC"},
    {"symbol": "ethereum", "coin_id": "ETH"},
    {"symbol": "solana", "coin_id": "SOL"},
]

MALAYSIA_TZ = pytz.timezone("Asia/Kuala_Lumpur")
scheduler = AsyncIOScheduler(timezone=MALAYSIA_TZ)

latest_prices = {}

def update_prices():
    logging.info("Updating prices from bot price API...")
    global latest_prices

    symbols = ",".join([t["symbol"] for t in TOKENS])
    url = f"{BOT_SERVER_URL}/bots/price"
    headers = {
        "X-API-TOKEN": BOT_API_TOKEN,
    }
    params = {
        "symbols": symbols,
    }

    try:
        res = requests.get(url, params=params, headers=headers, timeout=30)
        res.raise_for_status()
        latest_prices = res.json()
        logging.info(f"Updated prices: {latest_prices}")
    except Exception as e:
        logging.error(f"Failed to update prices from bot API: {e}")

def post_bot_api(endpoint, symbol, coin_id, price):
    url = f"{BOT_SERVER_URL}/{endpoint}"
    headers = {
        "X-API-TOKEN": BOT_API_TOKEN,
        "Content-Type": "application/json"
    }
    payload = {
        "symbol": symbol,
        "coin_id": coin_id,
        "price": str(price)
    }
    logging.info(f"POST {url} | payload={payload}")
    try:
        res = requests.post(url, json=payload, headers=headers, timeout=30)
        res.raise_for_status()
        logging.info(f"? {endpoint} success: {res.json()}")
    except Exception as e:
        logging.error(f"? Failed {endpoint}: {e}")

# === Simulation Logic ===
async def simulate(task_id):
    token = random.choice(TOKENS)
    symbol = token["symbol"]
    coin_id = token["coin_id"]

    price_data = latest_prices.get(symbol)
    if not price_data or "usd" not in price_data:
        logging.warning(f"No price data for {symbol}")
        return

    price = price_data["usd"]
    logging.info(f"?? Task {task_id}: symbol={symbol}, coin_id={coin_id}, price={price}")
    endpoint = "bots/create_bot"
    post_bot_api(endpoint, symbol, coin_id, price)

# === Hourly Task ===
async def hourly_simulation():
    now = datetime.now(MALAYSIA_TZ)
    count = random.randint(900, 1000) if now.weekday() >= 5 else random.randint(1000, 1100)

    logging.info("=" * 60)
    logging.info(f"?? START hourly_simulation at {now} | total jobs={count}")

    for i in range(count):
        delay = random.randint(0, 3599)  
        logging.info(f"? Scheduling Task {i+1}/{count}, delay={delay}s")
        asyncio.create_task(run_with_delay(i + 1, delay))

    logging.info(f"?? All {count} tasks scheduled for hour {now.hour}")
    logging.info("=" * 60)

async def run_with_delay(task_id, delay):
    await asyncio.sleep(delay)
    await simulate(task_id)

async def main():
    update_prices()
    await hourly_simulation()
    scheduler.add_job(update_prices, 'interval', hours=2)
    scheduler.add_job(lambda: asyncio.create_task(hourly_simulation()), 'cron', minute=0)
    scheduler.start()
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
