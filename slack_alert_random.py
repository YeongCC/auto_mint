import os
import sys
from slack_sdk import WebClient

SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_ALERT_CHANNEL = os.getenv("SLACK_ALERT_CHANNEL", "#alerts")

def send_slack_message(message):
    client = WebClient(token=SLACK_BOT_TOKEN)
    client.chat_postMessage(channel=SLACK_ALERT_CHANNEL, text=message)

if __name__ == "__main__":
    message = "❗️ randombotwash.py process was stopped unexpectedly. Supervisor triggered this alert."
    try:
        send_slack_message(message)
    except Exception as e:
        print(f"Slack alert failed: {e}", file=sys.stderr)
        sys.exit(1)
