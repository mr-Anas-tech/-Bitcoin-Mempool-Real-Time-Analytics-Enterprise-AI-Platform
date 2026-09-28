import os
import json
import time
import threading
import websocket
from flask import Flask
from azure.eventhub import EventHubProducerClient, EventData

app = Flask(__name__)

EVENT_HUB_CONNECTION_STR = os.environ.get("EVENT_HUB_CONNECTION_STR")
EVENT_HUB_NAME = os.environ.get("EVENT_HUB_NAME")

def producer_worker():
    
    print("Starting Multi-Table Mempool Producer worker...", flush=True)
    
    if not EVENT_HUB_CONNECTION_STR or not EVENT_HUB_NAME:
        print("ERROR: Missing Event Hub Connection parameters!", flush=True)
        return

    try:
        producer = EventHubProducerClient.from_connection_string(
            conn_str=EVENT_HUB_CONNECTION_STR,
            eventhub_name=EVENT_HUB_NAME
        )
        print("EventHubProducerClient initialized successfully.", flush=True)
    except Exception as e:
        print(f"Producer Init Error: {e}", flush=True)
        return

    def send_to_eventhub(payload_type, raw_data):
        """Helper to package and send payload to Event Hub with type label"""
        try:
            payload = {
                "payload_type": payload_type,
                "timestamp": int(time.time()),
                "data": raw_data
            }
            event_data = EventData(json.dumps(payload))
            producer.send_event(event_data)
            print(f"Sent [{payload_type}] data to Event Hub.", flush=True)
        except Exception as err:
            print(f"Failed to send {payload_type} to Event Hub: {err}", flush=True)

    def on_message(ws, message):
        try:
            data = json.loads(message)

            # 1. Blocks Table Data
            if "block" in data:
                send_to_eventhub("block_event", data["block"])

            # 2. Mempool Blocks (Projected templates) Data
            if "mempool-blocks" in data:
                send_to_eventhub("mempool_blocks_event", data["mempool-blocks"])

            # 3. Overall Network Stats Data
            if "stats" in data:
                send_to_eventhub("stats_event", data["stats"])

            # 4. Live Mempool Transactions Data
            if "transactions" in data:
                batch = producer.create_batch()
                count = 0
                for tx in data["transactions"]:
                    tx_payload = {
                        "payload_type": "mempool_tx",
                        "txid": tx.get("txid"),
                        "fee": tx.get("fee"),
                        "vsize": tx.get("vsize"),
                        "value": tx.get("value"),
                        "rate": tx.get("rate")
                    }
                    try:
                        batch.add(EventData(json.dumps(tx_payload)))
                        count += 1
                    except ValueError:
                        if len(batch) > 0:
                            producer.send_batch(batch)
                        batch = producer.create_batch()
                        batch.add(EventData(json.dumps(tx_payload)))
                        count = 1

                if count > 0:
                    producer.send_batch(batch)
                    print(f"Sent {count} live Mempool transactions batch.", flush=True)

        except Exception as e:
            print(f"Error processing websocket message: {e}", flush=True)

    def on_open(ws):
        print("Connected! Subscribing to ALL Mempool tables & channels...", flush=True)
        # Subscribe to Blocks, Mempool-Blocks, Stats, and Live Transactions
        subscribe_msg = {
            "action": "want",
            "data": ["blocks", "mempool-blocks", "stats", "live-2xx"]
        }
        ws.send(json.dumps(subscribe_msg))

    def on_error(ws, error):
        print(f"WebSocket Error: {error}", flush=True)

    def on_close(ws, status, msg):
        print(f"WebSocket Closed: {status} - {msg}", flush=True)

    while True:
        try:
            ws = websocket.WebSocketApp(
                "wss://mempool.space/api/v1/ws",
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            print(f"Stream dropped: {e}. Reconnecting in 5s...", flush=True)
            time.sleep(5)

# Background Thread for continuous streaming
worker_thread = threading.Thread(target=producer_worker, daemon=True)
worker_thread.start()

@app.route('/')
def health():
    return "Mempool Multi-Table Stream Producer is Running 24/7!", 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8100)
