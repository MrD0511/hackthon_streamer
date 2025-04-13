import asyncio
import websockets
import csv
from datetime import datetime, timedelta
import os
import json
from collections import defaultdict
from websockets.legacy.server import serve

# Directory where machine CSV files are stored
csv_dir = "./machines"

# Connected clients per machine
connected_clients = defaultdict(set)

# Broadcast tasks for each machine
broadcast_tasks = {}

# Jitter/remove columns if needed
columns_to_remove = []

machine_csv_map = {
    "cnc": "ai4i2020.csv",
    "velding": "sending_health.csv",
    # add more mappings as needed
}

columns_to_remove_map = {
    "cnc" : ["Machine failure", "TWF", "HDF", "PWF", "OSF", "RNF", "UID", "Type", "Product ID"],
    "velding": ["timestamp", "cycle_id", "health"]
}

# Read CSV from file
def read_csv_for_machine(machine_id):
    if machine_id not in machine_csv_map:
        return None

    file_path = os.path.join(csv_dir, machine_csv_map[machine_id])

    if not os.path.exists(file_path):
        return None
    
    with open(file_path, mode="r", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)
        data_list = []
        for row in reader:
            row = {key.strip(): value.strip() for key, value in row.items()}
            data_list.append(row)
        return data_list


async def broadcast_data(machine_id, data_rows):
    base_time = datetime.now()
    row_index = 0
    cols_to_remove = columns_to_remove_map.get(machine_id, [])

    while True:
        if connected_clients[machine_id]:
            row = data_rows[row_index % len(data_rows)].copy()

            for col in cols_to_remove:
                row.pop(col, None)

            row["timestamp"] = (base_time + timedelta(seconds=row_index * 3)).isoformat()
            message = json.dumps(row)

            disconnected = set()
            for client in connected_clients[machine_id]:
                try:
                    await client.send(message)
                except:
                    disconnected.add(client)

            connected_clients[machine_id].difference_update(disconnected)
            row_index += 1

        await asyncio.sleep(3)

# WebSocket handler
async def handler(websocket, path):
    machine_id = path.strip("/").lower()
    print(f"Client connected for {machine_id}: {websocket.remote_address}")

    if machine_id not in broadcast_tasks:
        data_rows = read_csv_for_machine(machine_id)
        if not data_rows:
            await websocket.send(json.dumps({"error": f"No data for machine '{machine_id}'"}))
            await websocket.close()
            return
        # Start broadcasting for this machine
        broadcast_tasks[machine_id] = asyncio.create_task(broadcast_data(machine_id, data_rows))

    connected_clients[machine_id].add(websocket)

    try:
        await websocket.wait_closed()
    finally:
        connected_clients[machine_id].remove(websocket)
        print(f"Client disconnected for {machine_id}: {websocket.remote_address}")

# Start WebSocket server
async def start_server():
    print("WebSocket server started at ws://0.0.0.0:8765")
    # server = await websockets.serve(handler, "0.0.0.0", 8765)
    server = await serve(handler, "0.0.0.0", 8765)
    await server.wait_closed()

# Run
if __name__ == "__main__":
    if not os.path.exists(csv_dir):
        print(f"Error: The directory {csv_dir} does not exist.")
    else:
        asyncio.run(start_server())
