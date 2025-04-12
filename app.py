import asyncio
import websockets
import csv
from datetime import datetime, timedelta
import os
import json

# CSV file path (update if needed)
csv_file_path = "./ai4i2020.csv"

# Columns to remove (last 6 columns)
columns_to_remove = [
    "Machine failure", "TWF", "HDF", "PWF", "OSF", "RNF", "UID", "Type"
]

# Global set of connected clients
connected_clients = set()

# Read CSV once
def read_csv(file_path):
    with open(file_path, mode="r", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)
        data_list = []
        for row in reader:
            # Strip column name spaces
            row = {key.strip(): value.strip() for key, value in row.items()}
            data_list.append(row)
        return data_list

# Shared background task to broadcast to all clients
async def broadcast_data():
    data_list = read_csv(csv_file_path)
    base_time = datetime.now()
    row_index = 0

    while True:
        if connected_clients:
            row = data_list[row_index % len(data_list)]  # loop over rows

            # Remove unwanted columns
            for col in columns_to_remove:
                row.pop(col, None)

            # Add timestamp
            row["timestamp"] = (base_time + timedelta(seconds=row_index * 3)).isoformat()

            # Serialize and broadcast
            message = json.dumps(row)

            # Send to all connected clients
            disconnected = set()
            for client in connected_clients:
                try:
                    await client.send(message)
                except:
                    disconnected.add(client)

            # Remove disconnected clients
            connected_clients.difference_update(disconnected)

            row_index += 1

        await asyncio.sleep(3)

# Client connection handler
async def handler(websocket):
    print(f"Client connected: {websocket.remote_address}")
    connected_clients.add(websocket)
    try:
        await websocket.wait_closed()
    finally:
        connected_clients.remove(websocket)
        print(f"Client disconnected: {websocket.remote_address}")

# Start the server
async def start_server():
    server = await websockets.serve(handler, "0.0.0.0", 8765)
    print("WebSocket server started at ws://0.0.0.0:8765")
    # Start the broadcaster
    asyncio.create_task(broadcast_data())
    await server.wait_closed()

# Run it
if __name__ == "__main__":
    if not os.path.exists(csv_file_path):
        print(f"Error: The file {csv_file_path} does not exist.")
    else:
        asyncio.run(start_server())
