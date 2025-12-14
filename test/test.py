# %%
# requests>=2.31.0
# websockets>=12.0

import requests
import json
import asyncio
import websockets

# Configuration
BASE_URL = "http://localhost:8000"
WS_BASE_URL = "ws://localhost:8000"


# Sample CSV content
CSV_CONTENT = """name,address,phone
General Hospital,123 Main St,555-1234
City Medical Center,456 Oak Ave,555-5678
Community Hospital,789 Pine Rd,555-9012
Regional Health,321 Elm St,555-3456
Memorial Hospital,654 Maple Dr,555-7890
Regional Health,322 Elm St,555-3456
Memorial Hospital,653 Maple Dr,
Community Hospital,789 Pine Rd,555-9012
Regional Health,323 Elm St,555-3456
Memorial Hospital,655 Maple Dr,555-7890
Regional Health,324 Elm St,555-3456
Memorial Hospital,656 Maple Dr,"""


def create_batch():
    # Create a file-like object from CSV content
    files = {
        'file': ('hospitals.csv', CSV_CONTENT.encode('utf-8'), 'text/csv')
    }

    try:
        response = requests.post(f"{BASE_URL}/api/v1/hospital/bulk", files=files)
        res_json = response.json()
    except Exception as e:
        res_json = {}
        print(f"Error: {e}")
    return res_json



async def get_status(batch_id):
    uri = f"{WS_BASE_URL}/api/v1/hospital/status?batch_id={batch_id}"
    success = True
    try:
        async with websockets.connect(uri) as websocket:
            print("✅ Connected to WebSocket")
            print("📡 Monitoring status updates...\n")
            
            update_count = 0
            while True:
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=2.0)
                    update_count += 1
                    
                    data = json.loads(message)
                    
                    if "error" in data:
                        print(f"Error: {data['error']}")
                        success = False
                        break
                    print(data)
                        
                except asyncio.TimeoutError:
                    success = False
                    continue
                except websockets.exceptions.ConnectionClosed:
                    print("🔌 Connection closed")
                    break
                    
    except Exception as e:
        print(f"Error: {repr(e)}")
        success = False
    return success


# %%
def retry_batch(batch_id):
    payload = {
        "batch_id": batch_id
    }
    try:
        response = requests.post(
            f"{BASE_URL}/api/v1/hospital/retry",
            json=payload
        )
        res_json = response.json()
    except Exception as e:
        res_json = {}
        print(f"Error: {e}")

def delete_all_entries():
    # delete all entries
    try:
        res = requests.get("https://hospital-directory.onrender.com/hospitals/")
        res_json = res.json()

        unique_ids = set()
        for hospital in res_json:
            if hospital["creation_batch_id"] not in unique_ids:
                unique_ids.add(hospital["creation_batch_id"])

        for id in unique_ids:
            del_res = requests.delete(f"https://hospital-directory.onrender.com/hospitals/batch/{id}")
            print(del_res.text)
    except Exception as e:
        res = {}
        print(f"Error: {repr(e)}")


if __name__ == "__main__":
    res_json = create_batch()
    if not res_json:
        print("Error creating batch")
        exit(1)
    print(res_json)
    batch_id = res_json["batch_id"]
    success = asyncio.run(get_status(batch_id))
    if success:
        print("Batch completed successfully")
    else:
        print("Batch failed, retrying...")
        retry_batch(batch_id)
