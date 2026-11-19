import requests
import time
import random

BASE_URL = "http://localhost:8000"
PRIORITIES = ["default", "high"]
WEIGHTS = [7, 3]  # 70% default priority, 30% VIP high priority

def send_traffic():
    print(f"🚀 Starting continuous traffic generator targeting {BASE_URL}...")
    print(f"📊 Traffic Profile: {WEIGHTS[0]*10}% Default Queue, {WEIGHTS[1]*10}% High Priority Queue")
    try:
        while True:
            priority = random.choices(PRIORITIES, weights=WEIGHTS)[0]
            delay = random.randint(1, 3)
            
            payload = {
                "payload": f"Automated traffic simulation payload [{priority.upper()}]",
                "delay_seconds": delay,
                "priority": priority
            }
            
            try:
                # Submit asynchronous job
                response = requests.post(f"{BASE_URL}/jobs", json=payload, timeout=5)
                if response.status_code == 202:
                    data = response.json()
                    print(f"✅ Enqueued Job {data['job_id'][:8]}... | Priority: {priority.upper():7} | Worker Delay: {delay}s")
                else:
                    print(f"❌ Failed: HTTP {response.status_code} - {response.text}")
                    
            except requests.exceptions.ConnectionError:
                print(f"⚠️  Connection error: Cannot reach FastAPI on {BASE_URL}. Is it running?")
                
            time.sleep(random.uniform(0.3, 1.5))
            
    except KeyboardInterrupt:
        print("\n🛑 Traffic generator stopped.")

if __name__ == "__main__":
    send_traffic()
