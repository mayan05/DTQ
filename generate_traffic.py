import requests
import time
import random

BASE_URL = "http://localhost:8000"
PRIORITIES = ["low", "default", "high"]
WEIGHTS = [2, 5, 3]

def send_traffic():
    print(f"Starting traffic generator hitting {BASE_URL}...")
    try:
        while True:
            priority = random.choices(PRIORITIES, weights=WEIGHTS)[0]
            delay = random.randint(1, 3)
            
            payload = {
                "payload": f"Automated traffic payload ({priority})",
                "delay_seconds": delay,
                "priority": priority
            }
            
            try:
                # Submit a job
                response = requests.post(f"{BASE_URL}/jobs", json=payload)
                if response.status_code == 202:
                    data = response.json()
                    print(f"✅ Submitted job {data['job_id']} [Priority: {priority}]")
                else:
                    print(f"❌ Failed to submit job: {response.status_code} - {response.text}")
                    
                # Occasionally check health
                if random.random() < 0.05:
                    health_res = requests.get(f"{BASE_URL}/health")
                    print(f"🏥 Health check: {health_res.status_code}")
                    
            except requests.exceptions.ConnectionError:
                print(f"⚠️ Connection error. Is the FastAPI server running on {BASE_URL}?")
                
            time.sleep(random.uniform(0.5, 2.0))
            
    except KeyboardInterrupt:
        print("\nTraffic generator stopped.")

if __name__ == "__main__":
    send_traffic()
