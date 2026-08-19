from locust import HttpUser, task, between
import random

class JobUser(HttpUser):
    wait_time = between(1, 5)

    @task(10)
    def submit_job(self):
        priorities = ["low", "default", "high"]
        priority = random.choices(priorities, weights=[2, 5, 3])[0]
        
        payload = {
            "payload": f"Fake traffic payload for {priority} priority",
            "delay_seconds": random.randint(1, 10),
            "priority": priority
        }
        
        self.client.post("/jobs", json=payload)

    @task(1)
    def check_health(self):
        self.client.get("/health")
