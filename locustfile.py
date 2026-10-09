from locust import HttpUser, task, between
import random

class JobUser(HttpUser):
    wait_time = between(1, 5)

    def on_start(self):
        # Keep track of submitted job IDs to check their status later
        self.job_ids = []

    @task(10)
    def submit_job(self):
        priorities = ["low", "default", "high"]
        priority = random.choices(priorities, weights=[2, 5, 3])[0]
        
        payload = {
            "payload": f"Fake traffic payload for {priority} priority",
            "delay_seconds": random.randint(1, 10),
            "priority": priority
        }
        
        with self.client.post("/jobs", json=payload, catch_response=True) as response:
            if response.status_code == 202:
                job_id = response.json().get("job_id")
                if job_id:
                    self.job_ids.append(job_id)
                
                # Keep the list from growing infinitely to save memory
                if len(self.job_ids) > 100:
                    self.job_ids.pop(0)

    @task(3)
    def check_job_status(self):
        if self.job_ids:
            # Pick a random job we previously submitted and check its status
            job_id = random.choice(self.job_ids)
            self.client.get(f"/jobs/{job_id}", name="/jobs/[job_id]")
            
    @task(1)
    def list_jobs(self):
        self.client.get("/jobs", name="/jobs")
