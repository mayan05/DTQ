from locust import HttpUser, task, between
import random

class JobUser(HttpUser):
    wait_time = between(1, 4)

    def on_start(self):
        # In-memory pool of submitted job IDs to simulate realistic user status polling
        self.job_ids = []

    @task(10)
    def submit_job(self):
        """
        Primary load: Submits asynchronous jobs to the Ingestion Layer.
        Evaluates 202 Accepted response time and throughput under heavy concurrent write load.
        """
        priorities = ["default", "high"]
        priority = random.choices(priorities, weights=[7, 3])[0]
        
        payload = {
            "payload": f"Benchmark task payload [{priority.upper()}]",
            "delay_seconds": random.randint(1, 5),
            "priority": priority
        }
        
        with self.client.post("/jobs", json=payload, catch_response=True) as response:
            if response.status_code == 202:
                job_id = response.json().get("job_id")
                if job_id:
                    self.job_ids.append(job_id)
                
                # Keep pool bounded to 150 items to manage test client memory
                if len(self.job_ids) > 150:
                    self.job_ids.pop(0)
            else:
                response.failure(f"Unexpected status code: {response.status_code}")

    @task(3)
    def check_job_status(self):
        """
        Secondary load: Polls the status of previously submitted jobs.
        Simulates end-user polling behavior until completion.
        """
        if self.job_ids:
            job_id = random.choice(self.job_ids)
            self.client.get(f"/jobs/{job_id}", name="/jobs/[job_id]")

    @task(1)
    def list_jobs(self):
        """
        Tertiary load: Queries the paginated jobs table.
        Tests SQLite read concurrency and indexing performance under active write stress.
        """
        self.client.get("/jobs?limit=25", name="/jobs")
