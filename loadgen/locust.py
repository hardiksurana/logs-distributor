import time
import random
from locust import HttpUser, task, between

SEVERITIES = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
SOURCES = ["api", "web", "database", "scheduler"]

class LogMessageSender(HttpUser):
    wait_time = between(0.1, 0.5)  # More frequent requests for real-time feel

    @task
    def send_log_message(self):
        message_data = {
            "timestamp": int(time.time()),  # Simplified
            "severity": random.choice(SEVERITIES),
            "source": random.choice(SOURCES),
            "message": f"Sample Log Event - Locust Test" 
        }
        self.client.post("/send_message", json=message_data)