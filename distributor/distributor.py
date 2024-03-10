# import asyncio
# import aiohttp
import logging
import threading
import random
import requests

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

class Distributor:
    def __init__(self):
        self.analyzers = [] 
        self.message_counts = []
        self.total_message_count = 0
        self.total_weight = 0
        
        self._analyzers_lock = threading.Lock()
        self._message_lock = threading.Lock()

    def set_analyzer(self, new_analyzer):
        """ Registers a new analyzer with the distributor """
        with self._analyzers_lock:
            found = False
            for i, analyzer in enumerate(self.analyzers):
                if analyzer['id'] == new_analyzer['id']:
                    found = True
                    if not new_analyzer['online']:
                        self.analyzers[i]['online'] = False
                    else:
                        self.analyzers[i] = new_analyzer
                    break
            
            if not found:
                self.analyzers.append(new_analyzer)
            
            # reset total weight and message stats
            self.total_weight = sum(analyzer['weight'] for analyzer in self.analyzers if analyzer['online'] == True)
            self.message_counts = [0] * len(self.analyzers)
            self.total_message_count = 0

    # async def distribute_message_async(self, message):
    #     async with aiohttp.ClientSession() as session:  # for managing asynchronous HTTP connections.
    #         for analyzer_id, weight in self.analyzers.items():
    #             analyzer_url = f"http://{analyzer_id}:5001/receive_message" 
    #             try:
    #                 #  for asynchronous HTTP requests and resource management.
    #                 async with session.post(analyzer_url, json={"message": message}) as response:
    #                     response.raise_for_status()  
    #                     print(f"Message sent to analyzer {analyzer_id} (weight: {weight})")
    #             except aiohttp.ClientError as e:
    #                 print(f"Error sending message to analyzer {analyzer_id}: {e}")

    def distribute_message(self, message):
        """ Distributes a log message among analyzers based on the predefined weights """
        with self._message_lock:
            self.total_message_count += 1
            
            random_value = random.random() * self.total_weight
            current_weight = 0

            for i, analyzer in enumerate(self.analyzers):
                if not analyzer['online']:
                    continue
                
                current_weight += analyzer['weight']
                if current_weight >= random_value:
                    self.message_counts[i] += 1
                    self.route_message(self.analyzers[i]['id'], self.analyzers[i]['port'], message)
                    break
    
    def route_message(self, analyzer_id, port, message):
        """ Forwards the log to the analyzer """
        analyzer_url = f"http://{analyzer_id}:{port}/receive_message"
        logger.info("Sending POST request to {analyzer_url}")
        try:
            response = requests.post(analyzer_url, json={"message": message})
            response.raise_for_status()
            logger.info(f"Message sent to analyzer {analyzer_id}. Status = {response.status_code}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Error sending message to analyzer {analyzer_id}: {e}")
    
    def get_distribution_stats(self):
        data = []
        for i in range(len(self.analyzers)):
            data.append({"id": self.analyzers[i]['id'], "online": self.analyzers[i]['online'], "weight": self.analyzers[i]['weight'], "total_messages_sent": self.message_counts[i]})
        
        return data