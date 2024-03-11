import asyncio
import aiohttp
import logging
import random
import copy

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

class Distributor:
    def __init__(self):
        self.analyzers = [] 
        self.message_counts = []
        self.total_message_count = 0
        self.total_weight = 0
        self._lock = asyncio.Lock() 

    async def update_analyzer(self, analyzer, new_analyzer):
        """ Updates existing analyzer """
        if analyzer['id'] == new_analyzer['id']:
            if not new_analyzer['online']: # de-register analyzer
                analyzer['online'] = False
            else: # update existing analyzer
                analyzer = new_analyzer
            return True
        return False
        
    async def set_analyzer_async(self, new_analyzer):
        """ Registers a new analyzer with the Distributor """
        async with self._lock:
            analyzers = copy.deepcopy(self.analyzers)
        
        results = await asyncio.gather(*[self.update_analyzer(analyzer, new_analyzer) for analyzer in analyzers])
        if not any(results): # register new analyzer
            analyzers.append(new_analyzer)
        
        async with self._lock:    
            self.analyzers = analyzers

            # reset total weight and message stats
            self.total_weight = sum(analyzer['weight'] for analyzer in self.analyzers if analyzer['online'] == True)
            self.message_counts = [0] * len(self.analyzers)
            self.total_message_count = 0

    async def distribute_message_async(self, message):
        """ Distributes a log message among Analyzers asynchronously based on the pre-defined weights """        
        async with self._lock:
            random_value = random.random() * self.total_weight
            current_weight = 0

            logger.info("Finding the analyzer to send to...")
            logger.info(self.analyzers)

            for i, analyzer in enumerate(self.analyzers):
                if not analyzer['online']:
                    continue
                
                current_weight += analyzer['weight']
                if current_weight >= random_value:
                    logger.info("Found analyzer, updating stats...")

                    self.message_counts[i] += 1
                    self.total_message_count += 1

                    logger.info("Sending log to analyzer now...")
                    await self.route_message(self.analyzers[i]['id'], self.analyzers[i]['port'], message)
                    break

    async def route_message(self, analyzer_id, port, message):
        """ Forwards the log to the analyzer """
        analyzer_url = f"http://{analyzer_id}:{port}/message/process"
        logger.info(f"Sending POST request to {analyzer_url}")
        
        # The aiohttp.ClientSession() object acts as a central hub for managing a pool of connections to a server. 
        # This connection pool enables efficient reuse of connections across multiple HTTP requests.
        async with aiohttp.ClientSession() as session:
            try:
                #  for asynchronous HTTP requests and resource management.
                async with session.post(analyzer_url, json={"data": message}) as response:
                    await response.json()
                    response.raise_for_status()
                    logger.info(f"Message sent to analyzer {analyzer_id}. Status = {response.status}")
            except aiohttp.ClientError as e:
                logger.error(f"Error sending message to analyzer {analyzer_id}: {e}")      


    async def get_distribution_stats(self):
        async with self._lock:
            analyzers = copy.deepcopy(self.analyzers)
            message_counts = copy.deepcopy(self.message_counts)
        
        return [
            {
                "id": analyzer['id'], 
                "online": analyzer['online'], 
                "weight": analyzer['weight'], 
                "messages_sent": message_counts[i] 
            }
            for i, analyzer in enumerate(analyzers)
        ]   