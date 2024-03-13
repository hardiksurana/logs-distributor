import asyncio
import aiohttp
import aioredis
import logging
import random

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)


class CustomRedis(aioredis.Redis):
    async def execute_command(self, *args, **kwargs):
        raw_response = await super().execute_command(*args, **kwargs)
        return self.decode_response(raw_response, args[0])

    def decode_response(self, response, command_name):
        if command_name in ('GET', 'HGET'):
            if isinstance(response, bytes):
                try:
                    return float(response.decode('utf-8'))
                except ValueError:
                    return response
            else:
                return response
        else:
            return response
        

class Distributor:
    def __init__(self, redis_host='redis', redis_port=6379):
        self._redis_host = redis_host
        self._redis_port = redis_port
        self._redis_pool = None
        
        # self._redis_client = None
        # self._redis_client = redis.StrictRedis(redis_host, redis_port, charset="utf-8", decode_responses=True)

        self._lock = asyncio.Lock()         

    async def connect(self):
        # self._redis_client = await aioredis.from_url(
        #     f"redis://{self._redis_host}:{self._redis_port}", decode_responses=True
        # )
        # self._redis_client.set_response_callback('GET', float)
        # self._redis_client.set_response_callback('HGET', float)
        
        try:
            self._redis_pool = await aioredis.create_pool(
                f"redis://{self._redis_host}:{self._redis_port}", 
                minsize=1, 
                maxsize=10, 
                decode_responses=True, 
                pool_cls = CustomRedis
            )
        except Exception as e:
            logger.error(f"Error creating pool: {e}")
        


    async def close(self):
        # await self._redis_client.close()
        
        self._redis_pool.close()
        await self._redis_pool.wait_closed() 
    
    async def set_analyzer_async(self, new_analyzer):
        """ Registers a new analyzer with the Distributor """
        analyzer_id = new_analyzer['id']
        online = new_analyzer['online']
        
        async with self._lock:
            try:
                async with self._redis_pool as redis:
                    logger.info(type(redis))

                    if not online: # deregister
                        await redis.delete(f"analyzer:{analyzer_id}")
                        await redis.delete(f"message_count:{analyzer_id}")
                    else: # update registration
                        new_analyzer.pop('online')
                        await redis.hset(f"analyzer:{analyzer_id}", mapping=new_analyzer)
                        await redis.set(f"message_count:{analyzer_id}", 0)
            
                    # reset message counts
                    async for key in redis.scan_iter("message_count:*"):
                        await redis.set(key, 0)
                    
                    # reset weights            
                    total_weight = 0
                    async for key in redis.scan_iter("analyzer:*"):
                        total_weight += await redis.hget(key, 'weight')
                    
                    await redis.set("total_weight", total_weight)
                    await redis.set("total_message_count", 0)
            except Exception as e:
                logger.error(f"Error using pool: {e}")

    async def distribute_message_async(self, message):
        """ Distributes a log message among Analyzers asynchronously based on the pre-defined weights """        
        async with self._lock:
            async with self._redis_pool as redis:
                random_value = random.random() * await redis.get("total_weight")
                current_weight = 0

                async for key in redis.scan_iter("analyzer:*"):
                    analyzer = await redis.hgetall(key)
                    
                    current_weight += float(analyzer['weight'])
                    if current_weight >= random_value:
                        logger.info("Found analyzer, updating stats...")
                        await redis.incr(f"message_count:{analyzer['id']}")
                        await redis.incr("total_message_count")
                        
                        # send message to analyzer
                        await self.route_message(analyzer['id'], analyzer['port'], message)
                        break

    async def route_message(self, analyzer_id, port, message):
        """ Forwards the log to the analyzer """
        analyzer_url = f"http://{analyzer_id}:{port}/message/process"

        async with aiohttp.ClientSession() as session:
            for _ in range(3):  # Retry up to 3 times
                try:
                    async with session.post(analyzer_url, json={"data": message}) as response:
                        await response.json()
                        response.raise_for_status()
                        logger.info(f"Message sent to analyzer {analyzer_id}. Status = {response.status}")
                        return
                except aiohttp.ClientError as e:
                    logger.error(f"Error sending message to analyzer {analyzer_id}. Status = {response.status}: {e}, retrying...")
                    await asyncio.sleep(0.5)  # Backoff before retry

        logger.error(f"Failed to send message to analyzer {analyzer_id} after retries.")    


    async def get_distribution_stats(self):
        analyzers = []

        async with self._lock:
            async with self._redis_pool as redis:
                async for key in redis.scan_iter("analyzer:*"):
                    data = await redis.hgetall(key)
                    message_count = await redis.get(f"message_count:{data['id']}")
                    if message_count is None:
                        data['messages_sent'] = 0
                    else:
                        data['messages_sent'] = message_count
                    analyzers.append(data)

        return analyzers