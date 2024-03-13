import asyncio
from aioredis import Redis, from_url
from pydantic import BaseModel, Field
import httpx
import logging
import random

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

class AnalyzerModel(BaseModel):
    id: str
    weight: float = Field(default=1.0, description="The weight of the analyzer")
    port: int
    online: bool = Field(default=True, description="Whether the analyzer is online")


class MessageModel(BaseModel):
    timestamp: int
    severity: str
    source: str
    message: str


class Distributor:
    def __init__(self, redis_host='redis', redis_port=6379, redis_db=0):
        self.redis_url = f"redis://{redis_host}:{redis_port}/{redis_db}"
        self.redis: Redis = None
        self._lock = asyncio.Lock()

    
    async def connect(self):
        self.redis = await from_url(self.redis_url, encoding="utf-8", decode_responses=True)


    async def close(self):
        await self.redis.close()
        

    async def set_analyzer_async(self, analyzer_data: dict):
        """ Registers a new analyzer with the Distributor """
        analyzer_id = analyzer_data['id']
        
        async with self._lock:
            if not analyzer_data.get('online', False):
                await self.redis.delete(f"analyzer:{analyzer_id}")
                await self.redis.delete(f"message_count:{analyzer_id}")
            else:
                analyzer_data.pop('online', None)
                await self.redis.hset(f"analyzer:{analyzer_id}", mapping=analyzer_data)
                await self.redis.set(f"message_count:{analyzer_id}", 0)
            
            # reset message counts
            keys = await self.redis.keys("message_count:*")
            await asyncio.gather(*(self.redis.set(key, 0) for key in keys))
            
            # reset weights           
            analyzer_keys = await self.redis.keys("analyzer:*")
            total_weight = sum([float(await self.redis.hget(key, 'weight')) for key in analyzer_keys])
            await self.redis.set("total_weight", total_weight)
            await self.redis.set("total_message_count", 0)

    
    async def distribute_message_async(self, message: dict):
        """Distributes a log message among Analyzers asynchronously based on the pre-defined weights"""
        async with self._lock:
            total_weight = await self.redis.get("total_weight")
            random_value = random.random() * float(total_weight)
            current_weight = 0.0

            keys = await self.redis.keys("analyzer:*")
            for key in keys:
                analyzer = await self.redis.hgetall(key)
                current_weight += float(analyzer['weight'])
                if current_weight >= random_value:
                    logger.info("Found analyzer, updating stats...")
                    await self.redis.incr(f"message_count:{analyzer['id']}")
                    await self.redis.incr("total_message_count")
                    
                    # send message to analyzer asynchronously
                    analyzer_url = f"http://{analyzer['id']}:{analyzer['port']}/message/process"
                    async with httpx.AsyncClient() as client:
                        response = await client.post(analyzer_url, json={"data": message})
                        logger.info(f"Message response status: {response.status_code}")
                
                    break
    

    async def get_distribution_stats(self):
        analyzers = []
        keys = await self.redis.keys("analyzer:*")
        for key in keys:
            data = await self.redis.hgetall(key)
            message_count = await self.redis.get(f"message_count:{data['id']}")
            data['messages_sent'] = int(message_count) if message_count else 0
            analyzers.append(data)
        return analyzers
