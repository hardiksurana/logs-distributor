from fastapi import FastAPI, HTTPException, BackgroundTasks, status
from pydantic import BaseModel
import logging

from distributor import Distributor

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

app = FastAPI()
distributor = Distributor()


class SendMessageRequest(BaseModel):
    timestamp: int
    severity: str
    source: str
    message: str

class RegisterAnalyzerRequest(BaseModel):
    id: str
    weight: float
    port: int
    online: bool = True

class DeregisterAnalyzerRequest(BaseModel):
    id: str
    online: bool = False


@app.on_event("startup")
async def startup_event():
    await distributor.connect()


@app.on_event("shutdown")
async def shutdown_event():
    await distributor.close()


@app.post('/message/send', status_code=status.HTTP_200_OK)
async def send(request: SendMessageRequest, background_tasks: BackgroundTasks):
    if not request:
        logging.error("Invalid request, missing payload.")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid request, missing payload.")

    background_tasks.add_task(distributor.distribute_message_async, request.dict())
    logging.info("Distributed message successfully.")
    return {"status": "Message queued for distribution"}


@app.post('/analyzer/register', status_code=status.HTTP_200_OK)
async def register(request: RegisterAnalyzerRequest, background_tasks: BackgroundTasks):
    background_tasks.add_task(distributor.set_analyzer_async, request.dict())
    logging.info(f"Registered analyzer with id={request.id}")
    return {"status": f"analyzer {request.id} registered"}


@app.post('/analyzer/deregister', status_code=status.HTTP_200_OK)
async def deregister(request: DeregisterAnalyzerRequest, background_tasks: BackgroundTasks):
    background_tasks.add_task(distributor.set_analyzer_async, {'id': request.id, 'online': False})
    logging.info(f"De-registered analyzer with id={request.id}")
    return {"status": f"analyzer {request.id} de-registered"}


@app.get('/analyzer/stats', status_code=status.HTTP_200_OK)
async def stats():
    data = await distributor.get_distribution_stats()
    return data
