import asyncio
from gunicorn.app.wsgiapp import WSGIApplication
import logging
from flask import Flask, request, jsonify

from distributor import Distributor

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

app = Flask(__name__)
distributor = Distributor()

# async def initialize_app():
#     try:
#         app.logger.info("Inside initialize_app()")
#         await distributor.connect()
#     except Exception as e:
#         app.logger.error(f"Error during initialization: {e}")
#     await distributor.connect()

@app.route('/message/send', methods=['POST'])
async def send():
    data = request.get_json()
    if not data:
        app.logger.error("Invalid request, missing payload.")
        return jsonify({'error': 'Invalid request, missing payload.'}), 400
    
    # schedules asynchronous message distribution without blocking the main request handling
    task = asyncio.create_task(distributor.distribute_message_async(data)) 
    
    try:
        await task
        app.logger.info("Distributed message successfully.")
        return jsonify({'status': 'Message queued for distribution'}), 200 
    except asyncio.CancelledError:
        app.logger.info("Request was cancelled.")
    

@app.route('/analyzer/register', methods=['POST'])
async def register():
    data = request.get_json()
    if not data or 'id' not in data or 'weight' not in data or 'port' not in data:
        app.logger.error("Invalid request, missing a required field.")
        return jsonify({'error': 'Invalid request, missing a required field'}), 400

    analyzer_id = data.get('id')
    weight = data.get('weight', 0)
    port = data.get('port')
    
    task = asyncio.create_task(distributor.set_analyzer_async({
        'id': analyzer_id, 
        'weight': weight, 
        'port': port, 
        'online': True
        }))

    try:
        await task
        app.logger.info(f"Registered analyzer with id={analyzer_id}")
        return jsonify({'status': 'analyzer {analyzer_id} registered'}), 200 
    except asyncio.CancelledError:
        app.logger.info("Request was cancelled.")


@app.route('/analyzer/deregister', methods=['POST'])
async def deregister():
    data = request.get_json()
    if not data or 'id' not in data:
        app.logger.error("Invalid request, missing 'id' field.")
        return jsonify({'error': 'Invalid request, missing "id" field'}), 400

    analyzer_id = data['id']

    task = asyncio.create_task(distributor.set_analyzer_async({
        'id': analyzer_id, 
        'online': False
        }))
    
    try:
        await task
        app.logger.info(f"De-registered analyzer with id={analyzer_id}")
        return jsonify({'status': 'analyzer {analyzer_id} de-registered'}), 200 
    except asyncio.CancelledError:
        app.logger.info("Request was cancelled.")


@app.route('/analyzer/stats', methods=['GET'])
async def stats():
    task = asyncio.create_task(distributor.get_distribution_stats())
    try:
        data = await task
        return jsonify(data), 200
    except asyncio.CancelledError:
        app.logger.info("Request was cancelled.")


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000, debug=True)

    # server = WSGIApplication("app:app") 
    # server.run(host="0.0.0.0", port=3000, worker_class="gevent")