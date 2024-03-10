# import asyncio
# import aiohttp
import logging
from flask import Flask, request, jsonify
from distributor import Distributor

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

app = Flask(__name__)
distributor = Distributor()

@app.route('/send_message', methods=['POST'])
def send_message():
    data = request.get_json()
    if not data or 'message' not in data:
        app.logger.error("Invalid request, missing 'message' field.")
        return jsonify({'error': 'Invalid request, missing "message" field'}), 400

    message = data['message']
    distributor.distribute_message(message)
    app.logger.info("Distributed message successfully.")
    return jsonify({'status': 'Message routed successfully'}), 200 

    # to schedule asynchronous message distribution without blocking the main request handling.
    # asyncio.create_task(distributor.distribute_message_async(message)) 
    # return jsonify({'status': 'Message queued for distribution'}), 200 

@app.route('/register_analyzer', methods=['POST'])
def register_analyzer():
    data = request.get_json()
    if not data or 'id' not in data or 'weight' not in data or 'port' not in data:
        app.logger.error("Invalid request, missing a required field.")
        return jsonify({'error': 'Invalid request, missing a required field'}), 400

    analyzer_id = data.get('id')
    weight = data.get('weight', 0)
    port = data.get('port')
    
    distributor.set_analyzer({'id': analyzer_id, 'weight': weight, 'port': port,'online': True})
    app.logger.info(f"Registered analyzer with id={analyzer_id}")
    return jsonify({'status': 'analyzer {analyzer_id} registered'}), 200 

@app.route('/deregister_analyzer', methods=['POST'])
def deregister_analyzer():
    data = request.get_json()
    if not data or 'id' not in data:
        app.logger.error("Invalid request, missing 'id' field.")
        return jsonify({'error': 'Invalid request, missing "id" field'}), 400

    analyzer_id = data['id']

    distributor.set_analyzer({'id': analyzer_id, 'online': False})
    app.logger.info(f"De-registered analyzer with id={analyzer_id}")
    return jsonify({'status': 'analyzer {analyzer_id} de-registered'}), 200 

@app.route('/analyzer_stats', methods=['GET'])
def analyzer_stats():
    data = distributor.get_distribution_stats()
    return jsonify(data), 200

if __name__ == '__main__':
    # distributor.set_analyzer({'id': 'analyzer_1', 'weight': 0.5, 'online': True})
    # distributor.set_analyzer({'id': 'analyzer_2', 'weight': 0.3, 'online': True})
    # distributor.set_analyzer({'id': 'analyzer_3', 'weight': 0.2, 'online': True})
    
    app.run(host='0.0.0.0', port=3000, debug=True) 