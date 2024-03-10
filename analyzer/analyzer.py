import requests
import time
import os
import logging
import atexit
import threading
from flask import Flask, request, jsonify

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

app = Flask(__name__)

DISTRIBUTOR_URL = os.environ.get('DISTRIBUTOR_URL', 'http://distributor:3000')
ANALYZER_ID = int(os.environ.get('ANALYZER_ID'))
ANALYZER_NAME = "analyzer_" + str(ANALYZER_ID)
ANALYZER_WEIGHT = float(os.environ.get('ANALYZER_WEIGHT'))

MESSAGE_COUNT = 0

def register():
    try:
        response = requests.post(f"{DISTRIBUTOR_URL}/register_analyzer", json={"id":  ANALYZER_NAME, "weight": ANALYZER_WEIGHT, "port": 3000 + ANALYZER_ID})
        response.raise_for_status()
        app.logger.info(f"Registered analyzer with id={ANALYZER_NAME} with Distributor.")
    except requests.exceptions.RequestException as e:
        app.logger.error(f"Error sending message to analyzer {id}: {e}")

def deregister():
    try:
        response = requests.post(f"{DISTRIBUTOR_URL}/deregister_analyzer", json={"id":  ANALYZER_NAME})
        response.raise_for_status()
        app.logger.info(f"De-registered analyzer with id={ANALYZER_NAME} with Distributor. Total messages received = {MESSAGE_COUNT}")
    except requests.exceptions.RequestException as e:
        app.logger.error(f"Error sending message to analyzer {id}: {e}")


@app.route('/receive_message', methods=['POST'])
def receive_message():
    global MESSAGE_COUNT

    message_data = request.get_json()
    if not message_data or 'message' not in message_data:
        return jsonify({'error': 'Invalid message format'}), 400

    message = message_data['message']
    MESSAGE_COUNT += 1
    
    # Process the message as needed 
    app.logger.info(f"Analyzer {ANALYZER_NAME} received message: {message}. Total so far = {MESSAGE_COUNT}")

    return jsonify({'status': 'Message processed'}), 200 

if __name__ == '__main__':
    # Register Analyzer with Distributor on startup
    register()

    """
    When the docker container is stopped, Docker sends a SIGTERM signal to the analyzer container's process.  
    This triggers the shutdown hook, executing the deregistration call.
    """
    atexit.register(deregister)
    
    app.run(host='0.0.0.0', port=3000 + int(ANALYZER_ID), debug=True) 