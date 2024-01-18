from flask import Flask, request
import os

app = Flask(__name__)

@app.route('/home', methods=['GET'])
def home():
    server_id = os.environ.get('SERVER_ID', 'Unknown')
    return f'Hello from Server: {server_id}\n'

@app.route('/heartbeat', methods=['GET'])
def heartbeat():
    return '', 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)