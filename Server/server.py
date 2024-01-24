from flask import Flask, request
import os,socket,subprocess,json

app = Flask(__name__)

@app.route('/home', methods=['GET'])
def home():
    server_id=os.environ.get('SERVER_ID')
    return f'Hello from Server: {server_id}', 200

@app.route('/heartbeat', methods=['GET'])
def heartbeat():
    return '', 200

@app.route('/<path:path>', methods=['GET'])
def endpoint_nonexistent(path):
    return f"<Error> '/{path}' endpoint does not exist in server replicas", 400

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)