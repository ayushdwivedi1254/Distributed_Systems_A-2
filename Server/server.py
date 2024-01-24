from flask import Flask, request
import os,socket,subprocess,json

app = Flask(__name__)

@app.route('/home', methods=['GET'])
def home():
    # container_name = os.environ.get('HOSTNAME')
    server_id=os.environ.get('SERVER_ID')
    # if container_name is None:
    #     with open('/etc/hostname', 'r') as file:
    #         container_name = file.read().strip()
    return f'Hello from Server: {server_id}'

@app.route('/heartbeat', methods=['GET'])
def heartbeat():
    return '', 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)