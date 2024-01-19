# load_balancer.py
from flask import Flask, jsonify, request
import requests,os

app = Flask(__name__)

backend_server = "http://server:5001"
count=0

@app.route('/home', methods=['GET'])
def proxy_request():
    response = requests.request(
        method=request.method,
        url=f"{backend_server}{request.full_path}",
        headers=request.headers,
        data=request.get_data(),
        cookies=request.cookies,
        allow_redirects=False
    )
    return jsonify({
        'status_code': response.status_code,
        'data': response.text
    }), response.status_code

@app.route('/spawn', methods=['GET'])
def new_server():
    global count
    res=os.popen(f'sudo docker run --name container{count} --network distributed_systems_a-1_net1 --network-alias container{count} -d distributed_systems_a-1-server').read()
    if len(res)==0:
        return jsonify({
        'status_code': 200,
        'data': "Unable to start container"
    }), 200
    else:
        count+=1
        return jsonify({
        'status_code': 200,
        'data': "Successfully started container"
    }), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
