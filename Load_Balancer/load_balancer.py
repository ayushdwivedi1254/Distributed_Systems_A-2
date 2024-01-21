# load_balancer.py
from flask import Flask, jsonify, request
import requests,os,random

app = Flask(__name__)

backend_server = "http://server:5000"
count=0
server_names=[]

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

@app.route('/add', methods=['POST'])
def add_server():

    global count
    global server_names

    payload=request.json
    n=payload.get('n')
    hostnames=payload.get('hostnames')

    for hostname in hostnames:
        if hostname in server_names:
            response_json={
                "message": f"<Error> Server name {hostname} already exists",
                "status": "failure"
            }
            return jsonify(response_json),400

    if n<len(hostnames):
        response_json={
            "message": "<Error> Length of hostname list is more than newly added instances",
            "status": "failure"
        }
        return jsonify(response_json),400

    for i in range(0,n):
        res=None
        hostname=None

        if(i<len(hostnames)):
            hostname=hostnames[i]
            res=os.popen(f'sudo docker run --name "{hostname}" --network distributed_systems_a-1_net1 --network-alias "{hostname}" -e HOSTNAME="{hostname}" -d distributed_systems_a-1-server').read()
        else:
            res=os.popen(f'sudo docker run --network distributed_systems_a-1_net1 -d distributed_systems_a-1-server').read()
            hostname=res

        if len(res)==0:
            response_json={
                "message": f"<Error> Failed to start server {hostname}",
                "status": "failure"
            }
            return jsonify(response_json),400
        else:
            count+=1
            server_names.append(hostname)

    response_json = {
        "message": {
            "N": count,
            "replicas": server_names
        },
        "status": "successful"
    }   
    return jsonify(response_json),200

@app.route('/rm', methods=['DELETE'])
def remove_server():
    global count
    global server_names

    payload=request.json
    n=payload.get('n')
    hostnames=payload.get('hostnames')

    if n>count:
        response_json={
            "message": f"<Error> Number of servers to be removed is more than those running",
            "status": "failure"
        }
        return jsonify(response_json),400
    
    if len(hostnames)>n:
        response_json={
            "message": "<Error> Length of hostname list is more than removable instances",
            "status": "failure"
        }
        return jsonify(response_json),400

    for hostname in hostnames:
        if hostname not in server_names:
            response_json={
                "message": f"<Error> Server name {hostname} does not exist",
                "status": "failure"
            }
            return jsonify(response_json),400
        
    for i in range(0,n):
        hostname=None

        if i<len(hostnames):
            hostname=hostnames[i]
        else:
            hostname=random.choice(server_names)

        res=os.system(f'sudo docker stop {hostname} && sudo docker rm {hostname}')
        
        if res!=0:
            response_json={
                "message": f"<Error> Failed to remove server {hostname}",
                "status": "failure"
            }
            return jsonify(response_json),400
        
        server_names.remove(hostname)
        count-=1
    
    response_json = {
        "message": {
            "N": count,
            "replicas": server_names
        },
        "status": "successful"
    }   
    return jsonify(response_json),200

@app.route('/<path:path>', methods=['GET'])
def endpoint_nonexistent(path):
    response_json = {
        "message": f"<Error> '/{path}' endpoint does not exist in server replicas",
        "status": "failure"
    }   
    return jsonify(response_json),400

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
