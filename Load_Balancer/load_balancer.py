# # load_balancer.py
# from flask import Flask, jsonify, request
# import requests,os,random

# app = Flask(__name__)

# backend_server = "http://server:5000"
# count=int(os.environ.get('COUNT', ''))
# server_names = os.environ.get('SERVER_NAMES', '')

# # Split the string into a list using the delimiter
# server_names = server_names.split(',')

# @app.route('/home', methods=['GET'])
# def proxy_request():
#     response = requests.request(
#         method=request.method,
#         url=f"{backend_server}{request.full_path}",
#         headers=request.headers,
#         data=request.get_data(),
#         cookies=request.cookies,
#         allow_redirects=False
#     )
#     return jsonify({
#         'status_code': response.status_code,
#         'data': response.text
#     }), response.status_code

# @app.route('/add', methods=['POST'])
# def add_server():

#     global count
#     global server_names

#     payload=request.json
#     n=payload.get('n')
#     hostnames=payload.get('hostnames')

#     for hostname in hostnames:
#         if hostname in server_names:
#             response_json={
#                 "message": f"<Error> Server name {hostname} already exists",
#                 "status": "failure"
#             }
#             return jsonify(response_json),400

#     if n<len(hostnames):
#         response_json={
#             "message": "<Error> Length of hostname list is more than newly added instances",
#             "status": "failure"
#         }
#         return jsonify(response_json),400

#     for i in range(0,n):
#         res=None
#         hostname=None

#         if(i<len(hostnames)):
#             hostname=hostnames[i]
#             res=os.popen(f'sudo docker run --name "{hostname}" --network distributed_systems_a-1_net1 --network-alias "{hostname}" -e HOSTNAME="{hostname}" -d distributed_systems_a-1-server').read()
#         else:
#             res=os.popen(f'sudo docker run --network distributed_systems_a-1_net1 -d distributed_systems_a-1-server').read()
#             hostname=res

#         if len(res)==0:
#             response_json={
#                 "message": f"<Error> Failed to start server {hostname}",
#                 "status": "failure"
#             }
#             return jsonify(response_json),400
#         else:
#             count+=1
#             server_names.append(hostname)

#     response_json = {
#         "message": {
#             "N": count,
#             "replicas": server_names
#         },
#         "status": "successful"
#     }   
#     return jsonify(response_json),200

# @app.route('/rm', methods=['DELETE'])
# def remove_server():
#     global count
#     global server_names

#     payload=request.json
#     n=payload.get('n')
#     hostnames=payload.get('hostnames')

#     if n>count:
#         response_json={
#             "message": f"<Error> Number of servers to be removed is more than those running",
#             "status": "failure"
#         }
#         return jsonify(response_json),400
    
#     if len(hostnames)>n:
#         response_json={
#             "message": "<Error> Length of hostname list is more than removable instances",
#             "status": "failure"
#         }
#         return jsonify(response_json),400

#     for hostname in hostnames:
#         if hostname not in server_names:
#             response_json={
#                 "message": f"<Error> Server name {hostname} does not exist",
#                 "status": "failure"
#             }
#             return jsonify(response_json),400
        
#     for i in range(0,n):
#         hostname=None

#         if i<len(hostnames):
#             hostname=hostnames[i]
#         else:
#             hostname=random.choice(server_names)

#         res1=os.system(f'sudo docker stop {hostname}')
#         res2=os.system(f'sudo docker rm {hostname}')
        
#         if res1!=0 or res2!=0:
#             response_json={
#                 "message": f"<Error> Failed to remove server {hostname}",
#                 "status": "failure"
#             }
#             return jsonify(response_json),400
        
#         server_names.remove(hostname)
#         count-=1
    
#     response_json = {
#         "message": {
#             "N": count,
#             "replicas": server_names
#         },
#         "status": "successful"
#     }   
#     return jsonify(response_json),200

# @app.route('/<path:path>', methods=['GET'])
# def endpoint_nonexistent(path):
#     response_json = {
#         "message": f"<Error> '/{path}' endpoint does not exist in server replicas",
#         "status": "failure"
#     }   
#     return jsonify(response_json),400

# if __name__ == '__main__':
#     app.run(host='0.0.0.0', port=5000)

from flask import Flask, jsonify, request
import requests
import os
import queue
import random
import threading

app = Flask(__name__)

backend_server = "http://server:5000"
count=int(os.environ.get('COUNT', ''))
server_names = os.environ.get('SERVER_NAMES', '')

# Split the string into a list using the delimiter
server_names = server_names.split(',')

# Shared queue for incoming requests
request_queue = queue.Queue()

def worker(thread_number):
    while True:
        # Get a request from the queue
        request_data = request_queue.get()

        print(f"Thread {thread_number} processing request")

        # Send the request to the backend server
        response = requests.request(
            method=request_data['method'],
            url=f"{backend_server}{request_data['path']}",
            headers=request_data['headers'],
            data=request_data['data'],
            cookies=request_data['cookies'],
            allow_redirects=False
        )

        # Send the response back to the client
        request_data['response_queue'].put({
            'status_code': response.status_code,
            'data': response.text,
            'thread_number': thread_number
        })

        # Mark the task as done
        request_queue.task_done()

# Create worker threads
num_workers = 100
for _ in range(num_workers):
    threading.Thread(target=worker, args=(_,), daemon=True).start()

@app.route('/home', methods=['GET'])
def proxy_request():
    # Create a queue for each request to handle its response
    response_queue = queue.Queue()

    # Put the request details into the shared queue
    request_queue.put({
        'method': request.method,
        'path': request.full_path,
        'headers': request.headers,
        'data': request.get_data(),
        'cookies': request.cookies,
        'response_queue': response_queue
    })

    # Wait for the response from the worker thread
    response_data = response_queue.get()

    return jsonify({
        'status_code': response_data['status_code'],
        'data': response_data['data'],
        'thread_number': response_data['thread_number']
    }), response_data['status_code']

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

        res1=os.system(f'sudo docker stop {hostname}')
        res2=os.system(f'sudo docker rm {hostname}')
        
        if res1!=0 or res2!=0:
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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
