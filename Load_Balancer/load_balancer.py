from flask import Flask, jsonify, request
import requests
import os
import queue
import random
import threading
import time
from collections import defaultdict
from sortedcontainers import SortedDict
from itertools import dropwhile

from ConsistentHashing import ConsistentHashing

app = Flask(__name__)

# backend_server = "http://server1:5000"
# N = int(os.environ.get('COUNT', ''))
N = 0
# server_names = os.environ.get('SERVER_NAMES', '')
count = N
# Split the string into a list using the delimiter
# server_names = server_names.split(',')
server_names = []

# Shared queue for incoming requests
request_queue = queue.Queue()

# server number counter and map
server_counter = N
server_name_to_number = {}
# Consistent hashing object
M = 512
K = 9
consistent_hashing = ConsistentHashing(count, M, K)

# Extra data structures for Assignment-2
MapT = defaultdict(set)
ShardT = SortedDict()
schema = {}
shards = {}
server_name_to_shards = {}
shard_id_to_consistent_hashing = {}
shard_id_to_consistent_hashing_lock = {}

# populating initial data
for i in range(0, len(server_names)):
    consistent_hashing.add_server(i+1, server_names[i])
    server_name_to_number[server_names[i]] = i+1

lock = threading.Lock()
server_name_lock = threading.Lock()

seed_value = int(time.time())
random.seed(seed_value)

################################## UTILITY FUNCTIONS ###########################################

# Function to convert a list of integers to the desired string format
def convert_to_string(integers):
    if len(integers) == 1:
        return f"Add Server:{integers[0]}"
    elif len(integers) == 2:
        return f"Add Server:{integers[0]} and Server:{integers[1]}"
    else:
        return "Add " + ", ".join(f"Server:{x}" for x in integers[:-1]) + f", and Server:{integers[-1]}"


# Function to find the key value of ordered_map which is less than or equal to num, giving priority to lesser value if exists
def lower_bound_entry(ordered_map, num):
    keys = list(ordered_map.keys())
    left, right = 0, len(keys)
    if right < 0:
        return -1
    if keys[0] >= num:
        return keys[0]
    while right - left > 1:
        mid = (left + right) // 2
        if keys[mid] < num:
            left = mid
        else:
            right = mid
    return keys[left]

################################################################################################


def worker(thread_number):
    global count
    global server_names
    global server_name_to_number
    global server_counter
    global server_name_lock
    global lock
    global consistent_hashing
    global shard_id_to_consistent_hashing
    global shard_id_to_consistent_hashing_lock

    while True:
        # Get a request from the queue
        request_data = request_queue.get()

        reqID = request_data["id"]
        shardID = request_data["shard_id"]
        low_stud_id = request_data["low_stud_id"]
        high_stud_id = request_data["high_stud_id"]

        while True:
            # with lock:
            #     serverName = consistent_hashing.allocate(reqID)
            with shard_id_to_consistent_hashing_lock[shardID]:
                serverName = shard_id_to_consistent_hashing[shardID].allocate(reqID)

            try:
                # Send the request to the backend server
                read_payload = {
                    "shard": shardID,
                    "Stud_id": {"low":low_stud_id, "high":high_stud_id}
                }
                url = f'http://{serverName}:5000/read'
                response = requests.post(url, json=read_payload)
                break
            except requests.RequestException as e:
                pass

        response_data = response.json()

        # Send the response back to the client
        request_data['response_queue'].put({
            'status_code': response.status_code,
            'data': response_data['data'],
            'thread_number': thread_number,
            'server_name': serverName,
            'reqID': id
        })

        # Mark the task as done
        request_queue.task_done()


def heartbeat():
    global count
    global server_names
    global server_name_to_number
    global server_counter
    global server_name_lock
    global lock
    global consistent_hashing

    while True:
        time.sleep(0.5)

        with server_name_lock:
            current_server_names = server_names.copy()

        for server_name in current_server_names:
            try:
                response = requests.get(
                    f"http://{server_name}:5000/heartbeat")
                response.raise_for_status()
            except requests.RequestException:

                with server_name_lock:
                    if server_name in server_names:
                        server_names.remove(server_name)
                        count -= 1

                with lock:
                    consistent_hashing.remove_server(
                        server_name_to_number[server_name], server_name)

        with server_name_lock:
            current_server_names = server_names.copy()
        if len(current_server_names) < N:
            servers_to_add = N-len(current_server_names)
            payload = {
                'n': servers_to_add,
                'hostnames': []
            }
            response = requests.post(
                "http://load_balancer:5000/add", json=payload)


# Create worker threads
num_workers = 100
for _ in range(num_workers):
    threading.Thread(target=worker, args=(_,), daemon=True).start()

# threading.Thread(target=heartbeat, daemon=True).start()

@app.route('/init', methods=['POST'])
def initialize_database():
    global schema

    payload = request.json

    # Processing the payload
    N = payload.get('N', 0)
    schema = payload.get('schema', {})
    shards_list = payload.get('shards', [])
    servers = payload.get('servers', {})

    add_endpoint_payload = {
        "n": N,
        "new_shards": shards_list,
        "servers": servers
    }

    add_response = requests.post('http://load_balancer:5000/add', json=add_endpoint_payload)

    # Check if the response from /add contains an error
    if add_response.status_code != 200:
        return jsonify(add_response.json()), add_response.status_code     
    
    # Responding with success message and status code 200
    response = {
        "message": "Configured Database",
        "status": "success"
    }
    return jsonify(response), 200

@app.route('/status', methods=['GET'])
def get_status():
    global server_name_lock
    global count
    global schema
    global shards
    global server_name_to_shards

    with server_name_lock:
        count_copy = count
    
    shards_list = [{"Stud_id_low": details["Stud_id_low"], "Shard_id": shard_id, "Shard_size": details["Shard_size"]} for shard_id, details in shards.items()]
    
    # Construct the response JSON
    response = {
        "N": count_copy,
        "schema": schema,
        "shards": shards_list,
        "servers": server_name_to_shards
    }
    
    return jsonify(response), 200


@app.route('/add', methods=['POST'])
def add_server():

    global count
    global server_names
    global server_name_to_number
    global server_counter
    global server_name_lock
    global lock
    global shards
    global schema
    global MapT
    global server_name_to_shards
    global ShardT
    global shard_id_to_consistent_hashing
    global shard_id_to_consistent_hashing_lock

    payload = request.json

    n = payload.get('n')
    new_shards = payload.get('new_shards', [])
    servers = payload.get('servers', {})
    hostnames = list(payload.get('servers', {}).keys())

    # Check if Shard_id already exists
    for new_shard in new_shards:
        shard_id = new_shard['Shard_id']
        if shard_id in shards:
            return jsonify({
                "error": "Shard ID already exists: {}".format(shard_id),
                "status": "error"
            }), 400

    # Check if server names already exist
    for hostname in hostnames:
        with server_name_lock:
            if hostname in server_names:
                response_json = {
                    "message": f"<Error> Server name {hostname} already exists",
                    "status": "failure"
                }
                return jsonify(response_json), 400

    if n > len(hostnames):
        response_json = {
            "message": "<Error> Number of new servers (n) is greater than newly added instances",
            "status": "failure"
        }
        return jsonify(response_json), 400
    
    if n < len(hostnames):
        response_json = {
            "message": "<Error> Number of new servers (n) is lesser than newly added instances",
            "status": "failure"
        }
        return jsonify(response_json), 400
    
    for new_shard in new_shards:
        shard_id = new_shard['Shard_id']
        # Add new shard details to shards_map
        shards[shard_id] = {
            'Stud_id_low': new_shard['Stud_id_low'],
            'Shard_size': new_shard['Shard_size']
        }
        ShardT[new_shard['Stud_id_low']] = {
            'Shard_id': shard_id,
            'Shard_size': new_shard['Shard_size'],
            'valid_idx': 0
        }
        shard_id_to_consistent_hashing_lock[shard_id] = threading.Lock()
        shard_id_to_consistent_hashing[shard_id] = ConsistentHashing(3, M, K)

    # list of server ids added
    server_id_list = []

    for i in range(0, n):
        res = None
        hostname = None
        # flag = 0
        if (i < len(hostnames)):
            hostname = hostnames[i]
            res = os.popen(
                f'sudo docker run --name "{hostname}" --network distributed_systems_a-2_net1 --network-alias "{hostname}" -e HOSTNAME="{hostname}" -e SERVER_ID="{server_counter+1}" -d distributed_systems_a-2-server').read()
        # else:
        #     res = os.popen(
        #         f'sudo docker run --network distributed_systems_a-2_net1 -e SERVER_ID="{server_counter+1}" -d distributed_systems_a-2-server').read()
        #     hostname = res
        #     flag = 1

        if len(res) == 0:
            response_json = {
                "message": f"<Error> Failed to start server {hostname}",
                "status": "failure"
            }
            return jsonify(response_json), 400
        else:
            # if flag:
            #     hostname = hostname[:12]
            while True:
                inspect_command = f'curl --fail --silent --output /dev/null --write-out "%{{http_code}}" http://{hostname}:5000/heartbeat'
                container_status = os.popen(inspect_command).read().strip()
                if container_status == '200':
                    break
                else:
                    time.sleep(0.1)
            
            server_id_list.append(server_counter+1)

            with server_name_lock:
                count += 1
                server_names.append(hostname)

            server_counter += 1
            server_name_to_number[hostname] = server_counter

            server_name_to_shards[hostname] = servers[hostname]
            # populating MapT
            for current_shard in servers[hostname]:
                MapT[current_shard].add(hostname)

            # configure each server
            config_payload = {
                "schema": schema,
                "shards": servers[hostname]
            }
            url = f'http://{hostname}:5000/config'
            config_response = requests.post(url, json=config_payload)
            if config_response.status_code != 200:
                return jsonify({
                    "error": "Error configuring server {}: {}".format(hostname, config_response.text),
                    "status": "error"
                }), 400
            
            # to the consistent_hashing of each shard, add the server
            for current_shard in servers[hostname]:
                with shard_id_to_consistent_hashing_lock[current_shard]:
                    shard_id_to_consistent_hashing[current_shard].add_server(server_counter, hostname)
                
            # with lock:
            #     consistent_hashing.add_server(server_counter, hostname)

    with server_name_lock:
        count_copy = count

    message_string = convert_to_string(server_id_list)

    response_json = {
        "N": count_copy,
        "message": message_string,
        "status": "successful"
    }
    return jsonify(response_json), 200

@app.route('/rm', methods=['DELETE'])
def remove_server():
    global count
    global server_names
    global server_name_to_number
    global server_counter
    global server_name_lock
    global lock
    # global consistent_hashing
    global shards
    global MapT
    global server_name_to_shards
    global ShardT
    global shard_id_to_consistent_hashing
    global shard_id_to_consistent_hashing_lock


    payload = request.json
    n = payload.get('n')
    hostnames = payload.get('servers')

    if n > count:
        response_json = {
            "message": f"<Error> Number of servers to be removed is more than those running",
            "status": "failure"
        }
        return jsonify(response_json), 400

    if len(hostnames) > n:
        response_json = {
            "message": "<Error> Length of server list is more than removable instances",
            "status": "failure"
        }
        return jsonify(response_json), 400

    for hostname in hostnames:
        if hostname not in server_names:
            response_json = {
                "message": f"<Error> Server name {hostname} does not exist",
                "status": "failure"
            }
            return jsonify(response_json), 400

    removed_server_name_list = []

    for i in range(0, n):
        hostname = None

        if i < len(hostnames):
            hostname = hostnames[i]
        else:
            hostname = random.choice(server_names)

        res1 = os.system(f'sudo docker stop {hostname}')
        res2 = os.system(f'sudo docker rm {hostname}')

        if res1 != 0 or res2 != 0:
            response_json = {
                "message": f"<Error> Failed to remove server {hostname}",
                "status": "failure"
            }
            return jsonify(response_json), 400
        with server_name_lock:
            if hostname in server_names:
                server_names.remove(hostname)
                count -= 1
        
        removed_server_name_list.append(hostname)

        for current_shard in server_name_to_shards[hostname]:
            MapT[current_shard].discard(hostname)
            with shard_id_to_consistent_hashing_lock[current_shard]:
                shard_id_to_consistent_hashing[current_shard].remove_server(server_name_to_number[hostname], hostname)
            if(len(MapT[current_shard]) == 0):
                del MapT[current_shard]
                del shards[current_shard]
                del ShardT[current_shard['Stud_id_low']]
                del shard_id_to_consistent_hashing[current_shard]
                del shard_id_to_consistent_hashing_lock[current_shard]
        
        del server_name_to_shards[hostname]
        
        # remove the server from consistent_hashing of all shards


        # with lock:
        #     consistent_hashing.remove_server(
        #         server_name_to_number[hostname], hostname)
        # # server_name_to_number.pop(hostname)

    with server_name_lock:
        count_copy = count

    response_json = {
        "message": {
            "N": count_copy,
            "servers": removed_server_name_list
        },
        "status": "successful"
    }
    return jsonify(response_json), 200


@app.route('/read', methods=['POST'])
def read_data():
    global ShardT

    # Extract low and high Student ID from the request payload
    payload = request.json
    low_id = payload["Stud_id"]["low"]
    high_id = payload["Stud_id"]["high"]

    shards_queried = []  # To store unique shard IDs queried
    shards_range = [] # The lower and upper id of students in each shard

    # Iterate over the range of Student IDs to determine shards to query
    data = []

    # find the shards
    starting_stud_id_low = lower_bound_entry(ShardT, low_id)

    # handle the case when shard list is empty
    if starting_stud_id_low == -1:
        response = {
            "shards_queried": [],
            "data": data,
            "status": "success"
        }
        return jsonify(response), 200
    
    for key, value in dropwhile(lambda item: item[0] < starting_stud_id_low, ShardT.items()):
        if high_id >= key:
            if low_id < key + value['Shard_size']:
                shards_queried.append(value['Shard_id'])
                temp_object = {
                    "low_id": max(low_id, key),
                    "high_id": min(high_id, key+value['Shard_size']-1)
                }
                shards_range.append(temp_object)
        else:
            break
    
    # if no shards found
    if len(shards_queried) == 0:
        response = {
            "shards_queried": [],
            "data": data,
            "status": "success"
        }
        return jsonify(response), 200
    
    response_queue_list = []

    # putting request for each shard in request_queue
    for i in range(0, len(shards_queried)):
        # Create a queue for each request to handle its response
        response_queue = queue.Queue()
        response_queue_list.append(response_queue)

        # Generate random 6 digit id
        id = random.randint(100000, 999999)

        # Put the request details into the shared queue
        request_queue.put({
            'method': request.method,
            'path': request.full_path,
            'headers': request.headers,
            'data': request.get_data(),
            'cookies': request.cookies,
            'response_queue': response_queue,
            'id': id,
            'shard_id': shards_queried[i],
            'low_stud_id': shards_range[i]['low_id'],
            'high_stud_id': shards_range[i]['high_id']
        })
   
    # waiting for response of each request and appending the results to data
    for response_queue in response_queue_list:
        # Wait for the response from the worker thread
        response_data = response_queue.get()
        data.extend(response_data['data'])

    response = {
        "shards_queried": shards_queried,
        "data": data,
        "status": "success"
    }

    return jsonify(response), 200


@app.route('/<path:path>', methods=['GET'])
def proxy_request(path):
    global count
    global server_names
    global server_name_to_number
    global server_counter
    global server_name_lock
    global lock
    global consistent_hashing

    # Create a queue for each request to handle its response
    response_queue = queue.Queue()

    # Generate random 6 digit id
    id = random.randint(100000, 999999)

    # Put the request details into the shared queue
    request_queue.put({
        'method': request.method,
        'path': request.full_path,
        'headers': request.headers,
        'data': request.get_data(),
        'cookies': request.cookies,
        'response_queue': response_queue,
        'id': id
    })

    # Wait for the response from the worker thread
    response_data = response_queue.get()

    if response_data['data'] == "":
        return "", response_data['status_code']
    else:
        return jsonify({
            'message': response_data['data'],
            'status': "successful" if response_data['status_code'] == 200 else "failure"
        }), response_data['status_code']

@app.route('/rep', methods=['GET'])
def get_replicas():
    global count
    global server_names
    global server_name_lock

    current_count=0
    current_server_names=[]

    with server_name_lock:
        current_server_names=server_names.copy()
        current_count=count 

    response_json={
        "message":{
            "N": current_count,
            "replicas": current_server_names
        },
        "status": "successful"
    },
    return jsonify(response_json),200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
