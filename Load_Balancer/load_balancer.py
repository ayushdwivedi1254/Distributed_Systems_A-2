# load_balancer.py
from flask import Flask, jsonify, request
import requests

app = Flask(__name__)

backend_server = "http://server:5001"

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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
