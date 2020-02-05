from flask import Flask, request, jsonify, session
from flasgger import Swagger
import os
from nameko.standalone.rpc import ClusterRpcProxy

'''
10001	Permission error
10002	Arguments error
10003	Data error
20000	Normal

'''

app = Flask(__name__)
app.config['SECRET_KEY'] = b'_5#y2L"F4Q8z\n\xec]/'
Swagger(app)
CONFIG = {'AMQP_URI': "amqp://guest:guest@localhost"}


@app.route("/api/v1/login", methods=['POST'])
def user_login():
    if not request.json:
        return pack_response(10002, "Missing login data")
    if 'username' not in request.json or 'password' not in request.json:
        return pack_response(10002, "Argument format error")
    username = request.json['username']
    password = request.json['password']
    with ClusterRpcProxy(CONFIG) as rpc:
        status, message, token = rpc.user_service.user_login(username, password)
    if status == 20000:
        session[token] = "Logged in"
    return pack_response(status, message, data={"token": token})


@app.route("/api/v1/logout", methods=['GET'])
def user_logout():
    if check_params(request.args, ['token']):
        session.pop(request.args.get("token"))
        return pack_response()
    return pack_response(10002, "Missing argument")


@app.route("/api/v1/register", methods=['POST'])
def user_register():
    if check_params(request.json, ['fullname', "username", "usertype", "email", "mobile", "preferred"]):
        if not request.json['email'] and not request.json['mobile']:
            return pack_response(10002, "Email and mobile must be provided at least one.")
        if request.json['usertype'] not in ['physician', 'nurse', 'patient']:
            return pack_response(10002, "usertype format error")
        with ClusterRpcProxy(CONFIG) as rpc:
            status, message, event_id = rpc.user_service.user_register(request.json)
        return pack_response(status, message, data={"eventID": event_id})


def check_params(params, essentials):
    for k, v in params.items():
        if k in essentials and not v:
            return False
    return True


def pack_response(status_code=20000, msg="ok", **kwargs):
    data = {
        "status": status_code,
        "msg": msg
    }
    for k, v in kwargs.items():
        data.update({k: v})
    res = jsonify(data)
    res.headers['Access-Control-Allow-Origin'] = "*"
    res.headers['X-XSS-Protection'] = "1"
    return res


def clean_params(params):
    _params = params.copy()
    for k, v in params.items():
        if not v:
            _params.pop(k)
    return _params


@app.before_request
def options_handler():
    if request.method == "OPTIONS":
        res = pack_response()
        res.headers['Access-Control-Allow-Methods'] = "GET, POST"
        res.headers['Access-Control-Allow-Headers'] = 'content-type'
        res.headers['Access-Control-Allow-Credentials'] = "true"
        res.headers['Access-Control-Max-Age'] = "1728000"
        return res
    else:
        pass


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=6000, debug=True)
