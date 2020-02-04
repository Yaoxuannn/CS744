from flask import Flask, request, jsonify
from flasgger import Swagger
from nameko.standalone.rpc import ClusterRpcProxy


app = Flask(__name__)
Swagger(app)
CONFIG = {'AMQP_URI': "amqp://guest:guest@localhost"}


@app.route("/api/v1/login", methods=['POST'])
def login():
    if not request.json:
        return pack_response(10002, "请求参数异常")
    username = request.json['username']
    password = request.json['password']
    with ClusterRpcProxy(CONFIG) as rpc:
        status, message = rpc.user_service.user_login(username, password)
    return pack_response(status, message)


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


app.run(host="0.0.0.0", debug=True)
