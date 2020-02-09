# coding=utf-8
from flask import Flask, request, jsonify
from nameko.standalone.rpc import ClusterRpcProxy

'''
10001	Permission error
10002	Arguments error
10003	Data error
20000	Normal
'''

app = Flask(__name__)
app.config['SECRET_KEY'] = b'_5#y2L"F4Q8z\n\xec]/'
CONFIG = {'AMQP_URI': "amqp://guest:guest@localhost"}


@app.route("/api/v1/login", methods=['POST'])
def user_login():
    if not request.json:
        return pack_response(10002, "Missing login data")
    if not check_params(request.get_json(), ["username", "password"]):
        return pack_response(10002, "Argument format error")
    username = request.json['username']
    password = request.json['password']
    with ClusterRpcProxy(CONFIG) as rpc:
        status, message, token, code = rpc.user_service.user_login(username, password)
        # if status == 20000:
        #     session[token] = code
    return pack_response(status, message, data={"token": token})


@app.route("/api/v1/logout", methods=['GET'])
def user_logout():
    if check_params(request.args, ['token']):
        with ClusterRpcProxy(CONFIG) as rpc:
            status, message = rpc.user_service.user_logout(request.args.get("token"))
        return pack_response(status, message)
    return pack_response(10002, "Missing argument")


@app.route("/api/v1/register", methods=['POST'])
def user_register():
    if check_params(request.get_json(), ['fullname', "username", "usertype", "email", "mobile", "preferred"]):
        if not request.json['email'] and not request.json['mobile']:
            return pack_response(10002, "Email and mobile must be provided at least one.")
        if request.json['usertype'] not in ['physician', 'nurse', 'patient']:
            return pack_response(10002, "usertype format error")
        with ClusterRpcProxy(CONFIG) as rpc:
            status, message, event_id = rpc.user_service.user_register(request.json)
        return pack_response(status, message, data={"eventID": event_id})
    return pack_response(10002, "Missing argument")


@app.route("/api/v1/validateCode", methods=['POST'])
def validate_code():
    if check_params(request.get_json(), ['token', 'code', 'ts']):
        with ClusterRpcProxy(CONFIG) as rpc:
            token = request.json['token']
            login_code = request.json['code']
            ts = request.json['ts']
            status, message, loginsuccess = rpc.user_service.validate_login_code(login_code, token, ts)
            return pack_response(status, message, data={"loginsuccess": loginsuccess})
    return pack_response(10002, "Missing argument")


@app.route("/api/v1/getRegisterList", methods=['GET'])
def get_register_list():
    if check_params(request.args, ["token"]):
        with ClusterRpcProxy(CONFIG) as rpc:
            user_type = rpc.user_service.check_user_type(request.args["token"])
            if user_type != "admin":
                return pack_response(10001, "Not authorized")
            register_list = rpc.event_service.get_all_events("register")
            data = []
            for event in register_list:
                user_info = rpc.user_service.get_user_info(event["target"])
                user_info.update({
                    "registertime": event['create_time']
                })
                data.append(user_info)
            return pack_response(data={"register_list": data})
    return pack_response(10002, "Missing argument")


def check_params(params, essentials):
    for n in essentials:
        if n not in params:
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
    app.run(host="0.0.0.0", port=5000, debug=True)
