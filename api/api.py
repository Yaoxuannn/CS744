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


@app.route("/api/v1/checkUserType", methods=['GET'])
def check_user_type():
    if check_params(request.args, ['userID']):
        with ClusterRpcProxy(CONFIG) as rpc:
            user_type = rpc.user_service.check_user_type(request.args['userID'])
            if user_type is None:
                return pack_response(10002, "User is not existed.")
            return pack_response(data={"usertype": user_type})
    return pack_response(10002, "Missing Argument")


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
    if check_params(request.get_json(), ['fullname', "username", "usertype", "email", "mobile", "preferred", "associateID"]):
        if not request.json['email'] and not request.json['mobile']:
            return pack_response(10002, "Email and mobile must be provided at least one.")
        if request.json['usertype'] not in ['physician', 'nurse', 'patient']:
            return pack_response(10002, "usertype format error")
        with ClusterRpcProxy(CONFIG) as rpc:
            status, message, event_id = rpc.user_service.user_register(request.json)
        return pack_response(status, message, data={"eventID": event_id})
    return pack_response(10002, "Missing argument")


@app.route("/api/v1/getUsers", methods=["GET"])
def get_users():
    user_type = "*"
    if "userType" in request.args:
        user_type = request.args["userType"]
    with ClusterRpcProxy(CONFIG) as rpc:
        users = rpc.user_service.get_user_list(user_type)
        return pack_response(data={"user_list": users})


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
    if check_params(request.args, ["userID"]):
        with ClusterRpcProxy(CONFIG) as rpc:
            user_type = rpc.user_service.check_user_type(request.args["userID"])
            if not user_type or user_type != "admin":
                return pack_response(10001, "Not authorized")
            register_list = rpc.event_service.get_all_events("register")
            data = []
            for event in register_list:
                user_info = rpc.user_service.get_user_info(event["target"])
                user_info.update({
                    "eventID": event['event_id'],
                    "registertime": event['create_time'],
                    "status": event['status']
                })
                data.append(user_info)
            return pack_response(data={"register_list": data})
    return pack_response(10002, "Missing argument")


@app.route("/api/v1/register/approve", methods=['GET'])
def approve_register():
    if check_params(request.args, ["userID", "eventID", "userID"]):
        with ClusterRpcProxy(CONFIG) as rpc:
            user_type = rpc.user_service.check_user_type(request.args["userID"])
            if user_type != "admin":
                return pack_response(10001, "Not authorized")
            if rpc.event_service.approve(request.args['eventID']) and \
                    rpc.user_service.verify_user(request.args["userID"]):
                return pack_response()
            return pack_response(10003, "Data Error.")
    return pack_response(10002, "Missing argument")


@app.route("/api/v1/register/reject", methods=["GET"])
def reject_register():
    if check_params(request.args, ["userID", "eventID", "userID"]):
        with ClusterRpcProxy(CONFIG) as rpc:
            user_type = rpc.user_service.check_user_type(request.args["userID"])
            if user_type != "admin":
                return pack_response(10001, "Not authorized")
            if rpc.event_service.reject(request.args['eventID']) and \
                    rpc.user_service.reject_user(request.args["userID"]):
                return pack_response()
            return pack_response(10003, "Data Error")
    return pack_response(10002, "Missing argument")


@app.route("/api/v1/changePassword", methods=['POST'])
def change_password():
    if check_params(request.json, ["token", "oldPassword", "newPassword"]):
        payload = request.get_json()
        with ClusterRpcProxy(CONFIG) as rpc:
            status, msg = rpc.user_service.change_password(payload.token, payload.oldPassword, payload.newPassword)
        return pack_response(status, msg)
    return pack_response(10002, "Missing argument")


@app.route("/api/v1/getGroupID", methods=['GET'])
def get_group_id():
    if check_params(request.args, ["userID"]):
        with ClusterRpcProxy(CONFIG) as rpc:
            groups = rpc.group_service.get_group_by_user_id(request.args['userID'])
            return pack_response(data={"gid": groups})


@app.route("/api/v1/addPosting", methods=['POST'])
def add_posting():
    if check_params(request.json, ['userID', 'type', 'topic', 'message', 'gid']):
        with ClusterRpcProxy(CONFIG) as rpc:
            event_id, discussion_id = rpc.posting_service.add_posting(
                sender=request.json['userID'],
                type=request.json['type'],
                topic=request.json['topic'],
                message=request.json['message'],
                gid=request.json['gid']
            )
        if event_id:
            return pack_response(data={"eventID": event_id, "discussionID": discussion_id})
        return pack_response(10003, "Data Error")


@app.route("/api/v1/getPosting", methods=['GET'])
def get_posting():
    if check_params(request.args, ["type", "userID"]):
        with ClusterRpcProxy(CONFIG) as rpc:
            last_id = rpc.user_service.get_last_read_id(request.args['userID'])
            if last_id:
                posting_data, new_last_id = rpc.posting_service.get_discussion_by_last_id(last_id)
            else:
                posting_data, new_last_id = rpc.posting_service.get_last_discussion()
            if posting_data:
                rpc.user_service.set_last_read_id(request.args['userID'], new_last_id)
                return pack_response(data={"postings": posting_data})
        return pack_response(10003, "Data Error")


@app.route("/api/v1/reply", methods=['POST'])
def reply_a_discussion():
    if check_params(request.json, ["sender", "discussionID", "message"]):
        with ClusterRpcProxy(CONFIG) as rpc:
            posting_id = rpc.posting_service.reply(request.json['sender'], request.json['discussionID'], request.json['message'])
        if posting_id:
            return pack_response(data={"postingID": posting_id})
        return pack_response(10003, "Data Error")


@app.route("/api/v1/getReplies", methods=['GET'])
def get_relies_from_discussion():
    if check_params(request.args, ['discussionID']):
        with ClusterRpcProxy(CONFIG) as rpc:
            replies = rpc.posting_service.get_replies(request.args['discussionID'])
        if replies:
            return pack_response(data={"replies": replies})
        return pack_response(10003, "Data Error")


@app.route("/api/v1/posting/loadMore", methods=['GET'])
def load_more_posting():
    pass


@app.route("/api/v1/reply/loadMore", methods=['GET'])
def load_more_reply():
    pass


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
