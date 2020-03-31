# coding=utf-8
from collections import namedtuple

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
            user_type = rpc.user_service.check_user_type_by_id(request.args['userID'])
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
        status, message, token, user_id = rpc.user_service.user_login(username, password)
        if token is not None:
            return pack_response(status, message, data={"token": token, "userID": user_id})
        return pack_response(status, message)


@app.route("/api/v1/logout", methods=['GET'])
def user_logout():
    if check_params(request.args, ['token']):
        with ClusterRpcProxy(CONFIG) as rpc:
            result = rpc.user_service.user_logout(request.args.get("token"))
            if result:
                return pack_response()
        return pack_response(10001, "token error/user not logged in")
    return pack_response(10002, "Missing argument")


@app.route("/api/v1/register", methods=['POST'])
def user_register():
    if check_params(request.get_json(),
                    ["firstname", "lastname", "username", "usertype", "email", "mobile", "preferred", "associateID",
                     "additionalInfo"]):
        if not request.json['email'] and not request.json['mobile']:
            return pack_response(10002, "Email and mobile must be provided at least one.")
        if request.json['usertype'] not in ['physician', 'nurse', 'patient']:
            return pack_response(10002, "usertype format error")
        with ClusterRpcProxy(CONFIG) as rpc:
            status, message = rpc.user_service.user_register(request.json)
        return pack_response(status, message)
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
    if check_params(request.args, ["token"]):
        with ClusterRpcProxy(CONFIG) as rpc:
            user_type = rpc.user_service.check_user_type_by_token(request.args["token"])
            if not user_type or user_type != "admin":
                return pack_response(10001, "Not authorized")
            register_list = rpc.event_service.get_all_events("register")
            data = []
            for event in register_list:
                user_info = rpc.user_service.get_user_info(event["target"])
                if user_info['user_type'] == "patient":
                    physician_id = user_info['associateID']
                data.append({
                    "eventID": event['event_id'],
                    "userID": user_info['user_id'],
                    "firstname": user_info['first_name'],
                    "lastname": user_info['last_name'],
                    "username": user_info['user_name'],
                    "usertype": user_info['user_type'],
                    "email": user_info['email'],
                    "mobile": user_info['mobile'],
                    "isValid": rpc.hospital_service.is_user_exist(event['additional_info']),
                    "nameMatch": rpc.hospital_service.check_user_name(event['additional_info'], user_info['first_name'],
                                                                      user_info['last_name']),
                    "physicianExist": rpc.hospital_service.is_user_exist(physician_id) if user_info['user_type'] == "patient" else "",
                    "registerTime": event['created_time']
                })
            return pack_response(data={"register_list": data})
    return pack_response(10002, "Missing argument")


@app.route("/api/v1/register/approve", methods=['GET'])
def approve_register():
    if check_params(request.args, ["token", "eventID"]):
        with ClusterRpcProxy(CONFIG) as rpc:
            user_type = rpc.user_service.check_user_type_by_token(request.args["token"])
            if user_type != "admin":
                return pack_response(10001, "Not authorized")
            event_info = rpc.event_service.get_event_info(request.args['eventID'])
            if event_info:
                operated_user_id = event_info["target"]
                rpc.group_service.add_user_into_group(operated_user_id)
                if rpc.event_service.approve(request.args['eventID']) and \
                        rpc.user_service.verify_user(operated_user_id):
                    return pack_response()
            return pack_response(10003, "Data Error.")
    return pack_response(10002, "Missing argument")


@app.route("/api/v1/register/reject", methods=["GET"])
def reject_register():
    if check_params(request.args, ["token", "eventID"]):
        with ClusterRpcProxy(CONFIG) as rpc:
            user_type = rpc.user_service.check_user_type_by_token(request.args["token"])
            if user_type != "admin":
                return pack_response(10001, "Not authorized")
            event_info = rpc.event_service.get_event_info(request.args['eventID'])
            if rpc.event_service.reject(request.args['eventID']) and \
                    rpc.user_service.reject_user(event_info['target']):
                return pack_response()
            return pack_response(10003, "Data Error")
    return pack_response(10002, "Missing argument")


@app.route("/api/v1/changePassword", methods=['POST'])
def change_password():
    if check_params(request.json, ["token", "oldPassword", "newPassword"]):
        payload = request.get_json()
        with ClusterRpcProxy(CONFIG) as rpc:
            status, msg = rpc.user_service.change_password(payload["token"], payload["oldPassword"],
                                                           payload["newPassword"])
        return pack_response(status, msg)
    return pack_response(10002, "Missing argument")


@app.route("/api/v1/getGroupID", methods=['GET'])
def get_group_id():
    if check_params(request.args, ["userID"]):
        with ClusterRpcProxy(CONFIG) as rpc:
            groups = rpc.group_service.get_group_by_user_id(request.args['userID'])
            return pack_response(data={"groups": groups})


@app.route("/api/v1/addPosting", methods=['POST'])
def add_posting():
    if check_params(request.json, ['senderID', 'type', 'topic', 'message', 'gid']):
        Result = namedtuple("Result", ["event_id", "discussion_id", "posting_time"])
        if type(request.json['gid']) is not list:
            return pack_response(10002, "Argument format error")
        if request.json['type'] == 'discussion' and len(request.json['gid']) > 1:
            return pack_response(10002, "Argument format error")
        with ClusterRpcProxy(CONFIG) as rpc:
            for g in request.json['gid']:
                result = rpc.posting_service.add_posting(
                    sender_id=request.json['senderID'],
                    posting_type=request.json['type'],
                    topic=request.json['topic'],
                    message=request.json['message'],
                    gid=g
                )
        if result is False:
            return pack_response(10002, "No keyword found")
        if result is True:
            return pack_response()
        if result:
            result = Result._make(result)
            return pack_response(data={"eventID": result.event_id, "discussionID": result.discussion_id,
                                       "posting_time": result.posting_time})
        return pack_response(10003, "Data Error")
    return pack_response(10002, "Missing argument")


@app.route("/api/v1/getPosting", methods=['GET'])
def get_posting():
    if check_params(request.args, ["type", "userID"]):
        with ClusterRpcProxy(CONFIG) as rpc:
            posting_data = []
            groups = rpc.group_service.get_group_by_user_id(request.args['userID'])
            if request.args['type'] == "dissemination":
                for g in groups:
                    posting_data.append(rpc.posting_service.get_dissemination(g['gid']))
            else:
                for g in groups:
                    p_data = rpc.posting_service.get_discussions(g['gid'])
                    posting_data.extend(p_data)
                if len(posting_data) == 0:
                    return pack_response(10003, "Empty Data")
            return pack_response(data={"postings": posting_data})
    return pack_response(10002, "Missing argument")


@app.route("/api/v1/reply", methods=['POST'])
def reply_a_discussion():
    if check_params(request.json, ["senderID", "discussionID", "message"]):
        Result = namedtuple("Result", ["posting_id", "posting_time"])
        with ClusterRpcProxy(CONFIG) as rpc:
            result = rpc.posting_service.reply(request.json['senderID'], request.json['discussionID'],
                                               request.json['message'])
        if result is False:
            return pack_response(10002, "No keywords found")
        if result:
            result = Result._make(result)
            return pack_response(data={"postingID": result.posting_id, "posting_time": result.posting_time})
        return pack_response(10003, "Data Error")


@app.route("/api/v1/getReplies", methods=['GET'])
def get_relies_from_discussion():
    if check_params(request.args, ['discussionID', 'offset']):
        with ClusterRpcProxy(CONFIG) as rpc:
            limit = request.args['limit'] if 'limit' in request.args else None
            if limit:
                replies = rpc.posting_service.get_replies(request.args['discussionID'], limit, request.args['offset'])
            else:
                replies = rpc.posting_service.get_replies(request.args['discussionID'], offset=request.args['offset'])
        if replies:
            return pack_response(data={"replies": replies})
        return pack_response(10003, "Empty Data")
    return pack_response(10002, "Missing argument")


@app.route("/api/v1/getPostingList", methods=['GET'])
def get_posting_list():
    if check_params(request.args, ['token']):
        with ClusterRpcProxy(CONFIG) as rpc:
            user_type = rpc.user_service.check_user_type_by_token(request.args["token"])
            if user_type != "admin":
                return pack_response(10001, "Not authorized")
            data = rpc.posting_service.get_posting_list()
        if data:
            return pack_response(data={"posting_list": data})
        return pack_response(10003, "Empty Data")
    return pack_response(10002, "Missing Argument")


@app.route("/api/v1/posting/approve", methods=['GET'])
def approve_posting():
    if check_params(request.args, ['token', 'postingID']):
        with ClusterRpcProxy(CONFIG) as rpc:
            user_type = rpc.user_service.check_user_type_by_token(request.args["token"])
            if user_type != "admin":
                return pack_response(10001, "Not authorized")
            if rpc.posting_service.approve_posting(request.args['postingID']):
                return pack_response()
            return pack_response(10003, "Data Error")
    return pack_response(10002, "Missing Argument")


@app.route("/api/v1/posting/reject", methods=['GET'])
def reject_posting():
    if check_params(request.args, ['token', 'postingID']):
        with ClusterRpcProxy(CONFIG) as rpc:
            user_type = rpc.user_service.check_user_type_by_token(request.args["token"])
            if user_type != "admin":
                return pack_response(10001, "Not authorized")
            if rpc.posting_service.reject_posting(request.args['postingID']):
                return pack_response()
            return pack_response(10003, "Data Error")
    return pack_response(10002, "Missing Argument")


@app.route("/api/v1/searchUser", methods=['GET'])
def search_user():
    if check_params(request.args, ['username']):
        with ClusterRpcProxy(CONFIG) as rpc:
            like_users = rpc.user_service.search_user(request.args["username"])
            return pack_response(data={"users": like_users})
    return pack_response(10002, "Missing Argument")


@app.route("/api/v1/searchPosting", methods=['POST'])
def search_posting():
    if check_params(request.json, ['userID', 'topic', 'from', 'to', 'sender']):
        data = []
        with ClusterRpcProxy(CONFIG) as rpc:
            user_groups = rpc.group_service.get_group_by_user_id(request.json['userID'])
            if request.json['sender']:
                sender_groups = rpc.group_service.get_group_by_user_id(request.json['sender'])
                if len(set([x["gid"] for x in user_groups]) & set([y["gid"] for y in sender_groups])) == 0:
                    return pack_response(10002, "Not in the same group")
            start_date = transfer_timestamp(request.json['from'])
            end_date = transfer_timestamp(request.json['to'])
            if start_date is False or end_date is False:
                return pack_response(10002, "Argument Format Error")
            for gid in [x['gid'] for x in user_groups]:
                postings = rpc.posting_service.search_posting(
                    gid,
                    request.json['topic'],
                    start_date,
                    end_date,
                    request.json['sender']
                )
                for posting in postings:
                    replies = rpc.posting_service.get_replies(posting['discussion_id'])
                    if len(replies) > 0:
                        posting.update({"replies": replies})
                    data.append(posting)
            if len(data) == 0:
                return pack_response(msg="No result")
            return pack_response(data={"result": data})
    return pack_response(10002, "Missing Argument")


@app.route("/api/v1/cite", methods=['GET'])
def cite_posting():
    if check_params(request.args, ['userID', 'postingID', 'type', 'reason']):
        with ClusterRpcProxy(CONFIG) as rpc:
            if not rpc.posting_service.has_this_posting(request.args['postingID']):
                return pack_response(10002, "postingID error")
            if rpc.user_service.get_user_info(request.args['userID']) is None:
                return pack_response(10002, "userID error")
            if rpc.event_service.get_cite_event(request.args['postingID']):
                return pack_response(10002, "This posting has been cited.")
            event_id = rpc.event_service.add_event("cite", request.args['userID'], request.args['postingID'],
                                                   additional_info=request.args['type'] + "@@" + request.args['reason'])
            if event_id:
                return pack_response()
            return pack_response(10003, "Data Error")
    return pack_response(10002, "Missing Argument")


@app.route("/api/v1/getCiteList", methods=['GET'])
def get_cite_list():
    if check_params(request.args, ['token']):
        with ClusterRpcProxy(CONFIG) as rpc:
            user_type = rpc.user_service.check_user_type_by_token(request.args["token"])
            if user_type != "admin":
                return pack_response(10001, "Not authorized")
            cite_list = rpc.event_service.get_all_events("cite")
            data = []
            for cite_event in cite_list:
                posting_info = rpc.posting_service.get_posting_info(cite_event['target'])
                informer_info = rpc.user_service.get_user_info(cite_event["initiator"])
                sender_info = rpc.user_service.get_user_info(posting_info['sender'])
                data.append({
                    "eventID": cite_event['event_id'],
                    "postingID": posting_info['posting_id'],
                    "informerID": informer_info["user_id"],
                    "informerName": informer_info['full_name'],
                    "senderID": posting_info['sender'],
                    "senderName": sender_info['full_name'],
                    "posting_time": posting_info['posting_time'],
                    "topic": posting_info['topic'],
                    "message": posting_info['message'],
                    "type": cite_event['additional_info'].split("@@")[0],
                    "reason": cite_event['additional_info'].split("@@")[1]
                })
            return pack_response(data={"cite_list": data})
    return pack_response(10002, "Missing Argument")


@app.route("/api/v1/removeCitedPosting", methods=['GET'])
def remove_cited_posting():
    if check_params(request.args, ['postingID', 'token']):
        warning_msg = "Hi, Your posting {} is cited for being {}. We welcome relevant and respectful postings. " \
                      "This is a warning."
        with ClusterRpcProxy(CONFIG) as rpc:
            user_type = rpc.user_service.check_user_type_by_token(request.args["token"])
            if user_type != "admin":
                return pack_response(10001, "Not authorized")
            cite_event = rpc.event_service.get_cite_event(request.args['postingID'])
            if cite_event:
                rpc.event_service.approve(cite_event["event_id"])
                deleted_posting = rpc.posting_service.remove_a_posting(request.args['postingID'])
                user_email = rpc.user_service.get_user_info(deleted_posting["sender"])["email"]
                rpc.mail_service.send_mail(user_email, "Warning: someone cited your posting",
                                           warning_msg.format(deleted_posting['message'][:10] + "...",
                                                              cite_event['additional_info']))
                return pack_response()
            return pack_response(10003, "Data Error")
    return pack_response(10002, "Missing Argument")


@app.route("/api/v1/ignoreCite", methods=['GET'])
def ignore_cite():
    if check_params(request.args, ['eventID', 'token']):
        with ClusterRpcProxy(CONFIG) as rpc:
            user_type = rpc.user_service.check_user_type_by_token(request.args["token"])
            if user_type != "admin":
                return pack_response(10001, "Not authorized")
            rpc.event_service.reject(request.args['eventID'])
            return pack_response()
    return pack_response(10002, "Missing Argument")


@app.route("/api/v1/terminatePosting", methods=['GET'])
def terminate_posting():
    if check_params(request.args, ['postingID', 'userID']):
        # postingID -> discussionID
        with ClusterRpcProxy(CONFIG) as rpc:
            target_posting = rpc.posting_service.get_posting_info(request.args['postingID'])
            if target_posting is None:
                return pack_response(10002, "Posting ID error.")
            if target_posting['posting_type'] == "discussion" and target_posting['posting_status'] == "open":
                if target_posting['sender'] == request.args['userID']:
                    rpc.posting_service.terminate_a_posting(request.args["postingID"])
                    return pack_response()
            return pack_response(10002, "Not authorized or argument error")
    return pack_response(10002, "Missing Argument")


@app.route("/api/v1/getAllKeywords", methods=['GET'])
def get_all_keywords():
    if check_params(request.args, ['token']):
        with ClusterRpcProxy(CONFIG) as rpc:
            user_type = rpc.user_service.check_user_type_by_token(request.args["token"])
            if user_type != "admin":
                return pack_response(10001, "Not authorized")
            keywords = rpc.keyword_service.get_all_keywords()
            return pack_response(data={"keywords": keywords})
    return pack_response(10002, "Missing Argument")


@app.route("/api/v1/addKeyword", methods=['GET'])
def add_a_keywords():
    if check_params(request.args, ['keyword', 'token']):
        with ClusterRpcProxy(CONFIG) as rpc:
            user_type = rpc.user_service.check_user_type_by_token(request.args["token"])
            if user_type != "admin":
                return pack_response(10001, "Not authorized")
            if rpc.keyword_service.add_a_keyword(request.args['keyword']):
                return pack_response()
            return pack_response(10003, "Duplicate keyword")
    return pack_response(10002, "Missing Argument")


@app.route("/api/v1/removeKeyword", methods=['GET'])
def remove_a_keyword():
    if check_params(request.args, ['keyword', 'token']):
        with ClusterRpcProxy(CONFIG) as rpc:
            user_type = rpc.user_service.check_user_type_by_token(request.args["token"])
            if user_type != "admin":
                return pack_response(10001, "Not authorized")
            if rpc.keyword_service.remove_a_keyword(request.args['keyword']):
                return pack_response()
            return pack_response(10003, "Non-existed keyword")
    return pack_response(10002, "Missing Argument")


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
    res.headers.add('Access-Control-Allow-Origin', '*')
    return res


def transfer_timestamp(javascript_ts):
    if not javascript_ts:
        return None
    try:
        javascript_ts = str(javascript_ts)
        length = len(javascript_ts)
        ts = int(javascript_ts)
    except ValueError:
        return False
    if length != 13:
        return False
    return ts / 1000


def clean_params(params):
    _params = params.copy()
    for k, v in params.items():
        if not v:
            _params.pop(k)
    return _params


# def format_time(timestamp):
#     if type(timestamp) is datetime:
#         return timestamp.strftime("%m/%d/%Y %H:%M %p")
#     if type(timestamp) is str:
#         return timestamp
#     return timestamp


@app.before_request
def options_handler():
    if request.method == "OPTIONS":
        res = pack_response()
        res.headers['Access-Control-Allow-Methods'] = "GET, POST"
        res.headers['Access-Control-Allow-Headers'] = 'content-type'
        res.headers['Access-Control-Allow-Credentials'] = "true"
        res.headers['Access-Control-Allow-Origin'] = "*"
        res.headers['Access-Control-Max-Age'] = "1728000"
        return res
    else:
        pass


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
