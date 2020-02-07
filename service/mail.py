# coding=utf-8
from nameko.rpc import rpc
from flask_mail import Message, Mail


@rpc
def send_mail(app, sender, recipient, subject, content):
    mail = Mail(app)
    msg = Message(subject, sender=sender, recipients=[recipient])
    msg.html = "Your code is: <br/><b>000000</b><br/><span>Do not share your code to others!</span>"
    mail.send(msg)
