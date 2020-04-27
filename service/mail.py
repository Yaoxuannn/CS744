# coding=utf-8
import yagmail
from nameko.rpc import rpc

yag = yagmail.SMTP("camellia.userservice@gmail.com", oauth2_file='./oauth2_creds.json')


class MailService(object):
    name = "mail_service"

    @rpc
    def send_mail(self, to, subject, content):
        yag.send(to, subject, content)
