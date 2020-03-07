# coding=utf-8
from nameko.rpc import rpc
import yagmail

yag = yagmail.SMTP("camellia.userservice@gmail.com", "Camellia_service")


class MailService(object):
    name = "mail_service"

    @rpc
    def send_mail(self, to, subject, content):
        yag.send(to, subject, content)
