# -*- coding: UTF-8 -*-
import smtplib
from email.mime.text import MIMEText
from tools import LogManager
import traceback


def send(subject,text, receiver):
    try:
        sender = 'shiyunchao@ctfund.com'
        username = 'shiyunchao@ctfund.com'
        password = '~123qwe'
        msg = MIMEText(text, 'plain', 'utf-8')
        msg['Subject'] = subject
        smtp = smtplib.SMTP()
        smtp.connect('mail.ctfund.com')
        smtp.starttls()
        smtp.login(username, password)
        smtp.sendmail(sender, receiver, msg.as_string())
        smtp.quit()
    except :
        logger = LogManager.create_logger('EmailSender')
        msg = traceback.format_exc()
        logger.error(msg)

if __name__ == '__main__':
    send('test', 'test', ['369030612@qq.com'])