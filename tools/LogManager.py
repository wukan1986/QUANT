# -*- coding: utf-8 -*-

import logging
import datetime as dt
import os
import smtplib
from email.mime.text import MIMEText
import functools
import time
from xutils.custom_logger import CustomLogger
from time import strftime, localtime

def create_logger(name):
    logger = logging.getLogger(name)
    dir_path = os.path.split(os.path.realpath(__file__))[0]
    file_name = '{2}/Log/{0}-{1}.log'.format(name, dt.datetime.now().strftime('%Y-%m-%d'), dir_path)
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler(file_name)
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(thread)s %(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
    handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.addHandler(console_handler)
    return logger

def record(logger):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            text = func(*args, **kwargs)
            # t = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            # logger.info(t + ": " + text)
            # logger.info(text)
            logger.info(text.encode('gbk'))
            return text
        return wrapper
    return decorator


def _send(subject, text, sender, username, password, host, receiver):
    msg = MIMEText(text, 'plain', 'utf-8')
    msg['Subject'] = subject
    smtp = smtplib.SMTP()
    smtp.connect(host)
    smtp.starttls()
    smtp.login(username, password)
    smtp.sendmail(sender, receiver, msg.as_string())
    smtp.quit()

def email(**kw_decorator):
    def decorator(query_func):
        @functools.wraps(query_func)
        def wrapper(*args, **kwargs):
            if kw_decorator['subject'] is not None:
                f = open('F:/logger_file/log%s.log' % strftime("%Y%m%d", localtime()), "r")
                lines = f.readlines()
                msg = ''.join(lines)
                _send(subject=kw_decorator['subject'],
                      text=msg,
                      sender=kw_decorator['sender'],
                      username=kw_decorator['username'],
                      password=kw_decorator['password'],
                      host=kw_decorator['host'],
                      receiver=kw_decorator['receiver'])
                f.close()
            return query_func(*args, **kwargs)
        return wrapper
    return decorator