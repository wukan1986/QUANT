import smtplib
from email.mime.text import MIMEText
import LogManager
import traceback


def send(subject,text, receiver):
    try:
        sender = 'xuexiaoliang@ctfund.com'
        #receiver = ['xuexiaoliang@ctfund.com']
        username = 'xuexiaoliang@ctfund.com'
        password = 'Pass!word123'
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

