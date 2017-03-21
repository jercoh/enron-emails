import email
import datetime
from email.utils import parsedate_tz


# Parses a string email and return a dictionary.
def string_to_dict(email_str):
    message = email.message_from_string(email_str)
    if message and len(message.items()):
        # parse date with timezone, return a datetime object
        date_tuple = parsedate_tz(message.get('Date'))
        if date_tuple:
            date = datetime.datetime.fromtimestamp(email.utils.mktime_tz(date_tuple))
        return {
            'id': message.get('Message-ID'),
            'date': date,
            'sender': message.get('From').strip(),
            'recipients': get_recipients(message),
            'subject': message.get('Subject'),
        }
    else:
        return None


# Returns the set of recipients of an email, including CCs and BCCs.
def get_recipients(message):
    recipients = []
    to = message.get('To')
    cc = message.get('X-cc')
    bcc = message.get('X-bcc')

    if to:
        recipients += list(map(str.strip, to.split(',')))
    if cc:
        recipients += list(map(str.strip, cc.split(',')))
    if bcc:
        recipients += list(map(str.strip, bcc.split(',')))

    return list(set(recipients))
