import email
from email.utils import parsedate_tz


# parse a string email and return a dictionary.
def string_to_dict(email_str):
    message = email.message_from_string(email_str)
    if message and len(message.items()):
        return {
            'id': message.get('Message-ID'),
            'date': parsedate_tz(message.get('Date')),
            'from': split_email_addresses(message.get('From')),
            'to': split_email_addresses(message.get('To')),
            'cc': split_email_addresses(message.get('X-cc')),
            'bcc': split_email_addresses(message.get('X-bcc')),
            'subject': message.get('Subject'),
        }
    else:
        return None


# parse a text file object and return a dictionary.
def file_to_dict(f):
    message = email.message_from_file(f)
    if message and len(message.items()):
        return {
            'id': message.get('Message-ID'),
            'date': parsedate_tz(message.get('Date')),
            'from': message.get('From'),
            'to': split_email_addresses(message.get('To')),
            'cc': split_email_addresses(message.get('X-cc')),
            'bcc': split_email_addresses(message.get('X-bcc')),
            'subject': message.get('Subject'),
        }
    else:
        return None


# parse a string containing a comma separated list of email addresses.
# return a list
def split_email_addresses(addresses_str):
    if addresses_str:
        addresses = addresses_str.split(',')
        addresses = list(map(str.strip, addresses))
    else:
        addresses = []
    return addresses
