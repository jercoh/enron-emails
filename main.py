import os
import email
from collections import defaultdict
import pandas as pd


def split_email_addresses(addresses_str):
    # separate multiple email addresses
    if addresses_str:
        addresses = addresses_str.split(',')
        addresses = set(map(lambda x: x.strip(), addresses))
    else:
        addresses = set()
    return addresses


def main():
    direct_emails_recipient = defaultdict(int)
    broadcast_email_senders = defaultdict(int)

    for directory_name, dirs, files in os.walk("enron_with_categories"):
        for filename in files:
            if filename.endswith('.txt'):
                file_path = os.path.join(directory_name, filename)
                with open(file_path) as f:
                    msg = email.message_from_file(f)
                    recipients = split_email_addresses(msg.get('To'))
                    cced = split_email_addresses(msg.get('X-cc'))
                    bcced = split_email_addresses(msg.get('X-bcc'))
                    recipients_count = len(recipients) + len(cced) + len(bcced)

                    # check if it's a direct email
                    if recipients_count == 1:
                        direct_emails_recipient[(msg.get('To') or '').strip()] += 1
                    # check if it's a broadcast email
                    elif recipients_count > 1:
                        broadcast_email_senders[(msg.get('From') or '').strip()] += 1

    df_direct_emails = pd.DataFrame(list(direct_emails_recipient.iteritems()), columns=['recipient', 'emails_count'])
    df_broadcast_emails = pd.DataFrame(list(broadcast_email_senders.iteritems()), columns=['sender', 'emails_count'])

    largest_der = df_direct_emails.nlargest(10, 'emails_count')
    largest_bes = df_broadcast_emails.nlargest(10, 'emails_count')

    print('The 3 recipients that have received the largest number of direct emails are:', largest_der.values)
    print('The 3 senders that have sent the largest number of broadcast emails are:', largest_bes.values)


if __name__ == '__main__':
    main()
