import six
from datetime import date
import time

from zc_events.aws import save_string_contents_to_s3, save_file_contents_to_s3

S3_BUCKET_NAME = 'zc-mp-email'


def generate_s3_folder_name(email_uuid):
    email_date = date.today().isoformat()
    email_timestamp = int(time.time())
    return "{}/{}_{}".format(email_date, email_timestamp, email_uuid)


def generate_s3_content_key(s3_folder_name, content_type, content_name=''):
    content_key = "{}/{}".format(s3_folder_name, content_type)
    if content_name:
        content_key += '_{}'.format(content_name)
    return content_key


def generate_email_data(email_uuid, from_email=None, to=None, cc=None, bcc=None, reply_to=None, subject=None,
                        plaintext_body=None, html_body=None, headers=None, files=None, attachments=None,
                        user_id=None, resource_type=None, resource_id=None):
    """
    files:       A list of file paths
    attachments: A list of tuples of the format (filename, content_type, content)
    """

    s3_folder_name = generate_s3_folder_name(email_uuid)

    to = to.split(',') if isinstance(to, six.string_types) else to
    cc = cc.split(',') if isinstance(cc, six.string_types) else cc
    bcc = bcc.split(',') if isinstance(bcc, six.string_types) else bcc
    reply_to = reply_to.split(',') if isinstance(reply_to, six.string_types) else reply_to
    for arg in (to, cc, bcc, reply_to):
        if arg and not isinstance(arg, list):
            msg = "Keyword arguments 'to', 'cc', 'bcc', and 'reply_to' should be of <type 'list'>"
            raise TypeError(msg)

    if not any([to, cc, bcc, reply_to]):
        msg = "Keyword arguments 'to', 'cc', 'bcc', and 'reply_to' can't all be empty"
        raise TypeError(msg)

    html_body_key = None
    if html_body:
        html_body_key = generate_s3_content_key(s3_folder_name, 'html')
        save_string_contents_to_s3(html_body, S3_BUCKET_NAME, html_body_key)

    plaintext_body_key = None
    if plaintext_body:
        plaintext_body_key = generate_s3_content_key(s3_folder_name, 'plaintext')
        save_string_contents_to_s3(plaintext_body, S3_BUCKET_NAME, plaintext_body_key)

    attachments_keys = []
    if attachments:
        for filename, mimetype, attachment in attachments:
            attachment_key = generate_s3_content_key(s3_folder_name, 'attachment',
                                                     content_name=filename)
            save_string_contents_to_s3(attachment, S3_BUCKET_NAME, attachment_key)
            attachments_keys.append(attachment_key)
    if files:
        for filepath in files:
            filename = filepath.split('/')[-1]
            attachment_key = generate_s3_content_key(s3_folder_name, 'attachment',
                                                     content_name=filename)
            save_file_contents_to_s3(filepath, S3_BUCKET_NAME, attachment_key)
            attachments_keys.append(attachment_key)

    event_data = {
        'from_email': from_email,
        'to': to,
        'cc': cc,
        'bcc': bcc,
        'reply_to': reply_to,
        'subject': subject,
        'plaintext_body_key': plaintext_body_key,
        'html_body_key': html_body_key,
        'attachments_keys': attachments_keys,
        'headers': headers,
        'user_id': user_id,
        'resource_type': resource_type,
        'resource_id': resource_id,
        'task_id': str(email_uuid)
    }

    return event_data
