from datetime import date
import time

import boto
from boto.s3.key import Key
from django.conf import settings


S3_BUCKET_NAME = 'zc-mp-email'


class MissingCredentialsError(Exception):
    pass


def get_s3_email_bucket():
    aws_access_key_id = settings.AWS_ACCESS_KEY_ID
    aws_secret_access_key = settings.AWS_SECRET_ACCESS_KEY
    if not (aws_access_key_id and aws_secret_access_key):
        msg = 'You need to set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in your settings file.'
        raise MissingCredentialsError(msg)

    conn = boto.connect_s3(aws_access_key_id, aws_secret_access_key)
    bucket = conn.get_bucket(S3_BUCKET_NAME)
    return bucket


def generate_s3_folder_name(email_uuid):
    email_date = date.today().isoformat()
    email_timestamp = int(time.time())
    return "{}/{}_{}".format(email_date, email_timestamp, email_uuid)


def generate_s3_content_key(s3_folder_name, content_type, content_name=''):
    content_key = "{}/{}".format(s3_folder_name, content_type)
    if content_name:
        content_key += '_{}'.format(content_name)
    return content_key


def upload_string_to_s3(bucket, content_key, content):
    if content:
        k = Key(bucket)
        k.key = content_key
        k.set_contents_from_string(content)


def upload_file_to_s3(bucket, content_key, filename):
    if filename:
        k = Key(bucket)
        k.key = content_key
        k.set_contents_from_filename(filename)
