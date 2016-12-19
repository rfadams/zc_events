import sys
import uuid
import boto
from boto.s3.key import Key

from django.conf import settings


class S3IOException(Exception):
    pass


def save_string_contents_to_s3(stringified_data, aws_bucket_name, content_key=None,
                               aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
                               aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY):
    """Save data (provided in string format) to S3 bucket and return s3 key."""
    try:
        if not content_key:
            content_key = str(uuid.uuid4())

        connection = boto.connect_s3(aws_access_key_id, aws_secret_access_key)
        bucket = connection.get_bucket(aws_bucket_name)

        key = Key(bucket, content_key)
        key.set_contents_from_string(stringified_data)
        return content_key
    except StandardError as error:
        msg = 'Failed to save contents to S3. aws_bucket_name: {}, content_key: {}, ' \
              'error_message: {}'.format(aws_bucket_name, content_key, error.message)
        raise S3IOException(msg), None, sys.exc_info()[2]


def save_file_contents_to_s3(filepath, aws_bucket_name, content_key=None,
                             aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
                             aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY):
    """Upload a local file to S3 bucket and return S3 key."""
    try:
        if not content_key:
            content_key = str(uuid.uuid4())

        connection = boto.connect_s3(aws_access_key_id, aws_secret_access_key)
        bucket = connection.get_bucket(aws_bucket_name)

        k = Key(bucket, content_key)
        k.set_contents_from_filename(filepath)
        return content_key
    except StandardError as error:
        msg = 'Failed to save contents to S3. filepath: {}, aws_bucket_name: {}, content_key: {}, ' \
              'error_message: {}'.format(filepath, aws_bucket_name, content_key, error.message)
        raise S3IOException(msg), None, sys.exc_info()[2]


def read_s3_file_as_string(aws_bucket_name, content_key, delete=False,
                           aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
                           aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY):
    """Get the contents of an S3 file as string and optionally delete the file from the bucket."""
    try:
        connection = boto.connect_s3(aws_access_key_id, aws_secret_access_key)
        bucket = connection.get_bucket(aws_bucket_name)

        key = Key(bucket, content_key)
        output = key.get_contents_as_string()

        if delete:
            key.delete()

        return output
    except StandardError as error:
        msg = 'Failed to save contents to S3. aws_bucket_name: {}, content_key: {}, delete: {}, ' \
              'error_message: {}'.format(aws_bucket_name, content_key, delete, error.message)
        raise S3IOException(msg), None, sys.exc_info()[2]
