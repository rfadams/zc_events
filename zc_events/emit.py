import logging
import uuid

from django.conf import settings


logger = logging.getLogger('django')


class EmitEventException(Exception):
    pass


def emit_microservice_event(event_type, *args, **kwargs):
    import pika
    import ujson

    url = settings.BROKER_URL

    params = pika.URLParameters(url)
    params.socket_timeout = 5

    event_queue_name = '{}-events'.format(settings.SERVICE_NAME)

    connection = pika.BlockingConnection(params)

    channel = connection.channel()
    channel.queue_declare(queue=event_queue_name, durable=True)

    task_id = str(uuid.uuid4())

    keyword_args = {'task_id': task_id}
    keyword_args.update(kwargs)

    message = {
        'task': 'microservice.event',
        'id': task_id,
        'args': [event_type] + list(args),
        'kwargs': keyword_args
    }

    event_body = ujson.dumps(message)

    logger.info('MICROSERVICE_EVENT::EMIT: Emitting [{}:{}] event for object ({}:{}) and user {}'.format(
        event_type, task_id, kwargs.get('resource_type'), kwargs.get('resource_id'),
        kwargs.get('user_id')))

    response = channel.basic_publish('microservice-events', '', event_body, pika.BasicProperties(
        content_type='application/json', content_encoding='utf-8'))

    if not response:
        logger.info(
            'MICROSERVICE_EVENT::EMIT_FAILURE: Failure emitting [{}:{}] event for object ({}:{}) and user {}'.format(
                event_type, task_id, kwargs.get('resource_type'), kwargs.get('resource_id'), kwargs.get('user_id')))
        raise EmitEventException("Message may have failed to deliver")

    return response
