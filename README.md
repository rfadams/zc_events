# zc_events

# Event Based Communication

In order to send and receive marketplace events through RabbitMQ there is some configuration that needs to occur at the service level.

## Settings changes

In the `settings.py` file, add the following lines

```python
events_exchange = Exchange('microservice-events', type='fanout')

CELERY_QUEUES = (
    # Add this line
    Queue(SERVICE_NAME + '-events', events_exchange),
)

CELERY_ROUTES = ('zc_events.routers.TaskRouter', )
```

## BROKER_URL

Make sure that the `BROKER_URL` either in ``.env.sample` or in `settings.py` if a default is defined there is in the following format. The old style of the trailing `//` won't work for development when emitting events.

```
BROKER_URL=amqp://guest:guest@rabbitmq:5672/%2F
```

## New requirements

Sending and receiving events requires the `pika` and `ujson` libraries to be installed via pip and added to the `requirements.txt` file.

## Set up service for listening to events

Create a file called `microservice_events.py` and place it somewhere like the `mp-slots-and-orders/slots_and_orders/tasks/` directory. For now this will be an empty file, but in the future this is where any events that this service listens to will go.

Next create a file called `microservice_events_listener.py` and place it in the same directory. This file will have the following contents

```python
import logging
from ..celery import app
from . import microservice_events

@app.task(name='microservice.event')
def microservice_event(event_type, *args, **kwargs):
    if hasattr(microservice_events, event_type):
        logger.info('MICROSERVICE_EVENT::PROCESSED: Received [{}:{}] event for object ({}:{}) and user {}'.format(
            event_type, kwargs.get('task_id'), kwargs.get('resource_type'), kwargs.get('resource_id'), kwargs.get('user_id')))

        getattr(microservice_events, event_type)(*args, **kwargs)
    else:
        logger.info('MICROSERVICE_EVENT::IGNORED: Received [{}:{}] event for object ({}:{}) and user {}'.format(
            event_type, kwargs.get('task_id'), kwargs.get('resource_type'), kwargs.get('resource_id'), kwargs.get('user_id')))

```

Note: Feel free to use explicit imports here, just make sure you're importing the correct things. They are specified as relative here to be as generic as possible.

Go back to `settings.py` and add the new event listener file to `CELERY_IMPORTS` which you may need to create if it doesn't already exist

```python
CELERY_IMPORTS = (
    'slots_and_orders.tasks.microservice_events_listener',
)
```
