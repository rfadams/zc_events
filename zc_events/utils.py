import datetime


def _get_attr(model_instance, attr_name):
    attr_value = getattr(model_instance, attr_name)

    if callable(attr_value):
        return attr_value()
    return attr_value


def model_to_dict(instance, included_attributes={}):
    """Returns a native python dict corresponding to the selected attributes of a model instance.

    Args:
        instance: model instance to serialize
        included_attributes: attributes or methods on instance.
            It should be formatted as {'attr_name_in_the_returned_dict': 'attr_name_on_instance'}
            attr_name_on_instance could be an attribute or a callable/method of the instance.
            You can also follow relationships as <relation>.<attribute> or <relation>.<some_function>.
            The relationship must be one-to-one. If it is one-to-many, the relationship attribute must be defined
            on instance.
    """
    data = {}

    for name, attr_name in included_attributes.iteritems():

        attr_value = instance
        for attr in attr_name.split('.'):
            attr_value = _get_attr(attr_value, attr)

            if not attr_value:
                break

        # Ensure that attr_value is a native python datatype
        if type(attr_value) in (datetime.date, datetime.datetime, datetime.time):
            attr_value = str(attr_value)

        if type(attr_value) not in (type(None), int, long, float, bool, str, unicode, list, dict):
            raise TypeError('Unexpected value for {} attribute. I found {}'.format(attr_name, type(attr_value)))

        data.setdefault(name, attr_value)

    return data


def notification_event_payload(resource_type, resource_id, user_id, meta):
    """Create event payload."""
    return {
        'resource_type': resource_type,
        'resource_id': resource_id,
        'user_id': user_id,
        'meta': meta
    }
