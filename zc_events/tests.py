from __future__ import division

import ujson
import math

from django.conf import settings

from zc_events.utils import notification_event_payload


class GlobalIndexRebuildTestMixin(object):
    """This is a base test case for all index rebuilding events.
    See in legacy (zerocater.tasks.emit_event_tasks.index_data_events.py module) how to use it.
    """

    index_rebuild_event_task = None
    resource_index_rebuild_task = None
    model = None
    attributes = []
    resource_type = None
    event_name = None
    serializer = None

    # Optional
    queryset = None
    objects_count = 10
    default_batch_size = 50
    custom_batch_size = 5

    def setUp(self):
        super(GlobalIndexRebuildTestMixin, self).setUp()
        self.create_test_data()
        self._queryset = self.queryset if self.queryset else self.model.objects.all()

    def create_test_data(self):
        raise NotImplementedError("Override this method to create test data")

    def test_emitting_event_with_default_batch_size__pass(self, mock_save_string_contents_to_s3,
                                                          mock_emit_microservice_event):
        self.resource_index_rebuild_task()

        events_count = int(math.ceil(self.objects_count / self.default_batch_size))
        self.assertEqual(mock_save_string_contents_to_s3.call_count, events_count)
        self.assertEqual(mock_emit_microservice_event.call_count, events_count)

    def test_emitting_event_with_custom_batch_size__pass(self, mock_save_string_contents_to_s3,
                                                         mock_emit_microservice_event):
        self.resource_index_rebuild_task(batch_size=self.custom_batch_size)

        events_count = int(math.ceil(self.objects_count / self.custom_batch_size))
        self.assertEqual(mock_save_string_contents_to_s3.call_count, events_count)
        self.assertEqual(mock_emit_microservice_event.call_count, events_count)

    def test_emitting_event_with_correct_attributes__pass(self, mock_save_string_contents_to_s3,
                                                          mock_emit_microservice_event):
        s3_key = 'k'
        mock_save_string_contents_to_s3.side_effect = s3_key

        self.resource_index_rebuild_task()

        data = []
        payloads = []

        total_events = int(math.ceil(self.objects_count / self.default_batch_size))

        for i in xrange(total_events):
            start_index = i * self.default_batch_size
            end_index = start_index + self.default_batch_size

            instance_data = [
                self.serializer.__func__(instance)
                for instance in self._queryset.order_by('id')[start_index:end_index]
            ]

            pld = notification_event_payload(self.resource_type, None, None, {'s3_key': s3_key})

            data.append(instance_data)
            payloads.append(pld)

        for instance_data, payload in zip(data, payloads):
            stringified_data = ujson.dumps(instance_data)
            mock_save_string_contents_to_s3.assert_any_call(stringified_data, settings.AWS_INDEXER_BUCKET_NAME)
            mock_emit_microservice_event.assert_any_call(self.event_name, **payload)

    def test_filtering_data_works__pass(self, mock_save_string_contents_to_s3, mock_emit_microservice_event):
        # If both model and queryset are defined, this test ensures filters applied on the queryset returns less
        # objects than using model.objects.all()
        if self.queryset and self.model:
            msg = 'Both queryset and model.objects.all() returns same objects count. ' \
                  'Make sure processed objects differ when using both querysets. ' \
                  'Otherwise, set queryset to None.'
            self.assertTrue(self.queryset.count() < self.model.objects.count(), msg)
