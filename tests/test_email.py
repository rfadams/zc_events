import mock
from unittest import TestCase

from zc_events.client import EventClient


class EmailTests(TestCase):

    def setUp(self):
        self.send_email_kwargs = {
            'from_email': 'from@test.com',
            'to': ['to@test.com'],
            'cc': ['cc_1@test.com', 'cc_2@test.com'],
            'bcc': ['bcc_1@test.com', 'bcc_2@test.com'],
            'reply_to': ['reply_to@test.com'],
            'subject': 'email subject',
            'html_body': '<p><strong>HTML</strong> email content</p>',
            'plaintext_body': 'Plaintext email content',
        }
        self.expected_event_args = ['from_email', 'to', 'cc', 'bcc', 'reply_to',
                                    'subject', 'html_body_key', 'plaintext_body_key',
                                    'headers', 'attachments_keys']
        self.event_client = EventClient()

    @mock.patch('boto.connect_s3', side_effect=Exception)
    def test_send_email__connection_fail(self, mock_connect_s3):
        with self.assertRaises(Exception):
            self.event_client.send_email()

    @mock.patch('zc_events.email.save_string_contents_to_s3')
    @mock.patch('zc_events.client.EventClient.emit_microservice_event')
    def test_send_email(self, mock_emit_event, mock_save_string_contents_to_s3):
        self.event_client.send_email(**self.send_email_kwargs)

        mock_emit_event.assert_called_once()
        event_args = mock_emit_event.call_args_list[0][1]

        for expected_arg in self.expected_event_args:
            self.assertTrue(expected_arg in event_args)

        self.assertTrue(event_args['task_id'])

    @mock.patch('zc_events.email.save_string_contents_to_s3')
    @mock.patch('zc_events.client.EventClient.emit_microservice_event')
    def test_send_email_no_html_body(self, mock_emit_event, mock_save_string_contents_to_s3):
        self.send_email_kwargs['html_body'] = ''
        self.event_client.send_email(**self.send_email_kwargs)

        mock_emit_event.assert_called_once()
        event_args = mock_emit_event.call_args_list[0][1]

        for expected_arg in self.expected_event_args:
            self.assertTrue(expected_arg in event_args)

        self.assertEqual(event_args['html_body_key'], None)
        self.assertNotEqual(event_args['plaintext_body_key'], None)

    @mock.patch('zc_events.email.save_string_contents_to_s3')
    @mock.patch('zc_events.client.EventClient.emit_microservice_event')
    def test_send_email_no_plaintext_body(self, mock_emit_event, mock_save_string_contents_to_s3):
        self.send_email_kwargs['plaintext_body'] = ''
        self.event_client.send_email(**self.send_email_kwargs)

        mock_emit_event.assert_called_once()
        event_args = mock_emit_event.call_args_list[0][1]

        for expected_arg in self.expected_event_args:
            self.assertTrue(expected_arg in event_args)

        self.assertEqual(event_args['plaintext_body_key'], None)
        self.assertNotEqual(event_args['html_body_key'], None)

    @mock.patch('zc_events.email.save_file_contents_to_s3')
    @mock.patch('zc_events.email.save_string_contents_to_s3')
    @mock.patch('zc_events.client.EventClient.emit_microservice_event')
    def test_send_email_multiple_attachments(self, mock_emit_event, mock_save_string_contents_to_s3,
                                             mock_save_file_contents_to_s3):
        attachments = [
            ('file1.pdf', 'application/pdf', None),
            ('file2.pdf', 'application/pdf', None),
            ('file3.pdf', 'application/pdf', None),
        ]
        self.send_email_kwargs['attachments'] = attachments

        self.event_client.send_email(**self.send_email_kwargs)
        mock_emit_event.assert_called_once()
        attachments_keys = mock_emit_event.call_args_list[0][1]['attachments_keys']
        self.assertEqual(len(attachments_keys), len(attachments))

    @mock.patch('zc_events.email.save_file_contents_to_s3')
    @mock.patch('zc_events.email.save_string_contents_to_s3')
    @mock.patch('zc_events.client.EventClient.emit_microservice_event')
    def test_send_email_files_and_attachments(self, mock_emit_event, mock_save_string_contents_to_s3,
                                              mock_save_file_contents_to_s3):
        attachments = [('file1.pdf', 'application/pdf', None), ]
        files = ['tests/file1.pdf']
        self.send_email_kwargs['attachments'] = attachments
        self.send_email_kwargs['files'] = files

        self.event_client.send_email(**self.send_email_kwargs)
        mock_emit_event.assert_called_once()
        attachments_keys = mock_emit_event.call_args_list[0][1]['attachments_keys']
        self.assertEqual(len(attachments_keys), len(attachments) + len(files))

    @mock.patch('zc_events.email.save_string_contents_to_s3')
    @mock.patch('zc_events.client.EventClient.emit_microservice_event')
    def test_send_email_headers(self, mock_emit_event, mock_save_string_contents_to_s3):
        headers = {'send_email_header': True}
        self.send_email_kwargs['headers'] = headers

        self.event_client.send_email(**self.send_email_kwargs)
        mock_emit_event.assert_called_once()
        headers = mock_emit_event.call_args_list[0][1]['headers']
        self.assertEqual(len(headers), len(headers))

    @mock.patch('zc_events.email.save_string_contents_to_s3')
    @mock.patch('zc_events.client.EventClient.emit_microservice_event')
    def test_send_email_use_string_parameters(self, mock_emit_event, mock_save_string_contents_to_s3):
        to = 'to_string@zerocater.com'
        cc = 'cc_string_1@zerocater.com,cc_string_2@zerocater.com'
        bcc = 'bcc_string_1@zerocater.com'
        reply_to = 'reply_to_string@zerocater.com'
        self.send_email_kwargs['to'] = to
        self.send_email_kwargs['cc'] = cc
        self.send_email_kwargs['bcc'] = bcc
        self.send_email_kwargs['reply_to'] = reply_to
        self.event_client.send_email(**self.send_email_kwargs)

        mock_emit_event.assert_called_once()
        event_to = mock_emit_event.call_args_list[0][1]['to']
        event_cc = mock_emit_event.call_args_list[0][1]['cc']
        event_bcc = mock_emit_event.call_args_list[0][1]['bcc']
        event_reply_to = mock_emit_event.call_args_list[0][1]['reply_to']
        self.assertEqual(event_to, to.split(','))
        self.assertEqual(event_cc, cc.split(','))
        self.assertEqual(event_bcc, bcc.split(','))
        self.assertEqual(event_reply_to, reply_to.split(','))

    @mock.patch('zc_events.email.save_string_contents_to_s3')
    @mock.patch('zc_events.client.EventClient.emit_microservice_event')
    def test_send_email_invalid_to__fail(self, mock_emit_event, mock_save_string_contents_to_s3):
        to = {'to': 'to_string@zerocater.com'}
        self.send_email_kwargs['to'] = to

        with self.assertRaises(TypeError):
            self.event_client.send_email(**self.send_email_kwargs)

        mock_emit_event.assert_not_called()

    @mock.patch('zc_events.email.save_string_contents_to_s3')
    @mock.patch('zc_events.client.EventClient.emit_microservice_event')
    def test_send_email_no_recipients__fail(self, mock_emit_event, mock_save_string_contents_to_s3):
        self.send_email_kwargs['to'] = None
        self.send_email_kwargs['cc'] = None
        self.send_email_kwargs['bcc'] = None
        self.send_email_kwargs['reply_to'] = None

        with self.assertRaises(TypeError):
            self.event_client.send_email(**self.send_email_kwargs)

        mock_emit_event.assert_not_called()
