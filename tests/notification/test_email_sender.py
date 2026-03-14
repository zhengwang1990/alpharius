import argparse

import pytest

import alpharius.data as data
from alpharius.notification import email_sender

from ..fakes import FakeDataClient


def test_send_summary(mock_smtp):
    client = email_sender.EmailSender()
    client.send_summary(FakeDataClient())

    mock_smtp.assert_called_once()


def test_send_alert(mock_smtp):
    client = email_sender.EmailSender()
    client.send_alert('error_message')

    mock_smtp.assert_called_once()


@pytest.mark.parametrize('mode', ['summary', 'alert'])
def test_main(mode, mocker, mock_smtp):
    mocker.patch.object(
        argparse.ArgumentParser, 'parse_args', return_value=argparse.Namespace(mode=mode, error_message='fake message')
    )
    mocker.patch.object(data, 'get_default_data_client', return_value=FakeDataClient())

    email_sender.main()

    mock_smtp.assert_called_once()
