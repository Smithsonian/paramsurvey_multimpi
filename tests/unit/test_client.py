import os.path
import stat
import pytest

import requests_mock

from paramsurvey_multimpi import client


def test_pubkey(fs):  # pyfakefs
    pubfile = os.path.expanduser('~/.ssh/id_rsa.pub')
    fs.create_file(pubfile)

    fake = 'a fake public key\n'
    with open(pubfile, 'w') as f:
        f.write(fake)
    assert client.get_pubkey() == fake

    keyfile = os.path.expanduser('~/.ssh/authorized_keys')
    client.deploy_pubkey(fake)
    assert os.path.exists(keyfile)
    assert stat.filemode(os.stat(keyfile).st_mode) == '-rw-------'
    with open(keyfile) as f:
        assert f.read() == fake

    client.deploy_pubkey(fake)
    assert stat.filemode(os.stat(keyfile).st_mode) == '-rw-------'
    with open(keyfile) as f:
        assert f.read() == fake  # make sure it doesn't write twice


def test_jsonrpc_retries():
    with requests_mock.Mocker() as m:
        kwargs = {'status_code': 500}
        m.post(requests_mock.ANY, **kwargs)

        with pytest.raises(ValueError):
            for i in range(1000):
                ret = client.leader_checkin(1, 1, 1, 1, 1)
        assert i > 1, 'leader does not immediately crash'
        assert 'result' in ret, 'leader returns a result'
        assert ret['result'] is None, 'leader returns a None result'

        with pytest.raises(ValueError):
            for i in range(1000):
                ret = client.follower_checkin(1, 1, 1)
        assert i > 1
        assert 'result' in ret
        assert ret['result'] is None
