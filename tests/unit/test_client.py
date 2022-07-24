import os.path
import stat
import socket
import sys
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


def test_unkey():
    assert client.unkey('foo_1')[0] == 'foo'
    assert client.unkey('foo_bar_1')[0] == 'foo_bar'


def test_unique_resources():
    me = socket.gethostname()
    ret = {'lcores': 3,
           'followers': [
               {'fkey': 'foo_1', 'cores': 3},
               {'fkey': 'foo_2', 'cores': 3},
           ]}
    sums = client.unique_resources(ret)
    assert sums == {'foo': 6, me: 3}


def test_machinefile_openmpi():
    me = socket.gethostname()
    user_kwargs = {'mpi': 'openmpi'}

    ret = {'lcores': 3,
           'followers': []}
    machinesfile = client.machinefile_openmpi({}, ret, 2, user_kwargs)
    assert machinesfile == me+' slots=3\n'
    with pytest.raises(ValueError):
        machinesfile = client.machinefile_openmpi({}, ret, 4, user_kwargs)
        assert machinesfile == me+' slots=3\n', 'too few cores raises'

    ret = {'lcores': 3,
           'followers': [
               {'fkey': 'foo_1', 'cores': 3},
               {'fkey': 'foo_2', 'cores': 3},
           ]}
    machinesfile = client.machinefile_openmpi({}, ret, 9, {'mpi': 'openmpi'})
    assert machinesfile == me+' slots=3\nfoo slots=6\n'

    ret = {'lcores': 3,
           'followers': [
               {'fkey': 'foo1_1', 'cores': 3},
               {'fkey': 'foo2_2', 'cores': 3},
           ]}
    machinesfile = client.machinefile_openmpi({}, ret, 9, {'mpi': 'openmpi'})
    assert machinesfile == me+' slots=3\nfoo1 slots=3\nfoo2 slots=3\n'


def test_openmpi_DiFX_machinefile(fs):  # pyfakefs
    me = socket.gethostname()
    jobname = 'test_openmpi_DiFX'
    user_kwargs = {'mpi': 'openmpi', 'machinefile': 'DiFX',
                   'DiFX_datastreams': 3, 'DiFX_jobname': jobname}
    sums = {me: 3, 'foo': 6}

    ret = client.machinefile_openmp_DiFX(user_kwargs, sums.copy())
    assert ret == ('{}\n{}\nfoo\n{}\nfoo\n'.format(me, me, me), '5\n')

    user_kwargs['DiFX_datastreams'] = 2
    ret = client.machinefile_openmp_DiFX(user_kwargs, sums.copy())
    assert ret == ('{}\n{}\nfoo\n{}\nfoo\n'.format(me, me, me), '1\n5\n')

    ret = client.machinefile_openmp_DiFX_file(user_kwargs, sums.copy())
    assert ret == '', 'machinesfile empty and files created'
    for suffix in ('.machines', '.threads'):
        # this will raise FileNotFoundError if not found
        os.remove(jobname + suffix)

    # 1 node, 3 datastreams
    # 3 nodes, 2 datstreams
    # 3 nodes, 4 datstreams
    # 3 nodes of 1 core each, 2 datstreams
    # too few cores
    pass
