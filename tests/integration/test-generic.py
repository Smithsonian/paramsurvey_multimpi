import subprocess
import os
import os.path
from io import StringIO
import sys
import signal

import pytest

import paramsurvey
import paramsurvey_multimpi
import paramsurvey_multimpi.client as client


def test_generic():
    tests = {
        'multiprocessing_test': {
            'backend': 'multiprocessing',
            'ncores': 4,
            'exe': './a.out',
            'tests': [
                {
                    'resources': '4x1',
                    'returncode': 0,
                    'stderr': '',
                },
                {
                    'resources': '2x2',
                    'exception': ValueError,  # multiprocessing only allows ncores=1
                },
            ],
        },
        'ray_test': {
            'backend': 'ray',
            'exe': './a.out',
            'expect_min_ncores': 4,
            'tests': [
                {
                    'resources': '2x2',
                    'returncode': 0,
                    'stderr': '',
                },
                {
                    'resources': '2x2',
                    'returncode': 0,
                    'stderr': '',
                },
            ],
        },
    }

    name = os.environ['TEST_GENERIC']
    if name not in tests:
        raise ValueError('unknown test name {}, options are: {}'.format(name, list(tests.keys())))
    tests = tests[name]

    with pytest.raises(ValueError):
        client.start_multimpi_server(hostport='localhost:8889')

    user_kwargs = {}
    proc = client.start_multimpi_server(hostport='localhost:8889', user_kwargs=user_kwargs)
    user_kwargs['mpi'] = 'openmpi'

    pslogger_fd = StringIO()
    kwargs = {
        'backend': tests['backend'],
        'pslogger_fd': pslogger_fd,
    }
    if 'ncores' in tests:
        kwargs['ncores'] = tests.get('ncores')

    paramsurvey.init(**kwargs)

    mydir = os.path.dirname(paramsurvey_multimpi.__file__) + '/../tests/integration'

    def fetch(name, t, tests):
        r = t.get(name)
        print('r1', r)
        if r is None:
            r = tests.get(name)
            print('r2', r)
        return r

    def inflate(t, tests):
        # apply defaults
        # what's the other way to do this? capture_output but where
        user_kwargs.update({'run_kwargs': {
            'stdout': subprocess.PIPE, 'encoding': 'utf-8',
            'stderr': subprocess.PIPE, 'encoding': 'utf-8',
        }})

        resources = fetch('resources', t, tests)
        nodes, ncores = resources.split('x', 1)
        nodes = int(nodes)
        ncores = int(ncores)
        wanted = nodes * ncores

        if tests['backend'] == 'multiprocessing' and ncores > 1:
            raise ValueError('multiprocessing backend only supports ncores == 1, e.g. 4x1. Saw {}'.format(resources))
        if tests['backend'] == 'ray':
            expect_min_ncores = int(fetch('expect_min_ncores', t, tests) or 0)
            if expect_min_ncores > paramsurvey.current_core_count():
                raise ValueError('ray cluster is not large enough, need at least {}'.format(expect_min_ncores))

        exe = fetch('exe', t, tests)

        run_args = 'mpirun --machinefile %MACHINEFILE% --oversubscribe -np {} {}'.format(wanted, exe)

        psets = [{'kind': 'leader', 'ncores': ncores, 'run_args': run_args, 'wanted': wanted}]

        followers = nodes - 1
        for _ in range(followers):
            psets.append({'kind': 'follower', 'ncores': ncores})

        if tests['backend'] == 'ray':
            # this is how ray backend args are specified
            # XXX shouldn't paramsurvey hide this?
            for p in psets:
                if 'ncores' in p:
                    p['ray'] = {'num_cores': p.get('ncores')}

        return psets, user_kwargs

    for t in tests['tests']:
        exception = fetch('exception', t, tests)

        if exception is not None:
            with pytest.raises(exception):
                psets, user_kwargs = inflate(t, tests)
            continue  # this is the only test allowed

        psets, user_kwargs = inflate(t, tests)
        results = paramsurvey.map(client.multimpi_worker, psets, user_kwargs=user_kwargs, chdir=mydir)
        assert results.progress.total == len(psets)
        assert results.progress.failures == 0
        assert results.progress.exceptions == 0

        # XXX split leaders and followers
        # XXX count the followers, maybe some generic tests?

        returncode = fetch('returncode', t, tests)
        for r in results.itertuples():
            if r.kind == 'follower':
                continue
            print(repr(r))
            assert r.cli.returncode == returncode

    print(pslogger_fd.getvalue(), file=sys.stderr)
    proc.send_signal(signal.SIGHUP)
    try:
        proc.communicate(timeout=5.0)
    except subprocess.TimeoutExpired:
        assert False, 'server process did not exit after 1 SIGHUP'


def test_signals():
    proc = client.start_multimpi_server(hostport='localhost:8889', user_kwargs={})

    # 1-2 ^C does not make it exit
    proc.send_signal(signal.SIGHUP)
    assert proc.poll() is None, 'server does not exit after 1 SIGINT'
    proc.send_signal(signal.SIGHUP)
    assert proc.poll() is None, 'server does not exit after 2 SIGINT'

    # 1 HUP does make it exit
    proc.send_signal(signal.SIGHUP)
    try:
        proc.communicate(timeout=5.0)
    except subprocess.TimeoutExpired:
        assert False, 'server process did not exit after 1 SIGHUP'
