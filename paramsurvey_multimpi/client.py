import os
import os.path
import socket
import subprocess
import time
import signal
import sys
import functools

import requests

import paramsurvey_multimpi


url = None
timeout = (4, 1)  # connect, read
sigint_count = 0
leader_exceptions = []
follower_exceptions = []
helper_server_proc = None


def get_pubkey():
    pub = os.path.expanduser('~/.ssh/id_rsa.pub')
    if not os.path.isfile(pub):
        print('No public key found (.ssh/id_rsa.pub), assuming empty is ok', file=sys.stderr)
        return ''
    with open(pub) as f:
        return f.read()


def deploy_pubkey(pubkey):
    keyfile = os.path.expanduser('~/.ssh/authorized_keys')
    if os.path.exists(keyfile):
        with open(keyfile) as f:
            existing = f.read()
        if pubkey in existing:
            return

    with open(keyfile, 'a') as f:  # XXX will fail if .ssh/ does not exist
        f.write(pubkey)
    os.chmod(keyfile, 0o600)


def leader_checkin(cores, wanted_cores, pubkey, state, lseq):
    pid = os.getpid()
    ip = socket.gethostname()
    payload = {
        'method': 'leader_checkin',
        'params': [ip, cores, pid, wanted_cores, pubkey, state, lseq],
        'jsonrpc': '2.0',
        'id': 0,
    }

    try:
        response = requests.post(url, json=payload, timeout=timeout).json()
        leader_exceptions.clear()
    except Exception as e:
        leader_exceptions.append(str(e))
        if len(leader_exceptions) > 100:
            raise ValueError('too many leader_checkin exceptions ({})'.format(len(leader_exceptions))) from e
        response = {'result': None}  # clients expect this
    return response


def follower_checkin(cores, state, fseq):
    pid = os.getpid()
    ip = socket.gethostname()
    payload = {
        'method': 'follower_checkin',
        'params': [ip, cores, pid, state, fseq],
        'jsonrpc': '2.0',
        'id': 0,
    }

    try:
        response = requests.post(url, json=payload, timeout=timeout).json()
        follower_exceptions.clear()
    except Exception as e:
        follower_exceptions.append(str(e))
        if len(follower_exceptions) > 100:
            raise ValueError('too many follower_checkin exceptions ({})'.format(len(follower_exceptions))) from e
        response = {'result': None}  # clients expect this
    return response


def hello_world():
    payload = {
        'method': 'hello_world',
        'params': [],
        'jsonrpc': '2.0',
        'id': 0,
    }
    try:
        response = requests.post(url, json=payload, timeout=timeout).json()
        #print(response, file=sys.stderr)
        assert response['result']['hello'] == 'world!'
    except Exception as e:
        return 'hello_world fail: '+str(e)
    return 'pass'


def leader_start_mpi(pset, ret, wanted, user_kwargs):
    # XXX generate special difx hostfile
    # ret['followers'] is a list of fkeys and core counts

    cmd = pset['run_args'].format(int(wanted)).split()
    #print('leader {} about to run'.format(os.getpid()), cmd)
    run_kwargs = pset.get('run_kwargs') or user_kwargs.get('run_kwargs') or {}
    mpi_proc = run_mpi(cmd, **run_kwargs)
    #print('leader just ran MPI and mpi_proc is', mpi_proc)
    return mpi_proc


def leader(pset, system_kwargs, user_kwargs):
    #print('I am leader and my pid is {}'.format(os.getpid()))
    pubkey = get_pubkey()
    mpi_proc = None
    ncores = pset['ncores']

    lseq = 0
    state = 'waiting'
    wanted = pset['wanted']

    #print('I am leader before loop')
    while True:
        #print('I am leader {} top of loop'.format(os.getpid()))
        sys.stdout.flush()
        ret = leader_checkin(ncores, wanted, pubkey, state, lseq)
        #print('driver: leader {} checkin returned'.format(os.getpid()), ret)
        sys.stdout.flush()
        ret = ret.get('result')
        if ret is None:
            # either server sent None or there was a network error
            time.sleep(0.1)
            continue
        if ret['state'] == 'exiting':
            # XXX consolidate with the duplicate code below
            mpi_proc.send_signal(signal.SIGINT)
            completed = finish_mpi(mpi_proc)
            status = check_mpi(mpi_proc)
            #print('driver: leader {}: received surprising exiting status'.format(os.getpid()))
            sys.stdout.flush()
            return {'cli': completed}

        if ret['state'] == 'running':
            if state == 'running':
                assert mpi_proc is not None
            else:
                mpi_proc = leader_start_mpi(pset, ret, wanted, user_kwargs)
                #print('driver: leader {} just started mpi proc and poll returns'.format(os.getpid()), check_mpi(mpi_proc))
                state = 'running'
        elif ret['state'] == 'waiting' and mpi_proc is not None:
            # oh oh! mpi-helper thinks something bad happened. perhaps one of my followers timed out?
            # XXX did the mpi helper server send all my followers to state=exiting?
            mpi_proc.send_signal(signal.SIGINT)
            completed = finish_mpi(mpi_proc)
            status = check_mpi(mpi_proc)
            #print('driver: leader {} bailing out on state==waiting post mpi_proc'.format(os.getpid()))
            sys.stdout.flush()
            return {'cli': completed}

        if mpi_proc:
            status = check_mpi(mpi_proc)
            #print('driver: leader {} checking mpirun: '.format(os.getpid()), status)
            #os.system('ps')
            if status is not None:
                #print('driver: leader {} observes normal exit'.format(os.getpid()))
                state = 'exiting'
                completed = finish_mpi(mpi_proc)  # should complete immediately
                for _ in range(100):
                    ret = leader_checkin(ncores, wanted, pubkey, state, lseq)
                    #print('driver: leader {} checkin post-normal exit returned'.format(os.getpid()), ret)
                    if ret['result'] and ret['result']['state'] == 'exiting':
                        break
                    time.sleep(0.1)
                return {'cli': completed}

        if not mpi_proc:
            time.sleep(0.1)

    raise ValueError('notreached')


def follower(pset, system_kwargs, user_kwargs):
    #print('I am follower and my pid is {}'.format(os.getpid()))
    fseq = 0
    state = 'available'
    ncores = pset['ncores']

    while True:
        #print('driver: follower checkin with state', state)
        sys.stdout.flush()
        ret = follower_checkin(ncores, state, fseq)
        #print('driver: follower checkin returned', ret)
        sys.stdout.flush()
        ret = ret['result']
        if ret is None:
            time.sleep(1.0)
            continue

        if ret['state'] == 'assigned' and state != 'assigned':
            # do this only once
            deploy_pubkey(ret['pubkey'])
        elif ret['state'] == 'exiting':
            #print('driver: follower told to exit')
            break

        state = ret['state']
        time.sleep(1.0)

    # for pandas type reasons, if cli is an object for the leader, it has to be an object for the follower
    # elsewise pandas will make the column a float
    return {'cli': 'hi pandas'}


def multimpi_worker(pset, system_kwargs, user_kwargs):
    if 'multimpi_server_url' in user_kwargs:
        global url
        url = user_kwargs['multimpi_server_url']
    else:
        raise ValueError('missing multimpi_server_url')

    if pset['kind'] == 'leader':
        return leader(pset, system_kwargs, user_kwargs)

    if pset['kind'] == 'follower':
        return follower(pset, system_kwargs, user_kwargs)


def mysignal(helper_server_proc, signum, frame):
    if signum == signal.SIGINT:
        global sigint_count
        sigint_count += 1
        if sigint_count == 1:
            print('driver: ^C seen, type it again to tear down', file=sys.stderr)
        elif sigint_count == 2:
            print('driver: tearing down for ^C', file=sys.stderr)
            # XXX this doesn't tear down ray workers
            tear_down_multimpi_server(helper_server_proc)
            sys.exit(1)
        else:
            print('driver: additional sigint ignored', file=sys.stderr)


def start_multimpi_server(hostport=':8889', user_kwargs={}):
    if ':' not in hostport:
        hostport = hostport + ':8889'
    host, port = hostport.split(':', maxsplit=1)
    if not host:
        host = socket.getfqdn()
        if '.' not in host:
            raise ValueError('did not find a valid FQDN, consider passing a hostname in hostport=')
    global url
    url = 'http://{}:{}/jsonrpc'.format(host, port)
    user_kwargs['multimpi_server_url'] = url

    global helper_server_proc
    daemon = paramsurvey_multimpi.__file__.replace('/__init__.py', '/server.py')
    helper_server_proc = subprocess.Popen(['python', daemon, host, port])

    status = check_multimpi_server(helper_server_proc, timeout=3.0)
    if status is not None:
        print('driver: mpi helper server exited immediately with status', status, file=sys.stderr)
        # at the moment this server doesn't use pipes so out,err are None
        outs, errs = helper_server_proc.communicate()
        if outs:
            print('driver: mpi helper stdout is', outs, file=sys.stderr)
        if errs:
            print('driver: mpi helper stderr is', errs, file=sys.stderr)
        raise ValueError('cannot continue without multimpi_server')

    hw = hello_world()
    if hw != 'pass':
        raise ValueError('hello world test of multimpi server returned: '+hw)

    # XXX add more checks, perhaps in a paramsurvey.map() timer function?

    mysignal_ = functools.partial(mysignal, helper_server_proc)
    signal.signal(signal.SIGINT, mysignal_)

    return user_kwargs


def tear_down_multimpi_server(helper_server_proc):
    helper_server_proc.send_signal(signal.SIGHUP)
    for _ in range(10):
        status = check_multimpi_server(helper_server_proc)
        if status is not None:
            break
        time.sleep(1.0)
    if status is None:
        helper_server_proc.kill()


def end_multimpi_server():    
    status = check_multimpi_server(helper_server_proc)
    if status is not None:
        print('looked at multimpi server and it had already exited with status', str(status), file=sys.stderr)
    else:
        print('multimpi server has not exited already, tearing it down', file=sys.stderr)
        tear_down_multimpi_server(helper_server_proc)


def check_multimpi_server(helper_server_proc, timeout=1.0):
    try:
        outs, errs = helper_server_proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        pass
    return helper_server_proc.poll()


def run_mpi(cmd, **kwargs):
    if 'capture_output' in kwargs:
        kwargs['stdout'] = subprocess.PIPE
        kwargs['stderr'] = subprocess.PIPE
        if 'encoding' not in kwargs:
            kwargs['encoding'] = 'utf-8'
    return subprocess.Popen(cmd, **kwargs)


def check_mpi(proc, timeout=0.1):
    try:
        # we have to call this enough to not block if pipes are used and fill up
        outs, errs = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        pass
    return proc.poll()


def finish_mpi(proc):
    # called either after sending sigint or a previous poll saw it exit
    outs, errs = proc.communicate()  # no timeout, will sleep until exit
    returncode = proc.poll()
    return subprocess.CompletedProcess(args=None, returncode=returncode, stdout=outs, stderr=errs)
