import signal
import time
from collections import defaultdict
import multiprocessing
import os
import sys
import ctypes
import ctypes.util

from aiohttp import web
import aiohttp_rpc

import psutil

exiting = False

leaders = defaultdict(dict)
followers = defaultdict(dict)
cache_lifetime = 30  # should be several times as long as the follower checkin time

jobnumber = 0  # used to disambiguate states

sigint_count = 0


def set_pdeathsig():
    libc = ctypes.CDLL(ctypes.util.find_library('c'), use_errno=True)
    PR_SET_PDEATHSIG = 1
    if libc.prctl(PR_SET_PDEATHSIG, signal.SIGKILL) != 0:
        raise OSError(ctypes.get_errno(), 'PR_SET_PDEATHSIG')


def clear():
    #print('clear')
    global leaders
    global followers
    leaders = defaultdict(dict)
    followers = defaultdict(dict)


def cache_timeout():
    now = time.time()

    timeouts = []
    for k, v in followers.items():
        if v['t'] < now - cache_lifetime:
            timeouts.append(k)
    if timeouts:
        #print('cache: timing out {} followers entries'.format(len(timeouts)))
        pass
    for fkey in timeouts:
        # followers have no idea when thei mpi job is done
        # once running it'll remain running (and checking in) until we tell it to exit
        # if it does stop checking in, we can't really do anything except hope mpi exits
        if 'jobnumber' in followers[fkey] and followers[fkey].get('state') != 'exiting':
            print('server: follower {} in job {} timed out, that is a bad sign'.format(fkey, followers[fkey]['jobnumber']))
        del followers[fkey]

    timeouts = []
    for k, v in leaders.items():
        if v['t'] < now - cache_lifetime:
            timeouts.append(k)
    if timeouts:
        #print('cache: timing out {} leaders entries'.format(len(timeouts)))
        pass
    for lkey in timeouts:
        state = leaders[lkey].get('state')
        jobnumber = leaders[lkey].get('jobnumber')
        print('server: leader {} in state {} jobnumber {} timed out'.format(lkey, state, jobnumber))
        del leaders[lkey]


def cache_clean_exiting():
    nuke = set()
    for l, v in leaders.items():
        if v['state'] == 'exiting':
            nuke.add(l)
    for l in nuke:
        del leaders[l]
    nuke = set()
    for f, v in followers.items():
        if v['state'] == 'exiting':
            nuke.add(f)
    for f in nuke:
        del followers[f]


def find_followers(wanted_cores):
    #print('  schedule: find followers, want {} cores'.format(wanted_cores))
    fkeys = []
    for k, v in followers.items():
        if v['state'] == 'available':
            wanted_cores -= v['cores']
            fkeys.append(k)
            if wanted_cores <= 0:
                break
    if wanted_cores <= 0:
        #print('  ff: did find enough cores:', ','.join(fkeys))
        return fkeys
    #print('  ff: did not find enough cores')


def schedule(lkey, l):
    global jobnumber
    cache_timeout()
    wanted_cores = l['wanted_cores'] - l['cores']
    print('  schedule: wanted {} cores in addition to leader cores {}'.format(wanted_cores, l['cores']))
    is_reschedule = False
    if l.get('fkeys'):
        is_reschedule = True
        wanted_cores -= sum(followers[f]['cores'] for f in l['fkeys'])
        print('  reschedule, after existing follower cores we still want', wanted_cores)

    if wanted_cores > 0:
        fkeys = find_followers(wanted_cores)
    else:
        fkeys = []

    print('  schedule: return of find_followers was', fkeys)  # None, [], list

    if wanted_cores <= 0 or fkeys is not None:
        if is_reschedule:
            print('  re-scheduled jobnumber', jobnumber)
            pass
        else:
            print('  scheduled jobnumber', jobnumber)
            pass

        for f in fkeys:
            followers[f]['state'] = 'assigned'
            followers[f]['leader'] = lkey
            followers[f]['pubkey'] = l['pubkey']
            followers[f]['jobnumber'] = jobnumber  # overwritten with the old number for is_reschedule
        if is_reschedule:
            l['fkeys'].extend(fkeys)
            for f in fkeys:
                followers[f]['jobnumber'] = l['jobnumber']
        else:
            l['jobnumber'] = jobnumber
            l['fkeys'] = fkeys
            jobnumber += 1

        l['state'] = 'scheduled'
        if len(l['fkeys']) == 0:  # job fits the leader
            print('  schedule: leader-only, setting state to running')
            l['state'] = 'running'
        return True
    print('  failed to schedule')


def make_leader_return(l):
    # this is the return value for the leader
    ret = []
    for f in l['fkeys']:
        ret.append({'fkey': f, 'cores': followers[f]['cores']})
    return {'followers': ret, 'state': l['state'], 'lcores': l['cores'], 'jobnumber': l['jobnumber']}


def key(ip, pid):
    '''makes a string key for storing state information'''
    return '_'.join((ip, str(pid)))


def get_valid_fkeys(l):
    valid_fkeys = []
    for f in l['fkeys']:
        if f not in followers:
            #print('      not in followers')
            pass
        elif followers[f]['state'] not in {'assigned', 'running'}:  # XXX test 'running'
            # for example, follower timed out and then checked in
            #print('      not assigned or running, but ', followers[f]['state'])
            pass
        elif followers[f]['jobnumber'] != l['jobnumber']:
            # for example, follower timed out, checked in, was assigned to some other job
            #print('      wrong jobnumber')
            pass
        else:
            valid_fkeys.append(f)
    return valid_fkeys


def leader_checkin(ip, cores, pid, wanted_cores, pubkey, remotestate, lseq_new):
    if exiting:
        #print('multimpi_server: saw leader checkin after I was HUPped', file=sys.stderr)
        # XXX if I'm in the leaders table, remove me
        return {'followers': None, 'state': 'exiting'}

    lkey = key(ip, pid)
    #print('leader checkin {}, wanted: {}, lseq_new: {}'.format(lkey, wanted_cores, lseq_new))
    # leader states: waiting -> scheduled -> running -> exiting

    if lkey in followers:
        # well, this job might or might not have finished... so all we can do is:
        #print('leader checkin used key of an existing follower')
        del followers[lkey]

    l = leaders[lkey]  # defaultdict dict

    if l.get('lseq') != lseq_new:
        if len(l):
            print('leader {} checked in with new sequence number, destroying old leader in state {}'.format(lkey, l.get('state')))
            if l.get('state') in {'scheduled', 'running'}:
                # XXX potentially free up all of the followers?
                pass
            l.clear()

    l['t'] = time.time()
    state = l.get('state')

    if state == 'exiting':
        # XXX shouldn't do this if sequence numbers do not match
        return {'followers': None, 'state': 'exiting'}

    if remotestate == 'exiting':
        # leader announcing an mpirun exit ... ought to be in the 'running' state
        if state == 'running':
            for f in l['fkeys']:
                if f in followers and followers[f]['state'] == 'running':
                    followers[f]['state'] = 'exiting'
            l['state'] = 'exiting'
        else:
            print('server surprised to see leader {} state {} announce remotestate exiting'.format(lkey, state))
            l['state'] = 'exiting'
        return {'followers': None, 'state': 'exiting'}

    try_to_schedule = ''
    if state in {'scheduled', 'running'}:
        #print('  leader is already scheduled')
        valid_fkeys = get_valid_fkeys(l)

        if len(valid_fkeys) != len(l['fkeys']):
            #print('  not all followers still exist, so triggering a new schedule')
            #print('    old valid fkeys:', l['fkeys'])
            #print('    new valid fkeys:', valid_fkeys)

            if state == 'scheduled':
                try_to_schedule = 'a follower disappeared when leader state was {}'.format(state)
            elif state == 'running':
                # if the leader is 'running' mpi is using the list it was already given
                print('server: leader {} is sad because a follower disappeared'.format(lkey))
            l['fkeys'] = valid_fkeys
        elif state == 'running':
            pass
        elif state == 'scheduled':
            if all([followers[f]['state'] == 'running' for f in valid_fkeys]):
                print('server: job number {} has reached the running state'.format(l['jobnumber']))
                l['state'] = 'running'
    else:
        # if state is None, this is a new-to-us leader
        # if it's waiting, we overwrite with identical information
        #print('  overwriting leader state, which was', state)
        if state is None:
            try_to_schedule = 'new leader'
        elif state == 'waiting':
            try_to_schedule = 'waiting leader'
        l['state'] = 'waiting'
        l['cores'] = cores
        l['wanted_cores'] = int(wanted_cores)
        l['lseq'] = lseq_new
        l['pubkey'] = pubkey
        l['jobnumber'] = None

    if try_to_schedule:
        print('server: trying schedule because of', try_to_schedule)
        if state == 'running':
            raise ValueError('we should never reschedule a job that is already running')
        ret = schedule(lkey, l)
        if not ret:
            if state == 'scheduled':
                if l['fkeys']:
                    print('server: after failed reschedule, freeing {} followers'.fprmat(len(l['fkeys'])))
                    for fkey in l['fkeys']:
                        f = followers[fkey]
                        f['state'] = 'available'
                        del f['leader']
                        del f['pubkey']
                    del l['fkeys']
                l['state'] = 'waiting'
            elif state is None or state == 'waiting':
                pass
            else:
                raise ValueError('surrised to see state {} after a failed schedule'.format(state))
    else:
        #print('  have an existing schedule with {} followers'.format(len(l['fkeys'])))
        pass

    # return schedule or watever

    if l['state'] in {'scheduled', 'running'}:
        #print('  returning a schedule with {} followers'.format(len(l['fkeys'])))
        return make_leader_return(l)
    else:
        #print('  did not schedule')
        pass


def follower_checkin(ip, cores, pid, remotestate, fseq_new):
    if exiting:
        #print('multimpi_server: saw follower checkin after I was HUPped', file=sys.stderr)
        # XXX remove me from the followers table?
        return {'state': 'exiting'}

    k = key(ip, pid)
    #print('follower checkin', k, 'with remotestate', remotestate)
    # follower states: available -> assigned -> running -> exiting

    if k in leaders:
        # existing leader is now advertising it is a follower
        #print('  existing leader {} is now advertising it is a follower'.format(k))
        fkeys = leaders[k].get('fkeys', [])
        for f in fkeys:
            if f in followers:
                # XXX need a leadersequence (jobseqeunce?) here
                # don't leave any of the fkeys in the assigned state
                ##print('  ... nuking follower', f)
                #del followers[f]
                #print('GREG here we are and follower state is', followers[f]['state'])
                # XXX hmph followers are already exiting.
                if followers[f]['state'] == 'assigned':
                    #print('GREG this happened')
                    followers[f]['state'] = 'exiting'
        del leaders[k]

    f = followers[k]  # defaultdict dict
    if f.get('fseq') != fseq_new:
        if len(f):
            print('follower {} checked in with new sequence number, destroying old follower in state {}'.format(k, f.get('state')))
            f.clear()

    f['t'] = time.time()
    f['fseq'] = fseq_new
    state = f.get('state')

    if state == 'exiting':
        return {'state': 'exiting'}

    if remotestate == 'assigned':
        #print('  GREG remotestate assigned, state is', state)
        if state == 'available':
            raise ValueError('should not see state available here?')
        if state == 'running':
            # all is well
            return {'state': 'assigned'}

    if state == 'assigned' and remotestate == 'available':
        f['state'] = 'running'
        #print('  returning a schedule to the follower')
        return {'leader': f['leader'], 'pubkey': f['pubkey'], 'state': 'assigned'}  # XXX how does the follower get to 'running'?

    #if f.get('state') == 'running':
    if state == 'running':
        #print('  destroying follower schedule')
        del f['leader']
        del f['pubkey']
    f['state'] = 'available'
    f['cores'] = cores


def hello_world():
    return {'hello': 'world!'}


def core_count():
    try:
        # recent Linux
        return len(os.sched_getaffinity(0))
    except (AttributeError, NotImplementedError, OSError):
        try:
            # Windows, MacOS, FreeBSD
            return len(psutil.Process().cpu_affinity())
        except (AttributeError, NotImplementedError, OSError):
            # older Linux, MacOS. Can raise NotImplementedError
            return multiprocessing.cpu_count()


def mysignal(signum, frame):
    if signum == signal.SIGHUP:
        global exiting
        exiting = True
        if leaders or followers:
            cache_clean_exiting()
            if leaders or followers:
                #print('multimpi server: exiting all work', file=sys.stderr)
                #print('multimpi server: {} leaders and {} followers remain'.format(len(leaders), len(followers)), file=sys.stderr)
                return
        exit(0)
    elif signum == signal.SIGHUP:
        global sigint_count
        sigint_count += 1
        # the process that starts us tears down on the 2nd ^C
        # we are in the same process group, so we get them too.
        if sigint_count > 2:
            print('server: tearing down for ^C', file=sys.stderr)
            sys.exit(1)
        pass
    else:
        #print('multimpi server: surprised to receive signal', signum, file=sys.stderr)
        pass


if __name__ == '__main__':
    signal.signal(signal.SIGHUP, mysignal)
    signal.signal(signal.SIGINT, mysignal)
    set_pdeathsig()

    aiohttp_rpc.rpc_server.add_methods([
        leader_checkin,
        follower_checkin,
        hello_world,
    ])

    app = web.Application()
    app.router.add_routes([
        web.post('/jsonrpc', aiohttp_rpc.rpc_server.handle_http_request),
    ])

    host, port = sys.argv[1:3]

    print('server: hello from the server, I am bound to host {} port {}'.format(host, port), file=sys.stderr)
    sys.stderr.flush()
    web.run_app(app, host=host, port=port)
