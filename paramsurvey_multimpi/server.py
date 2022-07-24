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
# XXX pdeathsig -- set exiting -- see existing SIGHUP code
# XXX make exiting actually exit this process

leaders = defaultdict(dict)
followers = defaultdict(dict)
cache_lifetime = 30  # should be several times as long as the follower checkin time

jobnumber = 0  # used to disambiguate states


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
    kills = []
    for k, v in followers.items():
        if v['t'] < now - cache_lifetime:
            kills.append(k)
    if kills:
        #print('cache: timing out {} followers entries'.format(len(kills)))
        pass
    for k in kills:
        # XXX if follower was 'assigned', not 'running' or 'available', set up for a reschedule
        # we expect running followers to never check in again
        del followers[k]

    kills = []
    for k, v in leaders.items():
        if v['t'] < now - cache_lifetime:
            kills.append(k)
    if kills:
        #print('cache: timing out {} leaders entries'.format(len(kills)))
        pass
    for k in kills:
        # XXX maybe do something if waiting, scheduled, but not running?
        # we expect running leaders to never check in
        del leaders[k]


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
    #print('  schedule: wanted {} cores in addition to leader cores {}'.format(wanted_cores, l['cores']))
    is_reschedule = False
    if l.get('fkeys'):
        is_reschedule = True
        wanted_cores -= sum(followers[f]['cores'] for f in l['fkeys'])
        #print('  reschedule, after existing follower cores we still want', wanted_cores)

    if wanted_cores > 0:
        fkeys = find_followers(wanted_cores)
    else:
        fkeys = []

    if wanted_cores <= 0 or fkeys is not None:
        if is_reschedule:
            #print('  re-scheduled jobnumber', jobnumber)
            pass
        else:
            #print('  scheduled jobnumber', jobnumber)
            pass
        for f in fkeys:
            followers[f]['state'] = 'assigned'
            followers[f]['leader'] = lkey
            followers[f]['pubkey'] = l['pubkey']
            followers[f]['jobnumber'] = jobnumber  # overwritten for is_reschedule
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
            #print('leader-only, setting state to running')
            l['state'] = 'running'
        return True
    #print('  failed to schedule')


def make_leader_return(l):
    # this is the return value for the leader
    ret = []
    for f in l['fkeys']:
        ret.append({'fkey': f, 'cores': followers[f]['cores']})
    return {'followers': ret, 'state': l['state'], 'lcores': l['cores'], 'jobnumber': l['jobnumber']}


def key(ip, pid):
    '''makes a string key for storing state information'''
    return '_'.join((ip, str(pid)))


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

    l = leaders[lkey]
    l['t'] = time.time()
    state = l.get('state')

    if remotestate == 'exiting':
        # leader announcing an mpirun exit
        if state == 'running':
            for f in l['fkeys']:
                if f in followers and followers[f]['state'] == 'running':
                    followers[f]['state'] = 'exiting'
            l['state'] = 'exiting'
        return {'followers': None, 'state': 'exiting'}

    if l.get('lseq') != lseq_new:
        #print('  leader sequence different, old: {}, new: {}, old-state: {}'.format(l.get('lseq'), lseq_new, state))
        # this leader is not the same one as in the table...
        if state == 'scheduled' or state == 'running':
            #print('  changing state from {} to None'.format(state))
            state = None
            # this job must have finished, one way or another
            # just throw this info away, followers will check back in for new work anyway
            if 'fkeys' in l:
                del l['fkeys']
        elif 'lseq' in l:
            # don't print this if the leader is brand new
            #print('  new state is', state)
            pass

    try_to_schedule = False
    if state in {'scheduled', 'running'}:
        #print('  leader is already scheduled')
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
        if len(valid_fkeys) != len(l['fkeys']):
            #print('  not all followers still exist, so triggering a new schedule')
            #print('    old valid fkeys:', l['fkeys'])
            #print('    new valid fkeys:', valid_fkeys)
            try_to_schedule = True
            l['fkeys'] = valid_fkeys
        else:
            if state == 'running':
                pass
            elif all([followers[f]['state'] == 'running' for f in valid_fkeys]):
                #print('GREG GREG GREG leader is running')
                l['state'] = 'running'
    else:
        # if state is None, this is a new-to-us leader
        # if it's waiting, we overwrite with identical information
        #print('  overwriting leader state, which was', state)
        l['state'] = 'waiting'
        l['cores'] = cores
        l['wanted_cores'] = int(wanted_cores)
        l['lseq'] = lseq_new
        l['pubkey'] = pubkey
        l['jobnumber'] = None
        try_to_schedule = True

    if try_to_schedule:
        #print('  before schedule:', l)
        schedule(lkey, l)
        #print('  after schedule:', l)
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

    f = followers[k]
    f['t'] = time.time()
    state = f.get('state')

    if state == 'exiting':
        return {'state': 'exiting'}

    if f.get('fseq') != fseq_new:
        #print('  follower sequence different old: {} new: {}  old-state: {}'.format(f.get('fseq'), fseq_new, state))
        if state == 'assigned':
            #print('  setting state to None')
            state = None
        elif 'fseq' in f:  # don't print this if the follower is brand new
            #print('  keeping state', state)
            pass

    f['fseq'] = fseq_new

    if remotestate == 'assigned':
        #print('  GREG remotestate assigned, state is', state)
        # XXX if state is available, what should I do?
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
    else:
        #print('multimpi server: surprised to receive signal', signum, file=sys.stderr)
        pass


if __name__ == '__main__':
    signal.signal(signal.SIGHUP, mysignal)
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
