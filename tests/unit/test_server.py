from functools import partial

from paramsurvey_multimpi.server import leader_checkin, follower_checkin, clear


# mock time.time()
# @mock.patch('time.time', mock.MagicMock(return_value=12345))
# make jobnumber visible and check it

def test_onlyleader():
    clear()
    seq = 0

    l = partial(leader_checkin, 'localhost', 1, 100, 1, 'pubkey')
    ret = l('waiting', seq)
    assert len(ret['followers']) == 0, 'leader has enough cores, schedules immediately'
    assert ret['state'] == 'running'
    assert ret['lcores'] == 1
    assert 'jobnumber' in ret
    assert ret['jobnumber'] is not None

    seq += 1
    l = partial(leader_checkin, 'localhost', 1, 100, 2, 'pubkey')
    for _ in range(10):
        ret = l('waiting', seq)
        assert not ret, 'leader has too few cores to schedule ever'


def test_leadfollow():
    clear()
    lseq = 0
    fseq = 0

    l = partial(leader_checkin, 'localhost', 1, 100, 3, 'pubkey')
    f = partial(follower_checkin, 'localhost', 1, 101)

    print('\nleadfollow too few to ever start')
    ret = l('waiting', lseq)
    assert not ret
    ret = f('available', fseq)
    assert not ret, 'leadfollow not enough cores to ever start'

    print('\nleadfollow enough cores to start f l f')
    fseq += 1
    f = partial(follower_checkin, 'localhost', 2, 101)
    ret = f('available', fseq)
    assert not ret, 'leadfollow leader must check in before scheduled'
    ret = l('waiting', lseq)
    assert len(ret['followers']) != 0, 'leadfollow leader should schedule'
    assert 'jobnumber' in ret
    assert ret['jobnumber'] is not None
    ret = f('available', fseq)
    assert ret, 'follower is scheduled'
    assert 'leader' in ret, 'leadfollow follower receives schedule'
    assert 'pubkey' in ret, 'leadfollow follower receives schedule'
    assert ret['state'] == 'assigned'

    print('\nleadfollow enough cores to start l f l f')
    lseq += 1
    fseq += 1
    ret = l('waiting', lseq)
    assert not ret
    ret = f('available', fseq)
    assert not ret
    ret = l('waiting', lseq)
    assert ret['followers'], 'leader sees job has scheduled'
    assert 'jobnumber' in ret
    assert ret['jobnumber'] is not None
    ret = f('available', fseq)
    assert ret, 'follower sees job has scheduled'
    assert 'leader' in ret
    assert 'pubkey' in ret
    assert ret['state'] == 'assigned'

    ret = f('running', fseq)
    assert not ret

    # plot twist: follower finishes and calls back in
    fseq += 1
    ret = f('available', fseq)
    assert not ret
    # multimpi server now should have f 'available', but there's no way to test that
    # never going to reschedule as long as the leader doesn't call in
    ret = f('available', fseq)
    assert not ret
    ret = f('available', fseq)
    assert not ret

    lseq += 1
    ret = l('waiting', lseq)
    assert ret['followers'], 'leader sees job rescheduled'
    assert ret['state'] == 'scheduled'
    assert 'lcores' in ret
    ret = f('available', fseq)
    assert ret, 'follower is scheduled'
    assert 'leader' in ret
    assert 'pubkey' in ret
    assert ret['state'] == 'assigned'
