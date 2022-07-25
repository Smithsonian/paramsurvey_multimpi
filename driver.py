import argparse
import subprocess

import paramsurvey

import paramsurvey_multimpi.client as client


# Google cloud HPC checklist https://cloud.google.com/architecture/best-practices-for-using-mpi-on-compute-engine#checklist
# Google storage: https://cloud.google.com/storage/docs/best-practices

def main():
    parser = argparse.ArgumentParser(description='difx_paramsurvey_driver, run inside ray')
    parser.add_argument('--resources', action='store', default='')
    parser.add_argument('--ray', action='store_true')
    args = parser.parse_args()

    #client.start_multimpi_server(hostport='localhost:8889')
    foo = client.start_multimpi_server(hostport=':8889')
    #client.start_multimpi_server(hostport='0.0.0.0:8889')
    #client.start_multimpi_server()
    server_url = foo['multimpi_server_url']
    print('GREG: server_url is', server_url)

    if args.ray:
        kwargs = {
            'backend': 'ray',
            'ray': {'address': 'auto'},
        }
    else:
        kwargs = {
            'backend': 'multiprocessing', 'ncores': 7,
        }
    paramsurvey.init(**kwargs)

    psets = [
        {'kind': 'leader', 'ncores': 1, 'run_args': 'mpirun -np {} ./a.out', 'wanted': 3},
        {'kind': 'follower', 'ncores': 1},
        {'kind': 'follower', 'ncores': 1},
    ]

    psets = psets * 3

    # this is how ray backend args are specified
    # XXX shouldn't paramsurvey hide this?
    for p in psets:
        if 'ncores' in p:
            p['ray'] = {'num_cores': p.get('ncores')}

    # example of how to return stdout from the cli process
    user_kwargs = {'run_kwargs': {
        'stdout': subprocess.PIPE, 'encoding': 'utf-8',
        'stderr': subprocess.PIPE, 'encoding': 'utf-8',
    }}
    user_kwargs = {}
    user_kwargs['mpi'] = 'openmpi'
    user_kwargs['multimpi_server_url'] = server_url

    results = paramsurvey.map(client.multimpi_worker, psets, user_kwargs=user_kwargs)

    client.end_multimpi_server()

    #assert results.progress.failures == 0
    print('driver: after map, failures is', results.progress.failures)

    for r in results.iterdicts():
        print('result:', r['cli'])

    for r in results.itertuples():
        if not isinstance(r.cli, str):
            print('result:', r.cli.returncode)  # needs stdout capture: , r.cli.stdout.rstrip())


if __name__ == '__main__':
    main()
    print('exiting')
