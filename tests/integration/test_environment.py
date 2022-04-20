import shutil
import os
import os.path
import subprocess
import paramsurvey_multimpi


def test_environment():
    assert shutil.which('mpicc'), 'mpicc is in the path; install openmpi if not'
    assert shutil.which('mpirun'), 'mpirun is in the path; install openmpi if not'

    mydir = os.path.dirname(paramsurvey_multimpi.__file__) + '/../tests/integration'
    os.chdir(mydir)

    assert os.path.isfile('hello.c'), 'hello.c should be in the test directory'

    proc = subprocess.run(['mpicc', 'hello.c'], capture_output=True, encoding='utf-8')
    assert proc.returncode == 0, 'mpicc return code should be zero'
    assert proc.stderr == '', 'stderr should be empty'

    proc = subprocess.run(['mpirun', '--oversubscribe', '-n', '4', './a.out'], capture_output=True, encoding='utf-8')
    assert proc.returncode == 0, 'mpirun return code should be zero'
    assert proc.stderr == '', 'stderr should be empty'
    assert len(proc.stdout.splitlines()) == 4, 'stdout should be 4 lines long'
