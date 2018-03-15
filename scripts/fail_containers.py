import os
import subprocess
import sys


if os.getuid() != 0:
    print('Run as sudo!')
    sys.exit(-1)

if len(sys.argv) != 2:
    print('Specify number of containers to kill!')
    sys.exit(-1)

kill_N = int(sys.argv[1])

jps_cmd = 'jps -l | grep LocalContainerRunner'

result = subprocess.run(jps_cmd, shell=True, stdout=subprocess.PIPE)
pids = []

for line in result.stdout.decode("utf-8").split('\n'):
    output = line.split(' ')
    if len(output) != 2:
        continue
    pids.append(output[0])

if kill_N > len(pids):
    print('To kill {}, found only {}'.format(kill_N, len(pids)))
    sys.exit(-1)

to_kill = ' '.join(pids[:kill_N])
print('Killing PIDs: {}'.format(to_kill))

kill_cmd = 'kill {}'.format(to_kill)
result = subprocess.run(kill_cmd, shell=True)

if result.returncode != 0:
    print('Failed to kill some processes')
