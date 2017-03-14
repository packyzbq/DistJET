from sys import argv
import os
import ConfigParser
import subprocess

if len(argv) <=1 :
    print("Too less parameter, exit")
    exit()

w_num = 0
w_capacity = 1
svc_name = None

cf = ConfigParser.ConfigParser()
kvs = cf.items("global")

if "service_name" in kvs:
    svc_name = kvs["service_name"]

assert("worker_number" in kvs and "worker_capacity" in kvs)

w_num = kvs["worker_number"]
w_capacity = kvs["worker_capacity"]

# check env
try:
    rc = subprocess.Popen(["mpich2version"], stdout=subprocess.PIPE)
    print('SETUP: find mpich tool')
except:
    print("can't find mpich tool, please setup mpich2 first")
    exit()

if 'Boost' not in os.environ['PATH']:
    print("can't find Boost.Python, setup Boost")
    rc = subprocess.Popen(['source', '/afs/ihep.ac.cn/users/z/zhaobq/env'])
else:
    print('SETUP: find Boost')

# check mpd running
rc = subprocess.Popen(['mpdtrace'], stdout=subprocess.PIPE)
stdout = rc.communicate()[0]
if 'no mpd is running' in stdout:
    print('no mpd running, exit')
    exit()

# start worker
os.system("mpiexec -n %d python worker.py %s %d"%(w_num,svc_name,w_capacity))