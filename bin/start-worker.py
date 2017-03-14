import sys
import os
import ConfigParser
import subprocess

if "DistJETPATH" not in os.environ:
    os.environ["DistJETPATH"] = "/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET/"

if len(sys.argv) <=1 :
    print("@worker.py,need 2 parameter(given %d), exit" % (len(sys.argv) - 1))
    exit()

w_num = 0
w_capacity = 1
svc_name = None

cf = ConfigParser.ConfigParser()
kvs = cf.items("global")

if "node" not in kvs:
    kvs['node'] = "local"

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

if kvs['node'] == "local":
    # start worker
    os.system("mpiexec -n %d python worker.py %s %d"%(w_num,svc_name,w_capacity))

elif kvs['node'] == "HTCondor":
    pass

