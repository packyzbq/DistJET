import os, sys
from optparse import OptionParser
import subprocess
import src


parser = OptionParser(usage="%prog [opts] [-n <worker number>] --ini <file>", description="start the whole tool with n worker on local/HTCondor")

parser.add_option("--local", dest="local", action="store_const", const="true")
parser.add_option("--condor", dest="condor", action="store_const", const="true")
parser.add_option("--debug", dest="loglevel", action="store_const", const="DEBUG")
parser.add_option("--ini", dest="script_file")
parser.add_option("-n", dest="worker_n")

(options, args) = parser.parse_args()

if options.loglevel:
    import src.logger
    src.logger.setlevel(options.loglevel)

# running locally
if options.local and not options.condor:
    # check runtime env
    try:
        rc = subprocess.Popen(["mpich2version"], stdout=subprocess.PIPE)
        print('SETUP: find mpich tool')
    except:
        print("can't find mpich tool, please setup mpich2 first")
        exit()

    if 'Boost' not in os.environ['PATH']:
        print("can't find Boost.Python, setup Boost")
        rc = subprocess.Popen(['source','/afs/ihep.ac.cn/users/z/zhaobq/env'])
    else:
        print('SETUP: find Boost')

    # check script file
    if not options.script_file:
        print("no app script file, stop running")
        exit()

    if options.worker_n <= 0:
        print("worker number no less than 1")
        exit()

    # start mpd
    # mpd start in bash script, not here; need to check mpd
        #try:
        #    os.system("mpd&")
        #except:
        #    print("Start mpd deamon process error, exit...")
        #    exit()
        #print("start mpd deamon process...")
    rc = subprocess.Popen(['mpdtrace'], stdout=subprocess.PIPE)
    stdout = rc.communicate()[0]
    if 'no mpd is running' in stdout:
        print('no mpd running, exit')
        exit()

    # Analyze config script
    script = options.script_file.split('.')[0]
    os.system("mpiexec python master.py %s"%script)

    # start master in bash script
        #print("starting master...")
        # start master
        #script_file = options.script_file
        #subprocess.Popen(["mpiexec","python","RunMaster.py", script_file], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # start worker
        #print("starting worker...")
        #subprocess.Popen(["mpiexec", "-n",str(options.worker_n),"python", "WorkerAgent.py"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

elif options.condor and not options.local:
    pass

else:
    print("you can't run both on local and condor")
