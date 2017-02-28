import os, sys
from optparse import OptionParser
import ConfigParser
import subprocess


parser = OptionParser(usage="%prog [opts] [-n <worker number>] --ini <file>", description="start the whole tool with n worker on local/HTCondor")

parser.add_option("--local", dest="local", action="store_const", const="true")
parser.add_option("--condor", dest="condor", action="store_const", const="true")
parser.add_option("--debug", dest="loglevel", action="store_const", const="DEBUG")
parser.add_option("--ini", dest="script_file")

(options, args) = parser.parse_args()

if options.loglevel:
    import src.logger
    src.logger.setlevel(options.loglevel)

# running locally
if options.local and not options.condor:
    # check runtime env
    try:
        rc = subprocess.Popen(["mpich2version"])
    except:
        print("can't find mpich tool, please setup mpich2 first")
        exit()

    if 'Boost' not in os.environ['PATH']:
        print("can't find Boost.Python, this may cause some problem")

    # check script file
    if not options.script_file:
        print("no app script file, stop running")
        exit()

    # start mpd
    if options.worker_n <= 0:
        print("worker number no less than 1")
        exit()

    subprocess.Popen(["mpd&"])
    print("start mpd deamon process...")

    print("starting master...")
    # start master
    script_file = options.script_file
    subprocess.Popen(["mpiexec","python","RunMaster.py", script_file])
    # start worker
    print("starting worker...")
    subprocess.Popen(["mpiexec", "-n",str(options.worker_n),"python", "WorkerAgent.py"])

elif options.condor and not options.local:
    pass

else:
    print("you can't run both on local and condor")