import os, sys
from optparse import OptionParser
import subprocess
import re
sys.path.append("..")

if "DistJETPATH" not in os.environ:
    os.environ["DistJETPATH"] = "/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET/"

parser = OptionParser(usage="%prog AppFile [opts] --ini <file> ", description="start the master on local/HTCondor with config file")

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
        print("no config script file, stop running")
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
    config_file = os.path.abspath(options.script_file)

    try:
        app_file = (re.match(".*/\w*",os.path.abspath(args[0]))).group(0)
    except IndexError:
        print('app script not specified, try --help')
        sys.exit(1)
    print("mpiexec python $DistJET/bin/master.py %s %s" % (app_file, config_file))
    os.system("mpiexec python $DistJETPATH/bin/master.py %s %s"%(app_file, config_file))




elif options.condor and not options.local:
    pass

else:
    print("you can't run both on local and condor")
