import os, sys
from optparse import OptionParser
import ConfigParser
import subprocess


parser = OptionParser(usage="%prog [opts] [-n <worker number>] --ini <file>", description="start the whole tool with n worker on local/HTCondor")

parser.add_option("--local", action="store_false")
parser.add_option("--condor", action="store_false")
parser.add_option("--debug", dest="loglevel", action="store_const", const="DEBUG")
parser.add_option("-n", dest="worker_n", default=1)
parser.add_option("--ini", dest="script_file")

(options, args) = parser.parse_args()

if options.loglevel:
    import src.logger
    src.logger.setlevel(options.loglevel)

# check runtime env
try:
    rc = subprocess.Popen(["mpich2version"])
except:
    print("can't find mpich tool, please setup mpich2 first")
    exit()

if 'Boost' not in os.environ['PATH']:
    print("can't find Boost.Python, this may cause some problem")

# read the config script
if not options.script_file:
    print("no app script file, stop running")
    exit()

conf = ConfigParser.ConfigParser()
conf.read(options.script_file)
app_conf_list = conf.sections()
applications=[]
import src.Application
if 'global' in conf.sections():
    workspace = conf.get('global','workspace')
    # MORE

for item in app_conf_list:
    if item == 'global':
        continue
    app = src.Application.UnitTestApp()
    app.set_boot(conf.get(item,"boot"))
    app.set_resdir(conf.get(item,"result_dir"))
    app.set_data(conf.get(item,"data"))
    applications.append(app)

# start mpd
if options.worker_n <= 0:
    print("worker number no less than 1")
    exit()

subprocess.Popen(["mpd&"])
print("start mpd deamon process...")

# start master
import src.RunMaster
src.RunMaster.main()

