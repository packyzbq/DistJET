import os,sys
import ConfigParser
import re
import traceback

sys.path.append(os.environ['DistJETPATH'])
# argv[1] = app file ; argv[2] = config file
if len(sys.argv) != 3:
    print("@master.py,need 2 parameter(given %d), exit"%(len(sys.argv)-1))
    exit()

svc_name = None

cf = ConfigParser.ConfigParser()
cf.read(sys.argv[2])
section = cf.sections()

kvs = cf.items("global")
if "service_name" in kvs:
    svc_name = kvs["service_name"]

module_path = re.match(r'.*/',sys.argv[1]).group(0)
sys.path.append(module_path)
module_name = (re.findall(r'/\w+', sys.argv[1])[-1])[1:]

try:
    module = __import__(module_name)
except ImportError:
    print('Import user define module error, exit=%s'%traceback.format_exc())
    exit()

from src.RunMaster import Master
applications = []
try:
    applications.append(module.run())
except:
    print('Error occurs when create application, error=%s'%traceback.format_exc())
    exit()

master = Master(applications, svc_name=svc_name)
print('master start running')
master.startProcessing()

