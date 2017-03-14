from sys import argv
import ConfigParser
# argv[1] = app file ; argv[2] = config file
if len(argv) <= 2:
    print("Too less parameter, exit")
    exit()

svc_name = None

cf = ConfigParser.ConfigParser()
cf.read(argv[2])
section = cf.sections()

kvs = cf.items("global")
if "service_name" in kvs:
    svc_name = kvs["service_name"]

module_name = argv[1].split('.')[0]

from src.RunMaster import Master
try:
    module = __import__(module_name)
except ImportError:
    print('Import user define module error, exit')
    exit()

applications = []
applications.append(module.run())

master = Master(applications, svc_name=svc_name)
print('master start running')
master.startProcessing()

