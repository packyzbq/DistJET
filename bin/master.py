from sys import argv

if len(argv) <= 1:
    print("Too less parameter, exit")
    exit()

module_name = argv[1]

from src.RunMaster import Master
try:
    module = __import__(module_name)
except ImportError:
    print('Import user define module error, exit')
    exit()

applications = []
applications.append(module.run())

master = Master(applications)
print('master start running')
master.startProcessing()

