from src import WorkerAgent
from sys import argv

# argv[1] = service name; argc[2] = capacity
if len(argv) != 3:
    print('@Worker.py, need 2 parameter(given %d), exit'%(len(argv)-1))
    exit()

agent = WorkerAgent.WorkerAgent(argv[1], int(argv[2]))
agent.run()