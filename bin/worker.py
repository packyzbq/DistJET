from src import WorkerAgent
from sys import argv

# argv[1] = service name; argc[2] = capacity
if len(argv) <=2:
    print('@Worker.py, too less parameter,exit')
    exit()

agent = WorkerAgent.WorkerAgent(argv[1], int(argv[2]))
agent.run()