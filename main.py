## jsqd.py
"""
A queueing system wherein a job chooses the smallest queue of d sampled queues to join.
This is a so-called load balancing routing algorithm and is meant for larger parallel
systems where parsing all queue sizes would be cost-inefficient
MIT License
ⓒ 2020 Aaron Janeiro Stone
"""
import random
from simpy import *
import pandas as pd
import matplotlib.pyplot as plt

number = 300  # Max number of jobs if infinite is false
noJobCap = True  # For infinite
maxTime = 100000.0  # Runtime limit
timeInBank = 20.0  # Mean time in bank
arrivalMean = 2.0  # Mean of arrival process
seed = 204204  # Seed for RNG
parallelism = 10  # Number of queues
d = 2  # Number of queues to parse
doPrint = False  # True => print every arrival and wait time

if noJobCap == True:
    number = 0

Queues = []


def job(env, name, counters):
    arrive = env.now
    Qlength = {i: NoInSystem(counters[i]) for i in QueueSelector(d, counters)}
    if doPrint:
        print("%7.4f %s: Arrival " % (arrive, name))
    Queues.append({i: len(counters[i].put_queue) for i in range(len(counters))})
    choice = [k for k, v in sorted(Qlength.items(), key=lambda a: a[1])][0]
    with counters[choice].request() as req:
        # Wait in queue
        yield req
        wait = env.now - arrive
        # We got to the server
        if doPrint:
            print('%7.4f %s: Waited %6.3f' % (env.now, name, wait))
        tib = random.expovariate(1.0 / timeInBank)
        yield env.timeout(tib)
        Queues.append({i: len(counters[i].put_queue) for i in range(len(counters))})
        if doPrint:
            print('%7.4f %s: Finished' % (env.now, name))


def NoInSystem(R):
    """Total number of jobs in the resource R"""
    return len(R.put_queue) + len(R.users)


def QueueSelector(d, counters):
    return random.sample(range(len(counters)), d)


def Source(env, number, interval, counters):
    if noJobCap == False:
        for i in range(number):
            c = job(env, 'job%02d' % i, counters)
            env.process(c)
            t = random.expovariate(1.0 / interval)
            yield env.timeout(t)
    else:
        while True:  # Needed for infinite case as True refers to "until".
            i = number
            number += 1
            c = job(env, 'job%02d' % i, counters)
            env.process(c)
            t = random.expovariate(1.0 / interval)
            yield env.timeout(t)


# Setup and start the simulation
random.seed(seed)
env = Environment()

counters = {i: Resource(env) for i in range(parallelism)}
env.process(Source(env, number, arrivalMean, counters))
print(f'\n Running simulation with seed {seed}... \n')
env.run(until=maxTime)
print('\n Done \n')

df = pd.DataFrame(Queues).plot()
plt.title("Individual Queue Loads")
plt.show()