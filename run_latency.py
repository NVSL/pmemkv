#!/usr/bin/python
import os
import sys
import subprocess
import numpy

poolPath = '/mnt/ram/pmemkv'
tracePath = 'traces/ycsb-a.txt'
environment = 'PMEM_IS_PMEM_FORCE=1 taskset -c 0'
benchmark = './bin/pmemkv_latency'

results = []
for valueSize in [1024, 2048, 4096, 8192]:

    # Prepare benchmark
    command = environment + ' ' + benchmark + ' ' + tracePath + ' ' + str(valueSize)
    mustRepeat = True

    while mustRepeat:

        mustRepeat = False
        durability = []
        logging = []
        locking = []
        allocation = []

        for attempt in range(0, 5):
            # Clean environment
            if os.path.isfile(poolPath):
                os.remove(poolPath)

            # Run benchmark
            p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
            (out, err) = p.communicate()
            status = p.wait()
            if status != 0:
                print('Failed to execute benchmark!')
                sys.exit(1)

            # Parse output
            lines = out.splitlines()

            totalCycles = int(lines[6].split(',')[1])
            durCycles = int(lines[1].split(',')[1])
            logCycles = int(lines[2].split(',')[1])
            lockCycles = int(lines[3].split(',')[1])
            allocCycles = int(lines[4].split(',')[1])

            durPercent = 100 * float(durCycles) / totalCycles
            durability.append(durPercent)
            logCycles = 100 * float(logCycles) / totalCycles
            logging.append(logCycles)
            lockCycles = 100 * float(lockCycles) / totalCycles
            locking.append(lockCycles)
            allocCycles = 100 * float(allocCycles) / totalCycles
            allocation.append(allocCycles)

        # Check if results are accurate
        for percents in [durability, logging, locking, allocation]:
            if numpy.std(percents) > 0.1:
                mustRepeat = True
                break
        if mustRepeat:
            continue

        # Compute average percentages
        result = [valueSize]
        for percents in [durability, logging, locking, allocation]:
            result.append(numpy.average(percents))
        results.append(result)

print(results)
