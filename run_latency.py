#!/usr/bin/python
import os
import sys
import subprocess
import numpy

poolPath = '/mnt/ram/pmemkv'
tracePath = 'traces/ycsb-a.txt'
environment = 'PMEM_IS_PMEM_FORCE=1 taskset -c 0'
benchmark = './bin/pmemkv_latency'
repeatLimit = 3

loadResults = []
updateResults = []
for valueSize in [64, 128, 256, 512, 1024, 2048, 4096, 8192]:

    # Prepare benchmark
    command = environment + ' ' + benchmark + ' ' + tracePath + ' ' + str(valueSize)
    mustRepeat = True
    repeats = 0
    threshold = 0.1 # 0.1%

    while mustRepeat:

        if repeats == repeatLimit:
            threshold = threshold * 2
            print('Failed to reach the accuracy threshold, increasing threshold to ' + str(threshold))
            repeats = 0
        repeats = repeats + 1

        mustRepeat = False
        loadDurability = []
        loadLogging = []
        loadLocking = []
        loadAllocation = []
        updateDurability = []
        updateLogging = []
        updateLocking = []
        updateAllocation = []

        for attempt in range(0, 5):
            # Clean environment
            if os.path.isfile(poolPath):
                os.remove(poolPath)

            # Step 1
            # Populate kv-store
            p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
            (out, err) = p.communicate()
            status = p.wait()
            if status != 0:
                print('Failed to populate kv-store!')
                sys.exit(1)

            # Parse output
            lines = out.splitlines()

            totalCycles = int(lines[6].split(',')[1])
            durCycles = int(lines[1].split(',')[1])
            logCycles = int(lines[2].split(',')[1])
            lockCycles = int(lines[3].split(',')[1])
            allocCycles = int(lines[4].split(',')[1])

            durPercent = 100 * float(durCycles) / totalCycles
            loadDurability.append(durPercent)
            logCycles = 100 * float(logCycles) / totalCycles
            loadLogging.append(logCycles)
            lockCycles = 100 * float(lockCycles) / totalCycles
            loadLocking.append(lockCycles)
            allocCycles = 100 * float(allocCycles) / totalCycles
            loadAllocation.append(allocCycles)

            # Step 2
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
            updateDurability.append(durPercent)
            logCycles = 100 * float(logCycles) / totalCycles
            updateLogging.append(logCycles)
            lockCycles = 100 * float(lockCycles) / totalCycles
            updateLocking.append(lockCycles)
            allocCycles = 100 * float(allocCycles) / totalCycles
            updateAllocation.append(allocCycles)

        # Check if results are accurate
        for percents in [loadDurability, loadLogging, loadLocking, loadAllocation, updateDurability, updateLogging, updateLocking, updateAllocation]:
            if numpy.std(percents) > threshold:
                mustRepeat = True
                break
        if mustRepeat:
            continue

        # Compute average percentages
        result = [valueSize]
        for percents in [loadDurability, loadLogging, loadLocking, loadAllocation]:
            result.append(numpy.average(percents))
        loadResults.append(result)
        print(result)
        result = [valueSize]
        for percents in [updateDurability, updateLogging, updateLocking, updateAllocation]:
            result.append(numpy.average(percents))
        updateResults.append(result)
        print(result)

# TODO draw stacked bar chart using *results*
