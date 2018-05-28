#!/usr/bin/python
import os
import sys
import subprocess
import numpy
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

poolPath = '/mnt/ram/pmemkv'
tracePath = 'traces/ycsb-a.txt'
environment = 'PMEM_IS_PMEM_FORCE=1 taskset -c 0'
benchmark = './bin/pmemkv_latency'
repeatLimit = 3
valueSizes = [64, 128, 256, 512, 1024, 2048, 4096, 8192]
legendsPMDK = ['Durability', 'Logging', 'Locking', 'Allocation', 'Other']
legendsPMEMKV = ['Lookup', 'vAlloc', 'pAlloc', 'Logging', 'Memcpy', 'GetDirectPtr', 'Pool Maintenance', 'Other']
enablePattern = False
runUpdatePhase = False

#colors = ['#01579B', '#0288D1', '#03A9F4', '#4FC3F7', '#B3E5FC']
colors = ['#dcebca','#b2dbca','#89cccc','#62bfd1','#45aacc','#3e93bd','#3678ad','#2d5f9c','#24458c','#1d2d7d','#162063','#11184a']
#colors = ['#169e83', '#f29811', '#28ad60', '#d15400', '#2a81b8', '#bf382c', '#8c44ab', '#2d3e4f', '#7e8b8c']
patterns = ['xx', '//', '++', '..', '--', 'oo', '**']

def parsePMDKOutput(out):
    lines = out.splitlines()
    totalCycles = int(lines[6].split(',')[1])
    durCycles = int(lines[1].split(',')[1])
    logCycles = int(lines[2].split(',')[1])
    lockCycles = int(lines[3].split(',')[1])
    allocCycles = int(lines[4].split(',')[1])

    durPercent = 100 * float(durCycles) / totalCycles
    logCycles = 100 * float(logCycles) / totalCycles
    lockCycles = 100 * float(lockCycles) / totalCycles
    allocCycles = 100 * float(allocCycles) / totalCycles

    output = [durPercent, logCycles, lockCycles, allocCycles]
    return output

def parsePMEMKVOutput(out):
    lines = out.splitlines()
    totalCycles = int(lines[14].split(',')[1])
    lookupCycles = int(lines[7].split(',')[1])
    vAllocCycles = int(lines[8].split(',')[1])
    pAllocCycles = int(lines[9].split(',')[1])
    loggingCycles = int(lines[10].split(',')[1])
    memcpyCycles = int(lines[11].split(',')[1])
    directPtrCycles = int(lines[12].split(',')[1])
    maintenanceCycles = int(lines[13].split(',')[1])

    lookupPercent = 100 * float(lookupCycles) / totalCycles
    vAllocPercent = 100 * float(vAllocCycles) / totalCycles
    pAllocPercent = 100 * float(pAllocCycles) / totalCycles
    loggingPercent = 100 * float(loggingCycles) / totalCycles
    memcpyPercent = 100 * float(memcpyCycles) / totalCycles
    directPtrPercent = 100 * float(directPtrCycles) / totalCycles
    maintenancePercent = 100 * float(maintenanceCycles) / totalCycles

    output = [lookupPercent,
              vAllocPercent,
              pAllocPercent,
              loggingPercent,
              memcpyPercent,
              directPtrPercent,
              maintenancePercent]
    return output

def saveStackedPlot(title, xAxis, data, legends, output):
    # DEBUG
    print(data)
    # DEBUG
    pp = PdfPages(output)
    N = len(xAxis)
    ind = numpy.arange(N)
    width = 0.35

    bars = []
    bottoms = [0 for t in xAxis]
    for category in data:
        myColor = colors[len(bars)]
        if len(data) < len(colors) / 2:
            myColor = colors[len(bars) * 2]
        if enablePattern:
            bars.append(plt.bar(ind, category, width, bottom=bottoms, color=myColor, hatch=patterns[len(bars)], zorder=3))
        else:
            bars.append(plt.bar(ind, category, width, bottom=bottoms, color=myColor, zorder=3))
        bottoms = [bottoms[i] + category[i] for i in range(0, len(xAxis))]

    plt.ylabel('Percentage of total execution time')
    plt.xlabel('Value size (bytes)')
    plt.title(title)
    plt.xticks(ind, xAxis)
    plt.yticks(numpy.arange(0, 101, 10))
    art = []
    lgd = plt.legend([p[0] for p in bars], legends, loc=9, bbox_to_anchor=(0.5, -0.1), ncol=3)
    art.append(lgd)
    plt.grid(True, zorder=0)
    plt.savefig(pp, format='pdf', additional_artists=art, bbox_inches="tight")
    pp.close()

def convertData(data):
    output = []
    other = []
    for c in range(0, len(data[0])):
        temp = []
        for row in data:
            temp.append(row[c])
        temp = list(numpy.around(temp, decimals=3))
        output.append(temp)
    for row in data:
        other.append(numpy.maximum(0, 100 - numpy.sum(list(numpy.around(row, decimals=3)))))
    other = list(numpy.around(other, decimals=3))
    output.append(other)
    return output

loadPMDK = []
updatePMDK = []
loadPMEMKV = []
updatePMEMKV = []
maxThreshold = 0

for valueSize in valueSizes:

    # Prepare benchmark
    command = environment + ' ' + benchmark + ' ' + tracePath + ' ' + str(valueSize)
    mustRepeat = True
    repeats = 0
    threshold = 0.5 # 0.1
    if maxThreshold == 0:
        maxThreshold = threshold

    while mustRepeat:

        if repeats == repeatLimit:
            threshold = threshold * 2
            if threshold > maxThreshold:
                maxThreshold = threshold
            print('Failed to reach the accuracy threshold, increasing threshold to ' + str(threshold))
            repeats = 0
        repeats = repeats + 1

        mustRepeat = False

        # PMDK output
        pmdkLoadTemp = []
        pmdkUpdateTemp = []

        # PMEMKV output
        pmemkvLoadTemp = []
        pmemkvUpdateTemp = []

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

            # Parse PMDK output
            pmdkLoadTemp.append(parsePMDKOutput(out))

            # Parse PMEMKV output
            pmemkvLoadTemp.append(parsePMEMKVOutput(out))

            if runUpdatePhase == False:
                continue

            # Step 2
            # Run benchmark
            p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
            (out, err) = p.communicate()
            status = p.wait()
            if status != 0:
                print('Failed to execute benchmark!')
                sys.exit(1)

            # Parse PMDK output
            pmdkUpdateTemp.append(parsePMDKOutput(out))

            # Parse PMEMKV output
            pmemkvUpdateTemp.append(parsePMEMKVOutput(out))

        # Check if results are accurate
        for tempResult in [pmdkLoadTemp, pmdkUpdateTemp, pmemkvLoadTemp, pmemkvUpdateTemp]:
            if mustRepeat == True:
                break
            # Only empty for update experiments iff runUpdatePhase is set to False
            if len(tempResult) == 0:
                continue
            for column in range(0, len(tempResult[0])):
                percents = [] # Holds values for each percent (e.g., Durability)
                for row in tempResult:
                    percents.append(row[column])
                if numpy.std(percents) > threshold:
                    mustRepeat = True
                    break
        if mustRepeat:
            continue

        # Compute average percentages
        def computeAvg(data):
            output = []
            for column in range(0, len(data[0])):
                percents = []
                for row in data:
                    percents.append(row[column])
                output.append(numpy.average(percents))
            return output

        loadPMDK.append(computeAvg(pmdkLoadTemp))
        loadPMEMKV.append(computeAvg(pmemkvLoadTemp))
        if runUpdatePhase == True:
            updatePMDK.append(computeAvg(pmdkUpdateTemp))
            updatePMEMKV.append(computeAvg(pmemkvUpdateTemp))

saveStackedPlot('PMDK - Load - STDEV < ' + str(maxThreshold), valueSizes, convertData(loadPMDK), legendsPMDK, 'pmdk-load.pdf')
saveStackedPlot('PMEMKV - Load - STDEV < ' + str(maxThreshold), valueSizes, convertData(loadPMEMKV), legendsPMEMKV, 'pmemkv-load.pdf')
if runUpdatePhase == True:
    saveStackedPlot('PMDK - Update - STDEV < ' + str(maxThreshold), valueSizes, convertData(updatePMDK), legendsPMDK, 'pmdk-update.pdf')
    saveStackedPlot('PMEMKV - Update - STDEV < ' + str(maxThreshold), valueSizes, convertData(updatePMEMKV), legendsPMEMKV, 'pmemkv-update.pdf')
