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
legendsPMEMKV = ['Lookup', 'New Leaf', 'Existing Leaf', 'Split Leaf', 'Maintenance']
legendsPMEMKV2 = ['Lookup',
                  'New Leaf',
                  'Old Leaf / Lookup',
                  'Old Leaf / Tx - Direct',
                  'Old Leaf / Tx - Logging',
                  'Old Leaf / Tx - Alloc',
                  'Old Leaf / Tx - Memcpy',
                  'Old Leaf / Tx - Other',
                  'Split Leaf / Find Key',
                  'Split Leaf / Tx',
                  'Split Leaf / Post Proc',
                  'Other']
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
    totalCycles = int(lines[13].split(',')[1])
    lookupCycles = int(lines[7].split(',')[1])
    newLeafCycles = int(lines[8].split(',')[1])
    existingLeafCycles = int(lines[9].split(',')[1])
    splitLeafCycles = int(lines[10].split(',')[1])
    maintenanceCycles = int(lines[11].split(',')[1])

    lookupPercent = 100 * float(lookupCycles) / totalCycles
    newLeafPercent = 100 * float(newLeafCycles) / totalCycles
    existingLeafPercent = 100 * float(existingLeafCycles) / totalCycles
    splitLeafPercent = 100 * float(splitLeafCycles) / totalCycles
    maintenancePercent = 100 * float(maintenanceCycles) / totalCycles

    output = [lookupPercent, newLeafPercent, existingLeafPercent, splitLeafPercent, maintenancePercent]
    return output

def parsePMEMKVOutputNoMaintenance(out):
    lines = out.splitlines()
    totalCycles = int(lines[13].split(',')[1]) - int(lines[11].split(',')[1])
    lookupCycles = int(lines[7].split(',')[1])
    newLeafCycles = int(lines[8].split(',')[1])
    oldLeafLookupCycles = int(lines[14].split(',')[1])
    oldLeafTxCycles = int(lines[15].split(',')[1])
    splitLeafFindKeyCycles = int(lines[16].split(',')[1])
    splitLeafTxCycles = int(lines[17].split(',')[1])
    splitLeafPostProcCycles = int(lines[18].split(',')[1])

    # Breakdown of oldLeafTxCycles
    oldLeafTxGetDirectCycles = int(lines[19].split(',')[1])
    oldLeafTxLoggingCycles = int(lines[20].split(',')[1])
    oldLeafTxAllocCycles = int(lines[21].split(',')[1])
    oldLeafTxMemcpyCycles = int(lines[22].split(',')[1])

    lookupPercent = 100 * float(lookupCycles) / totalCycles
    newLeafPercent = 100 * float(newLeafCycles) / totalCycles
    oldLeafLookupPercent = 100 * float(oldLeafLookupCycles) / totalCycles
    oldLeafTxPercent = 100 * float(oldLeafTxCycles) / totalCycles
    splitLeafFindKeyPercent = 100 * float(splitLeafFindKeyCycles) / totalCycles
    splitLeafTxPercent = 100 * float(splitLeafTxCycles) / totalCycles
    splitLeafPostProcPercent = 100 * float(splitLeafPostProcCycles) / totalCycles

    # Breakdown of oldLeafTxPercent
    oldLeafTxGetDirectPercent = 100 * float(oldLeafTxGetDirectCycles) / totalCycles
    oldLeafTxLoggingPercent = 100 * float(oldLeafTxLoggingCycles) / totalCycles
    oldLeafTxAllocPercent = 100 * float(oldLeafTxAllocCycles) / totalCycles
    oldLeafTxMemcpyPercent = 100 * float(oldLeafTxMemcpyCycles) / totalCycles
    oldLeafTxOtherPercent = oldLeafTxPercent - numpy.sum([
        oldLeafTxGetDirectPercent,
        oldLeafTxLoggingPercent,
        oldLeafTxAllocPercent,
        oldLeafTxMemcpyPercent,
    ])

    output = [lookupPercent,
              newLeafPercent,
              oldLeafLookupPercent,
              oldLeafTxGetDirectPercent,
              oldLeafTxLoggingPercent,
              oldLeafTxAllocPercent,
              oldLeafTxMemcpyPercent,
              oldLeafTxOtherPercent,
              splitLeafFindKeyPercent,
              splitLeafTxPercent,
              splitLeafPostProcPercent]
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
            temp.append(int(numpy.ceil(row[c])))
        output.append(temp)
    for row in data:
        other.append(numpy.maximum(0, 100 - int(numpy.sum([numpy.ceil(t) for t in row]))))
    output.append(other)
    return output

loadPMDK = []
updatePMDK = []
loadPMEMKV = []
loadPMEMKVNoMaintenance = []
updatePMEMKV = []
updatePMEMKVNoMaintenance = []
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
        loadDurability = []
        loadLogging = []
        loadLocking = []
        loadAllocation = []
        updateDurability = []
        updateLogging = []
        updateLocking = []
        updateAllocation = []

        # PMEMKV output
        loadLookup = []
        loadNewLeaf = []
        loadExistingLeaf = []
        loadSplitLeaf = []
        loadMaint = []
        updateLookup = []
        updateNewLeaf = []
        updateExistingLeaf = []
        updateSplitLeaf = []
        updateMaint = []

        # PMEMKV output - no maintenance
        noMaintenancePMEMKV = []

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
            data = parsePMDKOutput(out)
            loadDurability.append(data[0])
            loadLogging.append(data[1])
            loadLocking.append(data[2])
            loadAllocation.append(data[3])

            # Parse PMEMKV output
            data = parsePMEMKVOutput(out)
            loadLookup.append(data[0])
            loadNewLeaf.append(data[1])
            loadExistingLeaf.append(data[2])
            loadSplitLeaf.append(data[3])
            loadMaint.append(data[4])

            # Parse PMEMKV output - no maintenance
            data = parsePMEMKVOutputNoMaintenance(out)
            noMaintenancePMEMKV.append(data)

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
            data = parsePMDKOutput(out)
            updateDurability.append(data[0])
            updateLogging.append(data[1])
            updateLocking.append(data[2])
            updateAllocation.append(data[3])

            # Parse PMEMKV output
            data = parsePMEMKVOutput(out)
            updateLookup.append(data[0])
            updateNewLeaf.append(data[1])
            updateExistingLeaf.append(data[2])
            updateSplitLeaf.append(data[3])
            updateMaint.append(data[4])

            # Parse PMEMKV output - no maintenance
            # TODO

        # Check if results are accurate
        for percents in [loadDurability,
                         loadLogging,
                         loadLocking,
                         loadAllocation,
                         updateDurability,
                         updateLogging,
                         updateLocking,
                         updateAllocation,
                         loadLookup,
                         loadNewLeaf,
                         loadExistingLeaf,
                         loadSplitLeaf,
                         loadMaint,
                         updateLookup,
                         updateNewLeaf,
                         updateExistingLeaf,
                         updateSplitLeaf,
                         updateMaint]:
            if len(percents) == 0:
                continue
            if numpy.std(percents) > threshold:
                mustRepeat = True
                break
        if mustRepeat:
            continue

        # Compute average percentages: PMDK
        result = []
        for percents in [loadDurability, loadLogging, loadLocking, loadAllocation]:
            result.append(numpy.average(percents))
        loadPMDK.append(result)
        if runUpdatePhase == True:
            result = []
            for percents in [updateDurability, updateLogging, updateLocking, updateAllocation]:
                result.append(numpy.average(percents))
            updatePMDK.append(result)

        # Compute average percentages: PMEMKV
        result = []
        #for percents in [loadLookup, loadNewLeaf, loadExistingLeaf, loadSplitLeaf, loadMaint]:
        for percents in [loadLookup, loadNewLeaf, loadExistingLeaf, loadSplitLeaf]:
            result.append(numpy.average(percents))
        loadPMEMKV.append(result)
        if runUpdatePhase == True:
            result = []
            #for percents in [updateLookup, updateNewLeaf, updateExistingLeaf, updateSplitLeaf, updateMaint]:
            for percents in [updateLookup, updateNewLeaf, updateExistingLeaf, updateSplitLeaf]:
                result.append(numpy.average(percents))
            updatePMEMKV.append(result)

        # Compute average percentages : PMEMKV with no maintenance overhead
        result = []
        for c in range(0, len(noMaintenancePMEMKV[0])):
            temp = []
            for row in noMaintenancePMEMKV:
                temp.append(row[c])
            result.append(numpy.average(temp))
        loadPMEMKVNoMaintenance.append(result)
        # TODO update

saveStackedPlot('PMDK - Load - STDEV < ' + str(maxThreshold), valueSizes, convertData(loadPMDK), legendsPMDK, 'pmdk-load.pdf')
saveStackedPlot('PMEMKV - Load - STDEV < ' + str(maxThreshold), valueSizes, convertData(loadPMEMKV), legendsPMEMKV, 'pmemkv-load.pdf')
saveStackedPlot('PMEMKV - Load (no maintenance) - STDEV < ' + str(maxThreshold), valueSizes, convertData(loadPMEMKVNoMaintenance), legendsPMEMKV2, 'pmemkv-load-no-maint.pdf')
if runUpdatePhase:
    saveStackedPlot('PMDK - Update - STDEV < ' + str(maxThreshold), valueSizes, convertData(updatePMDK), legendsPMDK, 'pmdk-update.pdf')
    saveStackedPlot('PMEMKV - Update - STDEV < ' + str(maxThreshold), valueSizes, convertData(updatePMEMKV), legendsPMEMKV, 'pmemkv-update.pdf')
    # TODO
