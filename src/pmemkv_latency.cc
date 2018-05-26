#include <iostream>
#include <fstream>
#include <string>
#include <list>
#include <assert.h>
#include "pmemkv.h"

#define KV_ENGINE       "kvtree2"
#define POOL_PATH       "/mnt/ram/pmemkv"
#define POOL_SIZE       (size_t)16 << 30
#define OPS_COUNT       1E6
#define MAX_KEY_SIZE    255

using namespace std;
using namespace pmemkv;

int main(int argc, char **argv) {

    if (argc != 3) {
        cout << "Benchmark requires 3 arguments: " << endl;
        cout << "- Trace path (list of keys)" << endl;
        cout << "- Value size in bytes" << endl;
        return 1;
    }
    assert(argc == 3);

    string tracePath = argv[1];
    size_t valueSize = (size_t)stoi(argv[2]);

    // Load keys from trace file
    ifstream trace(tracePath);
    char buffer[MAX_KEY_SIZE];

    list<string> keys;
    while(trace) {
        trace.getline(buffer, MAX_KEY_SIZE);
        if(trace) keys.push_back(buffer);
    }
    trace.close();

    // Prepare value
    char *value = (char *)malloc(valueSize);
    assert(value != NULL);
    for (size_t i = 0; i < valueSize - 1; i++) {
        value[i] = (rand() % 2 == 0 ? 'a' : 'A') + (rand() % 26);
    }
    value[valueSize - 1] = '\0';
    string valueStr = value;

    // Open store
    KVEngine* kv = KVEngine::Open(KV_ENGINE, POOL_PATH, POOL_SIZE);

    // Run benchmark
    for (string key : keys) {
        KVStatus s = kv->Put(key, valueStr);
        assert(s == OK);
    }

    // Cleanup
    KVEngine::Close(kv);
    keys.clear();
    free(value);

    return 0;
}
