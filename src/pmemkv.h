/*
 * Copyright 2017, Intel Corporation
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *
 *     * Neither the name of the copyright holder nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#pragma once

#include <string>
#include <vector>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/make_persistent_array.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>

using std::string;
using std::vector;
using nvml::obj::p;
using nvml::obj::persistent_ptr;
using nvml::obj::make_persistent;
using nvml::obj::transaction;
using nvml::obj::delete_persistent;
using nvml::obj::pool;

namespace pmemkv {

#define INNER_KEYS 4                                       // maximum keys for inner nodes
#define INNER_KEYS_MIDPOINT (INNER_KEYS / 2)               // halfway point within the node
#define INNER_KEYS_UPPER ((INNER_KEYS / 2) + 1)            // index where upper half of keys begins
#define NODE_KEYS 48                                       // maximum keys in tree nodes
#define NODE_KEYS_MIDPOINT 24                              // halfway point within the node
#define SSO_CHARS 15                                       // chars for short string optimization
#define SSO_SIZE (SSO_CHARS + 1)                           // sso chars plus null terminator

class KVString {                                           // persistent string class
  public:                                                  // start public fields and methods
    char* data() const;                                    // returns data as c-style string
    bool is_short() const { return !str; }                 // returns true for short strings
    void set(const char* value);                           // copy data from c-style string
    void set_short(const char* value);                     // copy data from known short string
  private:                                                 // start private fields and methods
    char sso[SSO_SIZE];                                    // local storage for short strings
    persistent_ptr<char[]> str;                            // pointer to storage for longer strings
};

struct KVLeaf {                                            // persistent leaves of the tree
    p<uint8_t> hashes[NODE_KEYS];                          // 48 bytes, Pearson hashes of keys
    persistent_ptr<KVLeaf> next;                           // 16 bytes, points to next leaf
    p<KVString> keys[NODE_KEYS];                           // key strings stored in this leaf
    p<KVString> values[NODE_KEYS];                         // value strings stored in this leaf
};

struct KVRoot {                                            // persistent root object
    p<uint64_t> opened;                                    // number of times opened
    p<uint64_t> closed;                                    // number of times closed safely
    persistent_ptr<KVLeaf> head;                           // head of linked list of leaves
};

struct KVNode {                                            // volatile nodes of the tree
    bool is_leaf = false;                                  // indicate inner or leaf node
    KVNode* parent;                                        // parent of this node (null if top)
};

struct KVInnerNode : KVNode {                              // volatile inner nodes of the tree
    uint8_t keycount;                                      // count of keys in this node
    string keys[INNER_KEYS + 1];                           // child keys plus one overflow slot
    KVNode* children[INNER_KEYS + 2];                      // child nodes plus one overflow slot
};

struct KVLeafNode : KVNode {                               // volatile leaf nodes of the tree
    uint8_t hashes[NODE_KEYS];                             // Pearson hashes of keys
    string keys[NODE_KEYS];                                // keys stored in this leaf
    persistent_ptr<KVLeaf> leaf;                           // pointer to persistent leaf
    bool lock;                                             // boolean modification lock
};

struct KVRecoveredLeaf {                                   // temporary wrapper used for recovery
    KVLeafNode* leafnode;                                  // leaf node being recovered
    char* max_key;                                         // highest sorting key present
};

enum KVStatus {
    OK = 0,
    NOT_FOUND = 1,
    CORRUPTION = 2,
    NOT_SUPPORTED = 3,
    INVALID_ARGUMENT = 4,
    IO_ERROR = 5,
    MERGE_IN_PROGRESS = 6,
    INCOMPLETE = 7,
    SHUTDOWN_IN_PROGRESS = 8,
    TIMED_OUT = 9,
    ABORTED = 10,
    BUSY = 11,
    EXPIRED = 12,
    TRY_AGAIN = 13,
};

class KVTree {
  public:
    KVTree(const string& name, const size_t size);
    ~KVTree();
    const string& GetName() const { return name; }         // path when constructed
    const char* GetNamePtr() const { return name.c_str(); }
    const size_t GetSize() const { return size; }          // size when constructed
    KVStatus Delete(const string& key);                    // remove single key
    KVStatus Get(const string& key, string* value);        // read single key/value
    KVStatus Put(const string& key, const string& value);  // write single key/value
    vector<KVStatus> MultiGet(const vector<string>& keys,  // read multiple keys at once
                              vector<string>* values);
  protected:
    KVLeafNode* LeafSearch(const string& key);             // find node for key
    void LeafFillFirstEmptySlot(KVLeafNode* leafnode,      // fill first unoccupied slot
                                const uint8_t hash,
                                const string& key,
                                const string& value);
    bool LeafFillSlotForKey(KVLeafNode* leafnode,          // fill slot for matching key
                            const uint8_t hash,
                            const string& key,
                            const string& value);
    void LeafFillSpecificSlot(KVLeafNode* leafnode,        // fill slot at specific index
                              const uint8_t hash,
                              const string& key,
                              const string& value,
                              const int slot);
    void LeafSplit(KVLeafNode* leafnode,                   // split full leaf into two leaves
                   const uint8_t hash,
                   const string& key,
                   const string& value);
    void LeafUpdateParentsAfterSplit(KVNode* node,         // recursively update/split parents
                                     KVNode* new_node,
                                     string* split_key);
    uint8_t PearsonHash(const char* data,                  // calculate 1-byte hash for string
                        const size_t size);
    void RebuildNodes();
    void Recover();
    void Shutdown();
  private:
    KVTree(const KVTree&);                                 // prevent copying
    void operator=(const KVTree&);                         // prevent assignment
    const string name;                                     // name when constructed
    pool<KVRoot> pop_;                                     // pool for persistent root
    const size_t size;                                     // size when constructed
    KVNode* top_ = nullptr;                                // top of volatile tree
};

} // namespace pmemkv