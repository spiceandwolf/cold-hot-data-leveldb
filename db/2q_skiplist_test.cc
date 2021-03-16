#include "db/2q_skiplist.h"

#include <atomic>
#include <set>

#include "gtest/gtest.h"
#include "leveldb/env.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include "util/arena.h"
#include "util/hash.h"
#include "util/random.h"
#include "util/testutil.h"

namespace leveldb {

    typedef uint64_t Key;
} // namespace leveldb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}