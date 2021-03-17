#include "db/twoqueueskiplist.h"

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

  struct Comparator {
    int operator()(const Key& a, const Key& b) const {
      if (a < b) {
        return -1;
      } else if (a > b) {
        return +1;
      } else {
        return 0;
      }
    }
  };
    
  TEST(TwoqueueSkipListTest, Init) {
    Arena arena;
    Comparator cmp;
    Twoqueue_SkipList<Key, Comparator> list(cmp, &arena);
    ASSERT_TRUE(!list.Contains(10));

    Twoqueue_SkipList<Key, Comparator>::Iterator iter(&list);
    ASSERT_TRUE(!iter.Valid());
    iter.SeekToFirst();
    ASSERT_TRUE(!iter.Valid());
    iter.Seek(100);
    ASSERT_TRUE(!iter.Valid());
    iter.SeekToLast();
    ASSERT_TRUE(!iter.Valid());
  }
    
} // namespace leveldb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}