#include "db/twoqueueskiplist.h"
#include "include/leveldb/comparator.h"

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

  typedef char* Key;

  class TestComparator : public Comparator {
    // const Comparator* testcmp_;

    public:
    TestComparator() {}

    int operator()(const Key& a, const Key& b) const {
      if (strcmp(a, b) < 0) {
        return -1;
      } else if (strcmp(a, b) > 0) {
        return +1;
      } else {
        return 0;
      }
    }

    int Compare(const Slice& a, const Slice& b) const override;
    void FindShortestSeparator(std::string* start, const Slice& limit) const override;
    void FindShortSuccessor(std::string* key) const override;
    const char* Name() const override;
  };

  int TestComparator::Compare(const Slice& a, const Slice& b) const {
    return a.compare(b);
  }

  void TestComparator::FindShortestSeparator(std::string* start, const Slice& limit) const {}
  void TestComparator::FindShortSuccessor(std::string* key) const {}
  const char* TestComparator::Name() const {
    return "test comparator";
  }

  static std::string IKey(const std::string& user_key, uint64_t seq,
                        ValueType vt) {
  std::string encoded;
  AppendInternalKey(&encoded, ParsedInternalKey(user_key, seq, vt));
  return encoded;
  }

  TEST(TwoqueueSkipListTest, Iter) {
    Arena arena;
    TestComparator cmp;

    Twoqueue_SkipList<Key, TestComparator> list(cmp, &arena);
    char key[1];
    sprintf(key, "%d", 10);
    ASSERT_TRUE(!list.Contains(key));

    Twoqueue_SkipList<Key, TestComparator>::Iterator iter(&list);
    ASSERT_TRUE(!iter.Valid());
    iter.SeekToFirst();
    ASSERT_TRUE(!iter.Valid());
    sprintf(key, "%d", 100);
    iter.Seek(key);
    ASSERT_TRUE(!iter.Valid());
    iter.SeekToLast();
    ASSERT_TRUE(!iter.Valid());
  }

  TEST(TwoqueueSkipListTest, InsertAndLookup) {
    const int N = 2000;
    const int R = 5000;
    Random rnd(1000);
    std::set<Key> keys;
    Arena arena;
    TestComparator cmp;
    Twoqueue_SkipList<Key, TestComparator> list(cmp, &arena);
    for (int i = 0; i < N; i++) {
      std::string ikey = IKey(NumberToString((rnd.Next() % R)), i, kTypeValue);
      char* key = const_cast<char*>(ikey.c_str());
      if (keys.insert(key).second) {
        list.Insert(key);
      }
    }

    for (int i = 0; i < R; i++) {
      std::string ikey = IKey(NumberToString(i), i, kTypeValue);
      char* key = const_cast<char*>(ikey.c_str());
      if (list.Contains(key)) {
        ASSERT_EQ(keys.count(key), 1);
      } else {
        ASSERT_EQ(keys.count(key), 0);
      }
    }

    // Simple iterator tests
    {
      Twoqueue_SkipList<Key, TestComparator>::Iterator iter(&list);
      ASSERT_TRUE(!iter.Valid());

      iter.Seek(0);
      ASSERT_TRUE(iter.Valid());
      ASSERT_EQ(*(keys.begin()), iter.key());

      iter.SeekToFirst();
      ASSERT_TRUE(iter.Valid());
      ASSERT_EQ(*(keys.begin()), iter.key());

      iter.SeekToLast();
      ASSERT_TRUE(iter.Valid());
      ASSERT_EQ(*(keys.rbegin()), iter.key());
    }

    // Forward iteration test
    for (int i = 0; i < R; i++) {
      SkipList<Key, TestComparator>::Iterator iter(&list);
      std::string ikey = IKey(NumberToString(i), i, kTypeValue);
      char* key = const_cast<char*>(ikey.c_str());
      iter.Seek(key);

      // Compare against model iterator
      std::set<Key>::iterator model_iter = keys.lower_bound(key);
      for (int j = 0; j < 3; j++) {
        if (model_iter == keys.end()) {
          ASSERT_TRUE(!iter.Valid());
          break;
        } else {
          ASSERT_TRUE(iter.Valid());
          ASSERT_EQ(*model_iter, iter.key());
          ++model_iter;
          iter.Next();
        }
      }
    }

    // Backward iteration test
    {
      SkipList<Key, TestComparator>::Iterator iter(&list);
      iter.SeekToLast();

      // Compare against model iterator
      for (std::set<Key>::reverse_iterator model_iter = keys.rbegin();
          model_iter != keys.rend(); ++model_iter) {
        ASSERT_TRUE(iter.Valid());
        ASSERT_EQ(*model_iter, iter.key());
        iter.Prev();
      }
      ASSERT_TRUE(!iter.Valid());
    }
  }
    
} // namespace leveldb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}