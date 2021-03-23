#include "db/twoqueueskiplist.h"
#include "include/leveldb/comparator.h"
#include "db/dbformat.h"

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

  struct TestKeyComparator {

    const InternalKeyComparator comparator;

    public:
    explicit TestKeyComparator(const InternalKeyComparator& c) : comparator(c) {}

    int operator()(const char* aptr, const char* bptr) const {
      Slice a = GetLengthPrefixedSlice(aptr);
      Slice b = GetLengthPrefixedSlice(bptr);
      return comparator.Compare(a, b);
    }
    
  };

  class TestComparator : public Comparator {
     const char* Name() const override {
       return "testcomparator";
     }
     int Compare(const Slice& a, const Slice& b) const override {
       return BytewiseComparator()->Compare(a, b);
     }
     void FindShortestSeparator(std::string* start, const Slice& limit) const override {}
     void FindShortSuccessor(std::string* key) const override {}
  };

  std::string IKey(const std::string& user_key, uint64_t seq,
                        ValueType vt) {
    std::string rep_;
    PutLengthPrefixedSlice(&rep_, user_key);

    Slice input(rep_);
    Slice key;
    GetLengthPrefixedSlice(&input, &key);

    size_t key_size = key.size();
    size_t internal_key_size = key_size + 8;
    const size_t encoded_len = VarintLength(internal_key_size) + internal_key_size;
    Arena arena_;
    char* buf = arena_.Allocate(encoded_len);
    char* p = EncodeVarint32(buf, internal_key_size);
    std::memcpy(p, key.data(), key_size);
    p += key_size;
    EncodeFixed64(p, (seq << 8) | vt);
    p += 8;
    assert(p == buf + encoded_len);

    return buf;
  }

  TEST(TwoqueueSkipListTest, Iter) {
    Arena arena;
    TestComparator tcmp;
    InternalKeyComparator icmp(&tcmp);
    TestKeyComparator cmp(icmp);

    Twoqueue_SkipList<Key, TestKeyComparator> list(cmp, &arena);
    char key[1];
    sprintf(key, "%d", 10);
    ASSERT_TRUE(!list.Contains(key));

    Twoqueue_SkipList<Key, TestKeyComparator>::Iterator iter(&list);
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
    std::set<std::string> keys;
    Arena arena;
    TestComparator tcmp;
    InternalKeyComparator icmp(&tcmp);
    TestKeyComparator cmp(icmp);
    Twoqueue_SkipList<Key, TestKeyComparator> list(cmp, &arena);

    for (int i = 0; i < N/4; i++) {
      std::string ikey = IKey(NumberToString(1), i, kTypeValue);;
      if (keys.insert(ikey).second) {
        char* buf = new char[ikey.size()];
        strcpy(buf, ikey.c_str());
        list.Insert(buf);
      }
    }

    for (int i = N/4; i < N/2; i++) {
      std::string ikey = IKey(NumberToString((i % 500)), i, kTypeValue);;
      if (keys.insert(ikey).second) {
        char* buf = new char[ikey.size()];
        strcpy(buf, ikey.c_str());
        list.Insert(buf);
      }
    }

    for (int i = 0; i < R; i++) {
      std::string ikey = IKey(NumberToString((i % 500)), i, kTypeValue);
      size_t size = ikey.size();
      char buf[size];
      strcpy(buf, ikey.c_str());      
      if (list.Contains(buf)) {
        ASSERT_EQ(keys.count(ikey), 1);
      } else {
        ASSERT_EQ(keys.count(ikey), 0);
      }
    }

    // // Simple iterator tests
    // {
    //   Twoqueue_SkipList<Key, TestKeyComparator>::Iterator iter(&list);
    //   ASSERT_TRUE(!iter.Valid());

    //   std::string ikey = IKey(NumberToString(0), 0, kTypeValue);
    //   size_t size = ikey.size();
    //   char buf[size];
    //   strcpy(buf, ikey.c_str());
    //   iter.Seek(buf);
    //   ASSERT_TRUE(iter.Valid());
    //   ASSERT_EQ(*(keys.begin()), std::string(iter.key()));

    //   iter.SeekToFirst();
    //   ASSERT_TRUE(iter.Valid());
    //   ASSERT_EQ(*(keys.begin()), std::string(iter.key()));

    //   iter.SeekToLast();
    //   ASSERT_TRUE(iter.Valid());
    //   ASSERT_EQ(*(keys.rbegin()), std::string(iter.key()));
    // }

    // // Forward iteration test
    // for (int i = 0; i < R; i++) {
    //   SkipList<Key, TestKeyComparator>::Iterator iter(&list);
    //   std::string ikey = IKey(NumberToString(i % 1000), i, kTypeValue);
    //   size_t size = ikey.size();
    //   char buf[size];
    //   strcpy(buf, ikey.c_str());
    
    //   iter.Seek(buf);

    //   // Compare against model iterator
    //   std::set<std::string>::iterator model_iter = keys.lower_bound(ikey);
    //   for (int j = 0; j < 3; j++) {
    //     if (model_iter == keys.end()) {
    //       ASSERT_TRUE(!iter.Valid());
    //       break;
    //     } else {
    //       ASSERT_TRUE(iter.Valid());
    //       ASSERT_EQ(*model_iter, std::string(iter.key()));
    //       ++model_iter;
    //       iter.Next();
    //     }
    //   }
    // }

    // // Backward iteration test
    // {
    //   SkipList<Key, TestKeyComparator>::Iterator iter(&list);
    //   iter.SeekToLast();

    //   // Compare against model iterator
    //   for (std::set<std::string>::reverse_iterator model_iter = keys.rbegin();
    //       model_iter != keys.rend(); ++model_iter) {
    //     ASSERT_TRUE(iter.Valid());
    //     ASSERT_EQ(*model_iter, std::string(iter.key()));
    //     iter.Prev();
    //   }
    //   ASSERT_TRUE(!iter.Valid());
    // }
  }
    
} // namespace leveldb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}