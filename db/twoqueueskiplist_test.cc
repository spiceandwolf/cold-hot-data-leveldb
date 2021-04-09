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
  const size_t kHeader = 12;

  Slice GetLengthPrefixedSlice(const char* data) {
        uint32_t len;
        const char* p = data;
        p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
        return Slice(p, len);
    }

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

  //返回的string类型在转换成char*的过程中存在bug使二者不能完全相同
  std::string IKey( Arena& arena_, 
                        const std::string& user_key, const std::string& user_value, uint64_t seq,
                        ValueType vt, Twoqueue_SkipList<Key, TestKeyComparator>* table = nullptr) {
    Slice key(user_key);
    Slice value(user_value);

    size_t key_size = key.size();
    size_t val_size = value.size();
    size_t internal_key_size = key_size + 8;
    const size_t encoded_len = VarintLength(internal_key_size) + internal_key_size + VarintLength(val_size) +
                             val_size;
    
    char* buf = arena_.Allocate(encoded_len);
    char* p = EncodeVarint32(buf, internal_key_size);
    std::memcpy(p, key.data(), key_size);
    p += key_size;
    EncodeFixed64(p, (seq << 8) | vt);
    p += 8;
    p = EncodeVarint32(p, val_size);
    std::memcpy(p, value.data(), val_size);
    assert(p + val_size == buf + encoded_len);

    if (table != nullptr) table->Insert(buf, encoded_len);

    return buf;
  }

  TEST(TwoqueueSkipListTest, Iter) {
    Arena arena1;
    TestComparator tcmp;
    InternalKeyComparator icmp(&tcmp);
    TestKeyComparator cmp(icmp);

    Twoqueue_SkipList<Key, TestKeyComparator> list(cmp, &arena1, 10000);
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
   
    Arena arena;
    
    std::set<std::string> keys;
    TestComparator tcmp;
    InternalKeyComparator icmp(&tcmp);
    TestKeyComparator cmp(icmp);
    Twoqueue_SkipList<Key, TestKeyComparator> list(cmp, &arena, 5000);
    

    for (uint64_t i = 0; i < 100; i++) {
    
      std::string ikey = IKey(arena, std::to_string(i % 10), std::to_string(i), i + 1, kTypeValue, &list);
      size_t size = ikey.size();
      char buf[size];
      strcpy(buf, ikey.c_str());
      ASSERT_TRUE(keys.insert(buf).second);
    
    }

    ASSERT_TRUE(list.GetNormalAreaSize() <= 5000);
    ASSERT_TRUE((list.GetNormalAreaSize() + list.GetColdAreaSize()) < arena.MemoryUsage());

    // for (uint64_t i = 0; i < R; i++) {
    //   std::string ikey = IKey(std::to_string((i % 1000)), i + 1, kTypeValue);
    //   size_t size = ikey.size();
    //   char buf[size];
    //   strcpy(buf, ikey.c_str());      
    //   if (list.Contains(buf)) {
    //     ASSERT_EQ(keys.count(ikey), 1);
    //   } else {
    //     ASSERT_EQ(keys.count(ikey), 0);
    //   }
    // }

    // Simple iterator tests
    {
      Twoqueue_SkipList<Key, TestKeyComparator>::TQIterator iter(&list);
      ASSERT_TRUE(!iter.Valid());

      std::string ikey = IKey(arena, std::to_string(0 % 10), std::to_string(0), 1 + 0, kTypeValue);

      size_t size = ikey.size();
      char buf[size];
      strcpy(buf, ikey.c_str());

      iter.Seek(buf);
      ASSERT_TRUE(iter.Valid());
      ASSERT_EQ(*buf, *(iter.key()));

      iter.SeekToFirst();
      ASSERT_TRUE(iter.Valid());
      ikey = IKey(arena, std::to_string(90 % 10), std::to_string(90), 1 + 90, kTypeValue);
      ASSERT_EQ(ikey, std::string(iter.key()));

      iter.SeekToLast();
      ASSERT_TRUE(iter.Valid());
      ikey = IKey(arena, std::to_string(9 % 10), std::to_string(9), 1 + 9, kTypeValue);
      ASSERT_EQ(ikey, std::string(iter.key()));
    }

    // Forward iteration test
    // for (uint64_t i = 9; i < 100; i = i + 10) {
    //   Twoqueue_SkipList<Key, TestKeyComparator>::TQIterator iter(&list);
    //   std::string ikey = IKey(arena, std::to_string(i % 10), std::to_string(i), i + 1, kTypeValue);
    //   size_t size = ikey.size();
    //   char buf[size];
    //   strcpy(buf, ikey.c_str());
    
    //   iter.Seek(buf);

    //   // Compare against model iterator
    //   std::set<std::string>::iterator model_iter = keys.lower_bound(ikey);
    //   for (uint64_t j = 0; j < i / 10 + 1; j++) {
    //     if (model_iter == keys.end()) {
    //       ASSERT_TRUE(!iter.Valid());
    //       break;
    //     } else {
    //       ASSERT_TRUE(iter.Valid());
    //       ASSERT_EQ(*(model_iter), std::string(iter.key()));
    //       model_iter--;
    //       iter.Next();
    //     }
    //   }
    // }

    // Backward iteration test
    // {
    //   Twoqueue_SkipList<Key, TestKeyComparator>::TQIterator iter(&list);
    //   iter.SeekToLast();
    //   std::string key = std::string(iter.key());

    //   // Compare against model iterator
    //   for (std::set<std::string>::iterator model_iter = keys.find(key);
    //       model_iter != keys.end(); ++model_iter) {
    //     ASSERT_TRUE(iter.Valid());
    //     ASSERT_EQ(*model_iter, std::string(iter.key()));
    //     iter.Prev();
    //   }
      
    // }

    std::vector<std::pair<Slice, Slice>> normal_nodes_;
    int a = list.Seperate(normal_nodes_);
    ASSERT_TRUE(a == 0);
    Slice x[10];
    for (int i = 0; i < normal_nodes_.size(); i++) {
      std::cout << "key: " << normal_nodes_[i].first.ToString() <<
                  " value: " << normal_nodes_[i].second.ToString() << std::endl;
    }
  }
    
} // namespace leveldb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}