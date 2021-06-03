#include "db/tqmemtable.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"
#include <iostream>

namespace leveldb {

class TQMemTableIterator;

static Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data;
  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
  return Slice(p, len);
}

TQMemTable::TQMemTable(const InternalKeyComparator& comparator, const size_t& write_buffer_size)
    : comparator_(comparator), refs_(0), tqtable_(comparator_, &arena_, write_buffer_size) {}

TQMemTable::~TQMemTable() { assert(refs_ == 0); }

size_t TQMemTable::ApproximateMemoryUsage() { return arena_.MemoryUsage(); }

size_t TQMemTable::ApproximateColdArea() { return tqtable_.GetColdAreaSize(); }

size_t TQMemTable::ApproximateNormalArea() { return tqtable_.GetNormalAreaSize(); }

//normal_nodes_中存放热数据区的键值，用于重构包含有热数据的新memtable
int TQMemTable::CreateNewAndImm() {
  return tqtable_.Seperate();
}

int TQMemTable::KeyComparator::operator()(const char* aptr,
                                        const char* bptr) const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(aptr);
  Slice b = GetLengthPrefixedSlice(bptr);
  return comparator.Compare(a, b);
}

static const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, target.size());
  scratch->append(target.data(), target.size());
  return scratch->data();
}

class TQMemTableIterator : public Iterator {
 public:

  //使用2Q跳表
  explicit TQMemTableIterator(TQMemTable::TQTable* table) : iter_(table) {}

  TQMemTableIterator(const TQMemTableIterator&) = delete;
  TQMemTableIterator& operator=(const TQMemTableIterator&) = delete;

  ~TQMemTableIterator() override = default;

  bool Valid() const override { return iter_.Valid(); }
  void Seek(const Slice& k) override { iter_.Seek(EncodeKey(&tmp_, k)); }
  void SeekToFirst() override { iter_.SeekToFirst(); }
  void SeekToLast() override { iter_.SeekToLast(); }
  void Next() override { iter_.Next(); }
  void Prev() override { iter_.Prev(); }
  Slice key() const override { return GetLengthPrefixedSlice(iter_.key()); }
  Slice value() const override {
    Slice key_slice = GetLengthPrefixedSlice(iter_.key());
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }
  const char* Get() const { return iter_.key(); }
  void Newer() { iter_.Newer(); }
  void Older() { iter_.Older(); }
  void SeekToNormalHead() { iter_.SeekToNormalHead(); }
  size_t GetDataSize() { return iter_.GetDataSize(); }

  Status status() const override { return Status::OK(); }

 private:
  //2Q跳表
  TQMemTable::TQTable::TQIterator iter_;

  std::string tmp_;  // For passing to EncodeKey
};

Iterator* TQMemTable::NewIterator() { return new TQMemTableIterator(&tqtable_); }
TQMemTableIterator* TQMemTable::GetTQMemTableIterator() {
  return new TQMemTableIterator(&tqtable_);
}

//利用当前使用的MemTable的迭代器将热数据复制到这一MemTable中
void TQMemTable::Substitute(TQMemTableIterator* iter) {
  iter->SeekToNormalHead();
  while (iter->Valid()) {
    const char* entry = iter->Get();
    const size_t encoded_len = iter->GetDataSize();
    char* buf = arena_.Allocate(encoded_len);
    std::memcpy(buf, entry, encoded_len);

    tqtable_.Insert(buf, encoded_len);
    iter->Newer();
  }
}

//最后插入到TwoQueueSkipList中
void TQMemTable::Add(SequenceNumber s, ValueType type, const Slice& key,
                            const Slice& value) {
  size_t key_size = key.size();
  size_t val_size = value.size();
  size_t internal_key_size = key_size + 8;
  const size_t encoded_len = VarintLength(internal_key_size) +
                             internal_key_size + VarintLength(val_size) +
                             val_size;
  char* buf = arena_.Allocate(encoded_len);
  char* p = EncodeVarint32(buf, internal_key_size);
  std::memcpy(p, key.data(), key_size);
  p += key_size;
  EncodeFixed64(p, (s << 8) | type);
  p += 8;
  p = EncodeVarint32(p, val_size);
  std::memcpy(p, value.data(), val_size);
  assert(p + val_size == buf + encoded_len);
  tqtable_.Insert(buf, encoded_len);                         
}

bool TQMemTable::Get(const LookupKey& key, std::string* value, Status* s) {
  Slice memkey = key.memtable_key();
  TQTable::TQIterator iter(&tqtable_);
  iter.Seek(memkey.data());
  if (iter.Valid()) {
    // entry format is:
    //    klength  varint32
    //    userkey  char[klength]
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    const char* entry = iter.key();
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
    if (comparator_.comparator.user_comparator()->Compare(
            Slice(key_ptr, key_length - 8), key.user_key()) == 0) {
      // Correct user key
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      switch (static_cast<ValueType>(tag & 0xff)) {
        case kTypeValue: {
          Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
          value->assign(v.data(), v.size());
          return true;
        }
        case kTypeDeletion:
          *s = Status::NotFound(Slice());
          return true;
      }
    }
  }
  return false;
}

}  // namespace leveldb