#ifndef STORAGE_LEVELDB_DB_TQMEMTABLE_H_
#define STORAGE_LEVELDB_DB_TQMEMTABLE_H_

#include <string>

#include "db/dbformat.h"
#include "db/twoqueueskiplist.h"
#include "leveldb/db.h"
#include "util/arena.h"

namespace leveldb
{

class InternalKeyComparator;
class MemTableIterator;

class MemTable {

public:
  //使用2Q跳表的MemTable
  MemTable(const InternalKeyComparator& comparator, const size_t& write_buffer_size);

  MemTable(const MemTable&) = delete;
  MemTable& operator=(const MemTable&) = delete;
  
  void Ref() { ++refs_; }

  size_t ApproximateMemoryUsage();

  Iterator* NewIterator();

  //将一个entry添加到memtable的TwoQueueSkipList中，功能同Add()
  void Add(SequenceNumber seq, ValueType type, const Slice& key,
                    const Slice& value);

  bool Get(const LookupKey& key, std::string* value, Status* s);

private:
  friend class MemTableIterator;
  friend class MemTableBackwardIterator;

  struct KeyComparator {
    const InternalKeyComparator comparator;
    explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) {}
    int operator()(const char* a, const char* b) const;
  };

  //使用2Q跳表
  typedef Twoqueue_SkipList<const char*, KeyComparator> TQTable;

  ~MemTable();  // Private since only Unref() should be used to delete it

  KeyComparator comparator_;
  int refs_;
  Arena arena_;

  //2Q跳表
  TQTable tqtable_;
  //写内存的最大尺寸
//   const size_t write_buffer_size;                    
};

} // namespace leveldb


#endif  // STORAGE_LEVELDB_DB_TQMEMTABLE_H_