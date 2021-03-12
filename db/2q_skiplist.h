#ifndef STORAGE_LEVELDB_DB_TwoQueue_SkipList_H
#define STORAGE_LEVELDB_DB_TwoQueue_SkipList_H

#include <atomic>
#include <cassert>
#include <cstdlib>

#include "db/skiplist.h"

namespace leveldb
{
    template<typename Key, class Comparator>
    class TwoQueue_SkipList : public SkipList
    {
    private:
        /* data */
        struct Twoqueue_Node;//2qskiplist下的节点
        
    public:
        explicit TwoQueue_SkipList(Comparator cmp, Arena* arena) : SkipList(cmp, arena);

        TwoQueue_SkipList(const TwoQueue_SkipList&) = delete;
        TwoQueue_SkipList& operator=(const TwoQueue_SkipList&) = delete;

        //重定义insert()，在2qskiplist中插入2qNode
        void Insert(const Key& key);
        //Contains()函数没有变化
        //Iterator已有的功能不需要变化

    private:
        enum { kMaxHeight = 12};

        inline int GetMaxHeight() const {
            return max_height_.load(std::memory_order_relaxed);
        }

        Twoqueue_Node* NewTwoqueue_Node(const Key& key, int height);

        bool KeyIsAfterNode(const Key& key, Twoqueue_Node* n) const;
        Twoqueue_Node* FindGreaterOrEqual(const Key& key, Twoqueue_Node** prev) const;
        Twoqueue_Node* FindLessThan(const Key& key) const;
        Twoqueue_Node* FindLast() const;

        Comparator const compare_;//同skiplist
        Arena* const arena_;//同skiplist

        Twoqueue_Node* const normal_header;//热数据区
        Twoqueue_Node* const cold_header;//冷数据区

        Twoqueue_Node* cur_scan_node;//当前扫描到的节点
        Twoqueue_Node* cur_cold_node;//当前最新的冷数据
        Twoqueue_Node* cur_apply_node;//当前插入的最新数据

        std::atomic<int> max_height_;//同skiplist
        Random rnd_;//同skiplist
    };
    
    template <typename Key, class Comparator>
    struct TwoQueue_SkipList<Key, Comparator>::Twoqueue_Node {
        /* data */

    };
    
    
} // namespace leveldb

#endif // STORAGE_LEVELDB_DB_TwoQueue_SkipList_H