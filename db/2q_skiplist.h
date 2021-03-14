#ifndef STORAGE_LEVELDB_DB_Twoqueue_SkipList_H
#define STORAGE_LEVELDB_DB_Twoqueue_SkipList_H

#include <atomic>
#include <cassert>
#include <cstdlib>

#include "db/skiplist.h"
#include "db/dbformat.h"

namespace leveldb
{
    class Arena;

    template<typename Key, class Comparator>
    class Twoqueue_SkipList : public SkipList<Key, Comparator>
    {
    private:
        /* data */
        struct Twoqueue_Node;//2qskiplist下的节点
        
    public:
        explicit Twoqueue_SkipList(Comparator cmp, Arena* arena) : SkipList(cmp, arena);

        Twoqueue_SkipList(const Twoqueue_SkipList&) = delete;
        Twoqueue_SkipList& operator=(const Twoqueue_SkipList&) = delete;

        //重定义insert()，在2qskiplist中插入2qNode
        void Insert(const Key& key);
        //Contains()函数没有变化
        //Iterator已有的功能不需要变化

        //将数据插入2q队列
        void Insert_Twoqueue(const Twoqueue_Node*& node, const bool& is_same);

        int RandomHeight();
        bool Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0); }

    private:
        enum { kMaxHeight = 12};

        inline int GetMaxHeight() const {
            return max_height_.load(std::memory_order_relaxed);
        }

        //因为是在父类中是私有的，所以这几个函数都要重新定义
        Twoqueue_Node* NewTwoqueue_Node(const Key& key, int height);

        bool KeyIsAfterNode(const Key& key, Twoqueue_Node* n) const;
        Twoqueue_Node* FindGreaterOrEqual(const Key& key, Twoqueue_Node** prev) const;
        Twoqueue_Node* FindLessThan(const Key& key) const;
        Twoqueue_Node* FindLast() const;

        Comparator const compare_;//同skiplist
        Arena* const arena_;//同skiplist

        Twoqueue_Node* const head_;
        Twoqueue_Node* const normal_head_;//热数据区
        Twoqueue_Node* const cold_head_;//冷数据区

        Twoqueue_Node* cur_scan_node_;//当前扫描到的节点
        Twoqueue_Node* cur_cold_node_;//当前最新的冷数据
        Twoqueue_Node* cur_node_;//当前插入的最新数据

        std::atomic<int> max_height_;//同skiplist
        Random rnd_;//同skiplist

        InternalKeyComparator icmp_;
    };
    
    template <typename Key, class Comparator>
    struct Twoqueue_SkipList<Key, Comparator>::Twoqueue_Node {
        /* data */
        explicit Twoqueue_Node(const Key& k) : key(k) {}

        Key const key;

        //确保线程安全的方法
        Twoqueue_Node* Next(int n) {
            assert(n >= 0);
            return next_[n].load(std::memory_order_acquire);
        }

        Twoqueue_Node* Follow() {
            return follow_.load(std::memory_order_acquire);
        }

        void SetNext(int n, Twoqueue_Node* x) {
            assert(n >= 0);
            next_[n].store(x, std::memory_order_release);
        }

        void SetFollow(std::Twoqueue_Node* x) {
            assert(n >= 0);
            follow_.store(x, std::memory_order_release);
        }

        //在某些线程安全的情况下使用
        Twoqueue_Node* NoBarrier_Next(int n) {
            assert(n >= 0);
            return next_[n].load(std::memory_order_relaxed);
        }

        Twoqueue_Node* NoBarrier_Follow() {
            return follow_.load(std::memory_order_relaxed);
        }

        void NoBarrier_SetNext(int n, Twoqueue_Node* x) {
            assert(n >= 0);
            next_[n].store(x, std::memory_order_relaxed);
        }

        void NoBarrier_SetFollow(Twoqueue_Node* x) {
            follow_.store(x, std::memory_order_relaxed);
        }

        private:
        std::atomic<Twoqueue_Node*> next_[1];//在skiplist中的下一个
        std::atomic<Twoqueue_Node*> follow_;//在2q中FIFO顺序的下一值
        std::atomic<Twoqueue_Node*> new_;//在2q中的同一关键字的新的值
    };

    template <typename Key, class Comparator>
    typename Twoqueue_SkipList<Key, Comparator>::Twoqueue_Node* Twoqueue_SkipList<Key, Comparator>::NewTwoqueue_Node(
        const Key& key, int height
    ) {
        char* const node_memorey = arena_->AllocateAligned(
            sizeof(Twoqueue_Node) + sizeof(std::atomic<Twoqueue_Node*>) * (height - 1)
        );
        return new (node_memorey) Twoqueue_Node(key);
    }

    template <typename Key, class Comparator>
    int Twoqueue_SkipList<Key, Comparator>::RandomHeight() {
        static const unsigned int kBranching = 4;
        int height = 1;
        while (height < kMaxHeight && ((rnd_.Next() % kBranching) == 0)) {
            height++;
        }
        assert(height > 0);
        assert(height <= kMaxHeight);
        return height;
    }

    template <typename Key, class Comparator>
    bool Twoqueue_SkipList<Key, Comparator>::KeyIsAfterNode(const Key& key, Twoqueue_Node* n) const {
        return (n != nullptr) && (compare_(n->key, key) < 0);
    }

    template <typename Key, class Comparator>
    typename Twoqueue_SkipList<Key, Comparator>::Twoqueue_Node*
    Twoqueue_SkipList<Key, Comparator>::FindGreaterOrEqual(const Key& key, Twoqueue_Node** prev) const {
        Twoqueue_Node* x = head_;
        int level = GetMaxHeight() - 1;
        while (true)
        {
            /* code */
            Twoqueue_Node* next = x->Next(level);
            if (KeyIsAfterNode(key, next)) {
                /* code */
                x = next;
            } else {
                /* code */
                if (prev != nullptr) prev[level] = x;
                if (level == 0) {
                    return next;
                } else {
                    level--;
                }
            }
        }
    }

    template <typename Key, class Comparator>
    typename Twoqueue_SkipList<Key, Comparator>::Twoqueue_Node*
    Twoqueue_SkipList<Key, Comparator>::FindLessThan(const Key& key) const {
        Twoqueue_Node* x = head_;
        int level = GetMaxHeight() - 1;
        while (true) {
            assert(x == head_ || compare_(x->key, key) < 0);
            Twoqueue_Node* next = x->Next(level);
            if (next == nullptr || compare_(next->key, key) >= 0) {
                if (level == 0) {
                    return x;
                } else {
                    level--;
                }
            } else {
                x = next;
            }
        }
    }

    template <typename Key, class Comparator>
    typename Twoqueue_SkipList<Key, Comparator>::Twoqueue_Node*
    Twoqueue_SkipList<Key, Comparator>::FindLast() const {
        Twoqueue_Node* x = head_;
        int level = GetMaxHeight() - 1;
        while (true) {
            Twoqueue_Node* next = x->Next(level);
            if (next == nullptr) {
                if (level == 0) {
                    return x;
                } else {
                    level--
                }
            } else {
                x = next;
            }
        }
    }

    template <typename Key, class Comparator>
    Twoqueue_SkipList<Key, Comparator>::Twoqueue_SkipList(Comparator cmp, Arena* arena)
    : SkipList(cmp, arena), 
    compare_(cmp),
    arena_(arena),
    head_(NewTwoqueue_Node(0, kMaxHeight)),
    normal_head_(head_),
    cold_head_(nullptr),
    cur_scan_node_(head_),
    cur_node_(head_),
    cur_cold_node_(nullptr),
    max_height_(1), 
    rnd_(0xdeadbeef),
    icmp_(cmp) {
        for (int i = 0; i < kMaxHeight; i++) {
            head_->SetNext(i, nullptr);
        }
    }
    
    template <typename Key, class Comparator>
    void Twoqueue_SkipList<Key, Comparator>::Insert(const Key& key) {
        Twoqueue_Node* prev[kMaxHeight];
        Twoqueue_Node* x = FindGreaterOrEqual(key, prev);

        bool is_same = true;

        //将节点插入2q链表中,比较新插入的节点的userkey和与它紧邻的前一userkey,
        //如果是相同的userkey,则将该节点添加到前一节点的new_指针上，
        //否则添加到follow_指针上

        assert(x == nullptr || !Equal(key, x->key));

        int height = RandomHeight();
        //为新增的层初始化
        if (height > GetMaxHeight()) {
            for (int i = GetMaxHeight(); i < height; i++) {
                prev[i] = head_;
            }
            max_height_.store(height, std::memory_order_relaxed);
        }
        
        //插入skiplist
        x = NewTwoqueue_Node(key, height);
        for (int i = 0; i < height; i++) {
            x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
            prev[i]->SetNext(i, x);
        }
        
        //插入2q链表
        Insert_Twoqueue(x, is_same);
    }

    //根据is_same判断插入new_还是follow_
    template <typename Key, class Comparator>
    void Twoqueue_SkipList<Key, Comparator>::Insert_Twoqueue(const Twoqueue_Node*& node, const bool& is_same) {
        
        cur_node_->SetFollow(node);
        cur_node_ = node;
        
    }

    /*
    template <typename Key, class Comparator>
    bool Twoqueue_SkipList<Key, Comparator>::Contains(const Key& key) const {
        return SkipList<Key, Comparator>::Contains(key);
    }
    */
} // namespace leveldb

#endif // STORAGE_LEVELDB_DB_Twoqueue_SkipList_H