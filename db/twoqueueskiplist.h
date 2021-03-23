#ifndef STORAGE_LEVELDB_DB_Twoqueue_SkipList_H
#define STORAGE_LEVELDB_DB_Twoqueue_SkipList_H

#include <atomic>
#include <cassert>
#include <cstdlib>

#include "db/skiplist.h"
#include "db/dbformat.h"
#include "db/memtable.cc"

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
        explicit Twoqueue_SkipList(Comparator cmp, Arena* arena);

        Twoqueue_SkipList(const Twoqueue_SkipList&) = delete;
        Twoqueue_SkipList& operator=(const Twoqueue_SkipList&) = delete;

        //重定义insert()，在2qskiplist中插入2qNode
        void Insert(const Key& key);
        //Contains()函数没有变化
        //Iterator已有的功能不需要变化

        //将数据插入2q队列
        void Insert_Twoqueue(Twoqueue_Node* node, Twoqueue_Node* sameButOldest, const bool& is_new);

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
        //找到同一关键字最早的节点，若找不到则返回当前一节点
        Twoqueue_Node* FindNoSmaller(Twoqueue_Node* node) const;
        //抽取存储在每个节点key中的Userkey
        Slice GetUserKey(const char* entry) const;

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

        // InternalKeyComparator icmp_;
    };
    
    template <typename Key, class Comparator>
    struct Twoqueue_SkipList<Key, Comparator>::Twoqueue_Node {
        /* data */
        explicit Twoqueue_Node(const Key& k) : key(k), follow_(nullptr), new_(nullptr) {}

        Key const key;

        //确保线程安全的方法
        Twoqueue_Node* Next(int n) {
            assert(n >= 0);
            return next_[n].load(std::memory_order_acquire);
        }

        Twoqueue_Node* Follow() {
            return follow_.load(std::memory_order_acquire);
        }

        Twoqueue_Node* New() {
            return new_.load(std::memory_order_acquire);
        }

        void SetNext(int n, Twoqueue_Node* x) {
            assert(n >= 0);
            next_[n].store(x, std::memory_order_release);
        }

        void SetFollow(Twoqueue_Node* x) {
            follow_.store(x, std::memory_order_release);
        }

        void SetNew(Twoqueue_Node* x) {
            new_.store(x, std::memory_order_release);
        }

        //在某些线程安全的情况下使用
        Twoqueue_Node* NoBarrier_Next(int n) {
            assert(n >= 0);
            return next_[n].load(std::memory_order_relaxed);
        }

        Twoqueue_Node* NoBarrier_Follow() {
            return follow_.load(std::memory_order_relaxed);
        }

        Twoqueue_Node* NoBarrier_New() {
            return new_.load(std::memory_order_relaxed);
        }

        void NoBarrier_SetNext(int n, Twoqueue_Node* x) {
            assert(n >= 0);
            next_[n].store(x, std::memory_order_relaxed);
        }

        void NoBarrier_SetFollow(Twoqueue_Node* x) {
            follow_.store(x, std::memory_order_relaxed);
        }

        void NoBarrier_SetNew(Twoqueue_Node* x) {
            new_.store(x, std::memory_order_relaxed);
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
                    level--;
                }
            } else {
                x = next;
            }
        }
    }

    template <typename Key, class Comparator>
    typename Twoqueue_SkipList<Key, Comparator>::Twoqueue_Node*
    Twoqueue_SkipList<Key, Comparator>::FindNoSmaller(Twoqueue_Node* node) const {
        Twoqueue_Node* x = node;
        int level = GetMaxHeight() - 1;
        while (true) {
            Twoqueue_Node* next = x->Next(level);
            const char* aptr = x->key;
            const char* bptr = next->key;
            Slice a = GetUserKey(aptr);
            Slice b = GetUserKey(bptr);
            if ((next != nullptr) && 
            (compare_(a.data(), b.data()) == 0)) {
                x = next;
            } else {
                if (level == 0) {
                    return x;
                } else {
                    level--;
                }
            }
        }
    }

    template <typename Key, class Comparator>
    Twoqueue_SkipList<Key, Comparator>::Twoqueue_SkipList(Comparator cmp, Arena* arena)
    : SkipList<Key, Comparator>(cmp, arena), 
    compare_(cmp),
    arena_(arena),
    head_(NewTwoqueue_Node(0, kMaxHeight)),
    normal_head_(head_),
    cold_head_(nullptr),
    cur_scan_node_(head_),
    cur_node_(head_),
    cur_cold_node_(nullptr),
    max_height_(1), 
    rnd_(0xdeadbeef) {
        for (int i = 0; i < kMaxHeight; i++) {
            head_->SetNext(i, nullptr);
        }
    }
    
    template <typename Key, class Comparator>
    void Twoqueue_SkipList<Key, Comparator>::Insert(const Key& key) {
        Twoqueue_Node* prev[kMaxHeight];//存储要插入skiplist的节点的相邻的前一个节点
        Twoqueue_Node* x = FindGreaterOrEqual(key, prev);//存储要插入skiplist的节点的相邻的后一个节点
        Twoqueue_Node* sameButOldest = nullptr;//存储要插入skiplist的节点有相同关键字的第一个节点
        bool is_new = false;

        //将节点插入2q链表中,比较新插入的节点的userkey和与它紧邻的后一userkey,
        //如果相同则是该userkey的新值，否则是有新关键字的节点
        //对于相同的 user-key，最新的更新（SequnceNumber 更大）排在前面 
        assert(x == nullptr || !Equal(key, x->key));
        Slice a = GetUserKey(key);

        if (x != nullptr) {
            Slice a = GetUserKey(key);
            Slice b = GetUserKey(prev[0]->key);
            int r = compare_(a.data(), b.data());
            if (r == 0) {
                sameButOldest = FindNoSmaller(prev[0]);
                is_new = true;
            }            
        }

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
        Insert_Twoqueue(x, sameButOldest, is_new);
        cur_node_ = x;
    }

    //根据is_new判断插入new_还是follow_
    //is_new为true，说明是相同的userkey,
        //则将该节点添加到后一节点的new_指针上，cur_node_所指节点的follow_指针指向该userkry的第一个节点
    //is_new为false，说明是不同的userkey,
        //则将该节点添加到cur_node_所指节点的follow_指针上
    template <typename Key, class Comparator>
    void Twoqueue_SkipList<Key, Comparator>::Insert_Twoqueue(
        Twoqueue_Node* node, Twoqueue_Node* sameButOldest, const bool& is_new) {
        
        if (is_new) {
            node->Next(0)->SetNew(node);
            cur_node_->SetFollow(sameButOldest);
            
        } else {
            cur_node_->SetFollow(node);
            
        }

    }

    template <typename Key, class Comparator>
    Slice Twoqueue_SkipList<Key, Comparator>::GetUserKey(const char* entry) const { 
        uint32_t key_length;
        const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
        return Slice(key_ptr, key_length - 8);
    }

    /*
    template <typename Key, class Comparator>
    bool Twoqueue_SkipList<Key, Comparator>::Contains(const Key& key) const {
        return SkipList<Key, Comparator>::Contains(key);
    }
    */
} // namespace leveldb

#endif // STORAGE_LEVELDB_DB_Twoqueue_SkipList_H