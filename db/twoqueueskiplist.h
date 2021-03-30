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
        bool Contains(const Key& key) const;
        //Iterator已有的功能不需要变化

        //若插入了新的副本，则将旧版本移入废弃区。
        void thaw_Node();

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
        //找到同一关键字最早的节点，若找不到则返回当前节点
        Twoqueue_Node* FindNoSmaller(Twoqueue_Node* node) const;
        //抽取存储在每个节点key中的Userkey
        Slice GetUserKey(const char* entry) const;
        //冷却数据，将热数据添加到冷数据区
        void frooze_Nodes(Twoqueue_Node* node);

        Comparator const compare_;//同skiplist
        Arena* const arena_;//同skiplist

        Twoqueue_Node* const head_;
        Twoqueue_Node* normal_head_;//热数据区
        Twoqueue_Node* cold_head_;//冷数据区
        Twoqueue_Node* obsolete_;//废弃区

        Twoqueue_Node* cur_scan_node_;//当前扫描到的节点
        Twoqueue_Node* cur_cold_node_;//当前最新的冷数据
        Twoqueue_Node* cur_node_;//当前插入的最新数据

        std::atomic<int> max_height_;//同skiplist
        Random rnd_;//同skiplist
    };
    
    template <typename Key, class Comparator>
    struct Twoqueue_SkipList<Key, Comparator>::Twoqueue_Node {
        /* data */
        explicit Twoqueue_Node(const Key& k, const int& h) : 
            key(k), 
            follow_(nullptr), 
            new_(nullptr) {
                node_size = sizeof(Twoqueue_Node) + sizeof(std::atomic<Twoqueue_Node*>) * (h - 1);
            }

        Key const key;

        size_t getSize() {
            return node_size;
        }

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
        size_t node_size;//占有的层数
        std::atomic<Twoqueue_Node*> follow_;//在2q中FIFO顺序的下一值
        std::atomic<Twoqueue_Node*> new_;//在2q中的同一关键字的新的值
        std::atomic<Twoqueue_Node*> next_[1];//在skiplist中的下一个
        
    };

    template <typename Key, class Comparator>
    typename Twoqueue_SkipList<Key, Comparator>::Twoqueue_Node* Twoqueue_SkipList<Key, Comparator>::NewTwoqueue_Node(
        const Key& key, int height
    ) {
        char* const node_memorey = arena_->AllocateAligned(
            sizeof(Twoqueue_Node) + sizeof(std::atomic<Twoqueue_Node*>) * (height - 1)
        );
        return new (node_memorey) Twoqueue_Node(key, height);
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

    //找到同一关键字最早的节点，若找不到则返回当前节点
    template <typename Key, class Comparator>
    typename Twoqueue_SkipList<Key, Comparator>::Twoqueue_Node*
    Twoqueue_SkipList<Key, Comparator>::FindNoSmaller(Twoqueue_Node* node) const {
        Twoqueue_Node* x = node;
        const char* key = node->key;

        Slice a = GetUserKey(key);
        Twoqueue_Node* next = x->Next(0);

        while (true) {
            if (next != nullptr) {
                const char* bptr = next->key;
                Slice b = GetUserKey(bptr);
                
                if (a.compare(b) == 0) {
                    x = next;
                    next = next->Next(0);
                } else {
                    return x;
                }

            } else {
                return x;
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
    cold_head_(normal_head_),//如何初始化
    obsolete_(nullptr),
    cur_scan_node_(head_),
    cur_node_(head_),
    cur_cold_node_(cold_head_),
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

        if (x != nullptr) {
            Slice a = GetUserKey(key);
            Slice b = GetUserKey(x->key);
            int r = a.compare(b);
            if (r == 0) {
                sameButOldest = FindNoSmaller(x);
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
        
        //根据is_new判断，若is_new为true，说明是相同的userkey,
        //则将该节点添加到后一节点的new_指针上
        if (is_new) {
            x->Next(0)->SetNew(x);
        }
        //插入2q链表
        cur_node_->SetFollow(x);
        //完成插入后，节点x成为cur_node_
        cur_node_ = x;
    }

    //在热数据区中从cur_scan_node_开始沿FIFO的方向链表扫描，
    //根据经过扫描的节点所用空间的大小之和与插入节点的大小，判断需要移动的节点个数
    //每次扫描至少需要移动1个节点。
    template <typename Key, class Comparator>
    void Twoqueue_SkipList<Key, Comparator>::frooze_Nodes(Twoqueue_Node* node) {
        Twoqueue_Node* selected_node = cur_scan_node_->Follow();
        size_t wanted_size = node->getSize();
        size_t total_size = selected_node->getSize();

        while (wanted_size >= total_size) {
            selected_node = selected_node->Follow();
            total_size += selected_node->getSize();
        }

        cur_cold_node_ = selected_node;
        normal_head_ = selected_node;

    }

    //根据该数据的seq与normal_head_所指节点的seq相对比，若不小于则是热数据，若小于则是冷数据
    template <typename Key, class Comparator>
    void Twoqueue_SkipList<Key, Comparator>::thaw_Node() {}

    template <typename Key, class Comparator>
    Slice Twoqueue_SkipList<Key, Comparator>::GetUserKey(const char* entry) const { 
        uint32_t key_length;
        const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
        return Slice(key_ptr, key_length - 8);
    }

    
    template <typename Key, class Comparator>
    bool Twoqueue_SkipList<Key, Comparator>::Contains(const Key& key) const {
        Twoqueue_Node* x = FindGreaterOrEqual(key, nullptr);
        if (x != nullptr && Equal(key, x->key)) {
            return true;
        } else {
            return false;
        }
    }
    
} // namespace leveldb

#endif // STORAGE_LEVELDB_DB_Twoqueue_SkipList_H