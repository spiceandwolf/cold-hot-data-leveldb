#ifndef STORAGE_LEVELDB_DB_Twoqueue_SkipList_H
#define STORAGE_LEVELDB_DB_Twoqueue_SkipList_H

#include <atomic>
#include <cassert>
#include <cstdlib>
#include <vector>
#include <utility>

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
        explicit Twoqueue_SkipList(Comparator cmp, Arena* arena, const size_t& write_buffer_size);

        Twoqueue_SkipList(const Twoqueue_SkipList&) = delete;
        Twoqueue_SkipList& operator=(const Twoqueue_SkipList&) = delete;

        //重定义insert()，在2qskiplist中插入2qNode
        void Insert(const Key& key, const size_t& encoded_len);
        //Contains()函数没有变化
        bool Contains(const Key& key) const;
        
        //重写Iterator内部类
        class TQIterator
        {
        private:
            const Twoqueue_SkipList* list_;
            Twoqueue_Node* node_;
        public:
            explicit TQIterator(const Twoqueue_SkipList* list);
            bool Valid() const;
            const Key& key() const;
            void Next();
            void Prev();
            void Seek(const Key& target);
            void SeekToFirst();
            void SeekToLast();
        };
        
        int RandomHeight();
        bool Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0); }

        //返回冷数据区的大小
        size_t GetColdAreaSize() const { return cold_area_size; }
        //返回热数据区的大小
        size_t GetNormalAreaSize() const { return normal_area_size; }
        //遍历2Q跳表，将热数据区的键值对保存进
        //返回0的代表没有冷数据，1代表有
        int Seperate(std::vector<std::pair<Slice, Slice>>& normal_nodes_);

    private:
        enum { kMaxHeight = 12};

        inline int GetMaxHeight() const {
            return max_height_.load(std::memory_order_relaxed);
        }

        //因为是在父类中是私有的，所以这几个函数都要重新定义
        Twoqueue_Node* NewTwoqueue_Node(const Key& key, int height, const size_t& encoded_len);

        bool KeyIsAfterNode(const Key& key, Twoqueue_Node* n) const;
        Twoqueue_Node* FindGreaterOrEqual(const Key& key, Twoqueue_Node** prev) const;
        Twoqueue_Node* FindLessThan(const Key& key) const;
        Twoqueue_Node* FindLast() const;
        //找到同一关键字最早的节点，若找不到则返回当前节点
        Twoqueue_Node* FindNoSmaller(Twoqueue_Node* node) const;
        //抽取存储在每个节点key中的Userkey
        Slice GetUserKey(const Key& entry) const;
        //抽取存储在每个节点key中的seqnumber
        uint64_t GetSeqNumber(const Key& entry) const;
        //冷却数据，将热数据添加到冷数据区
        void FreezeNodes(Twoqueue_Node* node);
        //从2Q跳表中除去老版本
        void ThawNode(Twoqueue_Node* node);
        //从key中提取slice
        Slice GetLengthPrefixedSlice(const char* data);

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
        size_t normal_area_size;//热数据区所占总空间
        size_t cold_area_size;//冷数据区所占总空间
        size_t option_normal_size;//Option中设定的memtable的最大值
        float size_factor;
    };
    
    template <typename Key, class Comparator>
    struct Twoqueue_SkipList<Key, Comparator>::Twoqueue_Node {
        /* data */
        explicit Twoqueue_Node(const Key& k, const int& h, const size_t& encoded_len) : 
            key(k), 
            follow_(nullptr), 
            precede_(nullptr) {
                node_size = sizeof(Twoqueue_Node) + sizeof(std::atomic<Twoqueue_Node*>) * (h - 1)
                    + encoded_len;
            }

        Key const key;

        size_t GetSize() {
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

        Twoqueue_Node* Precede() {
            return precede_.load(std::memory_order_acquire);
        }

        void SetNext(int n, Twoqueue_Node* x) {
            assert(n >= 0);
            next_[n].store(x, std::memory_order_release);
        }

        void SetFollow(Twoqueue_Node* x) {
            follow_.store(x, std::memory_order_release);
        }

        void SetPrecede(Twoqueue_Node* x) {
            precede_.store(x, std::memory_order_release);
        }

        //在某些线程安全的情况下使用
        Twoqueue_Node* NoBarrier_Next(int n) {
            assert(n >= 0);
            return next_[n].load(std::memory_order_relaxed);
        }

        Twoqueue_Node* NoBarrier_Follow() {
            return follow_.load(std::memory_order_relaxed);
        }

        Twoqueue_Node* NoBarrier_Precede() {
            return precede_.load(std::memory_order_relaxed);
        }

        void NoBarrier_SetNext(int n, Twoqueue_Node* x) {
            assert(n >= 0);
            next_[n].store(x, std::memory_order_relaxed);
        }

        void NoBarrier_SetFollow(Twoqueue_Node* x) {
            follow_.store(x, std::memory_order_relaxed);
        }

        void NoBarrier_SetPrecede(Twoqueue_Node* x) {
            precede_.store(x, std::memory_order_relaxed);
        }

    private:
        size_t node_size;//占有的层数
        std::atomic<Twoqueue_Node*> follow_;//在2q中FIFO顺序的下一值
        std::atomic<Twoqueue_Node*> precede_;//在2q中FIFO顺序的前一值
        std::atomic<Twoqueue_Node*> next_[1];//在skiplist中的下一个
        
    };

    template <typename Key, class Comparator>
    typename Twoqueue_SkipList<Key, Comparator>::Twoqueue_Node* Twoqueue_SkipList<Key, Comparator>::NewTwoqueue_Node(
        const Key& key, int height, const size_t& encoded_len) {
        char* const node_memorey = arena_->AllocateAligned(
            sizeof(Twoqueue_Node) + sizeof(std::atomic<Twoqueue_Node*>) * (height - 1)
        );
        return new (node_memorey) Twoqueue_Node(key, height, encoded_len);
    }

    //TQIterator
    template <typename Key, class Comparator>
    inline Twoqueue_SkipList<Key, Comparator>::TQIterator::TQIterator(const Twoqueue_SkipList* list) {
        list_ = list;
        node_ = nullptr;
    }

    template <typename Key, class Comparator>
    inline bool Twoqueue_SkipList<Key, Comparator>::TQIterator::Valid() const {
        return node_ != nullptr;
    }

    template <typename Key, class Comparator>
    inline const Key& Twoqueue_SkipList<Key, Comparator>::TQIterator::key() const {
        assert(Valid());
        return node_->key;
    }

    template <typename Key, class Comparator>
    inline void Twoqueue_SkipList<Key, Comparator>::TQIterator::Next() {
        assert(Valid());
        node_ = node_->Next(0);
    }

    template <typename Key, class Comparator>
    inline void Twoqueue_SkipList<Key, Comparator>::TQIterator::Prev() {
        // Instead of using explicit "prev" links, we just search for the
        // last node that falls before key.
        assert(Valid());
        node_ = list_->FindLessThan(node_->key);
        if (node_ == list_->head_) {
            node_ = nullptr;
        }
    }

    template <typename Key, class Comparator>
    inline void Twoqueue_SkipList<Key, Comparator>::TQIterator::Seek(const Key& target) {
        node_ = list_->FindGreaterOrEqual(target, nullptr);
    }

    template <typename Key, class Comparator>
        inline void Twoqueue_SkipList<Key, Comparator>::TQIterator::SeekToFirst() {
        node_ = list_->head_->Next(0);
    }

    template <typename Key, class Comparator>
    inline void Twoqueue_SkipList<Key, Comparator>::TQIterator::SeekToLast() {
        node_ = list_->FindLast();
        if (node_ == list_->head_) {
            node_ = nullptr;
        }
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
        
        assert(node != nullptr);

        Twoqueue_Node* x = node;
        Twoqueue_Node* next = x->Next(0);

        Slice a = GetUserKey(node->key);

        while (true) {
            if (next != nullptr) {  
                Slice b = GetUserKey(next->key);
                
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
    Twoqueue_SkipList<Key, Comparator>::Twoqueue_SkipList(Comparator cmp, Arena* arena,
     const size_t& write_buffer_size) : SkipList<Key, Comparator>(cmp, arena), 
        compare_(cmp),
        arena_(arena),
        head_(NewTwoqueue_Node(0, kMaxHeight, 0)),
        normal_head_(head_),
        cold_head_(head_),
        obsolete_(nullptr),
        cur_scan_node_(nullptr),
        cur_node_(head_),
        cur_cold_node_(nullptr),
        max_height_(1), 
        rnd_(0xdeadbeef),
        normal_area_size(0), 
        cold_area_size(0),
        option_normal_size(write_buffer_size),
        size_factor(0.2) {
        for (int i = 0; i < kMaxHeight; i++) {
            head_->SetNext(i, nullptr);
            cold_area_size += head_->GetSize();
        }
    }
    
    template <typename Key, class Comparator>
    void Twoqueue_SkipList<Key, Comparator>::Insert(const Key& key, const size_t& encoded_len) {
        Twoqueue_Node* prev[kMaxHeight];//存储要插入skiplist的节点的相邻的前一个节点
        Twoqueue_Node* x = FindGreaterOrEqual(key, prev);//存储要插入skiplist的节点的相邻的后一个节点
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
        
        //创建新的节点
        x = NewTwoqueue_Node(key, height, encoded_len);

        //新节点一定插入在热数据区
        //如果新节点插入热数据区后超出阈值，则先移动出足够的空间
        normal_area_size += x->GetSize();
        if (normal_area_size > option_normal_size * size_factor) {
            FreezeNodes(x);
        }

        //插入skiplist
        for (int i = 0; i < height; i++) {
            x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
            prev[i]->SetNext(i, x);
        }

        //插入2q链表，为cur_node_添加follow_和把x的precede_指向cur_node_
        cur_node_->SetFollow(x);
        x->SetPrecede(cur_node_);
        x->SetFollow(nullptr);

        //如果normal_head_和cold_head_等于head_，说明插入的是2q跳表中的第一个节点
        //则让normal_head_，cold_head_指向该节点
        if (head_ == normal_head_ && cold_head_ == head_) {
            normal_head_ = x;
        }

        //完成插入后，节点x成为cur_node_
        cur_node_ = x;

        //如果是新值在2q链表中摘除旧版本节点
        if (is_new) {
            ThawNode(cur_node_);
        }
    }

    //以normal_head_指向的节点的seq为基准
    //取出seq不小于guardseq的节点,构造键值对放入vector中
    //在2Q链表中摘除这些点及对应的旧版本节点
    template <typename Key, class Comparator>
    int Twoqueue_SkipList<Key, Comparator>::Seperate(std::vector<std::pair<Slice, Slice>>& normal_nodes_) {
        Twoqueue_Node* guard = normal_head_;
        Twoqueue_Node* iter_ = normal_head_;
        uint64_t guard_seq = GetSeqNumber(guard->key);

        //从normal_head_遍历，构造键值对，将键值对插入vector中
        while (iter_ != nullptr) {
            //userkey
            const char* entry = iter_->key;
            uint32_t key_length;
            Slice userkey = GetUserKey(iter_->key);
            std::string tmpkey = userkey.ToString();
            
            //value
            Slice value = GetLengthPrefixedSlice(userkey.data() + userkey.size() + 8);
            std::string tmpval = value.ToString();

            //插入vector
            std::pair<Slice, Slice> item(userkey, value);
            normal_nodes_.push_back(item);

            iter_ = iter_->Follow();

        }

        //遍历整个skiplist,以normal_head_指向的节点的seq为基准，
        //摘除所有冷数据区节点的旧版本节点和热数据区节点及其旧版本节点

        //首先确定head_是从冷数据区开始
        iter_ = head_->Next(0);
        Twoqueue_Node* next;
        
        uint64_t seq = GetSeqNumber(iter_->key);

        while (seq >= guard_seq) {

            next = FindNoSmaller(iter_);
            iter_ = next->Next(0);
            
            if (iter_ == nullptr) break;
            seq = GetSeqNumber(iter_->key);
        }

        head_->SetNext(0, iter_);

        //可能会出现所有节点的新版本都是热数据的情况
        //此时没有冷数据
        if (iter_ == nullptr) {
            return 0;
        }

        Slice user_key = GetUserKey(iter_->key);

        //next指向下一关键字的最新版本节点
        next = FindNoSmaller(iter_)->Next(0);

        //判断next_指针指向的节点是否为旧版本节点
        //若next为nullptr,则iter_已指向最后一个节点
        while (next != nullptr) {           
            
            //Slice next_user_key = GetUserKey(next->key);
            uint64_t next_seq = GetSeqNumber(next->key);

            //此时next指向的是一个冷数据区的节点，保留该节点
            if (next_seq < guard_seq) {
                iter_->SetNext(0, next);

                iter_ = next;
            }
            
            next = FindNoSmaller(next)->Next(0);
        }

        return 1;

    }    

    //在热数据区中从normal_head_开始沿FIFO的方向链表扫描，
    //根据经过扫描的节点所用空间的大小之和与插入节点的大小，判断需要移动的节点个数
    //每次扫描至少需要移动1个节点
    //移动结束后，热数据区所占空间减少相应的大小，冷数据区增加相应的大小
    template <typename Key, class Comparator>
    void Twoqueue_SkipList<Key, Comparator>::FreezeNodes(Twoqueue_Node* node) {
        Twoqueue_Node* selected_node = normal_head_;
        size_t wanted_size = node->GetSize();
        size_t total_size = selected_node->GetSize();

        while (wanted_size >= total_size) {
            selected_node = selected_node->Follow();
            total_size += selected_node->GetSize();
        }

        cur_cold_node_ = selected_node;
        selected_node = selected_node->Follow();
        normal_head_ = selected_node;

        //有数据第一次成为冷数据时再确定cold_head_的值
        if (cold_head_ == head_) cold_head_ = cur_cold_node_;

        //两区域所占空间的变化
        normal_area_size -= total_size;
        cold_area_size += total_size;
    }

    //找到旧版本节点，将旧版本节点从FIFO链表中摘除
    //在FIFO链表中旧版本节点的前一节点follow_指针指向旧版本节点的后一节点，
    //旧版本节点的后一节点的precede_指针指向前一节点
    //用旧版本节点的follow_指针指向obsolete_的follow_所指向的节点，
    //然后令obsolete_的follow_指针指向旧版本节点
    //根据旧版本节点所在区域(冷/热数据区)修改两个区域所占空间的大小
    template <typename Key, class Comparator>
    void Twoqueue_SkipList<Key, Comparator>::ThawNode(Twoqueue_Node* node) {
        Twoqueue_Node* elder = node->Next(0);
        Twoqueue_Node* prev = elder->Precede();

        //判断旧版本节点存在于热数据区还是冷数据区
        //如果elder是最早插入该memtable的，则它的precede_为head_
        //若elder在热数据区，则normal_head_指向elder->follow_指针指向的数据
        //若elder在冷数据区，则cold_head_指向elder->follow_指针指向的数据
        uint64_t seq = GetSeqNumber(elder->key);
        uint64_t cur_seq = GetSeqNumber(normal_head_->key);
        if (seq >= cur_seq) {
            //说明旧版本节点在热数据区
            normal_area_size -= elder->GetSize();

            if (prev == head_) {
                normal_head_ = elder->Follow();
                elder->Follow()->SetPrecede(head_);
            } else {
                prev->SetFollow(elder->Follow());
                elder->Follow()->SetPrecede(prev);
            }
        } else {
            //说明旧版本节点在冷数据区
            cold_area_size -= elder->GetSize();

            if (prev == head_) {
                cold_head_ = elder->Follow();
                elder->Follow()->SetPrecede(head_);
            } else {
                prev->SetFollow(elder->Follow());
                elder->Follow()->SetPrecede(prev);
            }
        }
       
        //将旧版本节点移到废弃区
        elder->SetFollow(obsolete_);
        obsolete_ = elder;
    }

    //从key中抽取userkey
    template <typename Key, class Comparator>
    Slice Twoqueue_SkipList<Key, Comparator>::GetUserKey(const Key& entry) const { 
        uint32_t key_length;
        const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
        return Slice(key_ptr, key_length - 8);
    }

    //从key中抽取SeqNumber
    template <typename Key, class Comparator>
    uint64_t Twoqueue_SkipList<Key, Comparator>::GetSeqNumber(const Key& entry) const {
        uint32_t key_length;
        const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
        Slice key = Slice(key_ptr, key_length - 8);
        const uint64_t seq = DecodeFixed64(key.data() + key.size()) >> 8;
        return seq;
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

    template <typename Key, class Comparator>
    Slice Twoqueue_SkipList<Key, Comparator>::GetLengthPrefixedSlice(const char* data) {
        uint32_t len;
        const char* p = data;
        p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
        return Slice(p, len);
    }

} // namespace leveldb

#endif // STORAGE_LEVELDB_DB_Twoqueue_SkipList_H