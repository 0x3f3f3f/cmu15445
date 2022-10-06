
#include "container/hash/extendible_hash_table.h"
#include <iostream>
#include <string>
#include <utility>
#include <vector>
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  // 可扩展哈希，只有一个directory，直接存储第一次创建的dorectory_id
  this->directory_page_id_ = INVALID_PAGE_ID;
  // 读取测试文件，放到本地测试
  // std::ifstream file("/autograder/bustub/test/container/grading_hash_table_scale_test.cpp");
  // std::string str;
  // while (file.good()) {
  //   std::getline(file, str);
  //   std::cout << str << std::endl;
  // }
}

/*****************************************************************************
 * HELPERS 辅助函数不能加读写锁，因为后面insert和splitinsert,remove,merge加写锁调用这里的时候就会死锁。
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 * 题目已经实现
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Hash(KeyType key) -> uint32_t {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

// 根据key得到directory_idx， 这里作为辅助函数不用加锁
template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  uint32_t hash_value = Hash(key);
  uint32_t res = dir_page->GetGlobalDepthMask() & hash_value;
  return res;
}

// 作为辅助函数不能加锁
template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  uint32_t res = dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
  return res;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchDirectoryPage() -> HashTableDirectoryPage * {
  // 这里加的是单独定义的锁，多线程，这个函数被调用的时候必须防止错误，但是又不能用读写锁
  std::scoped_lock<std::mutex> lock(latch_);
  if (directory_page_id_ == INVALID_PAGE_ID) {
    // 得到directory页的内容，修改内容
    page_id_t directory_page_id;
    Page *directory_page = this->buffer_pool_manager_->NewPage(&directory_page_id);
    assert(directory_page != nullptr);
    HashTableDirectoryPage *res = reinterpret_cast<HashTableDirectoryPage *>(directory_page->GetData());
    res->SetPageId(directory_page_id);
    this->directory_page_id_ = directory_page_id;

    // 初始化第一个bucket，同时初始化directory的内容
    page_id_t bucket_page_id;
    Page *bucket_page = this->buffer_pool_manager_->NewPage(&bucket_page_id);
    assert(bucket_page != nullptr);
    res->SetLocalDepth(0, res->GetGlobalDepth());
    res->SetBucketPageId(0, bucket_page_id);

    // bucket必须释放，directory因为后面还是用，所以不能释放
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));
    return res;
  }
  // 只要初始化过直接获取，这也是为什么要单独加一个锁的原因
  Page *directory_page = buffer_pool_manager_->FetchPage(directory_page_id_);
  assert(directory_page != nullptr);
  return reinterpret_cast<HashTableDirectoryPage *>(directory_page->GetData());
}

// 修改了头文件，把获得HASH_TABLE_BUCKET_TYPE分成先获得page,然后获得HASH_TABLE_BUCKET_TYPE
// 最主要的目的是锁的颗粒度，给单独的一个bucket加锁
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchPage(page_id_t bucket_page_id) -> Page * {
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id);
  assert(page != nullptr);
  return page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchBucketPage(Page *page) -> HASH_TABLE_BUCKET_TYPE * {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
}

/*****************************************************************************
 * SEARCH 直接加读锁
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) -> bool {
  table_latch_.RLock();
  // 获得key对应的实际页的内容
  HashTableDirectoryPage *directory = FetchDirectoryPage();
  page_id_t page_id = KeyToPageId(key, directory);
  Page *page = FetchPage(page_id);
  page->RLatch();
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(page);
  // 获得key对应bucket所有等于key的value
  bool res = bucket_page->GetValue(key, comparator_, result);

  // 所有page用完就得释放
  assert(buffer_pool_manager_->UnpinPage(directory->GetPageId(), true));
  assert(buffer_pool_manager_->UnpinPage(page_id, false));
  page->RUnlatch();
  table_latch_.RUnlock();
  return res;
}

/*****************************************************************************
 * INSERTION 对于插入为什么加读锁，因为只有修改directory多个槽的时候，才写锁，因为具体哪个加锁不确定
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.RLock();
  // 获得key要插入的页，读锁相当于是可重入锁
  HashTableDirectoryPage *directory = this->FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, directory);
  Page *page = FetchPage(bucket_page_id);
  // 桶一个部分加锁，写锁这个槽位只能一个操作
  page->WLatch();
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(page);
  bool flag = bucket->Insert(key, value, comparator_);
  // 槽位快速解锁
  table_latch_.RUnlock();
  // 插入失败满了，或者已经有这个数据
  if (!flag) {
    if (bucket->IsFull()) {
      // splitinsert必须table_latch_加写锁，读锁必须解开
      page->WUnlatch();
      if (!SplitInsert(transaction, key, value)) {
        assert(buffer_pool_manager_->UnpinPage(directory->GetPageId(), true));
        assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
        return false;
      }
    } else {
      page->WUnlatch();
      assert(buffer_pool_manager_->UnpinPage(directory->GetPageId(), true));
      assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
      return false;
    }
  }
  // 读锁及时释放
  page->WUnlatch();
  assert(buffer_pool_manager_->UnpinPage(directory->GetPageId(), true));
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  // direcotry不确定对哪些槽位操作，直接加写锁锁定
  table_latch_.WLock();
  HashTableDirectoryPage *directory = this->FetchDirectoryPage();
  uint32_t directory_idx = KeyToDirectoryIndex(key, directory);
  if (directory->GetLocalDepth(directory_idx) >= directory->GetGlobalDepth()) {
    // local——depth和global_depth达到最大值，拒绝分离。
    if (directory->Size() >= DIRECTORY_ARRAY_SIZE) {
      assert(buffer_pool_manager_->UnpinPage(directory->GetPageId(), true));
      table_latch_.WUnlock();
      return false;
    }
    directory->IncrGlobalDepth();
  }
  // 确定可以分裂，直接增加LD
  directory->IncrLocalDepth(directory_idx);
  // 0分裂，分裂槽位就是1，1分裂的时候，对应的就是3，所以都是增加ld后的，distance找的是指向同一个page的槽
  uint32_t distance = 1 << directory->GetLocalDepth(directory_idx);

  // 指向同一个directory_idx修改directory槽位
  for (uint32_t st = directory_idx; st >= distance; st -= distance) {
    directory->SetBucketPageId(st, directory->GetBucketPageId(directory_idx));
    directory->SetLocalDepth(st, directory->GetLocalDepth(directory_idx));
  }

  for (uint32_t st = directory_idx; st < directory->Size(); st += distance) {
    directory->SetBucketPageId(st, directory->GetBucketPageId(directory_idx));
    directory->SetLocalDepth(st, directory->GetLocalDepth(directory_idx));
  }

  // 计算splitidx注意也是根据增加ld以后的值算的
  uint32_t split_page_idx = directory->GetSplitImageIndex(directory_idx);
  page_id_t split_page_id;
  Page *split_page_origin = this->buffer_pool_manager_->NewPage(&split_page_id);
  assert(split_page_origin != nullptr);
  HASH_TABLE_BUCKET_TYPE *split_page = FetchBucketPage(split_page_origin);

  directory->SetBucketPageId(split_page_idx, split_page_id);
  directory->SetLocalDepth(split_page_idx, directory->GetLocalDepth(directory_idx));

  // 设置split——idx对立面的page_id和ld为新的内容
  for (uint32_t st = split_page_idx; st >= distance; st -= distance) {
    directory->SetBucketPageId(st, directory->GetBucketPageId(split_page_idx));
    directory->SetLocalDepth(st, directory->GetLocalDepth(split_page_idx));
  }

  for (uint32_t st = split_page_idx; st < directory->Size(); st += distance) {
    directory->SetBucketPageId(st, directory->GetBucketPageId(split_page_idx));
    directory->SetLocalDepth(st, directory->GetLocalDepth(split_page_idx));
  }

  // 拿到目标插入的页
  page_id_t target_page_id = directory->GetBucketPageId(directory_idx);
  Page *target_page_origin = this->buffer_pool_manager_->FetchPage(target_page_id);
  assert(target_page_origin != nullptr);
  HASH_TABLE_BUCKET_TYPE *target_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(target_page_origin);
  // 取出目标页非空的内容，两个页重新插入内容
  std::vector<MappingType> res;
  target_page->GetExistedData(&res);
  target_page->ResetData();
  // 这里只能用
  for (uint32_t i = 0; i < res.size(); ++i) {
    uint32_t idx = Hash(res[i].first) & directory->GetLocalDepthMask(split_page_idx);
    // page_id_t page_id = directory->GetBucketPageId(idx);
    assert(idx == directory_idx || idx == split_page_idx);
    // uint32_t idx = KeyToDirectoryIndex(res[i].first, directory);
    // assert(idx == directory_idx || idx == split_page_idx);
    if (idx == directory_idx) {
      target_page->Insert(res[i].first, res[i].second, comparator_);
    } else {
      split_page->Insert(res[i].first, res[i].second, comparator_);
    }
  }

  assert(buffer_pool_manager_->UnpinPage(directory->GetPageId(), true));
  assert(buffer_pool_manager_->UnpinPage(target_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(split_page_id, true));
  table_latch_.WUnlock();
  return Insert(transaction, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  // 锁和插入一致
  table_latch_.RLock();
  // 获得目标页插入
  HashTableDirectoryPage *directory = this->FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, directory);
  Page *page = FetchPage(bucket_page_id);
  page->WLatch();
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(page);
  bool flag = bucket->Remove(key, value, comparator_);
  //  merge之前必须释放读锁
  table_latch_.RUnlock();
  // 这里必须注意，删除失败可能是因为没找到，但是这个时候也要判断这个页是不是空的，进行合并的过程
  if (bucket->IsEmpty()) {
    page->WUnlatch();
    Merge(transaction, key, value);
  } else {
    page->WUnlatch();
  }
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(directory->GetPageId(), true));
  return flag;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  // 获得目标页
  HashTableDirectoryPage *directory = this->FetchDirectoryPage();
  // key得到目录idx
  uint32_t directory_idx = KeyToDirectoryIndex(key, directory);
  page_id_t bucket_page_id = KeyToPageId(key, directory);
  Page *page = FetchPage(bucket_page_id);
  page->RLatch();
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(page);
  // 目标页空才进行删除
  if (!bucket_page->IsEmpty()) {
    page->RUnlatch();
    assert(buffer_pool_manager_->UnpinPage(directory->GetPageId(), true));
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
    table_latch_.WUnlock();
    return;
  }
  page->RUnlatch();
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
  // 首先判断分裂的那部分LD还和当前为空的bucket LD是否相等， 不相等不能合并， LD必须大于0
  uint32_t split_idx = directory->GetSplitImageIndex(directory_idx);
  uint8_t current_ld = directory->GetLocalDepth(directory_idx);
  // 位置必须放在这里，放到后面会越界
  if (current_ld == 0) {
    assert(buffer_pool_manager_->UnpinPage(directory->GetPageId(), true));
    table_latch_.WUnlock();
    return;
  }
  uint8_t split_ld = directory->GetLocalDepth(split_idx);
  if (split_ld != current_ld) {
    assert(buffer_pool_manager_->UnpinPage(directory->GetPageId(), true));
    table_latch_.WUnlock();
    return;
  }
  // 合并的时候，需要把目录中指向要删除的bucket的指针，指向splitImage,同时对目录的LD重置为0
  page_id_t target_page_id = directory->GetBucketPageId(directory_idx);
  page_id_t split_page_id = directory->GetBucketPageId(split_idx);
  directory->DecrLocalDepth(split_idx);
  directory->DecrLocalDepth(directory_idx);
  split_ld = directory->GetLocalDepth(split_idx);
  directory->SetBucketPageId(directory_idx, split_page_id);
  assert(directory->GetLocalDepth(split_idx) == directory->GetLocalDepth(directory_idx));
  for (uint32_t i = 0; i < directory->Size(); ++i) {
    // 还要设置split_page
    if (directory->GetBucketPageId(i) == target_page_id || directory->GetBucketPageId(i) == split_page_id) {
      directory->SetBucketPageId(i, split_page_id);
      directory->SetLocalDepth(i, split_ld);
    }
  }

  if (directory->CanShrink()) {
    directory->DecrGlobalDepth();
  }
  assert(buffer_pool_manager_->UnpinPage(directory->GetPageId(), true));
  table_latch_.WUnlock();
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetGlobalDepth() -> uint32_t {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
