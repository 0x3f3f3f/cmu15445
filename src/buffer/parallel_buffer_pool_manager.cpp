
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"
#include "common/logger.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager) {
  // Allocate and create individual BufferPoolManagerInstances
  this->buffer_pool_size_ = num_instances * pool_size;
  this->start_new_page_idx_ = 0;
  for (size_t i = 0; i < num_instances; ++i) {
    this->bpms_.emplace_back(new BufferPoolManagerInstance(pool_size, num_instances, i, disk_manager, log_manager));
  }
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  // 必须挨个进行释放，否则就会内存泄漏，直接clear(),不能释放内存
  for (auto &c : bpms_) {
    delete c;
    c = nullptr;
  }
}

auto ParallelBufferPoolManager::GetPoolSize() -> size_t {
  // Get size of all BufferPoolManagerInstances
  return this->buffer_pool_size_;
}

auto ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) -> BufferPoolManager * {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  int idx = page_id_to_instance_[page_id];
  return bpms_[idx];
}

auto ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) -> Page * {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  std::lock_guard<std::mutex> lock(latch_);
  if (page_id_to_instance_.count(page_id) == 0) {
    for (size_t i = 0; i < this->bpms_.size(); ++i) {
      Page *page = bpms_[i]->FetchPage(page_id);
      if (page != nullptr) {
        page_id_to_instance_[page_id] = i;
        return page;
      }
    }
    return nullptr;
  }
  return GetBufferPoolManager(page_id)->FetchPage(page_id);
}

auto ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  // Unpin page_id from responsible BufferPoolManagerInstance
  // 必须通过父类调用子类中protected修饰的函数
  // LOG_DEBUG("%lu", page_id_to_instance_[page_id]);
  return GetBufferPoolManager(page_id)->UnpinPage(page_id, is_dirty);
}

auto ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) -> bool {
  // Flush page_id from responsible BufferPoolManagerInstance
  return GetBufferPoolManager(page_id)->FlushPage(page_id);
}

auto ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) -> Page * {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  std::lock_guard<std::mutex> lock(latch_);
  size_t st = this->start_new_page_idx_;
  Page *target_page = nullptr;
  while (st < this->bpms_.size() && target_page == nullptr) {
    target_page = bpms_[st]->NewPage(page_id);
    ++st;
  }

  if (target_page == nullptr) {
    return nullptr;
  }

  page_id_to_instance_[*page_id] = st - 1;
  this->start_new_page_idx_ = st % this->bpms_.size();
  return target_page;
}

auto ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) -> bool {
  // Delete page_id from responsible BufferPoolManagerInstance
  std::lock_guard<std::mutex> lock(latch_);
  if (page_id_to_instance_.count(page_id) == 0) {
    return true;
  }

  bool res = GetBufferPoolManager(page_id)->DeletePage(page_id);
  // 即使返回删除不成功的情况，在对应的instance中没有了对应的page_id，在总的unordered_map中也可以直接删除。
  page_id_to_instance_.erase(page_id);
  // 写法冗余
  // if (!res) {
  //   return false;
  // }
  // return true;
  return res;
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for (auto c : bpms_) {
    c->FlushAllPages();
  }
}

}  // namespace bustub
