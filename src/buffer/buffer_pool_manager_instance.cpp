
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

#include "common/logger.h"

namespace bustub {
// log_manager头文件那里已经有默认值nullptr
BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    // 强制类型转换
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  // Make sure you call DiskManager::WritePage!
  std::lock_guard<std::mutex> lock(latch_);
  if (page_id == INVALID_PAGE_ID) {
    // LOG_DEBUG("因为invlid不能写入数据的，%d", page_id);
    return false;
  }
  if (page_table_.count(page_id) == 0) {
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  disk_manager_->WritePage(page_id, pages_[frame_id].data_);
  // push完以后赋值false
  pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  std::lock_guard<std::mutex> lock(latch_);
  // for (size_t i = 0; i < page_table_.size(); ++i) {
  //   if (pages_[i].is_dirty_) {
  //     disk_manager_->WritePage(pages_[i].page_id_, pages_[i].data_);
  //   }
  // }
  for (auto c : page_table_) {
    frame_id_t frame_id = c.second;
    page_id_t page_id = c.first;
    disk_manager_->WritePage(page_id, pages_[frame_id].data_);
  }
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.

  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  if (!free_list_.empty()) {
    page_id_t new_page_id = AllocatePage();
    // LOG_DEBUG("%d", new_page_id);
    *page_id = new_page_id;
    frame_id_t fra_id = free_list_.back();
    free_list_.pop_back();
    page_table_[new_page_id] = fra_id;
    // 不能上来就读，因为test.db文件不存在，会报告读不到一个pageSize大小的页
    // disk_manager_->ReadPage(new_page_id, pages_[fra_id].data_);
    pages_[fra_id].ResetMemory();
    pages_[fra_id].page_id_ = new_page_id;
    pages_[fra_id].is_dirty_ = false;
    pages_[fra_id].pin_count_ = 1;
    replacer_->Pin(fra_id);
    return pages_ + fra_id;
  }

  frame_id_t replace_frame;
  if (replacer_->Victim(&replace_frame)) {
    if (pages_[replace_frame].is_dirty_) {
      disk_manager_->WritePage(pages_[replace_frame].page_id_, pages_[replace_frame].data_);
    }
    page_id_t new_page_id = AllocatePage();
    // LOG_DEBUG("%d", new_page_id);
    *page_id = new_page_id;

    page_table_.erase(pages_[replace_frame].page_id_);
    pages_[replace_frame].ResetMemory();
    pages_[replace_frame].is_dirty_ = false;

    pages_[replace_frame].page_id_ = new_page_id;
    // 这里不能自加
    pages_[replace_frame].pin_count_ = 1;
    page_table_[new_page_id] = replace_frame;
    replacer_->Pin(replace_frame);
    // 只要是new，一定是一个新的页号，对于一个新建的文件肯定是没有这个页号，只要读就会出问题。
    // disk_manager_->ReadPage(new_page_id, pages_[replace_frame].data_);
    return pages_ + replace_frame;
  }
  return nullptr;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);

  //        Note that pages are always found from the free list first.
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  if (page_table_.count(page_id) > 0) {
    Page *page_tmp = pages_ + page_table_[page_id];
    page_tmp->pin_count_++;
    replacer_->Pin(page_table_[page_id]);
    return page_tmp;
  }
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  if (!free_list_.empty()) {
    // 取出空闲的物理块
    frame_id_t frame_id = free_list_.front();
    free_list_.pop_front();
    // 完成读取操作
    pages_[frame_id].ResetMemory();
    page_table_[page_id] = frame_id;
    disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
    pages_[frame_id].page_id_ = page_id;
    pages_[frame_id].is_dirty_ = false;
    pages_[frame_id].pin_count_ = 1;
    return pages_ + frame_id;
  }
  // 没有空闲块，而且page_id不在内存中
  // 2.     If R is dirty, write it back to the disk.
  frame_id_t frame_id;
  // 如果有可以替换的页框，没有只能直接返回nullptr，告诉这个时候没有空闲，没有可替换的块
  if (replacer_->Victim(&frame_id)) {
    if (pages_[frame_id].is_dirty_) {
      disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
    }
    // 3.     Delete R from the page table and insert P.
    page_table_.erase(pages_[frame_id].GetPageId());
    page_table_[page_id] = frame_id;
    pages_[frame_id].ResetMemory();
    // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
    disk_manager_->ReadPage(page_id, (pages_ + frame_id)->GetData());
    pages_[frame_id].is_dirty_ = false;
    pages_[frame_id].page_id_ = page_id;
    pages_[frame_id].pin_count_ = 1;
    return pages_ + frame_id;
  }

  return nullptr;
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> lock(latch_);
  DeallocatePage(page_id);
  if (page_table_.count(page_id) == 0) {
    return true;
  }
  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].pin_count_ > 0) {
    return false;
  }

  replacer_->Pin(page_table_[page_id]);

  if (pages_[frame_id].is_dirty_) {
    disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
  }
  page_table_.erase(page_id);
  pages_[frame_id].ResetMemory();
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].pin_count_ = 0;
  free_list_.emplace_back(frame_id);
  // DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  // 可能传入不存在的page_id
  if (page_table_.count(page_id) == 0) {
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].pin_count_ <= 0) {
    return false;
  }
  // 这里即使传入的is_dirty是false，也不能直接赋值false，因为即使这次使用没有更改，可能别的线程使用更改了已经
  if (is_dirty) {
    pages_[frame_id].is_dirty_ = true;
  }
  pages_[frame_id].pin_count_--;
  if (pages_[frame_id].pin_count_ == 0) {
    // LOG_DEBUG("%d", page_id);
    replacer_->Unpin(frame_id);
  }

  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
