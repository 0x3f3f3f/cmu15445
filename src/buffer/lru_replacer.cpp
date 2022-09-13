//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : capasity_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
  lock_.lock();
  if (Size() == 0) {
    lock_.unlock();
    return false;
  }
  *frame_id = list_.back();
  mp_.erase(*frame_id);
  list_.pop_back();
  lock_.unlock();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  lock_.lock();
  if (mp_.count(frame_id) == 0) {
    lock_.unlock();
    return;
  }
  auto c = mp_.find(frame_id);
  list_.erase(c->second);
  mp_.erase(frame_id);
  lock_.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  lock_.lock();
  if (mp_.count(frame_id) > 0) {
    lock_.unlock();
    return;
  }
  if (Size() >= capasity_) {
    mp_.erase(list_.back());
    list_.pop_back();
  }
  list_.push_front(frame_id);
  mp_[frame_id] = list_.begin();
  lock_.unlock();
}

auto LRUReplacer::Size() -> size_t { return list_.size(); }

}  // namespace bustub
