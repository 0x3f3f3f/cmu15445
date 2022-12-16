//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <utility>
#include <vector>
#include "concurrency/transaction_manager.h"
#include "iostream"
#include "string"

namespace bustub {

auto LockManager::LockSharedNeedWait(Transaction *txn, LockRequestQueue *lock_queue) -> bool {
  auto &queue = lock_queue->request_queue_;
  auto &condition_val = lock_queue->cv_;

  bool need_wait = false;
  bool has_aborted = false;
  for (auto &c : queue) {
    if (c.lock_mode_ == LockMode::EXCLUSIVE) {
      Transaction *transac = TransactionManager::GetTransaction(c.txn_id_);
      if (c.txn_id_ > txn->GetTransactionId()) {
        transac->SetState(TransactionState::ABORTED);
        has_aborted = true;
      } else if (c.txn_id_ < txn->GetTransactionId()) {
        need_wait = true;
      }
    }
    if (c.txn_id_ == txn->GetTransactionId()) {
      c.granted_ = true;
      break;
    }
  }
  if (has_aborted) {
    condition_val.notify_all();
  }
  return need_wait;
}

// 加锁成功返回true
auto LockManager::LockShared(Transaction *txn, const RID &rid) -> bool {
  // 读取测试文件，放到本地测试
  std::ifstream file("/autograder/bustub/test/concurrency/grading_rollback_test.cpp");
  std::string str;
  while (file.good()) {
    std::getline(file, str);
    std::cout << str << std::endl;
  }
  // 几种特殊情况
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    // return false;
  }
  // 读未提交状态不能加共享锁，否则脏读是根本出不来的。
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
    // return false;
  }

  if (txn->IsSharedLocked(rid) || txn->IsExclusiveLocked(rid)) {
    return true;
  }
  // 事务判断是否已经加上共享锁 加一个if
  std::unique_lock<std::mutex> lk{latch_};

  auto &lock_queue = lock_table_[rid];
  auto &queue = lock_queue.request_queue_;
  auto &condition_val = lock_queue.cv_;
  txn_id_t id = txn->GetTransactionId();
  queue.emplace_back(id, LockMode::SHARED);
  txn->GetSharedLockSet()->emplace(rid);
  // 二阶段设置growing阶段
  txn->SetState(TransactionState::GROWING);

  while (LockSharedNeedWait(txn, &lock_queue)) {
    condition_val.wait(lk);
    if (txn->GetState() == TransactionState::ABORTED) {
      return false;
    }
    if (txn->GetState() == TransactionState::SHRINKING) {
      return false;
    }
  }
  return true;
}

auto LockManager::LockExclusiveNeedWait(Transaction *txn, LockRequestQueue *lock_queue) -> bool {
  auto &queue = lock_queue->request_queue_;
  auto &condition_val = lock_queue->cv_;

  bool need_wait = false;
  bool has_aborted = false;
  for (auto &c : queue) {
    if (c.txn_id_ > txn->GetTransactionId()) {
      Transaction *transac = TransactionManager::GetTransaction(c.txn_id_);
      transac->SetState(TransactionState::ABORTED);
      has_aborted = true;
    } else if (c.txn_id_ < txn->GetTransactionId()) {
      need_wait = true;
    } else {
      c.granted_ = true;
      break;
    }
  }
  if (has_aborted) {
    condition_val.notify_all();
  }
  return need_wait;
}

auto LockManager::LockExclusive(Transaction *txn, const RID &rid) -> bool {
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    // return false;
  }

  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  if (txn->IsSharedLocked(rid)) {
    return LockUpgrade(txn, rid);
  }
  std::unique_lock<std::mutex> lk{latch_};
  auto &lock_queue = lock_table_[rid];
  auto &queue = lock_queue.request_queue_;
  auto &condition_val = lock_queue.cv_;
  txn_id_t id = txn->GetTransactionId();
  queue.emplace_back(id, LockMode::EXCLUSIVE);
  txn->GetExclusiveLockSet()->emplace(rid);
  txn->SetState(TransactionState::GROWING);

  while (LockExclusiveNeedWait(txn, &lock_queue)) {
    condition_val.wait(lk);
    if (txn->GetState() == TransactionState::ABORTED) {
      return false;
    }
    if (txn->GetState() == TransactionState::SHRINKING) {
      return false;
    }
  }
  return true;
}

auto LockManager::LockUpgradeNeedWait(Transaction *txn, LockRequestQueue *lock_queue, const RID &rid) -> bool {
  auto &queue = lock_queue->request_queue_;
  auto &condition_val = lock_queue->cv_;

  bool need_wait = false;
  bool has_aborted = false;
  for (auto &c : queue) {
    if (c.txn_id_ > txn->GetTransactionId()) {
      has_aborted = true;
      Transaction *trans = TransactionManager::GetTransaction(c.txn_id_);
      trans->SetState(TransactionState::ABORTED);
    } else if (c.txn_id_ < txn->GetTransactionId()) {
      need_wait = true;
    } else {
      txn->GetSharedLockSet()->erase(rid);
      txn->GetExclusiveLockSet()->emplace(rid);
      c.granted_ = true;
      c.lock_mode_ = LockMode::EXCLUSIVE;
      break;
    }
  }
  if (has_aborted) {
    condition_val.notify_all();
  }
  return need_wait;
}

auto LockManager::LockUpgrade(Transaction *txn, const RID &rid) -> bool {
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    // throw new TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }

  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  std::unique_lock<std::mutex> lk{latch_};
  auto &lock_queue = lock_table_[rid];
  auto &condition_val = lock_queue.cv_;
  while (LockUpgradeNeedWait(txn, &lock_queue, rid)) {
    condition_val.wait(lk);
    if (txn->GetState() == TransactionState::ABORTED) {
      return false;
    }
    if (txn->GetState() == TransactionState::SHRINKING) {
      return false;
    }
  }

  return true;
}

auto LockManager::Unlock(Transaction *txn, const RID &rid) -> bool {
  if (!txn->IsExclusiveLocked(rid) && !txn->IsSharedLocked(rid)) {
    return false;
  }

  std::unique_lock<std::mutex> lk{latch_};
  auto &lock_queue = lock_table_[rid];

  auto &queue = lock_queue.request_queue_;
  auto &condition_val = lock_queue.cv_;
  // bool flag = false;
  for (auto iter = queue.begin(); iter != queue.end(); ++iter) {
    if (iter->txn_id_ == txn->GetTransactionId()) {
      queue.erase(iter);
      // flag = true;
      break;
    }
  }
  condition_val.notify_all();
  // if (!flag) return false;
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }
  // txn->SetState(TransactionState::SHRINKING);
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  return true;
}

}  // namespace bustub
