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
  // 只要更大的优先级在前面，同时存在互斥的锁，就必须等待
  bool need_wait = false;
  // 只要之前有优先级更小的，代表wound wait 存在要abort的事务
  bool has_aborted = false;
  for (auto &c : queue) {
    // 读互斥的锁只有写锁
    if (c.lock_mode_ == LockMode::EXCLUSIVE) {
      Transaction *transac = TransactionManager::GetTransaction(c.txn_id_);
      // 事务id越小，优先级越高
      if (c.txn_id_ > txn->GetTransactionId()) {
        transac->SetState(TransactionState::ABORTED);
        has_aborted = true;
      } else if (c.txn_id_ < txn->GetTransactionId()) {
        need_wait = true;
      } else {
        break;
      }
    }
  }
  // 只要有被终止的，就需要让事务的线程主动退出去。
  // notify函数唤醒队列从前往后都会唤醒一遍，按顺序唤醒，让abort的事务自动退出
  if (has_aborted) {
    condition_val.notify_all();
  }
  return need_wait;
}

// 加锁成功返回true
auto LockManager::LockShared(Transaction *txn, const RID &rid) -> bool {
  // // 读取测试文件，放到本地测试
  // std::ifstream file("/autograder/bustub/test/concurrency/grading_rollback_test.cpp");
  // std::string str;
  // while (file.good()) {
  //   std::getline(file, str);
  //   std::cout << str << std::endl;
  // }
  // 几种特殊情况
  // 死锁预防，wounwait策略，破坏性等待,导致
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  // 2PL的实际体现
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    // return false;
  }
  // 读未提交状态不能加共享锁，否则脏读是根本出不来的。因为只要写锁加了以后
  // 为了避免最基本的脏写，写锁是必须一直到事务abort 和 commit以后才能起作用。
  // 读未提交产生脏读，就是读写过的事务，但是加了S锁就不可能读出写过的数据
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
    // return false;
  }
  if (txn->IsSharedLocked(rid) || txn->IsExclusiveLocked(rid)) {
    return true;  // 只要在读的set或者写的set里面，不用加读锁，写本身级别就达到了读的要求
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
  // 重复执行检查，知道当前事务之前没有优先级更高的时候，当前事务继续执行
  while (LockSharedNeedWait(txn, &lock_queue)) {
    condition_val.wait(lk);
    // 如果被wound以后，直接退出
    if (txn->GetState() == TransactionState::ABORTED) {
      return false;
    }
    // if (txn->GetState() == TransactionState::SHRINKING) {
    //   return false;
    // }
  }
  // 真的可以return true的时候grant赋值true
  for (auto &c : queue) {
    if (c.txn_id_ == txn->GetTransactionId()) {
      c.granted_ = true;
      break;
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
      // 同样直接赋值成功不合适
      // c.granted_ = true;
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
  // 读锁就进行锁的升级，不管什么情况，一个rid对应的队列都不会出现同一个事务加的两个锁。
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
    // if (txn->GetState() == TransactionState::SHRINKING) {
    //   return false;
    // }
  }
  // 必须确定此事务没事，正常加锁，才能赋值，无法和上面合并
  for (auto &c : queue) {
    if (c.txn_id_ == txn->GetTransactionId()) {
      c.granted_ = true;
      break;
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
  // 升级的目的就是找到事务中的读锁，把他升级为写锁
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  std::unique_lock<std::mutex> lk{latch_};
  auto &lock_queue = lock_table_[rid];
  auto &queue = lock_queue.request_queue_;
  auto &condition_val = lock_queue.cv_;
  while (LockUpgradeNeedWait(txn, &lock_queue, rid)) {
    condition_val.wait(lk);
    if (txn->GetState() == TransactionState::ABORTED) {
      return false;
    }
    // if (txn->GetState() == TransactionState::SHRINKING) {
    //   return false;
    // }
  }

  // 必须确定此事务没事，正常加锁，才能赋值，无法和上面合并
  for (auto &c : queue) {
    if (c.txn_id_ == txn->GetTransactionId()) {
      txn->GetSharedLockSet()->erase(rid);
      txn->GetExclusiveLockSet()->emplace(rid);
      c.granted_ = true;
      c.lock_mode_ = LockMode::EXCLUSIVE;
      break;
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
  // 当前事务退出，要释放占有线程
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
