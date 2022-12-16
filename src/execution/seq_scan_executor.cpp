//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {
// 有参构造对象作为成员变量必须要再初始化列表中赋值, 可以虚拟构造一个，重新赋值。父类的成员变量子类也是可以使用的。
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), iterator_(nullptr, RID(), nullptr) {}

void SeqScanExecutor::Init() {
  // table_info有操作对象和操作模式
  TableInfo *table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  this->table_heap_ = table_info->table_.get();
  iterator_ = table_heap_->Begin(exec_ctx_->GetTransaction());
}
// RID作为一条记录的identifier
auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 判断是否到达表尾，直接false
  if (iterator_ == table_heap_->End()) {
    return false;
  }
  LockManager *lock_manager = exec_ctx_->GetLockManager();
  Transaction *trans = exec_ctx_->GetTransaction();
  if (trans->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    if (!lock_manager->LockShared(trans, *rid)) {
      throw TransactionAbortException(trans->GetTransactionId(), AbortReason::DEADLOCK);
    }
  }
  // 获得rid用来赋值给形成的新的tuple
  RID target_rid = iterator_->GetRid();
  // 返回值const修饰必须用const修饰的变量接收
  const Schema *out_put_schema = GetOutputSchema();
  std::vector<Value> vals;
  vals.reserve(out_put_schema->GetColumnCount());
  // 获得schema
  TableInfo *table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  // 根据输出的schema ，可以通过column，每个tuple中获得对应位置的Value对象，构造新的Tuple
  for (size_t i = 0; i < vals.capacity(); ++i) {
    // 目的Value就是out_put_schema->GetColumn(i)，这个时候的column中的abstractExpressioncol_idx定义的是原始表的schema的，所以evaluate
    // 传入的时候要用原始表的tuple和schema
    vals.emplace_back(out_put_schema->GetColumn(i).GetExpr()->Evaluate(&(*(iterator_)), &table_info->schema_));
  }

  ++iterator_;
  // 新的Tuple一个是用来应用predicate谓词的，再有就是作为结果输出。
  Tuple tmp(vals, out_put_schema);
  // 谓词表达提前构造好的，什么和什么比都是确定的，只需要传入新的tuple和新的tuple的schema
  // 谓词表达式的构造一定是根据out_put_schema提前构造好的！！！看evaluate实现就能明白必须用新的schema，否则col_idx可能会越界、
  const AbstractExpression *predicate = plan_->GetPredicate();

  if (trans->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (!lock_manager->Unlock(trans, *rid)) {
      throw TransactionAbortException(trans->GetTransactionId(), AbortReason::DEADLOCK);
    }
  }

  // 有可能存在没有谓词逻辑的时候，比如全选。
  if (predicate == nullptr || predicate->Evaluate(&tmp, out_put_schema).GetAs<bool>()) {
    *tuple = tmp;
    *rid = target_rid;
    return true;
  }
  return Next(tuple, rid);
}

}  // namespace bustub
