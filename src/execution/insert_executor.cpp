//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "common/logger.h"
#include "execution/executors/insert_executor.h"
namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)) {}  // 一般可以右值move的时候都是传递的右值，比如这里，完全可以move

void InsertExecutor::Init() {
  catalog_ = GetExecutorContext()->GetCatalog();
  table_info_ = catalog_->GetTable(plan_->TableOid());
  // 本来写的是unique_ptr封装的table_heap_，move把内容转给table_heap_,但是出错了，原因很可能是因为move以后，
  // table_info_里面的table_heap被释放，上层用到这个东西，发生使用nullptr的错误。
  // 这里不用自己释放内存，tableheap在table_info，因为上层传递，上层会负责释放catalog
  table_heap_ = table_info_->table_.get();
}

void InsertExecutor::InsertIntoDataAndIndex(Tuple *tuple) {
  // 插入数据,插入数据的时候，rid初始是没有数据的，只有插入成功的时候，才会生成！！！
  // RID rid = tuple.GetRid();
  RID rid;
  if (!table_heap_->InsertTuple(*tuple, &rid, GetExecutorContext()->GetTransaction())) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "没有足够的内存插入");
  }
  // 插入索引，一个表的索引完全可能存在多个，对全部的索引进行更新.
  // std::vector<IndexInfo *> indexes = catalog_->GetTableIndexes(table_info_->name_);
  // for (auto &index : indexes) {
  // 可以不用构造返回值类型，直接auto接受
  for (auto &index : catalog_->GetTableIndexes(table_info_->name_)) {
    // 插入索引的时候，索引插入针对的tuple是索引关键字
    // 所有的索引信息都在index_info，所有的表的信息都在table_info， GetKeyAttrs再indexinfo里面的index中
    index->index_->InsertEntry(
        tuple->KeyFromTuple(table_info_->schema_, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs()),
        // 这里注意rid必须传入的是inserttuple成功创建的rid，实际上raw数据，没有rid
        rid, GetExecutorContext()->GetTransaction());
  }
  // LOG_DEBUG("********+++++++++++++++++++++++++++");
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  LockManager *lock_manager = exec_ctx_->GetLockManager();
  Transaction *trans = exec_ctx_->GetTransaction();

  if (!lock_manager->LockExclusive(trans, *rid)) {
    throw TransactionAbortException(trans->GetTransactionId(), AbortReason::DEADLOCK);
  }
  // 没有子执行语句的执行
  if (plan_->IsRawInsert()) {
    // LOG_DEBUG("%lu", vals.size());
    // 获得raw数据必须判断是原生数据插入
    const std::vector<std::vector<Value>> &vals = plan_->RawValues();
    // vals是const类型 auto遍历的时候，每一个变量尽量也是要用const
    for (auto &c : vals) {
      // LOG_DEBUG("****************************************");
      // 插入这里压根没有输出模式，InsertNode没有实现GetSchema
      Tuple tuple(c, &table_info_->schema_);
      InsertIntoDataAndIndex(&tuple);
    }

    // 成功必须返回false，excutor上层执行的时候使用的是while循环，会往复执行
    return false;
  }
  // 例子  INSERT INTO empty_table2 SELECT col_a, col_b FROM test_1 WHERE col_a < 500
  // 子执行语句，执行子执行语句，获得筛选后的值在进行插入
  std::vector<Tuple> arr;

  child_executor_->Init();
  try {
    // 注意创建动态内存必须释放！！！！
    Tuple tuple;
    RID rid;
    while (child_executor_->Next(&tuple, &rid)) {
      arr.push_back(tuple);
    }
  } catch (Exception &e) {
    throw Exception(ExceptionType::UNKNOWN_TYPE, "insert:executor_child_error");
    return false;
  }
  for (auto &c : arr) {
    InsertIntoDataAndIndex(&c);
  }
  // delete table_heap_;
  return false;
}

}  // namespace bustub
