//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
}

void DeleteExecutor::DeleteDataAndIndex(Tuple *tuple, RID *rid) {
  TableHeap *table_heap = table_info_->table_.get();
  if (!table_heap->MarkDelete(*rid, exec_ctx_->GetTransaction())) {
    throw Exception(ExceptionType::UNKNOWN_TYPE, "delete data error");
  }

  for (auto &index : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
    auto index_info = index->index_.get();
    index_info->DeleteEntry(
        tuple->KeyFromTuple(table_info_->schema_, *index_info->GetKeySchema(), index_info->GetKeyAttrs()), *rid,
        exec_ctx_->GetTransaction());
  }
}
auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  try {
    Tuple tuple;
    RID rid;
    while (child_executor_->Next(&tuple, &rid)) {
      DeleteDataAndIndex(&tuple, &rid);
    }
  } catch (Exception &e) {
    throw Exception(ExceptionType::UNKNOWN_TYPE, "error happen delete");
  }
  return false;
}

}  // namespace bustub
