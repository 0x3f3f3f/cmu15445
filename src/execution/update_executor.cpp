//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() { table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid()); }

void UpdateExecutor::UpdateDataAndIndex(Tuple *old_tuple, RID *rid) {
  TableHeap *table_heap = table_info_->table_.get();
  // 更新记录
  Tuple new_tuple = GenerateUpdatedTuple(*old_tuple);
  if (!table_heap->UpdateTuple(new_tuple, *rid, exec_ctx_->GetTransaction())) {
    throw Exception(ExceptionType::UNKNOWN_TYPE, "update data error");
  }

  // 还要更新索引
  for (const auto &index : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
    // 先删旧索引后增新索引
    auto index_info = index->index_.get();
    index_info->DeleteEntry(
        old_tuple->KeyFromTuple(table_info_->schema_, *index_info->GetKeySchema(), index_info->GetKeyAttrs()), *rid,
        exec_ctx_->GetTransaction());
    index_info->InsertEntry(
        new_tuple.KeyFromTuple(table_info_->schema_, *index_info->GetKeySchema(), index_info->GetKeyAttrs()), *rid,
        exec_ctx_->GetTransaction());
  }
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  child_executor_->Init();
  try {
    Tuple tuple;
    RID rid;
    while (child_executor_->Next(&tuple, &rid)) {
      UpdateDataAndIndex(&tuple, &rid);
    }
  } catch (Exception &e) {
    throw Exception(ExceptionType::UNKNOWN_TYPE, "select error during update");
  }
  return false;
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
