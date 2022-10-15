//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DistinctExecutor::Init() { child_executor_->Init(); }

auto DistinctExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  try {
    Tuple tmp;
    RID tmp_rid;
    while (child_executor_->Next(&tmp, &tmp_rid)) {
      std::vector<Value> vals;
      for (uint32_t i = 0; i < child_executor_->GetOutputSchema()->GetColumns().size(); ++i) {
        // 有tuple可以直接获得value可以直接通过tuple的getvalue函数获得
        // vals.push_back(coloum.GetExpr().Evaluate(&tuple, child_executor_->GetOutputSchema()));
        vals.push_back(tmp.GetValue(child_executor_->GetOutputSchema(), i));
      }
      DistinctKey key;
      key.value_ = vals;
      if (set_.find(key) == set_.end()) {
        *tuple = tmp;
        *rid = tmp_rid;
        set_.insert(key);
        return true;
      }
    }
  } catch (Exception &e) {
    throw Exception(ExceptionType::UNKNOWN_TYPE, "error happen in distict sql");
  }
  return false;
}

}  // namespace bustub
