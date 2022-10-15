//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  // 必须赋予初始值
  idx_ = 0;
}

void NestedLoopJoinExecutor::Init() {
  // 分开，不然可读性不好
  Tuple left_tuple;
  Tuple right_tuple;
  RID left_rid;
  RID right_rid;
  left_executor_->Init();

  const Schema *left_schema = left_executor_->GetOutputSchema();
  const Schema *right_schema = right_executor_->GetOutputSchema();

  const AbstractExpression *predicate = plan_->Predicate();
  try {
    while (left_executor_->Next(&left_tuple, &left_rid)) {
      right_executor_->Init();
      while (right_executor_->Next(&right_tuple, &right_rid)) {
        // std::cout << 1111 << std::endl;
        if (predicate == nullptr ||
            predicate->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema).GetAs<bool>()) {
          std::vector<Value> arr;
          for (auto &column : GetOutputSchema()->GetColumns()) {
            Value val = column.GetExpr()->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema);
            arr.emplace_back(val);
          }

          res_.emplace_back(Tuple(arr, GetOutputSchema()));
        }
      }
    }
  } catch (Exception &e) {
    throw Exception(ExceptionType::UNKNOWN_TYPE, "nested join happen error");
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (idx_ < res_.size()) {
    *tuple = res_[idx_];
    ++idx_;
    return true;
  }
  return false;
}

}  // namespace bustub
