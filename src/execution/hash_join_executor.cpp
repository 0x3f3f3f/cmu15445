//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  idx_ = 0;
}

// 总体过程和nested
// join本质一样，不同的是，对连接的列进行hash取值，相比nested的好处是用哈希降低时间复杂度，从N^2降低到了N，空间复杂度多了N
// 先对tuple利用plan_获得expression，获得对应的Value
// value需要封装进一个HashJoinKey的结构体，因为需要这个结构体计算一个hash
void HashJoinExecutor::Init() {
  // 必须分着声明，减少了可读性
  Tuple left_tuple;
  Tuple right_tuple;
  RID left_rid;
  RID right_rid;
  left_child_->Init();
  right_child_->Init();
  try {
    // 按道理应该先把小表放进哈希表中，可能出现一个哈希值多个Tuple的情况，所以需要value为vector
    while (left_child_->Next(&left_tuple, &left_rid)) {
      HashJoinKey key;
      key.column_value_ = plan_->LeftJoinKeyExpression()->Evaluate(&left_tuple, left_child_->GetOutputSchema());
      map_[key].emplace_back(left_tuple);
    }
    // 遍历第二个表
    while (right_child_->Next(&right_tuple, &right_rid)) {
      HashJoinKey key;
      key.column_value_ = plan_->RightJoinKeyExpression()->Evaluate(&right_tuple, right_child_->GetOutputSchema());
      // 只要找到了就需要全部进行组合
      if (map_.find(key) != map_.end()) {
        for (auto &tuple : map_[key]) {
          std::vector<Value> vals;
          for (auto &column : GetOutputSchema()->GetColumns()) {
            // 通过输出schema获得colomn获得Expression, 代码规范建议用emplace_back
            vals.emplace_back(column.GetExpr()->EvaluateJoin(&tuple, left_child_->GetOutputSchema(), &right_tuple,
                                                             right_child_->GetOutputSchema()));
          }
          res_.emplace_back(Tuple(vals, GetOutputSchema()));
        }
      }
    }
  } catch (Exception &e) {
    throw Exception(ExceptionType::UNKNOWN_TYPE, "hash join error");
  }
  // std::cout << res.size() << std::endl;
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // std::cout << res.size() << std::endl;
  if (idx_ < res_.size()) {
    *tuple = res_[idx_];
    *rid = tuple->GetRid();
    ++idx_;
    return true;
  }
  return false;
}

}  // namespace bustub
