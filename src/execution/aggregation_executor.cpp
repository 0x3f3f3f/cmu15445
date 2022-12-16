//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      map_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_{map_.Begin()} {}

void AggregationExecutor::Init() {
  child_->Init();
  try {
    Tuple tuple;
    RID rid;
    // 获得初始数据，放到aht类中的哈希表中，绑定到sql语句的各个聚合函数部分
    while (child_->Next(&tuple, &rid)) {
      // MakeAggregateKey(&tuple)用来作为key，拿group by
      // col_b来说，就是对应不同col_b的count,sum,min,max等组成的tuple，然后用后面的形成的MakeAggregateValue，对其更新。
      map_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
    }
  } catch (Exception &e) {
    throw Exception(ExceptionType::UNKNOWN_TYPE, "error happen in aggregate init");
  }
  // 这里必须重新赋予Begin，因为上面是为了构造，这里真的存储以后的。
  aht_iterator_ = map_.Begin();
}
// SELECT count(col_a), col_b, sum(col_c) FROM test_1 Group By col_b HAVING count(col_a) > 100完全可能产生多个结果
auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == map_.End()) {
    return false;
  }
  // key 和 value封装了evaluateAggregate需要的内容
  const AggregateKey &key = aht_iterator_.Key();
  const AggregateValue &value = aht_iterator_.Val();
  // having的谓词条件，having的谓词也是针对聚合函数的
  const AbstractExpression *having_predicate = plan_->GetHaving();
  // 只能使用前置++
  ++aht_iterator_;
  if (having_predicate == nullptr ||
      having_predicate->EvaluateAggregate(key.group_bys_, value.aggregates_).GetAs<bool>()) {
    std::vector<Value> vals;
    for (auto &coloum : GetOutputSchema()->GetColumns()) {
      vals.push_back(coloum.GetExpr()->EvaluateAggregate(key.group_bys_, value.aggregates_));
    }
    *tuple = Tuple(vals, GetOutputSchema());
    return true;
  }
  return Next(tuple, rid);
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
