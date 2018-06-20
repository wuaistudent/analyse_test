package parser

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
  * Created by Administrator on 2018/6/13.
  */
class EnchancedLogicalPlan(val plan: LogicalPlan) extends AnyVal{

  def optional(ctx: AnyRef)(f: => LogicalPlan): LogicalPlan = {
    if(ctx != null) {
      f
    } else {
      plan
    }
  }

  def optionalMap[C](ctx: C)(f: (C, LogicalPlan) => LogicalPlan): LogicalPlan = {
    if (ctx != null) {
      f(ctx, plan)
    } else {
      plan
    }
  }

}
