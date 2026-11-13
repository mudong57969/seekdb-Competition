/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_AI_EMBED_H_
#define OCEANBASE_SQL_OB_EXPR_AI_EMBED_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/ob_exec_context.h"
#include "ob_ai_func_client.h"
#include "ob_ai_func.h"
#include "ob_ai_func_utils.h"

namespace oceanbase 
{
namespace sql 
{
class ObExprAIEmbed : public ObFuncExprOperator 
{
public:
  explicit ObExprAIEmbed(common::ObIAllocator &alloc);
  virtual ~ObExprAIEmbed();
  virtual int calc_result_typeN(ObExprResType &type, 
                                ObExprResType *types_array,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const override;
  static int eval_ai_embed(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  // The batch callback merges the rows of one vectorized batch into a single
  // model request, so that a multi row query does not send one request per row.
  static int eval_ai_embed_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                 const ObBitVector &skip, const int64_t size);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, 
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  virtual bool need_rt_ctx() const override { return true; }
private:
  static constexpr int MODEL_IDX = 0;
  static constexpr int CONTENT_IDX = 1;
  static constexpr int DIM_IDX = 2;
  // The rows of one batch are grouped by the model id and by the requested
  // dimension, a group is answered by one model request.
  struct ObAIEmbedBatchGroup
  {
    ObString model_id_;
    int64_t dim_;
    ObArray<int64_t> row_idxs_;
    ObArray<ObString> contents_;
  };
  static int collect_batch_groups(const ObExpr &expr, ObEvalCtx &ctx,
                                  const ObBitVector &skip, const int64_t size,
                                  ObIAllocator &allocator,
                                  ObArray<ObAIEmbedBatchGroup> &groups,
                                  ObArray<int64_t> &null_rows);
  static int call_embed_group(const ObExpr &expr, ObEvalCtx &ctx,
                              ObIAllocator &allocator,
                              const ObAIEmbedBatchGroup &group);
  DISALLOW_COPY_AND_ASSIGN(ObExprAIEmbed);
};

} // namespace sql
} // namespace oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_AI_EMBED_H_
