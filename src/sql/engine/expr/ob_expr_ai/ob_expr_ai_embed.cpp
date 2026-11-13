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

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_ai_embed.h"
#include "query/ai/ob_ai_endpoint_resolver.h"
#include "share/rc/ob_server_runtime.h"
#include "sql/engine/expr/ob_expr_multi_mode_func_helper.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase 
{
namespace sql 
{
ObExprAIEmbed::ObExprAIEmbed(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, 
                        T_FUN_SYS_AI_EMBED, 
                        N_AI_EMBED, 
                        MORE_THAN_ZERO,
                        NOT_VALID_FOR_GENERATED_COL, 
                        NOT_ROW_DIMENSION) 
{
}

ObExprAIEmbed::~ObExprAIEmbed() 
{
}

int ObExprAIEmbed::calc_result_typeN(ObExprResType &type,
                                     ObExprResType *types_stack,
                                     int64_t param_num,
                                     common::ObExprTypeCtx &type_ctx) const 
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num > 3 || param_num < 2)) {
    ObString func_name_(get_name());
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, func_name_.length(), func_name_.ptr());
  } else {
    types_stack[MODEL_IDX].set_calc_type(ObVarcharType);
    types_stack[MODEL_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    // The content argument accepts ordinary strings and LOB string types.
    // TEXT/LONGTEXT inputs keep their LOB type so that long contents are
    // not truncated, and are read as real string data at runtime.
    // A null literal is accepted as well, it produces a null result.
    if (!ob_is_string_or_lob_type(types_stack[CONTENT_IDX].get_type())
        && types_stack[CONTENT_IDX].get_type() != ObNullType) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, content must be a string or lob type");
    } else {
      if (!ob_is_text_tc(types_stack[CONTENT_IDX].get_type())
          && types_stack[CONTENT_IDX].get_type() != ObNullType) {
        types_stack[CONTENT_IDX].set_calc_type(ObVarcharType);
      }
      types_stack[CONTENT_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    }
    if (OB_SUCC(ret) && param_num == 3) {
      // The dimension argument must be an integer. Non integer types are
      // rejected here, while the value range is checked at runtime.
      if (!ob_is_integer_type(types_stack[DIM_IDX].get_type())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, dimension parameter must be an integer");
      } else {
        types_stack[DIM_IDX].set_calc_type(ObIntType);
        types_stack[DIM_IDX].set_precision(10);
        types_stack[DIM_IDX].set_scale(0);
      }
    }
    if (OB_SUCC(ret)) {
      type.set_varchar();
      type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
      type.set_collation_level(CS_LEVEL_COERCIBLE);
    }
  }
  return ret;
}

int ObExprAIEmbed::eval_ai_embed(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res) 
{
  INIT_SUCC(ret);
  ObDatum *arg_model_id = nullptr;
  ObDatum *arg_content = nullptr;
  ObDatum *arg_dim = nullptr;
  if (expr.arg_cnt_ == 3 ? OB_FAIL(expr.eval_param_value(ctx, arg_model_id, arg_content, arg_dim))
                         : OB_FAIL(expr.eval_param_value(ctx, arg_model_id, arg_content))) {
  } else if (arg_model_id->is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, model id is null");
    res.set_null();
  } else if (arg_content->is_null()) {
    // A null content produces a null result instead of an error, so that
    // null rows do not break multi row queries.
    res.set_null();
  } else {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    
    MultimodeAlloctor temp_allocator(tmp_alloc_g.get_allocator());
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(N_AI_EMBED));
    ObAIFuncExprInfo *info = nullptr;
    share::ObAiModelEndpointInfo resolved_endpoint;
    const share::ObAiModelEndpointInfo *endpoint_info = &resolved_endpoint;
    query::ObIAiEndpointResolver *endpoint_resolver =
        ::oceanbase::share::server_service<::oceanbase::query::ObIAiEndpointResolver>();
    ObString model_id = arg_model_id->get_string();
    ObString content = arg_content->get_string();
    if (ob_is_text_tc(expr.args_[CONTENT_IDX]->datum_meta_.type_)) {
      // LOB contents must be read as real string data, otherwise only the
      // locator would be sent to the model service.
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                   ctx.exec_ctx_, temp_allocator, *arg_content,
                   expr.args_[CONTENT_IDX]->datum_meta_,
                   expr.args_[CONTENT_IDX]->obj_meta_.has_lob_header(),
                   content))) {
      }
    }
    if (OB_SUCC(ret) && (model_id.empty() || content.empty())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, model id or input is empty");
      res.set_null();
    }
    int64_t dim = 0;
    ObJsonInt *dim_json = nullptr;
    ObJsonObject *config = nullptr;
    if (OB_FAIL(ret)) {
    } else if (OB_NOT_NULL(arg_dim)) {
      if (arg_dim->is_null()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, dimension parameter is null");
        res.set_null();
      } else {
        dim = arg_dim->get_int();
        if (dim <= 0) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, dimension parameter must be a positive integer");
          res.set_null();
        } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(temp_allocator, config))) {
        } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_int(temp_allocator, dim, dim_json))) {
        } else if (OB_FAIL(config->add("dimensions", dim_json))) {
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(temp_allocator, model_id, info))) {
    } else if (OB_ISNULL(endpoint_resolver)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(endpoint_resolver->resolve_by_model_name(
                   model_id, temp_allocator, resolved_endpoint))) {
    } else {
      ObAIFuncModel model(temp_allocator, *info, *endpoint_info);
      ObString result;
      if (OB_FAIL(model.call_dense_embedding(content, config, result))) {
      } else if (OB_FAIL(ObAIFuncUtils::set_string_result(expr, ctx, res, result))) {
      }
    }
  }
  return ret;
}

int ObExprAIEmbed::collect_batch_groups(const ObExpr &expr,
                                        ObEvalCtx &ctx,
                                        const ObBitVector &skip,
                                        const int64_t size,
                                        ObIAllocator &allocator,
                                        ObArray<ObAIEmbedBatchGroup> &groups,
                                        ObArray<int64_t> &null_rows)
{
  int ret = OB_SUCCESS;
  ObDatumVector model_id_vector;
  ObDatumVector content_vector;
  ObDatumVector dim_vector;
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  const bool has_dim_arg = (expr.arg_cnt_ > DIM_IDX);
  const bool is_text_content = ob_is_text_tc(expr.args_[CONTENT_IDX]->datum_meta_.type_);
  if (has_dim_arg) {
    if (OB_FAIL(expr.eval_batch_param_value(ctx, skip, size,
                                            model_id_vector, content_vector, dim_vector))) {
    }
  } else if (OB_FAIL(expr.eval_batch_param_value(ctx, skip, size,
                                                 model_id_vector, content_vector))) {
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
      if (skip.at(i) || eval_flags.at(i)) {
        // The row is not part of this batch.
        continue;
      }
      ObDatum *model_id_datum = model_id_vector.at(i);
      ObDatum *content_datum = content_vector.at(i);
      if (OB_ISNULL(model_id_datum) || OB_ISNULL(content_datum)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the batch parameter of ai_embed is missing", K(ret), K(i));
      } else if (model_id_datum->is_null()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, model id is null");
      } else if (content_datum->is_null()) {
        // A null content produces a null result instead of an error, so that
        // null rows do not break multi row queries. The row is not sent to the
        // model service and it does not shift the answers of the other rows.
        if (OB_FAIL(null_rows.push_back(i))) {
          LOG_WARN("fail to push back the null row", K(ret), K(i));
        }
      } else {
        ObString model_id = model_id_datum->get_string();
        ObString content = content_datum->get_string();
        if (is_text_content) {
          // LOB contents must be read as real string data, otherwise only the
          // locator would be sent to the model service.
          if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                       ctx.exec_ctx_, allocator, *content_datum,
                       expr.args_[CONTENT_IDX]->datum_meta_,
                       expr.args_[CONTENT_IDX]->obj_meta_.has_lob_header(),
                       content))) {
          }
        }
        int64_t dim = 0;
        if (OB_SUCC(ret) && has_dim_arg) {
          ObDatum *dim_datum = dim_vector.at(i);
          if (OB_ISNULL(dim_datum)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("the dimension parameter of ai_embed is missing", K(ret), K(i));
          } else if (dim_datum->is_null()) {
            ret = OB_INVALID_ARGUMENT;
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, dimension parameter is null");
          } else {
            dim = dim_datum->get_int();
            if (dim <= 0) {
              ret = OB_INVALID_ARGUMENT;
              LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, dimension parameter must be a positive integer");
            }
          }
        }
        if (OB_SUCC(ret) && (model_id.empty() || content.empty())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, model id or input is empty");
        }
        if (OB_SUCC(ret)) {
          // The rows are grouped by the model id and by the requested
          // dimension, only the rows of one group share a model request.
          ObAIEmbedBatchGroup *group = nullptr;
          for (int64_t g = 0; OB_SUCC(ret) && g < groups.count(); ++g) {
            ObAIEmbedBatchGroup &candidate = groups.at(g);
            if (candidate.model_id_.compare_equal(model_id) && candidate.dim_ == dim) {
              group = &candidate;
              break;
            }
          }
          if (OB_ISNULL(group)) {
            ObAIEmbedBatchGroup new_group;
            new_group.model_id_ = model_id;
            new_group.dim_ = dim;
            if (OB_FAIL(groups.push_back(new_group))) {
              LOG_WARN("fail to push back the batch group", K(ret), K(i));
            } else {
              group = &groups.at(groups.count() - 1);
            }
          }
          if (OB_SUCC(ret) && OB_FAIL(group->row_idxs_.push_back(i))) {
            LOG_WARN("fail to push back the row index", K(ret), K(i));
          } else if (OB_SUCC(ret) && OB_FAIL(group->contents_.push_back(content))) {
            LOG_WARN("fail to push back the content", K(ret), K(i));
          }
        }
      }
    }
  }
  return ret;
}

int ObExprAIEmbed::call_embed_group(const ObExpr &expr,
                                     ObEvalCtx &ctx,
                                     ObIAllocator &allocator,
                                     const ObAIEmbedBatchGroup &group)
{
  int ret = OB_SUCCESS;
  ObAIFuncExprInfo *info = nullptr;
  share::ObAiModelEndpointInfo resolved_endpoint;
  const share::ObAiModelEndpointInfo *endpoint_info = &resolved_endpoint;
  query::ObIAiEndpointResolver *endpoint_resolver =
      ::oceanbase::share::server_service<::oceanbase::query::ObIAiEndpointResolver>();
  int64_t dim = group.dim_;
  ObJsonInt *dim_json = nullptr;
  ObJsonObject *config = nullptr;
  if (OB_ISNULL(endpoint_resolver)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(allocator, group.model_id_, info))) {
  } else if (OB_FAIL(endpoint_resolver->resolve_by_model_name(
                   group.model_id_, allocator, resolved_endpoint))) {
  } else if (dim > 0) {
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, config))) {
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_int(allocator, dim, dim_json))) {
    } else if (OB_FAIL(config->add("dimensions", dim_json))) {
    }
  }
  if (OB_SUCC(ret)) {
    ObAIFuncModel model(allocator, *info, *endpoint_info);
    ObArray<ObString> contents;
    ObArray<ObString> results;
    if (OB_FAIL(contents.assign(group.contents_))) {
      LOG_WARN("fail to copy the contents of the batch", K(ret));
    } else if (OB_FAIL(model.call_dense_embedding_batch(contents, config, results))) {
    } else if (results.count() != group.row_idxs_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the answer count does not match the row count", K(ret),
               K(results.count()), K(group.row_idxs_.count()));
    } else {
      ObDatum *res_datums = expr.locate_batch_datums(ctx);
      for (int64_t i = 0; OB_SUCC(ret) && i < results.count(); ++i) {
        const int64_t row_idx = group.row_idxs_.at(i);
        ObString &result = results.at(i);
        ObTextStringVectorResult<common::ObIVector> str_result(
            expr.datum_meta_.type_, &expr, &ctx, res_datums + row_idx);
        if (OB_FAIL(str_result.init_with_batch_idx(result.length(), row_idx))) {
          LOG_WARN("fail to init the result of the row", K(ret), K(row_idx));
        } else if (OB_FAIL(str_result.append(result))) {
          LOG_WARN("fail to append the result of the row", K(ret), K(row_idx));
        } else {
          str_result.set_result();
        }
      }
    }
  }
  return ret;
}

int ObExprAIEmbed::eval_ai_embed_batch(const ObExpr &expr,
                                        ObEvalCtx &ctx,
                                        const ObBitVector &skip,
                                        const int64_t size)
{
  INIT_SUCC(ret);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  MultimodeAlloctor temp_allocator(tmp_alloc_g.get_allocator());
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(N_AI_EMBED));
  ObArray<ObAIEmbedBatchGroup> groups;
  ObArray<int64_t> null_rows;
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObDatum *res_datums = expr.locate_batch_datums(ctx);
  if (OB_FAIL(collect_batch_groups(expr, ctx, skip, size, temp_allocator,
                                   groups, null_rows))) {
  } else {
    // The null rows are answered by a null result, they are not sent to the
    // model service.
    for (int64_t i = 0; OB_SUCC(ret) && i < null_rows.count(); ++i) {
      res_datums[null_rows.at(i)].set_null();
      eval_flags.set(null_rows.at(i));
    }
    for (int64_t g = 0; OB_SUCC(ret) && g < groups.count(); ++g) {
      if (OB_FAIL(call_embed_group(expr, ctx, temp_allocator, groups.at(g)))) {
      } else {
        for (int64_t i = 0; i < groups.at(g).row_idxs_.count(); ++i) {
          eval_flags.set(groups.at(g).row_idxs_.at(i));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    // The batch failed, the rows of the batch are answered by a null. The
    // rows which are not part of this batch keep their own result.
    for (int64_t i = 0; i < null_rows.count(); ++i) {
      res_datums[null_rows.at(i)].set_null();
    }
    for (int64_t g = 0; g < groups.count(); ++g) {
      for (int64_t i = 0; i < groups.at(g).row_idxs_.count(); ++i) {
        res_datums[groups.at(g).row_idxs_.at(i)].set_null();
      }
    }
  }
  return ret;
}

int ObExprAIEmbed::cg_expr(ObExprCGCtx &expr_cg_ctx, 
                           const ObRawExpr &raw_expr,
                           ObExpr &rt_expr) const 
{
  int ret = OB_SUCCESS;
  // TODO: support schema version match in plan cache for ai func
  // const ObRawExpr *model_key = raw_expr.get_param_expr(0);
  // if (OB_NOT_NULL(model_key)
  //     && (model_key->is_static_scalar_const_expr() || model_key->is_const_expr())
  //     && model_key->get_expr_type() != T_OP_GET_USER_VAR &&
  //     OB_NOT_NULL(expr_cg_ctx.schema_guard_)) {
  //   ObIAllocator *allocator = expr_cg_ctx.allocator_;
  //   ObExecContext *exec_ctx = expr_cg_ctx.session_->get_cur_exec_ctx();
  //   ObObj const_data;
  //   bool got_data = false;
  //   ObAIFuncExprInfo *info = nullptr;
  //   if (OB_ISNULL(allocator)) {
  //     ret = OB_ERR_UNEXPECTED;
  //     LOG_WARN("allocator is null", K(ret));
  //   } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(exec_ctx,
  //                                                         model_key,
  //                                                         const_data,
  //                                                         got_data,
  //                                                         *allocator))) {
  //     LOG_WARN("failed to calc offset expr", K(ret));
  //   } else if (!got_data || const_data.is_null()) {
  //   } else if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(*allocator, const_data.get_string(), *expr_cg_ctx.schema_guard_, info))) {
  //     LOG_WARN("failed to get ai func info", K(ret), K(const_data.get_string()));
  //   } else {
  //     rt_expr.extra_info_ = info;
  //   }
  // }

  if (OB_SUCC(ret)) {
    rt_expr.eval_func_ = ObExprAIEmbed::eval_ai_embed;
    rt_expr.eval_batch_func_ = ObExprAIEmbed::eval_ai_embed_batch;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
