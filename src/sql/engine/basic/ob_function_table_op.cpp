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

#include <charconv>

#include "sql/engine/basic/ob_function_table_op.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/basic/file_scan/file_csv_reader.h"
#include "sql/session/ob_sql_session_info.h"
#include <new>
#include "sql/engine/expr/ob_expr_lob_utils.h"


namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER((ObFunctionTableSpec, ObOpSpec), value_expr_, column_exprs_, has_correlated_expr_,
                    file_identity_, file_width_, file_column_indexes_, file_column_names_);

struct CsvScanState {
  file_scan::CsvReader reader_;
  std::vector<file_scan::Row> rows_;
};

int ObFunctionTableOp::open_csv()
{
  close_csv();
  int ret = OB_SUCCESS;
  bool enabled = false;
  ObArenaAllocator policy_allocator(ObModIds::OB_SQL_SESSION);
  ObString allowed;
  ObDatum *path = nullptr;
  ObSQLSessionInfo *session = ctx_.get_my_session();
  if (OB_ISNULL(session) || OB_ISNULL(MY_SPEC.value_expr_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ctx_.check_status())) {
  } else if (OB_FAIL(session->get_file_sql_access(policy_allocator, enabled, allowed))) {
  } else if (!enabled) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "file SQL while enable_file_sql is OFF");
  } else if (OB_FAIL(MY_SPEC.value_expr_->eval(eval_ctx_, path))) {
  } else if (OB_ISNULL(path) || path->is_null() || path->get_string().empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "read_csv path");
  } else if (OB_ISNULL(csv_ = new (std::nothrow) CsvScanState())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    const ObString &filename = path->get_string();
    const std::string directory = allowed.empty() ? std::string()
        : std::string(allowed.ptr(), allowed.length());
    const std::string identity(MY_SPEC.file_identity_.ptr(), MY_SPEC.file_identity_.length());
    const auto format = file_scan::format_from_identity(identity);
    std::vector<std::string> names;
    for (int64_t i = 0; i < MY_SPEC.file_column_names_.count(); ++i) {
      const auto &name = MY_SPEC.file_column_names_.at(i);
      names.emplace_back(name.ptr(), name.length());
    }
    if (!csv_->reader_.open(enabled, directory, std::string(filename.ptr(), filename.length()), format,
          [](void *context) { return OB_SUCCESS == static_cast<ObExecContext *>(context)->check_status(); },
          &ctx_)) {
      if (OB_SUCC(ret = ctx_.check_status())) {
        ret = OB_ERR_NO_PRIVILEGE;
        LOG_USER_ERROR(OB_ERR_NO_PRIVILEGE, "secure_file_priv");
      }
    } else if (!csv_->reader_.start_scan(identity, MY_SPEC.file_width_, names,
          [](void *context) { return OB_SUCCESS == static_cast<ObExecContext *>(context)->check_status(); },
          &ctx_)) {
      if (OB_SUCC(ret = ctx_.check_status())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, csv_->reader_.error().c_str());
      }
    }
  }
  if (OB_FAIL(ret)) {
    close_csv();
  }
  return ret;
}

void ObFunctionTableOp::close_csv()
{
  delete csv_;
  csv_ = nullptr;
}

int ObFunctionTableOp::inner_get_next_row_csv()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (OB_ISNULL(csv_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(ctx_.check_status())) {
  } else if (!csv_->reader_.next_batch(1, csv_->rows_, [](void *context) {
    return static_cast<ObExecContext *>(context)->check_status() == OB_SUCCESS;
  }, &ctx_)) {
    if (OB_SUCC(ret = ctx_.check_status())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, csv_->reader_.error().c_str());
    }
  } else if (csv_->rows_.empty()) {
    ret = OB_ITER_END;
  } else {
    const auto &row = csv_->rows_.front();
    for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.column_exprs_.count(); ++i) {
      ObExpr *expr = MY_SPEC.column_exprs_.at(i);
      const auto &field = row[MY_SPEC.file_column_indexes_.at(i)];
      ObDatum &datum = expr->locate_datum_for_write(eval_ctx_);
      if (field.is_null) {
        datum.set_null();
      } else if (expr->datum_meta_.type_ == ObVarcharType) {
        datum.set_string(ObString(field.value.size(), field.value.data()));
      } else if (expr->datum_meta_.type_ == ObNumberType) {
        ObNumStackOnceAlloc allocator;
        number::ObNumber value;
        OZ (value.from(field.value.data(), field.value.size(), allocator, nullptr, nullptr, false));
        OX (datum.set_number(value));
      } else if (expr->datum_meta_.type_ == ObDoubleType && MY_SPEC.file_identity_.prefix_match("parquet:")) {
        double value = 0;
        const auto *end = field.value.data() + field.value.size();
        const auto parsed = std::from_chars(field.value.data(), end, value);
        if (parsed.ec != std::errc{} || parsed.ptr != end) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "invalid Parquet floating-point value");
        } else {
          datum.set_double(value);
        }
      } else {
        file_scan::Type type = file_scan::Type::Null;
        switch (expr->datum_meta_.type_) {
          case ObIntType: type = file_scan::Type::Integer; break;
          case ObDoubleType: type = file_scan::Type::Double; break;
          case ObTinyIntType: type = file_scan::Type::Boolean; break;
          case ObDateType: type = file_scan::Type::Date; break;
          case ObDateTimeType: type = file_scan::Type::DateTime; break;
          default: break;
        }
        file_scan::Scalar value;
        if (!file_scan::parse_scalar(type, field.value, value)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "read_csv column type changed");
        } else {
          switch (type) {
            case file_scan::Type::Integer:
            case file_scan::Type::Boolean: datum.set_int(value.integer); break;
            case file_scan::Type::Double: datum.set_double(value.real); break;
            case file_scan::Type::Date: datum.set_date(value.integer); break;
            case file_scan::Type::DateTime: datum.set_datetime(value.integer); break;
            default: ret = OB_ERR_UNEXPECTED; break;
          }
        }
      }
      if (OB_SUCC(ret)) {
        expr->set_evaluated_projected(eval_ctx_);
      }
    }
  }
  if (OB_SUCCESS != ret && OB_ITER_END != ret) {
    close_csv();
  }
  return ret;
}

int ObFunctionTableOp::inner_open()
{
  int ret = OB_SUCCESS;
  node_idx_ = 0;
  already_calc_ = false;
  if (!MY_SPEC.file_identity_.empty()) {
    next_row_func_ = &ObFunctionTableOp::inner_get_next_row_csv;
    ret = open_csv();
  } else if (OB_ISNULL(MY_SPEC.value_expr_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (ObExtendType == MY_SPEC.value_expr_->datum_meta_.type_) {
    next_row_func_ = &ObFunctionTableOp::inner_get_next_row_udf;
  } else {
    next_row_func_ = &ObFunctionTableOp::inner_get_next_row_sys_func;
  }
  return ret;
}

int ObFunctionTableOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_rescan())) {
  } else {
    node_idx_ = 0;
    if (!MY_SPEC.file_identity_.empty()) {
      clear_evaluated_flag();
      ret = open_csv();
    } else if (MY_SPEC.has_correlated_expr_) {
      row_count_ = 0;
      col_count_ = 0;
      value_table_ = NULL;
      already_calc_ = false;
    }
  }
  return ret;
}

int ObFunctionTableOp::inner_close()
{
  int ret = OB_SUCCESS;
  close_csv();
  node_idx_ = 0;
  row_count_ = 0;
  col_count_ = 0;
  value_table_ = NULL;
  return ret;
}

void ObFunctionTableOp::destroy()
{
  close_csv();
  ObOperator::destroy();
}


int ObFunctionTableOp::get_current_result(ObObj &result)
{
  int ret = OB_SUCCESS;
  void *data = NULL;
  CK (already_calc_);
  if (node_idx_ < 0 || node_idx_ >= row_count_ * col_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get current result in table function",
              K(node_idx_), K(row_count_), K(col_count_));
  }
  do {
    CK (node_idx_ >= 0);
    if (OB_SUCC(ret) && node_idx_ >= row_count_) {
      ret = OB_ITER_END;
    }
    CK (OB_NOT_NULL(value_table_));
    OX (data = value_table_->get_data());
    CK (OB_NOT_NULL(data));
    OX (result = (static_cast<ObObj*>(data))[node_idx_++]);
  } while (OB_SUCC(ret) && result.get_meta().get_type() == ObMaxType);
  return ret;
}

int ObFunctionTableOp::inner_get_next_row()
{
  return (this->*next_row_func_)();
}

int ObFunctionTableOp::inner_get_next_row_udf()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = nullptr;
  clear_evaluated_flag();
  if (OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(MY_SPEC.value_expr_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (ObExtendType != MY_SPEC.value_expr_->datum_meta_.type_) {
    ret = OB_ERR_UNEXPECTED;
  } else if (FALSE_IT(plan_ctx->set_autoinc_id_tmp(0))) {
  } else if (OB_FAIL(ctx_.check_status())) {
  } else {
    ObDatum *value = nullptr;
    if (!already_calc_) {
      if (OB_FAIL(MY_SPEC.value_expr_->eval(eval_ctx_, value))) {
      } else if (value->is_null()) {
        //do nothing
      } else if (OB_ISNULL(value_table_ 
                 = reinterpret_cast<pl::ObPLCollection*>(value->get_ext()))) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        row_count_ = value_table_->is_inited() ? value_table_->get_count() : 0;
        col_count_ = value_table_->get_column_count();
      }
      already_calc_ = true;
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(node_idx_ >= row_count_)) {
      ret = OB_ITER_END;
    }
    CK (MY_SPEC.column_exprs_.count() >= col_count_);
    ObObj obj_stack[col_count_];
    if (OB_SUCC(ret)) {
      if (nullptr != value_table_ 
          && ObExtendType == value_table_->get_element_type().get_obj_type()) {
        pl::ObPLComposite *composite = NULL;
        pl::ObPLRecord *record = NULL;
        ObObj record_obj;
        OZ (get_current_result(record_obj));
        if (OB_FAIL(ret)) {
        } else if (ObUserDefinedSQLType == record_obj.get_type()) {
          obj_stack[0] = record_obj;   
        } else if (record_obj.is_pl_extend()) {
          CK (OB_NOT_NULL(composite = reinterpret_cast<pl::ObPLComposite*>(record_obj.get_ext())));
          if (OB_SUCC(ret)) {
            if (composite->is_record()) {
              OX (record = static_cast<pl::ObPLRecord*>(composite));
              CK (record->get_count() == col_count_);
              for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
                OZ (record->get_element(i, obj_stack[i]));
              }
            } else if (composite->is_collection()) {
              OX (obj_stack[0] = record_obj);
            } else {
              ret = OB_ERR_UNEXPECTED;
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected here", K(ret), K(record_obj), K(record_obj.meta_));
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
          if (OB_FAIL(get_current_result(obj_stack[i]))) {
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
        if (obj_stack[i].is_null()) {
          MY_SPEC.column_exprs_.at(i)->locate_datum_for_write(eval_ctx_).set_null();
        } else {
          const ObObjDatumMapType &datum_map = MY_SPEC.column_exprs_.at(i)->obj_datum_map_;
          ObExpr * const &expr = MY_SPEC.column_exprs_.at(i);
          ObDatum &datum = expr->locate_datum_for_write(eval_ctx_);
          if (OB_FAIL(datum.from_obj(obj_stack[i], datum_map))) {
          } else if (is_lob_storage(obj_stack[i].get_type()) &&
                     OB_FAIL(ob_adjust_lob_datum(get_exec_ctx(), obj_stack[i],
                                                 expr->obj_meta_, datum_map,
                                                 get_exec_ctx().get_allocator(), datum))) {
          }
        }
        if (OB_SUCC(ret)) {
          MY_SPEC.column_exprs_.at(i)->set_evaluated_projected(eval_ctx_);
        }
      }
    }
  }
  return ret;
}

int ObFunctionTableOp::inner_get_next_row_sys_func()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = nullptr;
  ObDatum *value = nullptr;
  clear_evaluated_flag();
  if (OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ctx_.check_status())) {
  } else if (OB_FAIL(MY_SPEC.value_expr_->eval(eval_ctx_, value))) {
    if (OB_ITER_END != ret) {
    }
  } else {
    MY_SPEC.column_exprs_.at(0)->locate_datum_for_write(eval_ctx_).set_datum(*value);
    MY_SPEC.column_exprs_.at(0)->set_evaluated_projected(eval_ctx_);
  }
  return ret;
}


} // end namespace sql
} // end namespace oceanbase
