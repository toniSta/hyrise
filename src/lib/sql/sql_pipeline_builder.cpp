#include "sql_pipeline_builder.hpp"

#include "global.hpp"
#if HYRISE_JIT_SUPPORT
#include "logical_query_plan/jit_aware_lqp_translator.hpp"
#endif

namespace opossum {

SQLPipelineBuilder::SQLPipelineBuilder(const std::string& sql) : _sql(sql) {}

SQLPipelineBuilder& SQLPipelineBuilder::with_mvcc(const UseMvcc use_mvcc) {
  _use_mvcc = use_mvcc;
  return *this;
}

SQLPipelineBuilder& SQLPipelineBuilder::with_lqp_translator(const std::shared_ptr<LQPTranslator>& lqp_translator) {
  _lqp_translator = lqp_translator;
  return *this;
}

SQLPipelineBuilder& SQLPipelineBuilder::with_optimizer(const std::shared_ptr<Optimizer>& optimizer) {
  _optimizer = optimizer;
  return *this;
}

SQLPipelineBuilder& SQLPipelineBuilder::with_prepared_statement_cache(
    const std::shared_ptr<PreparedStatementCache>& prepared_statements) {
  _prepared_statements = prepared_statements;
  return *this;
}

SQLPipelineBuilder& SQLPipelineBuilder::with_transaction_context(
    const std::shared_ptr<TransactionContext>& transaction_context) {
  _transaction_context = transaction_context;
  _use_mvcc = UseMvcc::Yes;

  return *this;
}

SQLPipelineBuilder& SQLPipelineBuilder::disable_mvcc() { return with_mvcc(UseMvcc::No); }

SQLPipelineBuilder& SQLPipelineBuilder::dont_cleanup_temporaries() {
  _cleanup_temporaries = CleanupTemporaries::No;
  return *this;
}

std::shared_ptr<LQPTranslator> get_lqp_translator(std::shared_ptr<LQPTranslator> lqp_translator) {
  if (lqp_translator) return lqp_translator;
#if HYRISE_JIT_SUPPORT
  if (Global::get().jit) {
    return std::make_shared<JitAwareLQPTranslator>();
  }
#endif
  return std::make_shared<LQPTranslator>();
}

SQLPipeline SQLPipelineBuilder::create_pipeline() const {
  auto lqp_translator = get_lqp_translator(_lqp_translator);
  auto optimizer = _optimizer ? _optimizer : Optimizer::create_default_optimizer();

  return {_sql, _transaction_context, _use_mvcc, lqp_translator, optimizer, _prepared_statements, _cleanup_temporaries};
}

SQLPipelineStatement SQLPipelineBuilder::create_pipeline_statement(
    std::shared_ptr<hsql::SQLParserResult> parsed_sql) const {
  auto lqp_translator = get_lqp_translator(_lqp_translator);
  auto optimizer = _optimizer ? _optimizer : Optimizer::create_default_optimizer();

  return {_sql,      std::move(parsed_sql), _use_mvcc,           _transaction_context, lqp_translator,
          optimizer, _prepared_statements,  _cleanup_temporaries};
}

}  // namespace opossum
