#include <pqxx/pqxx>

#include <future>
#include <thread>

#include "base_test.hpp"
#include "hyrise.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "sql/sql_plan_cache.hpp"

#include "server/server.hpp"

namespace opossum {

class ServerTestRunner : public BaseTest {
 protected:
  void SetUp() override {
    Hyrise::reset();

    _table_a = load_table("resources/test_data/tbl/int_float.tbl", 2);
    Hyrise::get().storage_manager.add_table("table_a", _table_a);

    // Set scheduler so that the server can execute the tasks on separate threads.
    Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

    auto server_runner = [](Server& server) { server.run(); };

    _server_thread = std::make_unique<std::thread>(server_runner, std::ref(*_server));

    // Get randomly assigned port number for client connection
    _connection_string = "hostaddr=127.0.0.1 port=" + std::to_string(_server->get_port());
  }

  void TearDown() override {
    _server->shutdown();
    _server_thread->join();
  }

  std::unique_ptr<Server> _server = std::make_unique<Server>(0, false);  // Port 0 to select random open port
  std::unique_ptr<std::thread> _server_thread;
  std::string _connection_string;

  std::shared_ptr<Table> _table_a;
};

TEST_F(ServerTestRunner, TestSimpleSelect) {
  pqxx::connection connection{_connection_string};

  // We use nontransactions because the regular transactions use SQL that we don't support. Nontransactions auto commit.
  pqxx::nontransaction transaction{connection};

  const auto result = transaction.exec("SELECT * FROM table_a;");
  EXPECT_EQ(result.size(), _table_a->row_count());
}

TEST_F(ServerTestRunner, TestInvalidStatement) {
  pqxx::connection connection{_connection_string};

  // We use nontransactions because the regular transactions use SQL that we don't support. Nontransactions auto commit.
  pqxx::nontransaction transaction{connection};

  // Ill-formed SQL statement
  EXPECT_THROW(transaction.exec("SELECT * FROM;"), pqxx::sql_error);

  // Well-formed but table does not exist
  EXPECT_THROW(transaction.exec("SELECT * FROM non_existent;"), pqxx::sql_error);

  // Check whether server is still running and connection established
  const auto result = transaction.exec("SELECT * FROM table_a;");
  EXPECT_EQ(result.size(), _table_a->row_count());
}

TEST_F(ServerTestRunner, TestMultipleConnections) {
  pqxx::connection connection1{_connection_string};
  pqxx::connection connection2{_connection_string};
  pqxx::connection connection3{_connection_string};

  pqxx::nontransaction transaction1{connection1};
  pqxx::nontransaction transaction2{connection2};
  pqxx::nontransaction transaction3{connection3};

  const std::string sql = "SELECT * FROM table_a;";
  const auto expected_num_rows = _table_a->row_count();

  const auto result1 = transaction1.exec(sql);
  EXPECT_EQ(result1.size(), expected_num_rows);

  const auto result2 = transaction2.exec(sql);
  EXPECT_EQ(result2.size(), expected_num_rows);

  const auto result3 = transaction3.exec(sql);
  EXPECT_EQ(result3.size(), expected_num_rows);
}

TEST_F(ServerTestRunner, TestSimpleInsertSelect) {
  pqxx::connection connection{_connection_string};
  pqxx::nontransaction transaction{connection};

  const auto expected_num_rows = _table_a->row_count() + 1;
  transaction.exec("INSERT INTO table_a VALUES (1, 1.0);");
  const auto result = transaction.exec("SELECT * FROM table_a;");
  EXPECT_EQ(result.size(), expected_num_rows);
}

TEST_F(ServerTestRunner, TestPreparedStatement) {
  pqxx::connection connection{_connection_string};
  pqxx::nontransaction transaction{connection};

  const std::string prepared_name = "statement1";
  connection.prepare(prepared_name, "SELECT * FROM table_a WHERE a > ?");

  const auto param = 1234u;
  const auto result1 = transaction.exec_prepared(prepared_name, param);
  EXPECT_EQ(result1.size(), 1u);

  transaction.exec("INSERT INTO table_a VALUES (55555, 1.0);");
  const auto result2 = transaction.exec_prepared(prepared_name, param);
  EXPECT_EQ(result2.size(), 2u);
}

TEST_F(ServerTestRunner, TestUnnamedPreparedStatement) {
  pqxx::connection connection{_connection_string};
  pqxx::nontransaction transaction{connection};

  const std::string prepared_name = "";
  connection.prepare(prepared_name, "SELECT * FROM table_a WHERE a > ?");

  const auto param = 1234u;
  const auto result1 = transaction.exec_prepared(prepared_name, param);
  EXPECT_EQ(result1.size(), 1u);

  connection.prepare(prepared_name, "SELECT * FROM table_a WHERE a <= ?");

  const auto result2 = transaction.exec_prepared(prepared_name, param);
  EXPECT_EQ(result2.size(), 2u);
}

TEST_F(ServerTestRunner, TestInvalidPreparedStatement) {
  pqxx::connection connection{_connection_string};
  pqxx::nontransaction transaction{connection};

  const std::string prepared_name = "";
  const auto param = 1234u;
  // Ill-formed prepared statement
  connection.prepare(prepared_name, "SELECT * FROM WHERE a > ?");
  EXPECT_THROW(transaction.exec_prepared(prepared_name, param), pqxx::sql_error);

  // // Well-formed but table does not exist
  connection.prepare(prepared_name, "SELECT * FROM non_existent WHERE a > ?");
  EXPECT_THROW(transaction.exec_prepared(prepared_name, param), pqxx::sql_error);

  // Wrong number of parameters
  connection.prepare(prepared_name, "SELECT * FROM table_a WHERE a > ? and a > ?");
  EXPECT_ANY_THROW(transaction.exec_prepared(prepared_name, param));

  // Check whether server is still running and connection established
  connection.prepare(prepared_name, "SELECT * FROM table_a WHERE a > ?");
  const auto result = transaction.exec_prepared(prepared_name, param);
  EXPECT_EQ(result.size(), 1u);
}

TEST_F(ServerTestRunner, TestParallelConnections) {
  // This test is by no means perfect, as it can show flaky behaviour. But it is rather hard to get reliable tests with
  // multiple concurrent connections to detect a randomly (but often) occurring bug. This test will/can only fail if a
  // bug is present but it should not fail if no bug is present. It just sends 100 parallel connections and if that
  // fails, there probably is a bug.
  const std::string sql = "SELECT * FROM table_a;";
  const auto expected_num_rows = _table_a->row_count();

  const auto connection_run = [&]() {
    pqxx::connection connection{_connection_string};
    pqxx::nontransaction transaction{connection};
    const auto result = transaction.exec(sql);
    EXPECT_EQ(result.size(), expected_num_rows);
  };

  const auto num_threads = 100u;
  std::vector<std::future<void>> thread_futures;
  thread_futures.reserve(num_threads);

  for (auto thread_num = 0u; thread_num < num_threads; ++thread_num) {
    // We want a future to the thread running, so we can kill it after a future.wait(timeout) or the test would freeze
    thread_futures.emplace_back(std::async(std::launch::async, connection_run));
  }

  for (auto& thread_fut : thread_futures) {
    // We give this a lot of time, not because we need that long for 100 threads to finish, but because sanitizers and
    // other tools like valgrind sometimes bring a high overhead that exceeds 10 seconds.
    if (thread_fut.wait_for(std::chrono::seconds(150)) == std::future_status::timeout) {
      ASSERT_TRUE(false) << "At least one thread got stuck and did not commit.";
    }
  }
}

}  // namespace opossum
