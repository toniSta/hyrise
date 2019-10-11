#include "base_test.hpp"
#include "gtest/gtest.h"

#include "mock_socket.hpp"

#include "server/postgres_protocol_handler.hpp"
#include "server/response_builder.hpp"

namespace opossum {

class ResponseBuilderTest : public BaseTest {
 protected:
  void SetUp() override {
    _test_table = load_table("resources/test_data/tbl/all_data_types_sorted.tbl", 2);
    Hyrise::get().storage_manager.add_table("_test_table", _test_table);

    _mocked_socket = std::make_shared<MockSocket>();
    _protocol_handler =
        std::make_shared<PostgresProtocolHandler<boost::asio::posix::stream_descriptor>>(_mocked_socket->get_socket());
  }

  uint32_t _get_message_length(std::string::const_iterator start) {
    uint32_t network_value = 0;
    std::copy_n(start, sizeof(uint32_t), reinterpret_cast<char*>(&network_value));
    return ntohl(network_value);
  }

  uint16_t _get_small_int(std::string::const_iterator start) {
    uint16_t network_value = 0;
    std::copy_n(start, sizeof(uint16_t), reinterpret_cast<char*>(&network_value));
    return ntohs(network_value);
  }

  std::shared_ptr<Table> _test_table;
  std::shared_ptr<MockSocket> _mocked_socket;
  std::shared_ptr<PostgresProtocolHandler<boost::asio::posix::stream_descriptor>> _protocol_handler;
};

TEST_F(ResponseBuilderTest, RowDescription) {
  ResponseBuilder::build_and_send_row_description(_test_table, _protocol_handler);
  _protocol_handler->force_flush();
  const std::string file_content = _mocked_socket->read();

  EXPECT_EQ(_get_message_length(file_content.cbegin() + 1), file_content.size() - 1);
  EXPECT_EQ(_get_small_int(file_content.cbegin() + 5), _test_table->column_count());
  for (ColumnID column_id{0}; column_id < _test_table->column_count(); column_id++) {
    EXPECT_NE(file_content.find(_test_table->column_name(column_id)), std::string::npos);
  }
}

TEST_F(ResponseBuilderTest, QueryResponse) {
  ResponseBuilder::build_and_send_query_response(_test_table, _protocol_handler);
  _protocol_handler->force_flush();
  const std::string file_content = _mocked_socket->read();

  // Count number of occurences with message type 'D'
  EXPECT_EQ(std::count(file_content.begin(), file_content.end(), 'D'), _test_table->row_count());
}

TEST_F(ResponseBuilderTest, CommandCompleteMessage) {
  EXPECT_EQ(ResponseBuilder::build_command_complete_message(OperatorType::Insert, 1), "INSERT 0 1");
  EXPECT_EQ(ResponseBuilder::build_command_complete_message(OperatorType::Update, 1), "UPDATE -1");
  EXPECT_EQ(ResponseBuilder::build_command_complete_message(OperatorType::Delete, 1), "DELETE -1");
  EXPECT_EQ(ResponseBuilder::build_command_complete_message(OperatorType::Projection, 1), "SELECT 1");
}

}  // namespace opossum
