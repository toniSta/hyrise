#pragma once

#include <boost/asio.hpp>

#include "ring_buffer_iterator.hpp"
#include "types.hpp"

using Socket = boost::asio::ip::tcp::socket;

namespace opossum {

// Dedicated buffer for write operations. The ring buffer gets extended by methods for writing different data types.
template <typename SocketType>
class WriteBuffer {
 public:
  explicit WriteBuffer(const std::shared_ptr<SocketType> socket) : _socket(socket) {}

  // Problem: full and empty might be same state, so head == tail
  // Solution: Full state is tail + 1 == head
  //           Empty state is head == tail
  size_t size() const;

  // See comment above
  size_t maximum_capacity() const;

  // Check if buffer is full
  bool full() const;

  // Put numerical values into the buffer. Values will be converted into the correct byte order if necessary.
  template <typename T>
  void put_value(const T network_value) {
    _flush_if_necessary(sizeof(T));
    T converted_value;
    if constexpr (std::is_same_v<T, uint16_t> || std::is_same_v<T, int16_t>) {
      converted_value = htons(network_value);
    } else if constexpr (std::is_same_v<T, uint32_t> || std::is_same_v<T, int32_t>) {
      converted_value = htonl(network_value);
    } else {
      converted_value = network_value;
    }
    std::copy_n(reinterpret_cast<const char*>(&converted_value), sizeof(T), _current_position);
    std::advance(_current_position, sizeof(T));
  }

  // Put string into the buffer. If the string is longer than the buffer itself the buffer will flush automatically.
  void put_string(const std::string& value,
                  const IgnoreNullTerminator ignore_null_terminator = IgnoreNullTerminator::No);

  // Flush whole buffer, e. g. after a finished request
  void flush(const size_t bytes_required = 0);

 private:
  void _flush_if_necessary(const size_t bytes_required);

  std::array<char, BUFFER_SIZE> _data;
  RingBufferIterator _start_position{_data};
  RingBufferIterator _current_position{_data};
  std::shared_ptr<SocketType> _socket;
};

}  // namespace opossum
