#pragma once
#include "chan.hpp"
#include <array>
#include <atomic>
#include <cassert>
#include <compare>
#include <nanobind/nanobind.h>
#include <nanobind/ndarray.h>
#include <nanobind/stl/set.h>
#include <nanobind/stl/shared_ptr.h>
#include <nanobind/stl/tuple.h>
#include <nanobind/stl/vector.h>
#include <set>
#include <type_traits>
#include <ucp/api/ucp_def.h>

struct Client;
struct ClientSendArgs;
struct ClientSendFuture;
struct ClientRecvArgs;
struct ClientRecvFuture;

struct Server;
struct ServerEndpoint;
struct ServerSendArgs;
struct ServerSendFuture;
struct ServerRecvArgs;
struct ServerRecvFuture;

namespace nb = nanobind;
using namespace nb::literals;

template <class T, class U>
concept UniRef =
    (std::is_lvalue_reference_v<T> || std::is_rvalue_reference_v<T>) &&
    std::same_as<std::remove_cvref_t<T>, std::remove_cvref_t<U>>;

// Global Context
struct Context {
  Context();
  ~Context();
  // disable copy and move
  Context(Context const &) = delete;
  auto operator=(Context const &) -> Context & = delete;
  Context(Context &&) = delete;
  auto operator=(Context &&) -> Context & = delete;
  ucp_context_h context_;
};

struct ClientSendArgs {
  nb::object send_future;
  uint64_t tag;
  std::byte *buf_ptr;
  size_t buf_size;
};
struct ClientSendFuture {

  ClientSendFuture(Client *client, auto &&done_callback, auto &&fail_callback)
    requires UniRef<decltype(done_callback), nb::object> &&
             UniRef<decltype(fail_callback), nb::object>;
  ClientSendFuture(ClientSendFuture const &) = delete;
  auto operator=(ClientSendFuture const &) -> ClientSendFuture & = delete;
  ClientSendFuture(ClientSendFuture &&) = delete;
  auto operator=(ClientSendFuture &&) -> ClientSendFuture & = delete;

  void set_result(ucs_status_t result);
  [[nodiscard]] bool done() const;
  [[nodiscard]] const char *excpetion() const;
  // no result for send future

  Client *client_; // Client should outlives ClientSendFuture
  void *req_;
  nb::object done_callback_;       // Callable[[ClientSendFuture], None]
  nb::object fail_callback_;       // Callable[[ClientSendFuture], None]
  std::atomic<uint8_t> status_{0}; // 0: sending  1: send done
  ucs_status_t done_status_{UCS_OK};
};
struct ClientRecvArgs {
  nb::object recv_future;
  uint64_t tag;
  uint64_t tag_mask;
  std::byte *buf_ptr;
  size_t buf_size;
};
struct ClientRecvFuture {
  using ResultType = std::tuple<uint64_t, size_t>;
  ClientRecvFuture(Client *client, auto &&done_callback, auto &&fail_callback)
    requires UniRef<decltype(done_callback), nb::object> &&
             UniRef<decltype(fail_callback), nb::object>;
  ClientRecvFuture(ClientRecvFuture const &) = delete;
  auto operator=(ClientRecvFuture const &) -> ClientRecvFuture & = delete;
  ClientRecvFuture(ClientRecvFuture &&) = delete;
  auto operator=(ClientRecvFuture &&) -> ClientRecvFuture & = delete;

  void set_result_value(ResultType result);
  void set_result(ucs_status_t result);
  [[nodiscard]] bool done() const;
  [[nodiscard]] const char *excpetion() const;
  [[nodiscard]] ResultType result() const;

  // no result for send future
  Client *client_; // Client should outlives ClientSendFuture
  void *req_;
  nb::object done_callback_;       // Callable[[ClientSendFuture], None]
  nb::object fail_callback_;       // Callable[[ClientSendFuture], None]
  std::atomic<uint8_t> status_{0}; // 0: sending  1: send done
  ucs_status_t done_status_{UCS_OK};
  ResultType result_;
};

struct ClientConnectArgs {
  nb::object connect_callback;
};
struct ClientCloseArgs {
  nb::object close_callback;
};

struct Client {
  Client(Context &ctx);
  ~Client();
  // disable copy and move
  Client(Client const &) = delete;
  auto operator=(Client const &) -> Client & = delete;
  Client(Client &&) = delete;
  auto operator=(Client &&) -> Client & = delete;

  void connect(std::string addr, uint64_t port, nb::object connect_callback);
  // Callable[[], None]
  void close(nb::object close_callback);
  // Callable[[], None]

  nb::object
  send(nb::ndarray<uint8_t, nb::ndim<1>, nb::device::cpu> const &buffer,
       uint64_t tag, nb::object done_callback, nb::object fail_callback);
  // Callable[[ClientSendFuture], None]

  nb::object recv(nb::ndarray<uint8_t, nb::ndim<1>, nb::device::cpu> &buffer,
                  uint64_t tag, uint64_t tag_mask, nb::object done_callback,
                  nb::object fail_callback);
  // Callable[[ClientRecvFuture], None]

  //   double evaluate_perf(size_t msg_size);
  void start_working(std::string addr, uint64_t port);

  ucp_context_h ctx_;
  std::thread working_thread_;
  std::atomic<uint8_t> status_{
      0}; // 0: void 1: initialized 2: running 3: closed
  MultiChannel<ClientSendArgs, ClientRecvArgs, ClientConnectArgs,
               ClientCloseArgs>
      chan_;
  std::set<nb::object> send_futures_;
  std::set<nb::object> recv_futures_;
};

struct ServerSendArgs {
  nb::object send_future;
  ucp_ep_h ep;
  uint64_t tag;
  std::byte *buf_ptr;
  size_t buf_size;
};
struct ServerSendFuture {
  ServerSendFuture(Server *server, auto &&done_callback, auto &&fail_callback)
    requires UniRef<decltype(done_callback), nb::object> &&
             UniRef<decltype(fail_callback), nb::object>;
  ;
  ServerSendFuture(ServerSendFuture const &) = delete;
  auto operator=(ServerSendFuture const &) -> ServerSendFuture & = delete;
  ServerSendFuture(ServerSendFuture &&) = delete;
  auto operator=(ServerSendFuture &&) -> ServerSendFuture & = delete;

  void set_result(ucs_status_t result);
  [[nodiscard]] bool done() const;
  [[nodiscard]] const char *excpetion() const;

  Server *server_; // Server should outlives ServerSendFuture
  void *req_;
  nb::object done_callback_;       // Callable[[ServerSendFuture], None]
  nb::object fail_callback_;       // Callable[[ServerSendFuture], None]
  std::atomic<uint8_t> status_{0}; // 0: sending  1: send done
  ucs_status_t done_status_{UCS_OK};
};
struct ServerRecvArgs {
  nb::object recv_future;
  uint64_t tag;
  uint64_t tag_mask;
  std::byte *buf_ptr;
  size_t buf_size;
};
struct ServerRecvFuture {
  using ResultType = std::tuple<uint64_t, size_t>;
  ServerRecvFuture(Server *server, auto &&done_callback, auto &&fail_callback)
    requires UniRef<decltype(done_callback), nb::object> &&
             UniRef<decltype(fail_callback), nb::object>;
  ServerRecvFuture(ServerRecvFuture const &) = delete;
  auto operator=(ServerRecvFuture const &) -> ServerRecvFuture & = delete;
  ServerRecvFuture(ServerRecvFuture &&) = delete;
  auto operator=(ServerRecvFuture &&) -> ServerRecvFuture & = delete;

  void set_result_value(ResultType result);
  void set_result(ucs_status_t result);
  [[nodiscard]] bool done() const;
  [[nodiscard]] const char *excpetion() const;
  [[nodiscard]] ResultType result() const;

  Server *server_; // Server should outlives ServerRecvFuture
  void *req_;
  nb::object done_callback_;       // Callable[[ServerRecvFuture], None]
  nb::object fail_callback_;       // Callable[[ServerRecvFuture], None]
  std::atomic<uint8_t> status_{0}; // 0: sending  1: send done
  ucs_status_t done_status_{UCS_OK};
  ResultType result_;
};

struct ServerCloseArgs {
  nb::object close_callback;
};

struct ServerEndpoint {
  ucp_ep_h ep;
  char const *name;
  char const *local_addr;
  uint16_t local_port;
  char const *remote_addr;
  uint16_t remote_port;
  size_t num_transports;
  std::array<ucp_transport_entry_t, 8> transports;
  auto view_transports() const
      -> std::vector<std::tuple<char const *, char const *>>;
  std::strong_ordering operator<=>(ServerEndpoint const &rhs) const;
};

struct Server {
  Server(Context &ctx);
  ~Server();
  // disable copy and move
  Server(Server const &) = delete;
  auto operator=(Server const &) -> Server & = delete;
  Server(Server &&) = delete;
  auto operator=(Server &&) -> Server & = delete;

  void set_accept_callback(nb::object accept_callback);
  void listen(std::string addr, uint16_t port);
  void close(nb::object close_callback);

  nb::object
  send(ServerEndpoint const &client_ep,
       nb::ndarray<uint8_t, nb::ndim<1>, nb::device::cpu> const &buffer,
       uint64_t tag, nb::object done_callback, nb::object fail_callback);
  // Callable[[ServerSendFuture], None]

  nb::object recv(nb::ndarray<uint8_t, nb::ndim<1>, nb::device::cpu> &buffer,
                  uint64_t tag, uint64_t tag_mask, nb::object done_callback,
                  nb::object fail_callback);
  // Callable[[ServerRecvFuture], None]

  auto list_clients() const -> std::set<ServerEndpoint> const &;
  //   double evaluate_perf(ServerEndpoint const &client_ep, size_t msg_size);

  void start_working(std::string addr, uint16_t port);

  ucp_context_h ctx_;
  std::thread working_thread_;
  std::atomic<uint8_t> status_{
      0}; // 0: void 1: initialized 2: running 3: closed
  MultiChannel<ServerSendArgs, ServerRecvArgs, ServerCloseArgs> chan_;
  nb::object accept_callback_;
  std::set<ServerEndpoint> eps_;
  std::set<nb::object> send_futures_;
  std::set<nb::object> recv_futures_;
};
