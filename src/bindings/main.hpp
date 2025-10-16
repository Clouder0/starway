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
struct ClientFlushArgs;
struct ClientFlushFuture;

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
  ClientSendFuture *send_future;
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

  void set_result();
  void set_exception(ucs_status_t result);
  // no result for send future

  Client *client_; // Client should outlives ClientSendFuture
  void *req_;
  nb::object done_callback_; // Callable[[ClientSendFuture], None]
  nb::object fail_callback_; // Callable[[ClientSendFuture], None]
};
struct ClientRecvArgs {
  ClientRecvFuture *recv_future;
  uint64_t tag;
  uint64_t tag_mask;
  std::byte *buf_ptr;
  size_t buf_size;
};
struct ClientRecvFuture {
  ClientRecvFuture(Client *client, auto &&done_callback, auto &&fail_callback)
    requires UniRef<decltype(done_callback), nb::object> &&
             UniRef<decltype(fail_callback), nb::object>;
  ClientRecvFuture(ClientRecvFuture const &) = delete;
  auto operator=(ClientRecvFuture const &) -> ClientRecvFuture & = delete;
  ClientRecvFuture(ClientRecvFuture &&) = delete;
  auto operator=(ClientRecvFuture &&) -> ClientRecvFuture & = delete;

  void set_result(uint64_t sender_tag, size_t length);
  void set_exception(ucs_status_t result);
  // no result for send future
  Client *client_; // Client should outlives ClientSendFuture
  void *req_;
  nb::object done_callback_; // Callable[[ClientSendFuture], None]
  nb::object fail_callback_; // Callable[[ClientSendFuture], None]
};

struct ClientPerfArgs {
  size_t msg_size;
};

struct ClientFlushFuture {
  ClientFlushFuture(Client *client, auto &&done_callback, auto &&fail_callback)
    requires UniRef<decltype(done_callback), nb::object> &&
             UniRef<decltype(fail_callback), nb::object>;
  ClientFlushFuture(ClientFlushFuture const &) = delete;
  auto operator=(ClientFlushFuture const &) -> ClientFlushFuture & = delete;
  ClientFlushFuture(ClientFlushFuture &&) = delete;
  auto operator=(ClientFlushFuture &&) -> ClientFlushFuture & = delete;

  void set_result();
  void set_exception(ucs_status_t result);

  Client *client_;
  void *req_;
  nb::object done_callback_; // Callable[[ClientFlushFuture], None]
  nb::object fail_callback_; // Callable[[ClientFlushFuture], None]
};

struct ClientFlushArgs {
  ClientFlushFuture *flush_future;
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

  void send(nb::ndarray<uint8_t, nb::ndim<1>, nb::device::cpu> const &buffer,
            uint64_t tag, nb::object done_callback, nb::object fail_callback);
  // Callable[[ClientSendFuture], None]

  void recv(nb::ndarray<uint8_t, nb::ndim<1>, nb::device::cpu> &buffer,
            uint64_t tag, uint64_t tag_mask, nb::object done_callback,
            nb::object fail_callback);
  // Callable[[ClientRecvFuture], None]

  void flush(nb::object done_callback, nb::object fail_callback);
  // Callable[[ClientFlushFuture], None]

  double evaluate_perf(size_t msg_size);
  void start_working(std::string addr, uint64_t port);
  void cancel_pending_reqs();

  ucp_context_h ctx_;
  std::thread working_thread_;
  std::atomic<uint8_t> status_{
      0}; // 0: void 1: initialized 2: running 3: closed
  Channel<ClientSendArgs> send_args_;
  Channel<ClientRecvArgs> recv_args_;
  Channel<ClientFlushArgs> flush_args_;
  std::atomic<uint8_t> perf_status_{0}; // 0: nothing 1: written
  ClientPerfArgs perf_args_;
  double perf_result_;

  nb::object connect_callback_;
  nb::object close_callback_;
  std::set<ClientSendFuture *> send_futures_;
  std::set<ClientRecvFuture *> recv_futures_;
  std::set<ClientFlushFuture *> flush_futures_;
};

struct ServerSendFuture {
  ServerSendFuture(Server *server, auto &&done_callback, auto &&fail_callback)
    requires UniRef<decltype(done_callback), nb::object> &&
             UniRef<decltype(fail_callback), nb::object>;
  ;
  ServerSendFuture(ServerSendFuture const &) = delete;
  auto operator=(ServerSendFuture const &) -> ServerSendFuture & = delete;
  ServerSendFuture(ServerSendFuture &&) = default;
  auto operator=(ServerSendFuture &&) -> ServerSendFuture & = default;

  void set_result();
  void set_exception(ucs_status_t result);

  Server *server_; // Server should outlives ServerSendFuture
  void *req_;
  nb::object done_callback_; // Callable[[ServerSendFuture], None]
  nb::object fail_callback_; // Callable[[ServerSendFuture], None]
};
struct ServerSendArgs {
  ServerSendFuture *send_future;
  ucp_ep_h ep;
  uint64_t tag;
  std::byte *buf_ptr;
  size_t buf_size;
};

struct ServerRecvFuture {
  ServerRecvFuture(Server *server, auto &&done_callback, auto &&fail_callback)
    requires UniRef<decltype(done_callback), nb::object> &&
             UniRef<decltype(fail_callback), nb::object>;
  ServerRecvFuture(ServerRecvFuture const &) = delete;
  auto operator=(ServerRecvFuture const &) -> ServerRecvFuture & = delete;
  ServerRecvFuture(ServerRecvFuture &&) = default;
  auto operator=(ServerRecvFuture &&) -> ServerRecvFuture & = default;

  void set_result(uint64_t sender_tag, size_t length);
  void set_exception(ucs_status_t result);

  Server *server_; // Server should outlives ServerRecvFuture
  void *req_;
  nb::object done_callback_; // Callable[[ServerRecvFuture], None]
  nb::object fail_callback_; // Callable[[ServerRecvFuture], None]
};
struct ServerRecvArgs {
  ServerRecvFuture *recv_future;
  uint64_t tag;
  uint64_t tag_mask;
  std::byte *buf_ptr;
  size_t buf_size;
};

struct ServerFlushFuture {
  ServerFlushFuture(Server *server, auto &&done_callback, auto &&fail_callback)
    requires UniRef<decltype(done_callback), nb::object> &&
             UniRef<decltype(fail_callback), nb::object>;
  ServerFlushFuture(ServerFlushFuture const &) = delete;
  auto operator=(ServerFlushFuture const &) -> ServerFlushFuture & = delete;
  ServerFlushFuture(ServerFlushFuture &&) = default;
  auto operator=(ServerFlushFuture &&) -> ServerFlushFuture & = default;
  void set_result();
  void set_exception(ucs_status_t result);

  Server *server_; // Server should outlives ServerFlushFuture
  void *req_;
  nb::object done_callback_; // Callable[[ServerFlushFuture], None]
  nb::object fail_callback_; // Callable[[ServerFlushFuture], None]
};

struct ServerFlushArgs {
  ServerFlushFuture *flush_future;
};

struct ServerFlushEpFuture {
  ServerFlushEpFuture(Server *server, ucp_ep_h ep, auto &&done_callback,
                      auto &&fail_callback)
    requires UniRef<decltype(done_callback), nb::object> &&
             UniRef<decltype(fail_callback), nb::object>;
  ServerFlushEpFuture(ServerFlushEpFuture const &) = delete;
  auto operator=(ServerFlushEpFuture const &) -> ServerFlushEpFuture & = delete;
  ServerFlushEpFuture(ServerFlushEpFuture &&) = default;
  auto operator=(ServerFlushEpFuture &&) -> ServerFlushEpFuture & = default;
  void set_result();
  void set_exception(ucs_status_t result);

  Server *server_;
  ucp_ep_h ep_;
  void *req_;
  nb::object done_callback_; // Callable[[ServerFlushEpFuture], None]
  nb::object fail_callback_; // Callable[[ServerFlushEpFuture], None]
};

struct ServerFlushEpArgs {
  ServerFlushEpFuture *flush_future;
  ucp_ep_h ep;
};

struct ServerPerfArgs {
  ucp_ep_h ep;
  size_t msg_size;
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

  void send(ServerEndpoint const &client_ep,
            nb::ndarray<uint8_t, nb::ndim<1>, nb::device::cpu> const &buffer,
            uint64_t tag, nb::object done_callback, nb::object fail_callback);
  // Callable[[ServerSendFuture], None]

  void recv(nb::ndarray<uint8_t, nb::ndim<1>, nb::device::cpu> &buffer,
            uint64_t tag, uint64_t tag_mask, nb::object done_callback,
            nb::object fail_callback);
  // Callable[[ServerRecvFuture], None]

  void flush(nb::object done_callback, nb::object fail_callback);
  // Callable[[ServerFlushFuture], None]

  void flush_ep(ServerEndpoint const &client_ep, nb::object done_callback,
                nb::object fail_callback);
  // Callable[[ServerFlushEpFuture], None]

  auto list_clients() const -> std::set<ServerEndpoint> const &;
  double evaluate_perf(ServerEndpoint const &client_ep, size_t msg_size);

  void start_working(std::string addr, uint16_t port);
  void cancel_pending_reqs();

  ucp_context_h ctx_;
  std::thread working_thread_;
  std::atomic<uint8_t> status_{
      0}; // 0: void 1: initialized 2: running 3: to close  4: closed
  Channel<ServerSendArgs> send_args_;
  Channel<ServerRecvArgs> recv_args_;
  Channel<ServerFlushArgs> flush_args_;
  Channel<ServerFlushEpArgs> flush_ep_args_;
  std::atomic<uint8_t> perf_status_{0}; // 0: nothing 1: written
  ServerPerfArgs perf_args_;
  double perf_result_;
  nb::object close_callback_;
  nb::object accept_callback_;
  std::set<ServerEndpoint> eps_;
  std::set<ServerSendFuture *> send_futures_;
  std::set<ServerRecvFuture *> recv_futures_;
  std::set<ServerFlushFuture *> flush_futures_;
  std::set<ServerFlushEpFuture *> flush_ep_futures_;
};
