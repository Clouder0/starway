#include <arpa/inet.h>
#include <array>
#include <atomic>
#include <cassert>
#include <memory>
#include <nanobind/nanobind.h>
#include <nanobind/ndarray.h>
#include <nanobind/stl/function.h>
#include <nanobind/stl/shared_ptr.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/string_view.h>
#include <nanobind/stl/tuple.h>
#include <nanobind/stl/vector.h>
#include <optional>
#include <set>
#include <thread>
#include <ucp/api/ucp.h>
#include <ucp/api/ucp_compat.h>
#include <ucs/type/status.h>
#include <ucs/type/thread_mode.h>
#include <utility>

namespace nb = nanobind;
using namespace nb::literals;

#ifdef NDEBUG
// --- RELEASE MODE ---
// In release mode (when NDEBUG is defined), this macro expands to nothing.
// The compiler will see an empty statement, and importantly, the arguments
// passed to the macro will NEVER be evaluated.
#define debug_print(...)                                                       \
  do {                                                                         \
  } while (0)

#else
// --- DEBUG MODE ---
// In debug mode, the macro expands to a std::println call to stderr.
// We use stderr for debug messages to separate them from normal program output
// (stdout). The __VA_ARGS__ preprocessor token forwards all arguments to
// std::println.
#include <format>
#include <iostream>
#define debug_print(...)                                                       \
  do {                                                                         \
    std::cout << std::format(__VA_ARGS__) << "\n";                             \
  } while (0)
#endif // NDEBUG

inline void ucp_check_status(ucs_status_t status, std::string_view msg) {
  if (status != UCS_OK) {
    throw std::runtime_error(
        "UCP error: " + std::string(ucs_status_string(status)) + " - " +
        std::string(msg));
  }
}

template <class T> struct Channel {
  Channel() = default;
  bool full() const { return status.load(std::memory_order_acquire) == 2; }
  bool try_consume(auto &&reader)
    requires requires(decltype(reader) r, std::optional<T> &&d) { r(d); }
  {
    if (status.load(std::memory_order_acquire) != 2) {
      return false;
    }
    reader(data);
    status.store(0, std::memory_order_release);
    return true;
  }
  void wait_emplace(auto &&writer)
    requires requires(decltype(writer) w, std::optional<T> &d) { w(d); }
  {
    // optimistic
    uint8_t expected{0};
    for (;;) {
      if (status.compare_exchange_weak(expected, 1,
                                       std::memory_order_acq_rel)) {
        writer(data);
        status.store(2, std::memory_order_release);
        return;
      }
      while (status.load(std::memory_order_acquire) != 0) {
        std::this_thread::yield();
      }
    }
  }
  std::atomic<uint8_t> status{0}; // 0 : spare, 1 : loading, 2 : load done
  std::optional<T> data{};
};

template <typename T, size_t Capacity = 512> struct SPSCQueue {
  SPSCQueue() = default;

  bool full() const {
    size_t tail = tail_.load(std::memory_order_relaxed);
    size_t next_tail = (tail + 1) % (Capacity + 1);
    return next_tail == head_.load(std::memory_order_acquire);
  }

  bool try_emplace(auto &&writer)
    requires requires(decltype(writer) w, T &ref) { w(ref); }
  {
    size_t tail = tail_.load(std::memory_order_relaxed);
    size_t next_tail = (tail + 1) % (Capacity + 1);
    if (next_tail == head_.load(std::memory_order_acquire)) {
      return false; // Full
    }
    writer(storage_[tail]);
    tail_.store(next_tail, std::memory_order_release);
    return true;
  }

  bool try_pop(auto &&reader)
    requires requires(decltype(reader) r, T &&ref) { r(ref); }
  {
    size_t head = head_.load(std::memory_order_relaxed);
    if (head == tail_.load(std::memory_order_acquire)) {
      return false; // Empty
    }
    reader(storage_[head]);
    head_.store((head + 1) % (Capacity + 1), std::memory_order_release);
    return true;
  }

private:
  std::array<T, Capacity + 1> storage_{};
  alignas(64) std::atomic<size_t> head_{0};
  alignas(64) std::atomic<size_t> tail_{0};
};

struct Client;
struct ClientSendFuture : std::enable_shared_from_this<ClientSendFuture> {
  ClientSendFuture(Client *client, void *req, nb::object done_callback,
                   nb::object fail_callback)
      : client_(client), req_(req), done_callback_(done_callback),
        fail_callback_(fail_callback) {}
  ClientSendFuture(ClientSendFuture const &) = delete;
  auto operator=(ClientSendFuture const &) -> ClientSendFuture & = delete;
  [[nodiscard]] auto done() const noexcept -> bool { return done_; }
  auto exception() {
    if (done_status_ == UCS_OK) {
      return "";
    }
    return ucs_status_string(done_status_);
  }
  void set_result(ucs_status_t status) {
    done_status_ = status;
    done_.store(true, std::memory_order_release);
    if (done_status_ == UCS_OK) {
      nb::handle obj = nb::find(this);
      debug_print("Done obj name {}, valid {}", nb::inst_name(obj).c_str(),
                  obj.is_valid());
      done_callback_(obj);
    } else {
      nb::handle obj = nb::find(this);
      assert(obj.is_valid());
      fail_callback_(obj);
    }
  }
  void wait() {
    while (!done_.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
  }
  auto client() const { return client_; }

  std::atomic<bool> done_{false};
  Client *client_;
  void *req_;
  ucs_status_t done_status_{UCS_OK};
  nb::object done_callback_, fail_callback_;
};

struct ClientFrame {};

struct ClientRecvFuture {
  ClientRecvFuture(Client *client, nb::object done_callback,
                   nb::object fail_callback)
      : done_callback_(done_callback), fail_callback_(fail_callback),
        client_(client) {}
  auto done() const { return done_.load(std::memory_order_acquire); }
  auto exception() const { return ucs_status_string(done_status_); }
  auto set_result(ucs_status_t status) {
    done_status_ = status;
    done_.store(true, std::memory_order_release);
    auto self = nb::find(this);
    assert(self.is_valid());
    if (status == UCS_OK) {
      done_callback_(self);
    } else {
      fail_callback_(self);
    }
  }
  auto info() const { return std::make_tuple(sender_tag_, length_); }
  void wait() {
    while (!done_.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
  }

  friend struct Client;
  std::atomic<bool> done_{false};
  ucs_status_t done_status_{UCS_OK};
  nb::object done_callback_;
  nb::object fail_callback_;
  void *req_;
  Client *client_;
  uint64_t sender_tag_{};
  size_t length_{};
};

struct ClientSendPack {
  nb::object send_future;
  uint64_t tag;
  size_t buf_size;
  uint8_t *buf_ptr;
};
struct ClientRecvPack {
  nb::object recv_future;
  uint64_t tag;
  uint64_t tag_mask;
  size_t buf_size;
  uint8_t *buf_ptr;
};

struct Client {
  Client(std::string_view addr, uint64_t port)
      : worker_thread_([this, addr, port]() { working_thread(addr, port); }) {
    nb::gil_scoped_release release;
  }
  ~Client() {
    nb::gil_scoped_release release;
    debug_print("Start to destroy Client.");
    closed_.store(true, std::memory_order_release);
    debug_print("Close sent.");
    // wait for it to close
    while (closed_.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    worker_thread_.join();
  }
  auto send(nb::ndarray<uint8_t, nb::ndim<1>, nb::device::cpu> &buffer,
            uint64_t tag, nb::object done_callback, nb::object fail_callback) {
    auto buf_ptr = buffer.data();
    auto buf_size = buffer.size();
    auto send_future_obj = nb::cast(
        new ClientSendFuture(this, nullptr, done_callback, fail_callback));
    {
      nb::gil_scoped_release release;
      send_q_.wait_emplace([&](auto &dst) {
        nb::gil_scoped_acquire acquire;
        dst.emplace(send_future_obj, tag, buf_size, buf_ptr);
      });
      return send_future_obj;
    }
  }
  auto recv(nb::ndarray<uint8_t, nb::ndim<1>, nb::device::cpu> &buffer,
            uint64_t tag, uint64_t tag_mask, nb::object done_callback,
            nb::object fail_callback) {
    auto buf_ptr = buffer.data();
    auto buf_size = buffer.size();
    auto recv_future = new ClientRecvFuture(this, done_callback, fail_callback);
    auto recv_future_obj = nb::cast(recv_future);
    {
      nb::gil_scoped_release release;
      recv_q_.wait_emplace([&](auto &dst) {
        dst.emplace(recv_future_obj, tag, tag_mask, buf_size, buf_ptr);
      });
      return recv_future_obj;
    }
  }
  void init_context() {
    ucp_params_t params{
        .field_mask = UCP_PARAM_FIELD_FEATURES,
        .features = UCP_FEATURE_TAG,
    };
    ucp_check_status(ucp_init(&params, NULL, &context_),
                     "Failed to init UCP context");
  }
  void init_worker() {
    ucp_worker_params_t worker_params{
        .field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE,
        .thread_mode = UCS_THREAD_MODE_SINGLE,
    };
    ucp_check_status(ucp_worker_create(context_, &worker_params, &worker_),
                     "Failed to create UCP worker");
  }
  void init_sock_ep(std::string_view addr, uint64_t port) {
    struct sockaddr_in connect_addr{
        .sin_family = AF_INET,
        .sin_port = htons(static_cast<uint16_t>(port)),
        .sin_addr = {inet_addr(addr.data())},
    };
    ucp_ep_params_t ep_params{
        .field_mask = UCP_EP_PARAM_FIELD_FLAGS | UCP_EP_PARAM_FIELD_SOCK_ADDR,
        .flags = UCP_EP_PARAMS_FLAGS_CLIENT_SERVER,
        .sockaddr =
            {
                .addr = reinterpret_cast<struct sockaddr *>(&connect_addr),
                .addrlen = sizeof(connect_addr),
            },
    };
    ucp_check_status(ucp_ep_create(worker_, &ep_params, &ep_),
                     "Failed to create UCP endpoint");
  }
  void working_thread(std::string_view addr, uint64_t port) {
    init_context();
    init_worker();
    init_sock_ep(addr, port);
    while (!closed_.load(std::memory_order_acquire)) {
      ucp_worker_progress(worker_);
      send_q_.try_consume([this](auto &&src) {
        nb::gil_scoped_acquire acquire;
        ClientSendPack const &pack = src.value();
        auto send_future = nb::inst_ptr<ClientSendFuture>(pack.send_future);
        ucp_request_param_t send_param{.op_attr_mask =
                                           UCP_OP_ATTR_FIELD_CALLBACK |
                                           UCP_OP_ATTR_FIELD_USER_DATA,
                                       .cb{.send = send_cb},
                                       .user_data = send_future};
        auto *req = ucp_tag_send_nbx(ep_, pack.buf_ptr, pack.buf_size, pack.tag,
                                     &send_param);
        if (UCS_PTR_STATUS(req) == UCS_OK) {
          send_future->set_result(UCS_OK);
        } else if (UCS_PTR_IS_ERR(req)) {
          send_future->set_result(UCS_PTR_STATUS(req));
        } else {
          send_future->req_ = req;
          sends_.insert(std::move(pack.send_future));
        }
        src.reset();
      });
      recv_q_.try_consume([this](auto &&src) {
        nb::gil_scoped_acquire acquire;
        ClientRecvPack const &pack = src.value();
        auto recv_future = nb::inst_ptr<ClientRecvFuture>(pack.recv_future);
        ucp_tag_recv_info_t tag_info{};
        ucp_request_param_t param{.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                                                  UCP_OP_ATTR_FIELD_USER_DATA |
                                                  UCP_OP_ATTR_FIELD_RECV_INFO,
                                  .cb{.recv = recv_cb},
                                  .user_data = recv_future,
                                  .recv_info{
                                      .tag_info = &tag_info,
                                  }};
        auto req = ucp_tag_recv_nbx(worker_, pack.buf_ptr, pack.buf_size,
                                    pack.tag, pack.tag_mask, &param);
        if (req == NULL) {
          recv_future->sender_tag_ = tag_info.sender_tag;
          recv_future->length_ = tag_info.length;
          recv_future->set_result(UCS_OK);
        } else if (UCS_PTR_IS_ERR(req)) {
          recv_future->set_result(UCS_PTR_STATUS(req));
        } else {
          recv_future->req_ = req;
          recv_reqs_.insert(std::move(pack.recv_future));
        }
        src.reset();
      });
    }
    debug_print("Start closing flush...");
    while (ucp_worker_progress(worker_) > 0) {
      debug_print("Client made some progress");
    }

    debug_print("Start cancelling all requests...");
    {
      nb::gil_scoped_acquire acquire;
      for (auto const &_req : sends_) {
        auto req = nb::inst_ptr<ClientSendFuture>(_req);
        if (ucp_request_check_status(req->req_) == UCS_INPROGRESS) {
          ucp_request_cancel(worker_, req->req_);
          debug_print("Some request cancelled.");
        }
      }
    }
    {
      nb::gil_scoped_acquire acquire;
      for (auto const &_req : recv_reqs_) {
        auto req = nb::inst_ptr<ClientRecvFuture>(_req);
        if (ucp_request_check_status(req->req_) == UCS_INPROGRESS) {
          ucp_request_cancel(worker_, req->req_);
          debug_print("Some request cancelled.");
        }
      }
    }
    while (ucp_worker_progress(worker_) > 0) {
      debug_print("Client made some progress");
    }

    ucp_request_param_t flush_params{};
    auto flush_status = ucp_worker_flush_nbx(worker_, &flush_params);
    if (UCS_PTR_STATUS(flush_status) == UCS_OK) {
      debug_print("Flush done!");
    } else if (UCS_PTR_IS_ERR(flush_status)) {
      debug_print("Flushed failed! {}",
                  ucs_status_string(UCS_PTR_STATUS(flush_status)));
    } else {
      ucs_status_t flush_req_status{};
      do {
        ucp_worker_progress(worker_);
        flush_req_status = ucp_request_check_status(flush_status);
        debug_print("Flush req status {}",
                    static_cast<int64_t>(flush_req_status));
      } while (flush_req_status == UCS_INPROGRESS);
      debug_print("Flush request done.");
      ucp_request_free(flush_status);
    }
    debug_print("Flush done. Start sending close...");

    debug_print("Start send close...");
    ucp_request_param_t close_param{.op_attr_mask = UCP_EP_PARAM_FIELD_FLAGS,
                                    .flags = UCP_EP_CLOSE_FLAG_FORCE};
    auto status = ucp_ep_close_nbx(ep_, &close_param);
    if (UCS_PTR_STATUS(status) == UCS_OK) {
      debug_print("Close done.");
    } else if (UCS_PTR_IS_ERR(status)) {
      debug_print("Close failed: {}",
                  ucs_status_string(UCS_PTR_STATUS(status)));
    } else {
      while (ucp_request_check_status(status) == UCS_INPROGRESS) {
        while (ucp_worker_progress(worker_) > 0) {
        }
      }
      debug_print("Close request done.");
      ucp_request_free(status);
    }

    ucp_worker_destroy(worker_);
    debug_print("Worker thread exit done.");
    ucp_cleanup(context_);
    debug_print("Cleanup done.");
    closed_.store(false, std::memory_order_release);
  }
  static void recv_cb(void *request, ucs_status_t status,
                      ucp_tag_recv_info_t const *info, void *args) {
    auto *recv_future = reinterpret_cast<ClientRecvFuture *>(args);
    {
      nb::gil_scoped_acquire acquire;
      auto obj = nb::find(recv_future);
      assert(obj.is_valid());
      recv_future->sender_tag_ = info->sender_tag;
      recv_future->length_ = info->length;
      assert(recv_future->req_ == request);
      recv_future->req_ = NULL;
      recv_future->set_result(status);
      recv_future->client_->recv_reqs_.erase(obj);
    }
    ucp_request_free(request);
  }
  static void send_cb(void *req, ucs_status_t status, void *user_data) {
    auto *future = reinterpret_cast<ClientSendFuture *>(user_data);
    {
      nb::gil_scoped_acquire acquire;
      nb::object obj = nb::find(future);
      assert(obj.is_valid());
      future->set_result(status);
      future->client()->sends_.erase(obj);
    }
    // free the request
    ucp_request_free(req);
  }

  std::jthread worker_thread_;
  ucp_context_h context_;
  ucp_worker_h worker_;
  ucp_ep_h ep_;

  std::atomic<bool> closed_{false};
  Channel<ClientSendPack> send_q_{};
  Channel<ClientRecvPack> recv_q_{};

  std::set<nb::object,
           decltype([](nb::object const &lhs, nb::object const &rhs) {
             return lhs.ptr() < rhs.ptr();
           })>
      sends_{};
  std::set<nb::object,
           decltype([](nb::object const &lhs, nb::object const &rhs) {
             return lhs.ptr() < rhs.ptr();
           })>
      recv_reqs_{};
};

// Server
struct Server;
struct ServerSendFuture {
  ServerSendFuture(Server *server, void *req, nb::object done_callback,
                   nb::object fail_callback)
      : server_(server), req_(req), done_callback_(done_callback),
        fail_callback_(fail_callback) {}
  ServerSendFuture(ServerSendFuture const &) = delete;
  auto operator=(ServerSendFuture const &) -> ServerSendFuture & = delete;
  [[nodiscard]] auto done() const noexcept -> bool { return done_; }
  auto exception() {
    if (done_status_ == UCS_OK) {
      return "";
    }
    return ucs_status_string(done_status_);
  }
  void set_result(ucs_status_t status) {
    done_status_ = status;
    done_.store(true, std::memory_order_release);
    if (done_status_ == UCS_OK) {
      nb::handle obj = nb::find(this);
      debug_print("Done obj name {}, valid {}", nb::inst_name(obj).c_str(),
                  obj.is_valid());
      done_callback_(obj);
    } else {
      nb::handle obj = nb::find(this);
      assert(obj.is_valid());
      fail_callback_(obj);
    }
  }
  void wait() {
    while (!done_.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
  }
  auto server() const { return server_; }

  std::atomic<bool> done_{false};
  Server *server_;
  void *req_;
  ucs_status_t done_status_{UCS_OK};
  nb::object done_callback_, fail_callback_;
};
struct ServerRecvFuture {
  ServerRecvFuture(Server *server, nb::object done_callback,
                   nb::object fail_callback)
      : done_callback_(done_callback), fail_callback_(fail_callback),
        server_(server) {}
  auto done() const { return done_.load(std::memory_order_acquire); }
  auto exception() const { return ucs_status_string(done_status_); }
  auto set_result(ucs_status_t status) {
    done_status_ = status;
    done_.store(true, std::memory_order_release);
    auto self = nb::find(this);
    assert(self.is_valid());
    if (status == UCS_OK) {
      done_callback_(self);
    } else {
      fail_callback_(self);
    }
  }
  auto info() const { return std::make_tuple(sender_tag_, length_); }
  void wait() {
    while (!done_.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
  }

  friend struct Server;
  std::atomic<bool> done_{false};
  ucs_status_t done_status_{UCS_OK};
  nb::object done_callback_;
  nb::object fail_callback_;
  void *req_;
  Server *server_;
  uint64_t sender_tag_{};
  size_t length_{};
};
struct ServerRecvPack {
  nb::object recv_future;
  uint64_t tag, tag_mask;
  size_t buf_size;
  uint8_t *buf_ptr;
};
struct ServerSendPack {
  nb::object send_future;
  uintptr_t dst_ep;
  uint64_t tag;
  size_t buf_size;
  uint8_t *buf_ptr;
};
struct Server {
  void init_context() {
    ucp_params_t params{
        .field_mask = UCP_PARAM_FIELD_FEATURES,
        .features = UCP_FEATURE_TAG,
    };
    ucp_check_status(ucp_init(&params, NULL, &context_),
                     "Failed to init UCP context");
  }
  void init_worker() {
    ucp_worker_params_t worker_params{
        .field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE,
        .thread_mode = UCS_THREAD_MODE_SINGLE,
    };
    ucp_check_status(ucp_worker_create(context_, &worker_params, &worker_),
                     "Failed to create UCP worker");
  }
  static void accept_cb(ucp_ep_h ep, void *arg) {
    auto *cur = reinterpret_cast<Server *>(arg);
    cur->connected_.insert(ep);
  }
  void init_listener(std::string_view addr, uint64_t port) {
    struct sockaddr_in listen_addr{
        .sin_family = AF_INET,
        .sin_port = htons(static_cast<uint16_t>(port)),
        .sin_addr = {inet_addr(addr.data())},
    };

    ucp_listener_params_t params{
        .field_mask = UCP_LISTENER_PARAM_FIELD_SOCK_ADDR |
                      UCP_LISTENER_PARAM_FIELD_ACCEPT_HANDLER,
        .sockaddr{
            .addr = reinterpret_cast<struct sockaddr *>(&listen_addr),
            .addrlen = sizeof(listen_addr),
        },
        .accept_handler{.cb = accept_cb, .arg = this}};
    ucp_check_status(ucp_listener_create(worker_, &params, &listener_),
                     "Failed to create listener.");
  }
  auto recv(nb::ndarray<uint8_t, nb::ndim<1>, nb::device::cpu> &buffer,
            uint64_t tag, uint64_t tag_mask, nb::object done_callback,
            nb::object fail_callback) {
    auto buf_ptr = buffer.data();
    auto buf_size = buffer.size();
    auto recv_future = new ServerRecvFuture(this, done_callback, fail_callback);
    auto recv_future_obj = nb::cast(recv_future);
    {
      nb::gil_scoped_release release;
      recv_q_.wait_emplace([&](auto &dst) {
        nb::gil_scoped_acquire acquire;
        dst.emplace(recv_future_obj, tag, tag_mask, buf_size, buf_ptr);
      });
      return recv_future_obj;
    }
  }
  auto list_clients() -> std::vector<uintptr_t> {
    std::vector<uintptr_t> clients;
    for (auto ep : connected_) {
      clients.push_back(reinterpret_cast<uintptr_t>(ep));
    }
    return clients;
  }
  auto send(uintptr_t client_ep,
            nb::ndarray<uint8_t, nb::ndim<1>, nb::device::cpu> &buffer,
            uint64_t tag, nb::object done_callback, nb::object fail_callback) {
    auto buf_ptr = buffer.data();
    auto buf_size = buffer.size();
    auto send_future_obj = nb::cast(
        new ServerSendFuture(this, nullptr, done_callback, fail_callback));
    {
      nb::gil_scoped_release release;
      send_q_.wait_emplace([&](auto &dst) {
        dst.emplace(send_future_obj, reinterpret_cast<uintptr_t>(client_ep),
                    tag, buf_size, buf_ptr);
      });
      return send_future_obj;
    }
  }
  Server(std::string_view addr, uint64_t port)
      : worker_thread_(
            [this, addr, port]() { this->wokring_thread(addr, port); }) {
    nb::gil_scoped_release release;
    // block until thread prepared
    while (closed_.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
  }
  ~Server() {
    nb::gil_scoped_release release;
    closed_.store(true, std::memory_order_release);
    debug_print("Send close signal.");
    while (closed_.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    worker_thread_.join();
    debug_print("Close done in main.");
  }
  static void send_cb(void *req, ucs_status_t status, void *user_data) {
    auto *future = reinterpret_cast<ServerSendFuture *>(user_data);
    {
      nb::gil_scoped_acquire acquire;
      nb::object obj = nb::find(future);
      assert(obj.is_valid());
      future->set_result(status);
      future->server()->send_reqs_.erase(obj);
    }
    // free the request
    ucp_request_free(req);
  }
  static void recv_cb(void *request, ucs_status_t status,
                      ucp_tag_recv_info_t const *info, void *args) {
    auto *recv_future = reinterpret_cast<ServerRecvFuture *>(args);
    {
      nb::gil_scoped_acquire acquire;
      auto obj = nb::find(recv_future);
      assert(obj.is_valid());
      recv_future->sender_tag_ = info->sender_tag;
      recv_future->length_ = info->length;
      assert(recv_future->req_ == request);
      recv_future->req_ = NULL;
      recv_future->set_result(status);
      recv_future->server_->recv_reqs_.erase(obj);
    }
    ucp_request_free(request);
  }

  void wokring_thread(std::string_view addr, uint64_t port) {
    init_context();
    init_worker();
    init_listener(addr, port);
    closed_.store(false, std::memory_order_release);
    while (!closed_.load(std::memory_order_acquire)) {
      ucp_worker_progress(worker_);
      recv_q_.try_consume([&](auto &&src) {
        nb::gil_scoped_acquire acquire;
        auto const &pack = src.value();
        auto recv_future = nb::inst_ptr<ServerRecvFuture>(pack.recv_future);
        ucp_tag_recv_info_t tag_info{};
        ucp_request_param_t param{.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                                                  UCP_OP_ATTR_FIELD_USER_DATA |
                                                  UCP_OP_ATTR_FIELD_RECV_INFO,
                                  .cb{.recv = recv_cb},
                                  .user_data = recv_future,
                                  .recv_info{
                                      .tag_info = &tag_info,
                                  }};
        auto status = ucp_tag_recv_nbx(worker_, pack.buf_ptr, pack.buf_size,
                                       pack.tag, pack.tag_mask, &param);
        if (status == NULL) {
          recv_future->sender_tag_ = tag_info.sender_tag;
          recv_future->length_ = tag_info.length;
          recv_future->set_result(UCS_OK);
        } else if (UCS_PTR_IS_ERR(status)) {
          recv_future->set_result(UCS_PTR_STATUS(status));
        } else {
          recv_future->req_ = status;
          recv_reqs_.insert(pack.recv_future);
        }
        src.reset();
      });

      send_q_.try_consume([&](auto &&src) {
        nb::gil_scoped_acquire acquire;
        ServerSendPack const &pack = src.value();
        auto send_future = nb::inst_ptr<ServerSendFuture>(pack.send_future);

        ucp_request_param_t send_param{.op_attr_mask =
                                           UCP_OP_ATTR_FIELD_CALLBACK |
                                           UCP_OP_ATTR_FIELD_USER_DATA,
                                       .cb{.send = send_cb},
                                       .user_data = send_future};
        auto *req = ucp_tag_send_nbx(reinterpret_cast<ucp_ep_h>(pack.dst_ep),
                                     pack.buf_ptr, pack.buf_size, pack.tag,
                                     &send_param);
        if (UCS_PTR_STATUS(req) == UCS_OK) {
          send_future->set_result(UCS_OK);
        } else if (UCS_PTR_IS_ERR(req)) {
          send_future->set_result(UCS_PTR_STATUS(req));
        } else {
          send_future->req_ = req;
          send_reqs_.insert(std::move(pack.send_future));
        }
        src.reset();
      });
    }
    // detect closed, cancel all outgoing reqs
    debug_print("Detect close in worker thread.");
    debug_print("Hangling recv requests: {}", recv_reqs_.size());
    debug_print("Hangling send requests: {}", send_reqs_.size());

    {
      nb::gil_scoped_acquire acquire;
      for (auto _req : recv_reqs_) {
        auto req = nb::inst_ptr<ServerRecvFuture>(_req);
        if (ucp_request_check_status(req->req_) == UCS_INPROGRESS) {
          ucp_request_cancel(worker_, req->req_);
          debug_print("Some recv request cancelled.");
        }
      }
    }
    {
      nb::gil_scoped_acquire acquire;
      for (auto _req : send_reqs_) {
        auto req = nb::inst_ptr<ServerSendFuture>(_req);
        if (ucp_request_check_status(req->req_) == UCS_INPROGRESS) {
          ucp_request_cancel(worker_, req->req_);
          debug_print("Some send request cancelled.");
        }
      }
    }
    while (ucp_worker_progress(worker_) > 0) {
    }

    {
      // additional safe guard to clean ref counts
      nb::gil_scoped_acquire acquire;
      recv_reqs_.clear();
      send_reqs_.clear();
    }
    // close all eps
    for (auto ep : connected_) {
      ucp_request_param_t param{};
      auto status = ucp_ep_close_nbx(ep, &param);
      if (UCS_PTR_STATUS(status) == UCS_OK) {
        debug_print("Close some ep.");
        continue;
      } else if (UCS_PTR_IS_ERR(status)) {
        if (UCS_PTR_STATUS(status) == UCS_ERR_CONNECTION_RESET) {
          debug_print("Some ep has been conn_reset.");
          continue;
        }
        debug_print("Error when trying to close ep: {}",
                    ucs_status_string(UCS_PTR_STATUS(status)));
      } else {
        while (ucp_request_check_status(status) == UCS_INPROGRESS) {
          while (ucp_worker_progress(worker_) > 0) {
          }
        }
        debug_print("Wait close some ep.");
        ucp_request_free(status);
      }
    }
    ucp_listener_destroy(listener_);
    ucp_worker_destroy(worker_);
    ucp_cleanup(context_);
    debug_print("Server cleanup done!");
    closed_.store(false, std::memory_order_release);
  }

  std::atomic<bool> closed_{true};
  std::jthread worker_thread_;
  ucp_context_h context_;
  ucp_worker_h worker_;
  ucp_listener_h listener_;
  std::set<ucp_ep_h> connected_;
  std::set<nb::object,
           decltype([](nb::object const &lhs, nb::object const &rhs) {
             return lhs.ptr() < rhs.ptr();
           })>
      recv_reqs_;
  std::set<nb::object,
           decltype([](nb::object const &lhs, nb::object const &rhs) {
             return lhs.ptr() < rhs.ptr();
           })>
      send_reqs_;
  Channel<ServerRecvPack> recv_q_;
  Channel<ServerSendPack> send_q_;
};

int client_send_future_wrapper_tp_traverse(PyObject *self, visitproc visit,
                                           void *arg) {
// On Python 3.9+, we must traverse the implicit dependency
// of an object on its associated type object.
#if PY_VERSION_HEX >= 0x03090000
  Py_VISIT(Py_TYPE(self));
#endif

  // The tp_traverse method may be called after __new__ but before or during
  // __init__, before the C++ constructor has been completed. We must not
  // inspect the C++ state if the constructor has not yet completed.
  if (!nb::inst_ready(self)) {
    return 0;
  }

  // Get the C++ object associated with 'self' (this always succeeds)
  ClientSendFuture *w = nb::inst_ptr<ClientSendFuture>(self);

  // If w->value has an associated Python object, return it.
  // If not, value.ptr() will equal NULL, which is also fine.
  nb::handle value = nb::find(w->done_callback_);

  // Inform the Python GC about the instance
  Py_VISIT(value.ptr());

  nb::handle value2 = nb::find(w->fail_callback_);
  Py_VISIT(value2.ptr());

  return 0;
}

int client_send_future_wrapper_tp_clear(PyObject *self) {
  // Get the C++ object associated with 'self' (this always succeeds)
  ClientSendFuture *w = nb::inst_ptr<ClientSendFuture>(self);

  // Break the reference cycle!
  w->done_callback_ = {};
  w->fail_callback_ = {};

  return 0;
}

// Table of custom type slots we want to install
PyType_Slot client_send_future_wrapper_slots[] = {
    {Py_tp_traverse, (void *)client_send_future_wrapper_tp_traverse},
    {Py_tp_clear, (void *)client_send_future_wrapper_tp_clear},
    {0, 0}};

int client_recv_future_wrapper_tp_traverse(PyObject *self, visitproc visit,
                                           void *arg) {
#if PY_VERSION_HEX >= 0x03090000
  Py_VISIT(Py_TYPE(self));
#endif
  if (!nb::inst_ready(self)) {
    return 0;
  }
  ClientRecvFuture *w = nb::inst_ptr<ClientRecvFuture>(self);
  nb::handle value = nb::find(w->done_callback_);
  Py_VISIT(value.ptr());
  nb::handle value2 = nb::find(w->fail_callback_);
  Py_VISIT(value2.ptr());
  return 0;
}
int client_recv_future_wrapper_tp_clear(PyObject *self) {
  ClientRecvFuture *w = nb::inst_ptr<ClientRecvFuture>(self);
  w->done_callback_ = {};
  w->fail_callback_ = {};
  return 0;
}
PyType_Slot client_recv_future_wrapper_slots[] = {
    {Py_tp_traverse, (void *)client_recv_future_wrapper_tp_traverse},
    {Py_tp_clear, (void *)client_recv_future_wrapper_tp_clear},
    {0, 0}};

int server_send_future_wrapper_tp_traverse(PyObject *self, visitproc visit,
                                           void *arg) {
#if PY_VERSION_HEX >= 0x03090000
  Py_VISIT(Py_TYPE(self));
#endif
  if (!nb::inst_ready(self)) {
    return 0;
  }
  ServerSendFuture *w = nb::inst_ptr<ServerSendFuture>(self);
  nb::handle value = nb::find(w->done_callback_);
  Py_VISIT(value.ptr());
  nb::handle value2 = nb::find(w->fail_callback_);
  Py_VISIT(value2.ptr());
  return 0;
}
int server_send_future_wrapper_tp_clear(PyObject *self) {
  ServerSendFuture *w = nb::inst_ptr<ServerSendFuture>(self);
  w->done_callback_ = {};
  w->fail_callback_ = {};
  return 0;
}
PyType_Slot server_send_future_wrapper_slots[] = {
    {Py_tp_traverse, (void *)server_send_future_wrapper_tp_traverse},
    {Py_tp_clear, (void *)server_send_future_wrapper_tp_clear},
    {0, 0}};

int server_recv_future_wrapper_tp_traverse(PyObject *self, visitproc visit,
                                           void *arg) {
#if PY_VERSION_HEX >= 0x03090000
  Py_VISIT(Py_TYPE(self));
#endif
  if (!nb::inst_ready(self)) {
    return 0;
  }
  ServerRecvFuture *w = nb::inst_ptr<ServerRecvFuture>(self);
  nb::handle value = nb::find(w->done_callback_);
  Py_VISIT(value.ptr());
  nb::handle value2 = nb::find(w->fail_callback_);
  Py_VISIT(value2.ptr());
  return 0;
}
int server_recv_future_wrapper_tp_clear(PyObject *self) {
  ServerRecvFuture *w = nb::inst_ptr<ServerRecvFuture>(self);
  w->done_callback_ = {};
  w->fail_callback_ = {};
  return 0;
}
PyType_Slot server_recv_future_wrapper_slots[] = {
    {Py_tp_traverse, (void *)server_recv_future_wrapper_tp_traverse},
    {Py_tp_clear, (void *)server_recv_future_wrapper_tp_clear},
    {0, 0}};

int server_wrapper_tp_traverse(PyObject *self, visitproc visit, void *arg) {
#if PY_VERSION_HEX >= 0x03090000
  Py_VISIT(Py_TYPE(self));
#endif
  if (!nb::inst_ready(self)) {
    return 0;
  }
  auto *w = nb::inst_ptr<Server>(self);
  for (auto req : w->recv_reqs_) {
    Py_VISIT(req.ptr());
  }
  for (auto req : w->send_reqs_) {
    Py_VISIT(req.ptr());
  }
  if (w->recv_q_.data.has_value()) {
    Py_VISIT(w->recv_q_.data->recv_future.ptr());
  }
  if (w->send_q_.data.has_value()) {
    Py_VISIT(w->send_q_.data->send_future.ptr());
  }
  return 0;
}
int server_wrapper_tp_clear(PyObject *self) {
  auto *w = nb::inst_ptr<Server>(self);
  w->recv_reqs_.clear();
  w->send_reqs_.clear();
  w->recv_q_.data.reset();
  w->send_q_.data.reset();
  w->recv_q_.status.store(0, std::memory_order_release);
  w->send_q_.status.store(0, std::memory_order_release);
  return 0;
}
PyType_Slot server_wrapper_slots[] = {
    {Py_tp_traverse, (void *)server_wrapper_tp_traverse},
    {Py_tp_clear, (void *)server_wrapper_tp_clear},
    {0, 0}};
int client_wrapper_tp_traverse(PyObject *self, visitproc visit, void *arg) {
#if PY_VERSION_HEX >= 0x03090000
  Py_VISIT(Py_TYPE(self));
#endif
  if (!nb::inst_ready(self)) {
    return 0;
  }
  auto *w = nb::inst_ptr<Client>(self);
  for (auto const &send : w->sends_) {
    Py_VISIT(send.ptr());
  }
  for (auto const &recv : w->recv_reqs_) {
    Py_VISIT(recv.ptr());
  }
  if (w->send_q_.data.has_value()) {
    Py_VISIT(w->send_q_.data.value().send_future.ptr());
  }
  if (w->recv_q_.data.has_value()) {
    Py_VISIT(w->recv_q_.data.value().recv_future.ptr());
  }
  return 0;
}
int client_wrapper_tp_clear(PyObject *self) {
  auto *w = nb::inst_ptr<Client>(self);
  w->sends_.clear();
  w->recv_reqs_.clear();
  w->send_q_.data.reset();
  w->send_q_.status.store(0, std::memory_order_release);
  w->recv_q_.data.reset();
  w->recv_q_.status.store(0, std::memory_order_release);
  return 0;
}
PyType_Slot client_wrapper_slots[] = {
    {Py_tp_traverse, (void *)client_wrapper_tp_traverse},
    {Py_tp_clear, (void *)client_wrapper_tp_clear},
    {0, 0}};

NB_MODULE(_bindings, m) {
  nb::class_<ClientSendFuture>(m, "ClientSendFuture",
                               nb::type_slots(client_send_future_wrapper_slots))
      .def("done", &ClientSendFuture::done)
      .def("wait", &ClientSendFuture::wait,
           nb::call_guard<nb::gil_scoped_release>())
      .def("exception", &ClientSendFuture::exception);

  nb::class_<ClientRecvFuture>(m, "ClientRecvFuture",
                               nb::type_slots(client_recv_future_wrapper_slots))
      .def("done", &ClientRecvFuture::done)
      .def("exception", &ClientRecvFuture::exception)
      .def("info", &ClientRecvFuture::info)
      .def("wait", &ClientRecvFuture::wait,
           nb::call_guard<nb::gil_scoped_release>());

  nb::class_<Client>(m, "Client", nb::type_slots(client_wrapper_slots))
      .def(nb::init<std::string_view, uint64_t>(), "addr"_a, "port"_a)
      .def("send", &Client::send, "buffer"_a, "tag"_a, "done_callback"_a,
           "fail_callback"_a,
           nb::sig(
               "def send(self, buffer: Annotated[NDArray[numpy.uint8], "
               "dict(shape=(None,), device='cpu')], tag: int, done_callback: "
               "Callable[[ClientSendFuture], None], fail_callback: "
               "Callable[[ClientSendFuture], None]) -> ClientSendFuture"))
      .def("recv", &Client::recv, "buffer"_a, "tag"_a, "tag_mask"_a,
           "done_callback"_a, "fail_callback"_a,
           nb::sig("def recv(self, buffer: Annotated[NDArray[numpy.uint8], "
                   "dict(shape=(None,), device='cpu')], tag: int, tag_mask: "
                   "int, done_callback: Callable[[ClientRecvFuture], None], "
                   "fail_callback: Callable[[ClientRecvFuture], None]) -> "
                   "ClientRecvFuture"));

  nb::class_<ServerSendFuture>(m, "ServerSendFuture",
                               nb::type_slots(server_send_future_wrapper_slots))
      .def("done", &ServerSendFuture::done)
      .def("wait", &ServerSendFuture::wait,
           nb::call_guard<nb::gil_scoped_release>())
      .def("exception", &ServerSendFuture::exception);

  nb::class_<ServerRecvFuture>(m, "ServerRecvFuture",
                               nb::type_slots(server_recv_future_wrapper_slots))
      .def("done", &ServerRecvFuture::done)
      .def("exception", &ServerRecvFuture::exception)
      .def("info", &ServerRecvFuture::info)
      .def("wait", &ServerRecvFuture::wait,
           nb::call_guard<nb::gil_scoped_release>());

  nb::class_<Server>(m, "Server", nb::type_slots(server_wrapper_slots))
      .def(nb::init<std::string_view, uint64_t>(), "addr"_a, "port"_a)
      .def("recv", &Server::recv, "buffer"_a, "tag"_a, "tag_mask"_a,
           "done_callback"_a, "fail_callback"_a,
           nb::sig("def recv(self, buffer: Annotated[NDArray[numpy.uint8], "
                   "dict(shape=(None,), device='cpu')], tag: int, tag_mask: "
                   "int, done_callback: Callable[[ServerRecvFuture], None], "
                   "fail_callback: Callable[[ServerRecvFuture], None]) -> "
                   "ServerRecvFuture"))
      .def("list_clients", &Server::list_clients)
      .def("send", &Server::send, "client_ep"_a, "buffer"_a, "tag"_a,
           "done_callback"_a, "fail_callback"_a,
           nb::sig("def send(self, client_ep: int, buffer:  "
                   "Annotated[NDArray[numpy.uint8], dict(shape=(None,), "
                   "device='cpu')], tag: int, done_callback: "
                   "Callable[[ServerSendFuture], None], fail_callback: "
                   "Callable[[ServerSendFuture], None]) -> ServerSendFuture"));
}
