#include "main.hpp"
#include "nanobind/nanobind.h"
#include "ucp/api/ucp_compat.h"
#include "ucp/api/ucp_def.h"
#include "ucs/type/status.h"
#include <arpa/inet.h>
#include <cassert>
#include <compare>
#include <iostream>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>
#include <netinet/in.h>
#include <string>
#include <thread>
#include <type_traits>
#include <ucp/api/ucp.h>

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

#define fatal_print(...)                                                       \
  do {                                                                         \
    std::cerr << std::format(__VA_ARGS__) << "\n";                             \
  } while (0)

inline void static ucp_check_status(ucs_status_t status, std::string_view msg) {
  if (status != UCS_OK) {
    throw std::runtime_error(
        "UCP error: " + std::string(ucs_status_string(status)) + " - " +
        std::string(msg));
  }
}

Context::Context() {
  ucp_params_t params{.field_mask = UCP_PARAM_FIELD_FEATURES |
                                    UCP_PARAM_FIELD_ESTIMATED_NUM_EPS,
                      .features = UCP_FEATURE_TAG,
                      .estimated_num_eps = 1};
  ucp_check_status(ucp_init(&params, NULL, &context_),
                   "Failed to init UCP context");
}
Context::~Context() { ucp_cleanup(context_); }

ClientSendFuture::ClientSendFuture(Client *client, auto &&done_callback,
                                   auto &&fail_callback)
  requires UniRef<decltype(done_callback), nb::object> &&
               UniRef<decltype(fail_callback), nb::object>
    : client_(client),
      done_callback_(std::forward<decltype(done_callback)>(done_callback)),
      fail_callback_(std::forward<decltype(fail_callback)>(fail_callback)) {}

void ClientSendFuture::set_result(ucs_status_t result) {
  done_status_ = result;
  status_.store(1, std::memory_order_release);
  if (done_status_ == UCS_OK) {
    nb::handle obj = nb::find(this);
    debug_print("Client Send Future done.");
    done_callback_(obj);
  } else {
    nb::handle obj = nb::find(this);
    debug_print("Client Send Future failed with {}.",
                ucs_status_string(result));
    fail_callback_(obj);
  }
}

[[nodiscard]] bool ClientSendFuture::done() const {
  return status_.load(std::memory_order_acquire) == 1;
}

[[nodiscard]] const char *ClientSendFuture::excpetion() const {
  return ucs_status_string(done_status_);
}

ClientRecvFuture::ClientRecvFuture(Client *client, auto &&done_callback,
                                   auto &&fail_callback)
  requires UniRef<decltype(done_callback), nb::object> &&
               UniRef<decltype(fail_callback), nb::object>
    : client_(client),
      done_callback_(std::forward<decltype(done_callback)>(done_callback)),
      fail_callback_(std::forward<decltype(fail_callback)>(fail_callback)) {}

void ClientRecvFuture::set_result_value(std::tuple<uint64_t, size_t> result) {
  result_ = result;
}
void ClientRecvFuture::set_result(ucs_status_t result) {
  done_status_ = result;
  status_.store(1, std::memory_order_release);
  if (done_status_ == UCS_OK) {
    nb::handle obj = nb::find(this);
    debug_print("Client Recv Future done.");
    done_callback_(obj);
  } else {
    nb::handle obj = nb::find(this);
    debug_print("Client Recv Future failed with {}.",
                ucs_status_string(result));
    fail_callback_(obj);
  }
}

[[nodiscard]] bool ClientRecvFuture::done() const {
  return status_.load(std::memory_order_acquire) == 1;
}

[[nodiscard]] const char *ClientRecvFuture::excpetion() const {
  return ucs_status_string(done_status_);
}

[[nodiscard]] std::tuple<uint64_t, size_t> ClientRecvFuture::result() const {
  return result_;
}

Client::Client(Context &ctx) : ctx_(ctx.context_) {}

static auto init_sock_ep_pararms(std::string_view addr, uint64_t port) {
}
struct PinTrait {
  PinTrait() = default;
  ~PinTrait() = default;
  PinTrait(const PinTrait &) = delete;
  PinTrait &operator=(const PinTrait &) = delete;
  PinTrait(PinTrait &&) = delete;
  PinTrait &operator=(PinTrait &&) = delete;
};

struct WorkerOwner : PinTrait {
  WorkerOwner(ucp_context_h ctx) {
    ucp_worker_params_t worker_params{
        .field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE,
        .thread_mode = UCS_THREAD_MODE_SINGLE,
    };
    ucp_check_status(ucp_worker_create(ctx, &worker_params, &worker_),
                     "Client: Failed to create UCP worker");
  }
  ~WorkerOwner() { ucp_worker_destroy(worker_); }
  ucp_worker_h worker_;
};

struct EpOwner : PinTrait {
  EpOwner(ucp_worker_h worker, ucp_ep_params_t *ep_params) {
    ucp_check_status(ucp_ep_create(worker, ep_params, &ep_),
                     "Client: Failed to create UCP ep");
  }
  ~EpOwner() { ucp_ep_destroy(ep_); }
  ucp_ep_h ep_;
};

void client_send_cb(void *req, ucs_status_t status, void *user_data) {
  auto *send_future = reinterpret_cast<ClientSendFuture *>(user_data);
  {
    nb::gil_scoped_acquire acquire;
    debug_print("Client: waited send future done.");
    nb::object obj = nb::find(send_future);
    assert(obj.is_valid());
    send_future->set_result(status);
    send_future->client_->send_futures_.erase(obj);
  }
  // free the request
  ucp_request_free(req);
}

void client_recv_cb(void *request, ucs_status_t status,
                    ucp_tag_recv_info_t const *info, void *args) {
  auto *recv_future = reinterpret_cast<ClientRecvFuture *>(args);
  {
    nb::gil_scoped_acquire acquire;
    debug_print("Client: waited recv future done.");
    auto obj = nb::find(recv_future);
    assert(obj.is_valid());
    assert(recv_future->req_ == request);
    recv_future->set_result_value(
        std::make_tuple(info->sender_tag, info->length));
    recv_future->set_result(status);
    recv_future->client_->recv_futures_.erase(obj);
  }
  ucp_request_free(request);
}

void Client::start_working(std::string addr, uint64_t port) {
  // we don't hold GIL from the very beginning of this thread

  // utilize RAII for exception handling
  WorkerOwner worker_owner(ctx_);
  ucp_worker_h worker = worker_owner.worker_;

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
  EpOwner ep_owner(worker_owner.worker_, &ep_params);
  ucp_ep_h ep = ep_owner.ep_;

  // initialized
  status_.store(1, std::memory_order_release);

  // ensure connection has been established when init
  debug_print("Client: init ep start flushing to ensure connection.");
  auto connect_success = [&]() {
    debug_print("Client: init ep flush done");
    status_.store(2, std::memory_order_release);
    bool consumed = false;
    chan_.try_consume([&](auto *ptr) {
      if constexpr (std::is_same_v<std::decay_t<decltype(ptr)>,
                                   ClientConnectArgs *>) {
        nb::gil_scoped_acquire acquire;
        nb::object &obj = ptr->connect_callback;
        obj(nb::cast(""));
        ptr->~ClientConnectArgs();
        consumed = true;
      }
    });
    if (!consumed) {
      fatal_print("Client: init channel not filled with connect args");
      throw std::runtime_error("Client: init channel not failled with connect "
                               "args, bad internal state");
    }
  };
  auto connect_failed = [&](ucs_status_t reason) {
    fatal_print("Client: init ep flush failed: {}", ucs_status_string(reason));
    bool consumed = false;
    chan_.try_consume([&](auto *ptr) {
      if constexpr (std::is_same_v<std::decay_t<decltype(*ptr)>,
                                   ClientConnectArgs *>) {
        nb::gil_scoped_acquire acquire;
        nb::object &obj = ptr->connect_callback;
        obj(nb::cast(ucs_status_string(reason)));
        ptr->~ClientConnectArgs();
        consumed = true;
      }
    });
    if (!consumed) {
      fatal_print("Client: init channel not filled with connect args");
      return;
    }
  };
  ucp_request_param_t flush_params{};
  auto status = ucp_ep_flush_nbx(ep, &flush_params);
  if (UCS_PTR_STATUS(status) == UCS_OK) {
    connect_success();
  } else if (UCS_PTR_IS_ERR(status)) {
    connect_failed(UCS_PTR_STATUS(status));
    return;
  } else {
    while (ucp_request_check_status(status) == UCS_INPROGRESS) {
      ucp_worker_progress(worker);
    }
    auto final_status = ucp_request_check_status(status);
    if (final_status != UCS_OK) {
      ucp_request_free(status);
      connect_failed(final_status);
      return;
    }
    ucp_request_free(status);
    connect_success();
  }

  debug_print("Client: start main loop.");
  bool to_close{false};
  std::array<std::byte, sizeof(nb::object)> _close_callback;
  nb::object &close_callback =
      *reinterpret_cast<nb::object *>(_close_callback.data());

  for (;;) {
    ucp_worker_progress(worker);
    chan_.try_consume([&](auto *ptr) {
      using P = std::decay_t<decltype(*ptr)>;
      if constexpr (std::is_same_v<P, ClientSendArgs>) {
        nb::gil_scoped_acquire acquire;
        ClientSendArgs &args = *ptr;
        nb::object &ref_future = args.send_future;
        auto p_future = nb::inst_ptr<ClientSendFuture>(ref_future);
        ucp_request_param_t send_param{.op_attr_mask =
                                           UCP_OP_ATTR_FIELD_CALLBACK |
                                           UCP_OP_ATTR_FIELD_USER_DATA,
                                       .cb{.send = client_send_cb},
                                       .user_data = p_future};
        auto req = ucp_tag_send_nbx(ep, args.buf_ptr, args.buf_size, args.tag,
                                    &send_param);
        if (req == NULL) {
          debug_print("Client: send request immediate success.");
          p_future->set_result(UCS_OK);
          ptr->~P();
          return;
        }
        if (UCS_PTR_IS_ERR(req)) {
          fatal_print("Client: send request failed with {}",
                      ucs_status_string(UCS_PTR_STATUS(req)));
          p_future->set_result(UCS_PTR_STATUS(req));
          ptr->~P();
          return;
        }
        // add to vector to trace its lifetime
        p_future->req_ = req;
        send_futures_.insert(std::move(ref_future)); // takes ownership
        ptr->~P();
        return;
      }
      if constexpr (std::is_same_v<P, ClientRecvArgs>) {
        nb::gil_scoped_acquire acquire;
        ClientRecvArgs &args = *ptr;
        nb::object &ref_future = args.recv_future;
        auto *p_future = nb::inst_ptr<ClientRecvFuture>(ref_future);
        ucp_request_param_t recv_param{.op_attr_mask =
                                           UCP_OP_ATTR_FIELD_CALLBACK |
                                           UCP_OP_ATTR_FIELD_USER_DATA,
                                       .cb{.recv = client_recv_cb},
                                       .user_data = p_future};
        auto req = ucp_tag_recv_nbx(worker, args.buf_ptr, args.buf_size,
                                    args.tag, args.tag_mask, &recv_param);
        if (req == NULL) {
          debug_print("Client: recv request immediate success.");
          p_future->set_result(UCS_OK);
          ptr->~P();
          return;
        }
        if (UCS_PTR_IS_ERR(req)) {
          fatal_print("Client: recv request failed with {}",
                      ucs_status_string(UCS_PTR_STATUS(req)));
          p_future->set_result(UCS_PTR_STATUS(req));
          ptr->~P();
          return;
        }
        p_future->req_ = req;
        recv_futures_.insert(std::move(ref_future)); // takes ownership
          ptr->~P();
        return;
      }
      if constexpr (std::is_same_v<P, ClientCloseArgs>) {
        nb::gil_scoped_acquire acquire;
        debug_print("Client: recv close request, breaking loop.");
        to_close = true;
        new (_close_callback.data()) nb::object(std::move(ptr->close_callback));
        ptr->~P();
      }
      fatal_print("Client: recevied message that should not be handled in "
                  "Client working loop, tag index: {}. Message  Ignored.",
                  decltype(chan_)::index_of<P>());
    });
    if (to_close) {
      break;
    }
  }
  // final cleanup process
  debug_print("Client: enter cleanup.");
  while (ucp_worker_progress(worker) > 0) {
  }
  debug_print("Client: done tailing worker.");

  // cleanup requests, cancel them all
  debug_print("Client: start cancelling requests.");
  {
    nb::gil_scoped_acquire acquire;
    for (nb::object const &req : send_futures_) {
      assert(req.is_valid());
      auto *p = nb::inst_ptr<ClientSendFuture>(req);
      assert(ucp_request_check_status(p->req_) == UCS_INPROGRESS);
      ucp_request_cancel(worker, p->req_);
    }
    for (nb::object const &req : recv_futures_) {
      auto *p = nb::inst_ptr<ClientSendFuture>(req);
      assert(ucp_request_check_status(p->req_) == UCS_INPROGRESS);
      ucp_request_cancel(worker, p->req_);
    }
    send_futures_.clear();
    recv_futures_.clear();
  }
  debug_print("Client: done cancelling requests.");

  // close endpoint
  debug_print("Client: start closing ep.");
  {
    ucp_request_param_t param{};
    auto status = ucp_ep_close_nbx(ep, &param);
    if (status == NULL) {
      debug_print("Client: close ep immediate success.");
    } else if (UCS_PTR_IS_ERR(status)) {
      fatal_print("Client: close ep failed with {}",
                  ucs_status_string(UCS_PTR_STATUS(status)));
    } else {
      while (ucp_request_check_status(status) == UCS_INPROGRESS) {
        ucp_worker_progress(worker);
      }
      auto final_status = ucp_request_check_status(status);
      if (final_status != UCS_OK) {
        fatal_print("Client: close ep failed with {}",
                    ucs_status_string(final_status));
      }
    }
  }
  debug_print("Client: done closing ep.");
  {
    nb::gil_scoped_acquire acquire;
    if(close_callback.is_valid()) {
      close_callback();
    }
    close_callback.~object();
  }
  status_.store(3, std::memory_order_release);
}

void Client::connect(std::string addr, uint64_t port, nb::object callback) {
  if (status_.load(std::memory_order_acquire) != 0) {
    throw std::runtime_error("Client: already connected. You can only connect "
                             "once, and cannot reconnect after close.");
  }
  chan_.wait_emplace<ClientConnectArgs>(
      [&](auto *ptr) { new (ptr) ClientConnectArgs(callback); });
  working_thread_ =
      std::thread([this, addr, port]() { this->start_working(addr, port); });
}
void Client::close(nb::object callback) {
  if (status_.load(std::memory_order_acquire) != 2) {
    throw std::runtime_error("Client: not running. You can only close "
                             "once, after connect done.");
  }
  nb::gil_scoped_release release;
  chan_.wait_emplace<ClientCloseArgs>([&](auto *ptr) {
    nb::gil_scoped_acquire acquire;
    new (ptr) ClientCloseArgs(std::move(callback));
  });
}

nb::object
Client::send(nb::ndarray<uint8_t, nb::ndim<1>, nb::device::cpu> const &buffer,
             uint64_t tag, nb::object done_callback, nb::object fail_callback) {
  if (status_.load(std::memory_order_acquire) != 2) {
    std::runtime_error(
        "Client: not running. You can only  send , after connect.");
  }
  auto buf_ptr = reinterpret_cast<std::byte *>(buffer.data());
  auto buf_size = buffer.size();
  auto *p_future = new ClientSendFuture(this, std::move(done_callback),
                                        std::move(fail_callback));
  nb::object send_future_obj = nb::cast(p_future);
  {
    nb::gil_scoped_release release;
    chan_.wait_emplace<ClientSendArgs>([&](auto *ptr) {
      // GIL may not be required here as we "move" the object
      nb::gil_scoped_acquire acquire;
      new (ptr)
          ClientSendArgs(std::move(send_future_obj), tag, buf_ptr, buf_size);
    });
  }
  return send_future_obj;
}
nb::object
Client::recv(nb::ndarray<uint8_t, nb::ndim<1>, nb::device::cpu> &buffer,
             uint64_t tag, uint64_t tag_mask, nb::object done_callback,
             nb::object fail_callback) {
  if (status_.load(std::memory_order_acquire) != 2) {
    std::runtime_error(
        "Client: not running. You can only  recv , after connect.");
  }
  auto buf_ptr = reinterpret_cast<std::byte *>(buffer.data());
  auto buf_size = buffer.size();
  auto *p_future = new ClientRecvFuture(this, std::move(done_callback),
                                        std::move(fail_callback));
  nb::object recv_future_obj = nb::cast(p_future);
  {
    nb::gil_scoped_release release;
    chan_.wait_emplace<ClientRecvArgs>([&](auto *ptr) {
      // GIL may not be required here as we "move" the object
      nb::gil_scoped_acquire acquire;
      new (ptr) ClientRecvArgs(std::move(recv_future_obj), tag, tag_mask,
                               buf_ptr, buf_size);
    });
  }
  return recv_future_obj;
}

Client::~Client() {
  nb::gil_scoped_release release;
  auto cur = status_.load(std::memory_order_acquire);
  if (cur == 1 || cur == 2) {
    fatal_print("Client: not closed, trying to close in dtor...");
    chan_.wait_emplace<ClientCloseArgs>([&](auto *ptr) {
      nb::gil_scoped_acquire acquire;
      new (ptr) ClientCloseArgs(nb::object());
    });
  }
  if (working_thread_.joinable()) {
    debug_print("Client: start to join working thread...");
    working_thread_.join();
  }
}

ServerSendFuture::ServerSendFuture(Server *server, auto &&done_callback,
                                   auto &&fail_callback)
  requires UniRef<decltype(done_callback), nb::object> &&
               UniRef<decltype(fail_callback), nb::object>
    : server_(server),
      done_callback_(std::forward<decltype(done_callback)>(done_callback)),
      fail_callback_(std::forward<decltype(fail_callback)>(fail_callback)) {}

void ServerSendFuture::set_result(ucs_status_t result) {
  done_status_ = result;
  status_.store(1, std::memory_order_release);
  nb::handle obj = nb::find(this);
  if (done_status_ == UCS_OK) {
    debug_print("Server Send Future done.");
    done_callback_(obj);
  } else {
    debug_print("Server Send Future failed with {}.",
                ucs_status_string(result));
    fail_callback_(obj);
  }
}

[[nodiscard]] bool ServerSendFuture::done() const {
  return status_.load(std::memory_order_acquire) == 1;
}

[[nodiscard]] const char *ServerSendFuture::excpetion() const {
  return ucs_status_string(done_status_);
}

ServerRecvFuture::ServerRecvFuture(Server *server, auto &&done_callback,
                                   auto &&fail_callback)
  requires UniRef<decltype(done_callback), nb::object> &&
               UniRef<decltype(fail_callback), nb::object>
    : server_(server),
      done_callback_(std::forward<decltype(done_callback)>(done_callback)),
      fail_callback_(std::forward<decltype(fail_callback)>(fail_callback)) {}

void ServerRecvFuture::set_result_value(ResultType result) { result_ = result; }

void ServerRecvFuture::set_result(ucs_status_t result) {
  done_status_ = result;
  status_.store(1, std::memory_order_release);
  nb::handle obj = nb::find(this);
  if (done_status_ == UCS_OK) {
    debug_print("Server Recv Future done.");
    done_callback_(obj);
  } else {
    debug_print("Server Recv Future failed with {}.",
                ucs_status_string(result));
    fail_callback_(obj);
  }
}

[[nodiscard]] bool ServerRecvFuture::done() const {
  return status_.load(std::memory_order_acquire) == 1;
}

[[nodiscard]] const char *ServerRecvFuture::excpetion() const {
  return ucs_status_string(done_status_);
}

[[nodiscard]] ServerRecvFuture::ResultType ServerRecvFuture::result() const {
  return result_;
}

auto ServerEndpoint::view_transports() const
    -> std::vector<std::tuple<char const *, char const *>> {
  std::vector<std::tuple<char const *, char const *>> res;
  res.reserve(num_transports);
  for (size_t i = 0; i < num_transports; i++) {
    res.emplace_back(transports[i].device_name, transports[i].transport_name);
  }
  return res;
}

std::strong_ordering
ServerEndpoint::operator<=>(ServerEndpoint const &rhs) const {
  return ep <=> rhs.ep;
}

Server::Server(Context &ctx) : ctx_(ctx.context_) {}
void Server::set_accept_callback(nb::object accept_callback) {
  accept_callback_ = std::move(accept_callback);
}
void Server::listen(std::string addr, uint16_t port) {
  if (status_.load(std::memory_order_acquire) != 0) {
    throw std::runtime_error("Server: already listening. You can only "
                             "listen once, and cannot listen again after "
                             "close.");
  }
  nb::gil_scoped_release release;
  working_thread_ =
      std::thread([this, addr, port]() { this->start_working(addr, port); });
  while (status_.load(std::memory_order_acquire) != 2) {
    std::this_thread::yield();
  }
}

void server_accept_cb(ucp_ep_h ep, void *arg) {
  auto *cur = reinterpret_cast<Server *>(arg);
  ServerEndpoint endpoint{};
  ucp_ep_attr_t attrs{.field_mask = UCP_EP_ATTR_FIELD_NAME |
                                    UCP_EP_ATTR_FIELD_LOCAL_SOCKADDR |
                                    UCP_EP_ATTR_FIELD_REMOTE_SOCKADDR |
                                    UCP_EP_ATTR_FIELD_TRANSPORTS,
                      .transports{.entries = endpoint.transports.data(),
                                  .num_entries = endpoint.transports.size(),
                                  .entry_size = sizeof(ucp_transport_entry_t)}};
  auto status = ucp_ep_query(ep, &attrs);
  if (status != UCS_OK) {
    fatal_print("Server: ERR when querying ep on connection: {}",
                ucs_status_string(status));
    return;
  }
  sockaddr_in *local_addr =
      reinterpret_cast<sockaddr_in *>(&attrs.local_sockaddr);
  sockaddr_in *remote_addr =
      reinterpret_cast<sockaddr_in *>(&attrs.remote_sockaddr);
  endpoint.ep = ep;
  endpoint.name = attrs.name;
  endpoint.local_addr = inet_ntoa(local_addr->sin_addr);
  endpoint.local_port = ntohs(local_addr->sin_port);
  endpoint.remote_addr = inet_ntoa(remote_addr->sin_addr);
  endpoint.remote_port = ntohs(remote_addr->sin_port);
  endpoint.num_transports = attrs.transports.num_entries;
  auto [endpoint_it, _] = cur->eps_.emplace(std::move(endpoint));
  auto &endpoint_ref = *endpoint_it;
  {
    nb::gil_scoped_acquire acquire;
    if (cur->accept_callback_.is_valid()) {
      cur->accept_callback_(nb::cast(endpoint_ref));
    }
  }
}

auto init_listener_params(std::string_view addr, uint16_t port,
                          Server *server) {
}

void server_recv_cb(void *req, ucs_status_t status,
                    ucp_tag_recv_info_t const *info, void *args) {
  auto *recv_future = reinterpret_cast<ServerRecvFuture *>(args);
  {
    nb::gil_scoped_acquire acquire;
    debug_print("Server: waited recv future done.");
    auto obj = nb::find(recv_future);
    assert(obj.is_valid());
    assert(recv_future->req_ == req);
    recv_future->set_result_value(
        std::make_tuple(info->sender_tag, info->length));
    recv_future->set_result(status);
    recv_future->server_->recv_futures_.erase(obj);
  }
  ucp_request_free(req);
}
void server_send_cb(void *req, ucs_status_t status, void *user_data) {}
void Server::start_working(std::string addr, uint16_t port) {
  debug_print("Server: worker thread started.");
  WorkerOwner worker_owner(ctx_);
  ucp_worker_h worker = worker_owner.worker_;
  debug_print("Server: worker init.");

  ucp_listener_h listener;
  {
    struct sockaddr_in listen_addr{
        .sin_family = AF_INET,
        .sin_port = htons(static_cast<uint16_t>(port)),
        .sin_addr = {inet_addr(addr.data())},
    };
    ucp_listener_params_t listener_params{
        .field_mask = UCP_LISTENER_PARAM_FIELD_SOCK_ADDR |
                      UCP_LISTENER_PARAM_FIELD_ACCEPT_HANDLER,
        .sockaddr{
            .addr = reinterpret_cast<struct sockaddr *>(&listen_addr),
            .addrlen = sizeof(listen_addr),
        },
        .accept_handler{.cb = server_accept_cb, .arg = this}};
    ucp_check_status(ucp_listener_create(worker, &listener_params, &listener),
                     "Server: failed to create listener.");
  }
  debug_print("Server: listener init.");

  status_.store(2, std::memory_order_release);
  debug_print("Server: init done.");
  std::array<std::byte, sizeof(nb::object)> _close_callback;
  nb::object &close_callback =
      *reinterpret_cast<nb::object *>(_close_callback.data());
  bool to_close = false;
  for (;;) {
    ucp_worker_progress(worker);
    chan_.try_consume([&](auto *ptr) {
      nb::gil_scoped_acquire acquire;
      using P = std::decay_t<decltype(*ptr)>;
      if constexpr (std::is_same_v<P, ServerSendArgs>) {
        debug_print("Server: handle send message.");
        ServerSendArgs &args = *ptr;
        nb::object &ref_future = args.send_future;
        auto *p_future = nb::inst_ptr<ServerSendFuture>(ref_future);
        ucp_request_param_t send_param{.op_attr_mask =
                                           UCP_OP_ATTR_FIELD_CALLBACK |
                                           UCP_OP_ATTR_FIELD_USER_DATA,
                                       .cb{.send = server_send_cb},
                                       .user_data = p_future};
        auto *req = ucp_tag_send_nbx(args.ep, args.buf_ptr, args.buf_size,
                                     args.tag, &send_param);
        if (UCS_PTR_STATUS(req) == UCS_OK) {
          debug_print("Server: send success immediately");
          p_future->set_result(UCS_OK);
          ptr->~P();
          return;
        }
        if (req == NULL) {
          debug_print("Server: send failed immediately");
          p_future->set_result(UCS_ERR_NO_RESOURCE);
          ptr->~P();
          return;
        }
        debug_print("Server: async send request pending  created.");
        p_future->req_ = req;
        send_futures_.emplace(nb::cast(p_future));
        return;
      }
      if constexpr (std::is_same_v<P, ServerRecvArgs>) {
        debug_print("Server: handle recv message.");
        ServerRecvArgs &args = *ptr;
        nb::object &ref_future = args.recv_future;
        auto *p_future = nb::inst_ptr<ServerRecvFuture>(ref_future);
        ucp_tag_recv_info_t tag_info{};
        ucp_request_param_t param{.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                                                  UCP_OP_ATTR_FIELD_USER_DATA |
                                                  UCP_OP_ATTR_FIELD_RECV_INFO,
                                  .cb{.recv = server_recv_cb},
                                  .user_data = p_future,
                                  .recv_info{
                                      .tag_info = &tag_info,
                                  }};

        auto status = ucp_tag_recv_nbx(worker, args.buf_ptr, args.buf_size,
                                       args.tag, args.tag_mask, &param);
        if (status == NULL) {
          debug_print("Server: recv success immediately");
          p_future->set_result_value({tag_info.sender_tag, tag_info.length});
          p_future->set_result(UCS_OK);
          ptr->~P();
          return;
        }
        if (UCS_PTR_IS_ERR(status)) {
          fatal_print("Server: recv failed with {}",
                      ucs_status_string(UCS_PTR_STATUS(ptr)));
          p_future->set_result(UCS_PTR_STATUS(ptr));
          ptr->~P();
          return;
        }
        debug_print("Server: async recv future created pending.");
        p_future->req_ = status;
        recv_futures_.emplace(std::move(args.recv_future));
        ptr->~P();
        return;
      }
      if constexpr (std::is_same_v<P, ServerCloseArgs>) {
        debug_print("Server: handle close message.");
        // GIL may be required here
        to_close = true;
        // let's use the magic emplacement new to avoid ref count
        new (_close_callback.data()) nb::object(std::move(ptr->close_callback));
        ptr->~P();
        return;
      }
      fatal_print("Server: unknown message, tag {}. Ignored.",
                  decltype(chan_)::index_of<P>());
    });
    if (to_close) {
      break;
    }
  }
  debug_print("Server: start close...");

  // first close listener to avoid more incoming conns
  debug_print("Server: start close listener.");
  ucp_listener_destroy(listener);
  debug_print("Server: done close listener.");

  // cancel all existing requests
  debug_print("Server: start cancel requests.");
  {
    nb::gil_scoped_acquire acquire;
    for (auto &req : send_futures_) {
      assert(req.is_valid());
      auto *p_future = nb::inst_ptr<ServerSendFuture>(req);
      assert(ucp_request_check_status(p_future->req_) == UCS_INPROGRESS);
      ucp_request_cancel(worker, p_future->req_);
    }
    for (auto const &req : recv_futures_) {
      assert(req.is_valid());
      auto *p_future = nb::inst_ptr<ServerSendFuture>(req);
      assert(ucp_request_check_status(p_future->req_) == UCS_INPROGRESS);
      ucp_request_cancel(worker, p_future->req_);
    }
    send_futures_.clear();
    recv_futures_.clear();
  }
  debug_print("Server: done cancel requests.");

  // close all endpoints
  debug_print("Server: start close endpoints.");
  for (auto const &ep : eps_) {
    ucp_request_param_t params{};
    auto status = ucp_ep_close_nbx(ep.ep, &params);
    if (status == NULL) {
      debug_print("Server: closed endpoint immediately.");
    } else if (UCS_PTR_IS_ERR(status)) {
      fatal_print("Server: failed to close endpoint with {}",
                  ucs_status_string(UCS_PTR_STATUS(status)));
    } else {
      while (ucp_request_check_status(status) == UCS_INPROGRESS) {
        ucp_worker_progress(worker);
      }
      auto final_status = ucp_request_check_status(status);
      if (final_status != UCS_OK) {
        fatal_print("Server: failed to close endpoint with {}",
                    ucs_status_string(final_status));
      }
      ucp_request_free(status);
    }
  }
  debug_print("Server: done close endpoints.");

  // invoke  callback
  debug_print("Server: worker thread close done.");
  {
    nb::gil_scoped_acquire acquire;
    if(close_callback.is_valid()) {
      close_callback();
    }
    close_callback.~object();
  }
  status_.store(3, std::memory_order_release);
}

void Server::close(nb::object close_callback) {
  if (status_.load(std::memory_order_acquire) != 2) {
    throw std::runtime_error("Server: not running. You can only close "
                             "once, after listen done.");
  }
  nb::gil_scoped_release release;
  chan_.wait_emplace<ServerCloseArgs>([&](auto *ptr) {
    // GIL may be required here
    nb::gil_scoped_acquire acquire;
    new (ptr) ServerCloseArgs(std::move(close_callback));
  });
}

nb::object
Server::send(ServerEndpoint const &client_ep,
             nb::ndarray<uint8_t, nb::ndim<1>, nb::device::cpu> const &buffer,
             uint64_t tag, nb::object done_callback, nb::object fail_callback) {
  if (status_.load(std::memory_order_acquire) != 2) {
    std::runtime_error(
        "Server: not running. You can only  send , after listen.");
  }
  auto buf_ptr = reinterpret_cast<std::byte *>(buffer.data());
  auto buf_size = buffer.size();
  auto *p_future = new ServerSendFuture(this, std::move(done_callback),
                                        std::move(fail_callback));
  nb::object send_future_obj = nb::cast(p_future);
  {
    nb::gil_scoped_release release;
    chan_.wait_emplace<ServerSendArgs>([&](auto *ptr) {
      // GIL may not be required here as we "move" the object
      nb::gil_scoped_acquire acquire;
      new (ptr) ServerSendArgs(std::move(send_future_obj), client_ep.ep, tag,
                               buf_ptr, buf_size);
    });
  }
  return send_future_obj;
}

nb::object
Server::recv(nb::ndarray<uint8_t, nb::ndim<1>, nb::device::cpu> &buffer,
             uint64_t tag, uint64_t tag_mask, nb::object done_callback,
             nb::object fail_callback) {
  if (status_.load(std::memory_order_acquire) != 2) {
    std::runtime_error(
        "Server: not running. You can only  recv , after listen.");
  }
  auto buf_ptr = reinterpret_cast<std::byte *>(buffer.data());
  auto buf_size = buffer.size();
  auto *p_future = new ServerRecvFuture(this, std::move(done_callback),
                                        std::move(fail_callback));
  nb::object recv_future_obj = nb::cast(p_future);
  {
    nb::gil_scoped_release release;
    chan_.wait_emplace<ServerRecvArgs>([&](auto *ptr) {
      // GIL may not be required here as we "move" the object
      nb::gil_scoped_acquire acquire;
      new (ptr) ServerRecvArgs(std::move(recv_future_obj), tag, tag_mask,
                               buf_ptr, buf_size);
    });
  }
  return recv_future_obj;
}

auto Server::list_clients() const -> std::set<ServerEndpoint> const & {
  return eps_;
}

Server::~Server() {
  nb::gil_scoped_release release;
  auto cur = status_.load(std::memory_order_acquire);
  if (cur == 1 || cur == 2) {
    fatal_print("Server: not closed, trying to close in dtor...");
    assert(working_thread_.joinable());
    chan_.wait_emplace<ServerCloseArgs>([&](auto *ptr) {
      // GIL may be required here
      nb::gil_scoped_acquire acquire;
      new (ptr) ServerCloseArgs(nb::object());
    });
    if (working_thread_.joinable()) {
      debug_print("Server: start to join working thread...");
      working_thread_.join();
    }
  }
  assert(working_thread_.joinable() == false);
  debug_print("Server: dtor main done.");
}

NB_MODULE(_bindings, m) {
  nb::class_<Context>(m, "Context").def(nb::init<>());

  nb::class_<ServerEndpoint>(m, "ServerEndpoint")
      .def_ro("name", &ServerEndpoint::name)
      .def_ro("local_addr", &ServerEndpoint::local_addr)
      .def_ro("local_port", &ServerEndpoint::local_port)
      .def_ro("remote_addr", &ServerEndpoint::remote_addr)
      .def_ro("remote_port", &ServerEndpoint::remote_port)
      .def("view_transports", &ServerEndpoint::view_transports);

  nb::class_<ServerSendFuture>(m, "ServerSendFuture")
      .def(nb::init<Server *, nb::object, nb::object>())
      .def("done", &ServerSendFuture::done)
      .def("exception", &ServerSendFuture::excpetion);

  nb::class_<ServerRecvFuture>(m, "ServerRecvFuture")
      .def(nb::init<Server *, nb::object, nb::object>())
      .def("done", &ServerRecvFuture::done)
      .def("exception", &ServerRecvFuture::excpetion)
      .def("result", &ServerRecvFuture::result);

  nb::class_<Server>(m, "Server")
      .def(nb::init<Context &>(), "ctx"_a)
      .def("set_accept_callback", &Server::set_accept_callback, "callback"_a)
      .def("listen", &Server::listen, "addr"_a, "port"_a)
      .def("close", &Server::close, "callback"_a)
      .def("send", &Server::send, "client_ep"_a, "buffer"_a, "tag"_a,
           "done_callback"_a, "fail_callback"_a)
      .def("recv", &Server::recv, "buffer"_a, "tag"_a, "tag_mask"_a,
           "done_callback"_a, "fail_callback"_a)
      .def("list_clients", &Server::list_clients,
           nb::rv_policy::reference_internal);

  nb::class_<ClientSendFuture>(m, "ClientSendFuture")
      .def(nb::init<Client *, nb::object, nb::object>())
      .def("done", &ClientSendFuture::done)
      .def("exception", &ClientSendFuture::excpetion);
  nb::class_<ClientRecvFuture>(m, "ClientRecvFuture")
      .def(nb::init<Client *, nb::object, nb::object>())
      .def("done", &ClientRecvFuture::done)
      .def("exception", &ClientRecvFuture::excpetion)
      .def("result", &ClientRecvFuture::result);

  nb::class_<Client>(m, "Client")
      .def(nb::init<Context &>(), "ctx"_a)
      .def("connect", &Client::connect, "addr"_a, "port"_a, "callback"_a)
      .def("close", &Client::close, "callback"_a)
      .def("send", &Client::send, "buffer"_a, "tag"_a, "done_callback"_a,
           "fail_callback"_a)
      .def("recv", &Client::recv, "buffer"_a, "tag"_a, "tag_mask"_a,
           "done_callback"_a, "fail_callback"_a);
}
