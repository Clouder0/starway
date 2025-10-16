#include "main.hpp"
#include "nanobind/nanobind.h"
#include "ucp/api/ucp_compat.h"
#include "ucp/api/ucp_def.h"
#include "ucs/type/status.h"
#include <arpa/inet.h>
#include <atomic>
#include <cassert>
#include <compare>
#include <format>
#include <iostream>
#include <nanobind/stl/shared_ptr.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>
#include <netinet/in.h>
#include <string>
#include <thread>
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

inline bool static ucp_check_valid_close_status(ucs_status_t status) {
  return status == UCS_OK || status == UCS_ERR_CONNECTION_RESET;
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

void ClientSendFuture::set_result() {
  debug_print("Client Send Future done.");
  assert(done_callback_.is_valid());
  done_callback_();
}
void ClientSendFuture::set_exception(ucs_status_t result) {
  debug_print("Client Send Future failed with {}.", ucs_status_string(result));
  assert(fail_callback_.is_valid());
  fail_callback_(nb::cast(ucs_status_string(result)));
}

ClientRecvFuture::ClientRecvFuture(Client *client, auto &&done_callback,
                                   auto &&fail_callback)
  requires UniRef<decltype(done_callback), nb::object> &&
               UniRef<decltype(fail_callback), nb::object>
    : client_(client),
      done_callback_(std::forward<decltype(done_callback)>(done_callback)),
      fail_callback_(std::forward<decltype(fail_callback)>(fail_callback)) {}

void ClientRecvFuture::set_result(uint64_t sender_tag, size_t length) {
  debug_print("Client Recv Future done.");
  assert(done_callback_.is_valid());
  done_callback_(nb::cast(sender_tag), nb::cast(length));
}
void ClientRecvFuture::set_exception(ucs_status_t result) {
  debug_print("Client Recv Future failed with {}.", ucs_status_string(result));
  assert(fail_callback_.is_valid());
  fail_callback_(nb::cast(ucs_status_string(result)));
}

ClientFlushFuture::ClientFlushFuture(Client *client, auto &&done_callback,
                                     auto &&fail_callback)
  requires UniRef<decltype(done_callback), nb::object> &&
               UniRef<decltype(fail_callback), nb::object>
    : client_(client), req_(nullptr),
      done_callback_(std::forward<decltype(done_callback)>(done_callback)),
      fail_callback_(std::forward<decltype(fail_callback)>(fail_callback)) {}

void ClientFlushFuture::set_result() {
  debug_print("Client Flush Future done.");
  assert(done_callback_.is_valid());
  done_callback_();
}
void ClientFlushFuture::set_exception(ucs_status_t result) {
  fatal_print("Client Flush Future failed with {}.",
              ucs_status_string(result));
  assert(fail_callback_.is_valid());
  fail_callback_(nb::cast(ucs_status_string(result)));
}

Client::Client(Context &ctx) : ctx_(ctx.context_) {}

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
    if (status == UCS_OK) {
      debug_print("Client: waited send future done.");
      assert(send_future->req_ == req);
      send_future->set_result();
    } else {
      fatal_print("Client: waited send future done. Bad status. {}",
                  ucs_status_string(status));
      assert(send_future->req_ == req);
      send_future->set_exception(status);
    }
    send_future->client_->send_futures_.erase(send_future);
    delete send_future;
  }
  // free the request
  ucp_request_free(req);
}

void client_flush_cb(void *req, ucs_status_t status, void *user_data) {
  auto *flush_future = reinterpret_cast<ClientFlushFuture *>(user_data);
  {
    nb::gil_scoped_acquire acquire;
    if (status == UCS_OK) {
      debug_print("Client: waited flush future done.");
      assert(flush_future->req_ == req);
      flush_future->set_result();
    } else {
      fatal_print("Client: waited flush future done. Bad status. {}",
                  ucs_status_string(status));
      assert(flush_future->req_ == req);
      flush_future->set_exception(status);
    }
    flush_future->client_->flush_futures_.erase(flush_future);
    delete flush_future;
  }
  ucp_request_free(req);
}

void client_recv_cb(void *req, ucs_status_t status,
                    ucp_tag_recv_info_t const *info, void *args) {
  auto *recv_future = reinterpret_cast<ClientRecvFuture *>(args);
  {
    nb::gil_scoped_acquire acquire;
    if (status == UCS_OK) {
      debug_print("Client: waited recv future done.");
      assert(recv_future->req_ == req);
      recv_future->set_result(info->sender_tag, info->length);
    } else {
      fatal_print("Client: waited recv future done. Bad status. {}",
                  ucs_status_string(status));
      assert(recv_future->req_ == req);
      recv_future->set_exception(status);
    }
    recv_future->client_->recv_futures_.erase(recv_future);
    delete recv_future;
  }
  ucp_request_free(req);
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
    nb::gil_scoped_acquire acquire;
    assert(connect_callback_.is_valid());
    connect_callback_(nb::cast(""));
    connect_callback_.reset();
  };
  auto connect_failed = [&](ucs_status_t reason) {
    fatal_print("Client: init ep flush failed: {}", ucs_status_string(reason));
    nb::gil_scoped_acquire acquire;
    assert(connect_callback_.is_valid());
    connect_callback_(nb::cast(ucs_status_string(reason)));
    connect_callback_.reset();
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
  while (status_.load(std::memory_order_acquire) == 2) {
    ucp_worker_progress(worker);
    send_args_.try_consume([&](auto *ptr) {
      ClientSendArgs &args = *ptr;
      ucp_request_param_t send_param{.op_attr_mask =
                                         UCP_OP_ATTR_FIELD_CALLBACK |
                                         UCP_OP_ATTR_FIELD_USER_DATA,
                                     .cb{.send = client_send_cb},
                                     .user_data = args.send_future};
      auto req = ucp_tag_send_nbx(ep, args.buf_ptr, args.buf_size, args.tag,
                                  &send_param);
      if (req == NULL) {
        debug_print("Client: send request immediate success.");
        nb::gil_scoped_acquire acquire;
        args.send_future->set_result();
        delete args.send_future;
        return;
      }
      if (UCS_PTR_IS_ERR(req)) {
        fatal_print("Client: send request failed with {}",
                    ucs_status_string(UCS_PTR_STATUS(req)));
        nb::gil_scoped_acquire acquire;
        args.send_future->set_exception(UCS_PTR_STATUS(req));
        delete args.send_future;
        return;
      }
      // add to vector to trace its lifetime
      args.send_future->req_ = req;
      send_futures_.emplace(args.send_future); // takes ownership
      // args.send_future = nullptr;  // not needed as we use PIPE mechanism
      return;
    });
    recv_args_.try_consume([&](auto *ptr) {
      ClientRecvArgs &args = *ptr;
      ucp_tag_recv_info_t tag_info{};
      ucp_request_param_t recv_param{
          .op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                          UCP_OP_ATTR_FIELD_USER_DATA |
                          UCP_OP_ATTR_FIELD_RECV_INFO,
          .cb{.recv = client_recv_cb},
          .user_data = args.recv_future,
          .recv_info{.tag_info = &tag_info},
      };
      auto req = ucp_tag_recv_nbx(worker, args.buf_ptr, args.buf_size, args.tag,
                                  args.tag_mask, &recv_param);
      if (req == NULL) {
        debug_print("Client: recv request immediate success.");
        nb::gil_scoped_acquire acquire;
        args.recv_future->set_result(tag_info.sender_tag, tag_info.length);
        delete args.recv_future;
        return;
      }
      if (UCS_PTR_IS_ERR(req)) {
        fatal_print("Client: recv request failed with {}",
                    ucs_status_string(UCS_PTR_STATUS(req)));
        nb::gil_scoped_acquire acquire;
        args.recv_future->set_exception(UCS_PTR_STATUS(req));
        delete args.recv_future;
        return;
      }
      args.recv_future->req_ = req;
      recv_futures_.emplace(args.recv_future); // takes ownership
      // args.recv_future = nullptr;
      return;
    });
    flush_args_.try_consume([&](auto *ptr) {
      ClientFlushArgs &args = *ptr;
      ucp_request_param_t param{.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                                               UCP_OP_ATTR_FIELD_USER_DATA,
                                .cb{.send = client_flush_cb},
                                .user_data = args.flush_future};
      auto *req = ucp_worker_flush_nbx(worker, &param);
      if (UCS_PTR_STATUS(req) == UCS_OK) {
        debug_print("Client: flush success immediately");
        nb::gil_scoped_acquire acquire;
        args.flush_future->set_result();
        delete args.flush_future;
        return;
      } else if (UCS_PTR_IS_ERR(req)) {
        fatal_print("Client: flush failed with {}",
                    ucs_status_string(UCS_PTR_STATUS(req)));
        nb::gil_scoped_acquire acquire;
        args.flush_future->set_exception(UCS_PTR_STATUS(req));
        delete args.flush_future;
        return;
      }
      debug_print("Client: async flush future created pending.");
      args.flush_future->req_ = req;
      flush_futures_.emplace(args.flush_future);
      return;
    });
    if (perf_status_.load(std::memory_order_acquire) == 1) {
      ucp_ep_evaluate_perf_attr_t perf_attr{
          .field_mask = UCP_EP_PERF_ATTR_FIELD_ESTIMATED_TIME,
      };
      ucp_ep_evaluate_perf_param_t params{
          .field_mask = UCP_EP_PERF_PARAM_FIELD_MESSAGE_SIZE,
          .message_size = perf_args_.msg_size,
      };
      auto status = ucp_ep_evaluate_perf(ep, &params, &perf_attr);
      if (status != UCS_OK) [[unlikely]] {
        fatal_print("Server: evaluate perf failed with {}",
                    ucs_status_string(status));
      }
      perf_result_ = perf_attr.estimated_time;
      perf_status_.store(0, std::memory_order_release);
    }
  }
  // final cleanup process
  debug_print("Client: start close...");
  while (ucp_worker_progress(worker) > 0) {
  }
  debug_print("Client: done tailing worker.");

  debug_print("Client: close channels");
  send_args_.close();
  recv_args_.close();
  flush_args_.close();
  // potential pending requests would be resovled as cancelled in dtor
  cancel_pending_reqs();

  // cleanup requests, cancel them all
  debug_print("Client: start cancel requests.");
  {
    // hold a temporary cache to avoid invalidation after delete
    std::vector<ClientSendFuture *> cur_send_futures{send_futures_.begin(),
                                                     send_futures_.end()};
    std::vector<ClientRecvFuture *> cur_recv_futures{recv_futures_.begin(),
                                                     recv_futures_.end()};
    std::vector<ClientFlushFuture *> cur_flush_futures{flush_futures_.begin(),
                                                       flush_futures_.end()};
    for (auto *p : cur_send_futures) {
      assert(ucp_request_check_status(p->req_) == UCS_INPROGRESS);
      // this should trigger callback and delete the pointer
      ucp_request_cancel(worker, p->req_);
      // now that p has been freed
    }
    for (auto *p : cur_recv_futures) {
      assert(ucp_request_check_status(p->req_) == UCS_INPROGRESS);
      ucp_request_cancel(worker, p->req_);
      // now that p has been freed
    }
    for (auto *p : cur_flush_futures) {
      assert(ucp_request_check_status(p->req_) == UCS_INPROGRESS);
      ucp_request_cancel(worker, p->req_);
    }
  }
  debug_print("Client: done cancel requests.");

  // close endpoint
  debug_print("Client: start close endpoint.");
  {
    ucp_request_param_t param{};
    auto status = ucp_ep_close_nbx(ep, &param);
    if (status == NULL) {
      debug_print("Client: close ep immediate success.");
    } else if (UCS_PTR_IS_ERR(status)) {
      if (ucp_check_valid_close_status(UCS_PTR_STATUS(status))) {
        debug_print("Client: close ep immediate success with valid ERR {}",
                    ucs_status_string(UCS_PTR_STATUS(status)));
      } else {
        fatal_print("Client: close ep failed with {}",
                    ucs_status_string(UCS_PTR_STATUS(status)));
      }
    } else {
      while (ucp_request_check_status(status) == UCS_INPROGRESS) {
        ucp_worker_progress(worker);
      }
      auto final_status = ucp_request_check_status(status);
      if (!ucp_check_valid_close_status(final_status)) {
        fatal_print("Client: close ep failed with {}",
                    ucs_status_string(final_status));
      }
      ucp_request_free(status);
    }
  }
  debug_print("Client: done close endpoint.");
  debug_print("Client: worker thread close done.");
  {
    nb::gil_scoped_acquire acquire;
    if (!close_callback_.is_none() && close_callback_.is_valid()) {
      close_callback_();
      close_callback_.reset();
    } else {
      fatal_print("Client: close callback is invalid.");
    }
  }
  status_.store(4, std::memory_order_release);
}

void Client::connect(std::string addr, uint64_t port, nb::object callback) {
  if (status_.load(std::memory_order_acquire) != 0) {
    throw std::runtime_error("Client: already connected. You can only connect "
                             "once, and cannot reconnect after close.");
  }
  connect_callback_ = std::move(callback);
  working_thread_ =
      std::thread([this, addr, port]() { this->start_working(addr, port); });
}
void Client::close(nb::object callback) {
  if (status_.load(std::memory_order_acquire) != 2) {
    throw std::runtime_error("Client: not running. You can only close "
                             "once, after connect done.");
  }
  close_callback_ = std::move(callback);
  status_.store(3, std::memory_order_release);
}

void Client::send(
    nb::ndarray<uint8_t, nb::ndim<1>, nb::device::cpu> const &buffer,
    uint64_t tag, nb::object done_callback, nb::object fail_callback) {
  if (status_.load(std::memory_order_acquire) != 2) {
    throw std::runtime_error(
        "Client: not running. You can only  send , after connect.");
  }
  auto buf_ptr = reinterpret_cast<std::byte *>(buffer.data());
  auto buf_size = buffer.size();
  auto p_future = new ClientSendFuture(this, std::move(done_callback),
                                       std::move(fail_callback));
  {
    nb::gil_scoped_release release;
    auto sucecess = send_args_.wait_emplace([&](auto *ptr) {
      new (ptr) ClientSendArgs(std::move(p_future), tag, buf_ptr, buf_size);
    });
    if (!sucecess) {
      p_future->set_exception(UCS_ERR_NOT_CONNECTED);
      delete p_future;
    }
  }
}
void Client::recv(nb::ndarray<uint8_t, nb::ndim<1>, nb::device::cpu> &buffer,
                  uint64_t tag, uint64_t tag_mask, nb::object done_callback,
                  nb::object fail_callback) {
  if (status_.load(std::memory_order_acquire) != 2) {
    throw std::runtime_error(
        "Client: not running. You can only  recv , after connect.");
  }
  auto buf_ptr = reinterpret_cast<std::byte *>(buffer.data());
  auto buf_size = buffer.size();
  auto p_future = new ClientRecvFuture(this, std::move(done_callback),
                                       std::move(fail_callback));
  {
    nb::gil_scoped_release release;
    auto sucecess = recv_args_.wait_emplace([&](auto *ptr) {
      new (ptr)
          ClientRecvArgs(std::move(p_future), tag, tag_mask, buf_ptr, buf_size);
    });
    if (!sucecess) [[unlikely]] {
      p_future->set_exception(UCS_ERR_NOT_CONNECTED);
      delete p_future;
    }
  }
}

void Client::flush(nb::object done_callback, nb::object fail_callback) {
  if (status_.load(std::memory_order_acquire) != 2) [[unlikely]] {
    throw std::runtime_error(
        "Client: not running. You can only flush, after connect.");
  }
  auto p_future = new ClientFlushFuture(this, std::move(done_callback),
                                        std::move(fail_callback));
  {
    nb::gil_scoped_release release;
    auto success = flush_args_.wait_emplace(
        [&](auto *ptr) { new (ptr) ClientFlushArgs{p_future}; });
    if (!success) [[unlikely]] {
      p_future->set_exception(UCS_ERR_NOT_CONNECTED);
      delete p_future;
    }
  }
}
double Client::evaluate_perf(size_t msg_size) {
  if (status_.load(std::memory_order_acquire) != 2) [[unlikely]] {
    throw std::runtime_error(
        "Server: not running. You can only evaluate perf, after listen.");
  }
  nb::gil_scoped_release release;
  perf_args_ = ClientPerfArgs(msg_size);
  perf_status_.store(1, std::memory_order_release);
  while (perf_status_.load(std::memory_order_acquire) == 1) {
    std::this_thread::yield();
  }
  return perf_result_;
}

void Client::cancel_pending_reqs() {
  // now that pipe closed, however there may be some pending requests,
  // resolve them
  send_args_.try_consume([&](auto *ptr) {
    ClientSendArgs &args = *ptr;
    nb::gil_scoped_acquire acquire;
    args.send_future->set_exception(UCS_ERR_CANCELED);
    delete args.send_future;
  });
  recv_args_.try_consume([&](auto *ptr) {
    ClientRecvArgs &args = *ptr;
    nb::gil_scoped_acquire acquire;
    args.recv_future->set_exception(UCS_ERR_CANCELED);
    delete args.recv_future;
  });
  flush_args_.try_consume([&](auto *ptr) {
    ClientFlushArgs &args = *ptr;
    nb::gil_scoped_acquire acquire;
    args.flush_future->set_exception(UCS_ERR_CANCELED);
    delete args.flush_future;
  });
}

Client::~Client() {
  nb::gil_scoped_release release;
  auto cur = status_.load(std::memory_order_acquire);
  if (cur == 1 || cur == 2) {
    fatal_print("Client: not closed, trying to close in dtor...");
    assert(working_thread_.joinable());
    status_.store(3, std::memory_order_release);
  }
  if (working_thread_.joinable()) {
    debug_print("Client: start to join working thread...");
    working_thread_.join();
    debug_print("Client: join working thread done.");
  }

  cancel_pending_reqs();
  debug_print("Client: dtor main done.");
}

ServerSendFuture::ServerSendFuture(Server *server, auto &&done_callback,
                                   auto &&fail_callback)
  requires UniRef<decltype(done_callback), nb::object> &&
               UniRef<decltype(fail_callback), nb::object>
    : server_(server),
      done_callback_(std::forward<decltype(done_callback)>(done_callback)),
      fail_callback_(std::forward<decltype(fail_callback)>(fail_callback)) {}

void ServerSendFuture::set_result() {
  debug_print("Server Send Future done.");
  assert(done_callback_.is_valid());
  done_callback_();
}
void ServerSendFuture::set_exception(ucs_status_t result) {
  fatal_print("Server Send Future failed with {}.", ucs_status_string(result));
  assert(fail_callback_.is_valid());
  fail_callback_(nb::cast(ucs_status_string(result)));
}

ServerRecvFuture::ServerRecvFuture(Server *server, auto &&done_callback,
                                   auto &&fail_callback)
  requires UniRef<decltype(done_callback), nb::object> &&
               UniRef<decltype(fail_callback), nb::object>
    : server_(server),
      done_callback_(std::forward<decltype(done_callback)>(done_callback)),
      fail_callback_(std::forward<decltype(fail_callback)>(fail_callback)) {}

void ServerRecvFuture::set_result(uint64_t sender_tag, size_t length) {
  debug_print("Server Recv Future done.");
  assert(done_callback_.is_valid());
  done_callback_(nb::cast(sender_tag), nb::cast(length));
}
void ServerRecvFuture::set_exception(ucs_status_t result) {
  fatal_print("Server Recv Future failed with {}.", ucs_status_string(result));
  assert(fail_callback_.is_valid());
  fail_callback_(nb::cast(ucs_status_string(result)));
}
ServerFlushFuture::ServerFlushFuture(Server *server, auto &&done_callback,
                                     auto &&fail_callback)
  requires UniRef<decltype(done_callback), nb::object> &&
               UniRef<decltype(fail_callback), nb::object>
    : server_(server),
      done_callback_(std::forward<decltype(done_callback)>(done_callback)),
      fail_callback_(std::forward<decltype(fail_callback)>(fail_callback)) {}
void ServerFlushFuture::set_result() {
  debug_print("Server Flush Future done.");
  assert(done_callback_.is_valid());
  done_callback_();
}
void ServerFlushFuture::set_exception(ucs_status_t result) {
  fatal_print("Server Flush Future failed with {}.", ucs_status_string(result));
  assert(fail_callback_.is_valid());
  fail_callback_(nb::cast(ucs_status_string(result)));
}

ServerFlushEpFuture::ServerFlushEpFuture(Server *server, ucp_ep_h ep,
                                         auto &&done_callback,
                                         auto &&fail_callback)
  requires UniRef<decltype(done_callback), nb::object> &&
               UniRef<decltype(fail_callback), nb::object>
    : server_(server), ep_(ep), req_(nullptr),
      done_callback_(std::forward<decltype(done_callback)>(done_callback)),
      fail_callback_(std::forward<decltype(fail_callback)>(fail_callback)) {}
void ServerFlushEpFuture::set_result() {
  debug_print("Server Flush Ep Future done.");
  assert(done_callback_.is_valid());
  done_callback_();
}
void ServerFlushEpFuture::set_exception(ucs_status_t result) {
  fatal_print("Server Flush Ep Future failed with {}.",
              ucs_status_string(result));
  assert(fail_callback_.is_valid());
  fail_callback_(nb::cast(ucs_status_string(result)));
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
                          Server *server) {}

void server_send_cb(void *req, ucs_status_t status, void *user_data) {
  auto *send_future = reinterpret_cast<ServerSendFuture *>(user_data);
  {
    nb::gil_scoped_acquire acquire;
    if (status == UCS_OK) {
      debug_print("Server: waited send future done.");
      send_future->set_result();
    } else {
      fatal_print("Server: waited send future done. Bad status. {}",
                  ucs_status_string(status));
      send_future->set_exception(status);
    }
    send_future->server_->send_futures_.erase(send_future);
    delete send_future;
  }
  // free the request
  ucp_request_free(req);
}
void server_flush_cb(void *req, ucs_status_t status, void *user_data) {
  auto *flush_future = reinterpret_cast<ServerFlushFuture *>(user_data);
  {
    nb::gil_scoped_acquire acquire;
    if (status == UCS_OK) {
      debug_print("Server: waited flush future done.");
      flush_future->set_result();
    } else {
      fatal_print("Server: waited flush future done. Bad status. {}",
                  ucs_status_string(status));
      flush_future->set_exception(status);
    }
    flush_future->server_->flush_futures_.erase(flush_future);
    delete flush_future;
  }
  ucp_request_free(req);
}
void server_flush_ep_cb(void *req, ucs_status_t status, void *user_data) {
  auto *flush_future = reinterpret_cast<ServerFlushEpFuture *>(user_data);
  {
    nb::gil_scoped_acquire acquire;
    if (status == UCS_OK) {
      debug_print("Server: waited flush-ep future done.");
      flush_future->set_result();
    } else {
      fatal_print("Server: waited flush-ep future done. Bad status. {}",
                  ucs_status_string(status));
      flush_future->set_exception(status);
    }
    flush_future->server_->flush_ep_futures_.erase(flush_future);
    delete flush_future;
  }
  ucp_request_free(req);
}
void server_recv_cb(void *req, ucs_status_t status,
                    ucp_tag_recv_info_t const *info, void *args) {
  auto *recv_future = reinterpret_cast<ServerRecvFuture *>(args);
  {
    nb::gil_scoped_acquire acquire;
    if (status == UCS_OK) {
      debug_print("Server: waited recv future done.");
      assert(recv_future->req_ == req);
      recv_future->set_result(info->sender_tag, info->length);
    } else {
      fatal_print("Server: waited recv future done. Bad status. {}",
                  ucs_status_string(status));
      assert(recv_future->req_ == req);
      recv_future->set_exception(status);
    }
    recv_future->server_->recv_futures_.erase(recv_future);
    delete recv_future;
  }
  ucp_request_free(req);
}

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
  while (status_.load(std::memory_order_acquire) == 2) {
    ucp_worker_progress(worker);
    send_args_.try_consume([&](auto *ptr) {
      debug_print("Server: handle send message.");
      ServerSendArgs &args = *ptr;
      ucp_request_param_t send_param{.op_attr_mask =
                                         UCP_OP_ATTR_FIELD_CALLBACK |
                                         UCP_OP_ATTR_FIELD_USER_DATA,
                                     .cb{.send = server_send_cb},
                                     .user_data = args.send_future};
      auto *req = ucp_tag_send_nbx(args.ep, args.buf_ptr, args.buf_size,
                                   args.tag, &send_param);
      if (UCS_PTR_STATUS(req) == UCS_OK) {
        nb::gil_scoped_acquire acquire;
        debug_print("Server: send success immediately");
        args.send_future->set_result();
        delete args.send_future;
        return;
      }
      if (UCS_PTR_IS_ERR(req)) {
        debug_print("Server: send failed immediately, err: {}",
                    ucs_status_string(UCS_PTR_STATUS(req)));
        nb::gil_scoped_acquire acquire;
        args.send_future->set_exception(UCS_PTR_STATUS(ptr));
        delete args.send_future;
        return;
      }
      debug_print("Server: async send request pending  created.");
      args.send_future->req_ = req;
      send_futures_.emplace(args.send_future); // transfer ownership
      // args.send_future = nullptr;
      return;
    });
    recv_args_.try_consume([&](auto *ptr) {
      debug_print("Server: handle recv message.");
      ServerRecvArgs &args = *ptr;
      ucp_tag_recv_info_t tag_info{};
      ucp_request_param_t param{.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                                                UCP_OP_ATTR_FIELD_USER_DATA |
                                                UCP_OP_ATTR_FIELD_RECV_INFO,
                                .cb{.recv = server_recv_cb},
                                .user_data = args.recv_future,
                                .recv_info{
                                    .tag_info = &tag_info,
                                }};

      auto status = ucp_tag_recv_nbx(worker, args.buf_ptr, args.buf_size,
                                     args.tag, args.tag_mask, &param);
      if (status == NULL) {
        debug_print("Server: recv success immediately");
        nb::gil_scoped_acquire acquire;
        args.recv_future->set_result(tag_info.sender_tag, tag_info.length);
        delete args.recv_future;
        return;
      }
      if (UCS_PTR_IS_ERR(status)) {
        fatal_print("Server: recv failed with {}",
                    ucs_status_string(UCS_PTR_STATUS(ptr)));
        nb::gil_scoped_acquire acquire;
        args.recv_future->set_exception(UCS_PTR_STATUS(ptr));
        delete args.recv_future;
        return;
      }
      debug_print("Server: async recv future created pending.");
      args.recv_future->req_ = status;
      recv_futures_.emplace(args.recv_future);
      // args.recv_future = nullptr;
      return;
    });
    flush_args_.try_consume([&](auto *ptr) {
      debug_print("Server: handle flush message.");
      ServerFlushArgs &args = *ptr;
      ucp_request_param_t param{.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                                                UCP_OP_ATTR_FIELD_USER_DATA,
                                .cb{.send = server_flush_cb},
                                .user_data = args.flush_future};
      auto *req = ucp_worker_flush_nbx(worker, &param);
      if (UCS_PTR_STATUS(req) == UCS_OK) {
        debug_print("Server: flush success immediately");
        nb::gil_scoped_acquire acquire;
        args.flush_future->set_result();
        delete args.flush_future;
        return;
      } else if (UCS_PTR_IS_ERR(req)) {
        fatal_print("Server: flush failed with {}",
                    ucs_status_string(UCS_PTR_STATUS(req)));
        nb::gil_scoped_acquire acquire;
        args.flush_future->set_exception(UCS_PTR_STATUS(req));
        delete args.flush_future;
        return;
      } else {
        debug_print("Server: async flush future created pending.");
        args.flush_future->req_ = req;
        flush_futures_.emplace(args.flush_future); // transfer ownership
        // args.flush_future = nullptr;
        return;
      }
    });
    flush_ep_args_.try_consume([&](auto *ptr) {
      debug_print("Server: handle flush-ep message.");
      ServerFlushEpArgs &args = *ptr;
      ucp_request_param_t param{.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                                                UCP_OP_ATTR_FIELD_USER_DATA,
                                .cb{.send = server_flush_ep_cb},
                                .user_data = args.flush_future};
      auto *req = ucp_ep_flush_nbx(args.ep, &param);
      if (UCS_PTR_STATUS(req) == UCS_OK) {
        debug_print("Server: flush-ep success immediately");
        nb::gil_scoped_acquire acquire;
        args.flush_future->set_result();
        delete args.flush_future;
        return;
      } else if (UCS_PTR_IS_ERR(req)) {
        fatal_print("Server: flush-ep failed with {}",
                    ucs_status_string(UCS_PTR_STATUS(req)));
        nb::gil_scoped_acquire acquire;
        args.flush_future->set_exception(UCS_PTR_STATUS(req));
        delete args.flush_future;
        return;
      } else {
        debug_print("Server: async flush-ep future created pending.");
        args.flush_future->req_ = req;
        flush_ep_futures_.emplace(args.flush_future);
        return;
      }
    });
    if (perf_status_.load(std::memory_order_acquire) == 1) {
      ucp_ep_evaluate_perf_attr_t perf_attr{
          .field_mask = UCP_EP_PERF_ATTR_FIELD_ESTIMATED_TIME,
      };
      ucp_ep_evaluate_perf_param_t params{
          .field_mask = UCP_EP_PERF_PARAM_FIELD_MESSAGE_SIZE,
          .message_size = perf_args_.msg_size,
      };
      auto status = ucp_ep_evaluate_perf(perf_args_.ep, &params, &perf_attr);
      if (status != UCS_OK) [[unlikely]] {
        fatal_print("Server: evaluate perf failed with {}",
                    ucs_status_string(status));
      }
      perf_result_ = perf_attr.estimated_time;
      perf_status_.store(0, std::memory_order_release);
    }
  }
  debug_print("Server: start close...");
  while (ucp_worker_progress(worker) > 0) {
  }
  debug_print("Server: done tailing worker.");

  // close channel, pending requests should abort
  debug_print("Server: close channels");
  send_args_.close();
  recv_args_.close();
  flush_args_.close();
  flush_ep_args_.close();
  cancel_pending_reqs();

  // first close listener to avoid more incoming conns
  debug_print("Server: start close listener.");
  ucp_listener_destroy(listener);
  debug_print("Server: done close listener.");

  // cancel all existing requests
  debug_print("Server: start cancel requests.");
  {
    // avoid invalidation after delete
    std::vector<ServerSendFuture *> cur_send_futures{send_futures_.begin(),
                                                     send_futures_.end()};
    std::vector<ServerRecvFuture *> cur_recv_futures{recv_futures_.begin(),
                                                     recv_futures_.end()};
    std::vector<ServerFlushFuture *> cur_flush_futures{flush_futures_.begin(),
                                                       flush_futures_.end()};
    std::vector<ServerFlushEpFuture *> cur_flush_ep_futures{
        flush_ep_futures_.begin(), flush_ep_futures_.end()};

    for (auto *p : cur_send_futures) {
      assert(ucp_request_check_status(p->req_) == UCS_INPROGRESS);
      // this should trigger callback and delete the pointer
      ucp_request_cancel(worker, p->req_);
    }
    for (auto *p : cur_recv_futures) {
      assert(ucp_request_check_status(p->req_) == UCS_INPROGRESS);
      ucp_request_cancel(worker, p->req_);
    }
    for (auto *p : cur_flush_futures) {
      assert(ucp_request_check_status(p->req_) == UCS_INPROGRESS);
      ucp_request_cancel(worker, p->req_);
    }
    for (auto *p : cur_flush_ep_futures) {
      assert(ucp_request_check_status(p->req_) == UCS_INPROGRESS);
      ucp_request_cancel(worker, p->req_);
    }
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
      if (ucp_check_valid_close_status(UCS_PTR_STATUS(status))) {
        debug_print("Server: closed endpoint immediately with valid ERR {}.",
                    ucs_status_string(UCS_PTR_STATUS(status)));
      } else {
        fatal_print("Server: failed to close endpoint with {}",
                    ucs_status_string(UCS_PTR_STATUS(status)));
      }
    } else {
      while (ucp_request_check_status(status) == UCS_INPROGRESS) {
        ucp_worker_progress(worker);
      }
      auto final_status = ucp_request_check_status(status);
      if (!ucp_check_valid_close_status(final_status)) {
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
    if (!close_callback_.is_none() && close_callback_.is_valid()) {
      close_callback_();
    } else {
      fatal_print("Server: close callback is invalid.");
    }
  }
  status_.store(4, std::memory_order_release);
}

void Server::close(nb::object close_callback) {
  if (status_.load(std::memory_order_acquire) != 2) {
    throw std::runtime_error("Server: not running. You can only close "
                             "once, after listen done.");
  }
  close_callback_ = std::move(close_callback);
  status_.store(3, std::memory_order_release);
}

void Server::send(
    ServerEndpoint const &client_ep,
    nb::ndarray<uint8_t, nb::ndim<1>, nb::device::cpu> const &buffer,
    uint64_t tag, nb::object done_callback, nb::object fail_callback) {
  if (status_.load(std::memory_order_acquire) != 2) [[unlikely]] {
    throw std::runtime_error(
        "Server: not running. You can only send, after listen.");
  }
  auto buf_ptr = reinterpret_cast<std::byte *>(buffer.data());
  auto buf_size = buffer.size();
  auto p_future = new ServerSendFuture(this, std::move(done_callback),
                                       std::move(fail_callback));
  {
    nb::gil_scoped_release release;
    auto success = send_args_.wait_emplace([&](auto *ptr) {
      // GIL may not be required here as we "move" the object
      new (ptr) ServerSendArgs(p_future, client_ep.ep, tag, buf_ptr, buf_size);
    });
    if (!success) [[unlikely]] {
      p_future->set_exception(UCS_ERR_NOT_CONNECTED);
      delete p_future;
    }
  }
}

void Server::recv(nb::ndarray<uint8_t, nb::ndim<1>, nb::device::cpu> &buffer,
                  uint64_t tag, uint64_t tag_mask, nb::object done_callback,
                  nb::object fail_callback) {
  if (status_.load(std::memory_order_acquire) != 2) {
    throw std::runtime_error(
        "Server: not running. You can only recv, after listen.");
  }
  auto buf_ptr = reinterpret_cast<std::byte *>(buffer.data());
  auto buf_size = buffer.size();
  auto p_future = new ServerRecvFuture(this, std::move(done_callback),
                                       std::move(fail_callback));
  {
    nb::gil_scoped_release release;
    auto success = recv_args_.wait_emplace([&](auto *ptr) {
      new (ptr) ServerRecvArgs(p_future, tag, tag_mask, buf_ptr, buf_size);
    });
    // pipe closed
    if (!success) [[unlikely]] {
      p_future->set_exception(UCS_ERR_NOT_CONNECTED);
      delete p_future;
    }
  }
}

void Server::flush(nb::object done_callback, nb::object fail_callback) {
  if (status_.load(std::memory_order_acquire) != 2) [[unlikely]] {
    throw std::runtime_error(
        "Server: not running. You can only flush, after listen.");
  }
  auto p_future = new ServerFlushFuture(this, std::move(done_callback),
                                        std::move(fail_callback));
  {
    nb::gil_scoped_release release;
    auto success = flush_args_.wait_emplace(
        [&](auto *ptr) { new (ptr) ServerFlushArgs(p_future); });
    // pipe closed
    if (!success) [[unlikely]] {
      p_future->set_exception(UCS_ERR_NOT_CONNECTED);
      delete p_future;
    }
  }
}

void Server::flush_ep(ServerEndpoint const &client_ep, nb::object done_callback,
                      nb::object fail_callback) {
  if (status_.load(std::memory_order_acquire) != 2) [[unlikely]] {
    throw std::runtime_error(
        "Server: not running. You can only flush ep, after listen.");
  }
  auto p_future = new ServerFlushEpFuture(this, client_ep.ep,
                                          std::move(done_callback),
                                          std::move(fail_callback));
  {
    nb::gil_scoped_release release;
    auto success = flush_ep_args_.wait_emplace([&](auto *ptr) {
      new (ptr) ServerFlushEpArgs(p_future, client_ep.ep);
    });
    if (!success) [[unlikely]] {
      p_future->set_exception(UCS_ERR_NOT_CONNECTED);
      delete p_future;
    }
  }
}

auto Server::list_clients() const -> std::set<ServerEndpoint> const & {
  return eps_;
}
void Server::cancel_pending_reqs() {
  // now that pipe closed, however there may be some pending requests,
  // resolve them
  send_args_.try_consume([&](auto *ptr) {
    ServerSendArgs &args = *ptr;
    nb::gil_scoped_acquire acquire;
    args.send_future->set_exception(UCS_ERR_CANCELED);
    delete args.send_future;
  });
  recv_args_.try_consume([&](auto *ptr) {
    ServerRecvArgs &args = *ptr;
    nb::gil_scoped_acquire acquire;
    args.recv_future->set_exception(UCS_ERR_CANCELED);
    delete args.recv_future;
  });
  flush_args_.try_consume([&](auto *ptr) {
    ServerFlushArgs &args = *ptr;
    nb::gil_scoped_acquire acquire;
    args.flush_future->set_exception(UCS_ERR_CANCELED);
    delete args.flush_future;
  });
  flush_ep_args_.try_consume([&](auto *ptr) {
    ServerFlushEpArgs &args = *ptr;
    nb::gil_scoped_acquire acquire;
    args.flush_future->set_exception(UCS_ERR_CANCELED);
    delete args.flush_future;
  });
}

double Server::evaluate_perf(ServerEndpoint const &client_ep, size_t msg_size) {
  if (status_.load(std::memory_order_acquire) != 2) [[unlikely]] {
    throw std::runtime_error(
        "Server: not running. You can only evaluate perf, after listen.");
  }
  nb::gil_scoped_release release;
  perf_args_ = ServerPerfArgs(client_ep.ep, msg_size);
  perf_status_.store(1, std::memory_order_release);
  while (perf_status_.load(std::memory_order_acquire) == 1) {
    std::this_thread::yield();
  }
  return perf_result_;
}

Server::~Server() {
  nb::gil_scoped_release release;
  auto cur = status_.load(std::memory_order_acquire);
  if (cur == 1 || cur == 2) {
    fatal_print(
        "Server: not closed, trying to close in dtor... FATAL: this "
        "would cause subtle bugs and may SIGABRT!!! Please call close first.");
    assert(working_thread_.joinable());
    status_.store(3, std::memory_order_release);
  }
  if (working_thread_.joinable()) {
    debug_print("Server: start to join working thread...");
    working_thread_.join();
    debug_print("Server: join working thread done.");
  }
  cancel_pending_reqs();
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

  nb::class_<Server>(m, "Server")
      .def(nb::init<Context &>(), "ctx"_a)
      .def("set_accept_callback", &Server::set_accept_callback, "callback"_a)
      .def("listen", &Server::listen, "addr"_a, "port"_a)
      .def("close", &Server::close, "callback"_a)
      .def("send", &Server::send, "client_ep"_a, "buffer"_a, "tag"_a,
           "done_callback"_a, "fail_callback"_a)
      .def("recv", &Server::recv, "buffer"_a, "tag"_a, "tag_mask"_a,
           "done_callback"_a, "fail_callback"_a)
      .def("flush", &Server::flush, "done_callback"_a, "fail_callback"_a)
      .def("flush_ep", &Server::flush_ep, "client_ep"_a, "done_callback"_a,
           "fail_callback"_a)
      .def("list_clients", &Server::list_clients,
           nb::rv_policy::reference_internal)
      .def("evaluate_perf", &Server::evaluate_perf, "client_ep"_a,
           "msg_size"_a);

  nb::class_<Client>(m, "Client")
      .def(nb::init<Context &>(), "ctx"_a)
      .def("connect", &Client::connect, "addr"_a, "port"_a, "callback"_a)
      .def("close", &Client::close, "callback"_a)
      .def("send", &Client::send, "buffer"_a, "tag"_a, "done_callback"_a,
           "fail_callback"_a)
      .def("recv", &Client::recv, "buffer"_a, "tag"_a, "tag_mask"_a,
           "done_callback"_a, "fail_callback"_a)
      .def("flush", &Client::flush, "done_callback"_a, "fail_callback"_a)
      .def("evaluate_perf", &Client::evaluate_perf, "msg_size"_a);
}
