#include <arpa/inet.h>
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
#include <print>
#include <set>
#include <thread>
#include <ucp/api/ucp.h>
#include <ucp/api/ucp_compat.h>
#include <ucs/type/status.h>
#include <ucs/type/thread_mode.h>
#include <utility>

namespace nb = nanobind;
using namespace nb::literals;

template <typename... _Args>
inline void constexpr debug_print(std::format_string<_Args...> __fmt,
                                  _Args &&...__args) {
#ifdef DEBUG
  std::println(stdout, __fmt, std::forward<_Args>(__args)...);
#else
#endif
}

inline void ucp_check_status(ucs_status_t status, std::string_view msg) {
  if (status != UCS_OK) {
    throw std::runtime_error(
        "UCP error: " + std::string(ucs_status_string(status)) + " - " +
        std::string(msg));
  }
}

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
    auto send_future_obj = nb::cast(
        new ClientSendFuture(this, nullptr, done_callback, fail_callback));
    auto send_future = nb::inst_ptr<ClientSendFuture>(send_future_obj);
    // wait for todo release
    {
      nb::gil_scoped_release release;
      uint8_t expected{0};
      for (;;) {
        if (todo_.compare_exchange_weak(expected, 1,
                                        std::memory_order_acq_rel)) {
          break;
        }
        while (todo_.load(std::memory_order_acquire) != 0) {
          std::this_thread::yield();
        }
      }
    }
    todo_content_ =
        std::make_tuple(send_future_obj, tag, buffer.data(), buffer.size());
    todo_.store(2, std::memory_order_release);
    // spin until pick up
    // {
    //   nb::gil_scoped_release release;
    //   while (frame->todo_.load(std::memory_order_acquire) == 2) {
    //     std::this_thread::yield();
    //   }
    // }
    return send_future;
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
      while (ucp_worker_progress(worker_) > 0) {
        debug_print("Client made some progress");
      }
      auto cur_todo = todo_.load(std::memory_order_acquire);
      if (cur_todo == 2) {
        static size_t received{0};
        nb::gil_scoped_acquire acquire;
        received++;
        debug_print("Total Client received send request: {}", received);
        auto [send_future_obj, tag, buf_ptr, buf_size] = todo_content_;
        todo_content_ = {};
        todo_.store(0, std::memory_order_release);
        auto send_future = nb::inst_ptr<ClientSendFuture>(send_future_obj);

        ucp_request_param_t send_param{.op_attr_mask =
                                           UCP_OP_ATTR_FIELD_CALLBACK |
                                           UCP_OP_ATTR_FIELD_USER_DATA,
                                       .cb{.send = send_cb},
                                       .user_data = send_future};
        auto *req = ucp_tag_send_nbx(ep_, buf_ptr, buf_size, tag, &send_param);
        if (UCS_PTR_STATUS(req) == UCS_OK) {
          // completed immediately
          debug_print("Req done immediately.");
          send_future->set_result(UCS_OK);
        } else if (UCS_PTR_IS_ERR(req)) {
          debug_print("Req fail immediately.");
          send_future->set_result(UCS_PTR_STATUS(req));
        } else {
          send_future->req_ = req;
          sends_.insert(send_future_obj);
        }
        debug_print("Worker handled send request.");
      }
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
        if (ucp_request_check_status(req) == UCS_INPROGRESS) {
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
                    std::to_underlying(flush_req_status));
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
  std::atomic<uint8_t> todo_{false}; // 0: spare 1: loading 2 : loaded
  std::tuple<nb::object, uint64_t, uint8_t *, size_t> todo_content_{};
  std::set<nb::object,
           decltype([](nb::object const &lhs, nb::object const &rhs) {
             return lhs.ptr() < rhs.ptr();
           })>
      sends_{};
};

// Server
struct Server;
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
      // block until free
      nb::gil_scoped_release release;
      uint8_t expected{0};
      for (;;) {
        if (to_recv_.compare_exchange_weak(expected, 1,
                                           std::memory_order_acq_rel)) {
          break;
        }
        while (to_recv_.load(std::memory_order_acquire) != 0) {
          std::this_thread::yield();
        }
      }
    }
    recv_args_ =
        std::make_tuple(recv_future_obj, buf_ptr, buf_size, tag, tag_mask);
    to_recv_.store(2, std::memory_order_release);
    // wait until consumed
    // {
    //   nb::gil_scoped_release release;
    //   while (to_recv_.load(std::memory_order_acquire) == 2) {
    //   }
    // }
    return recv_future_obj;
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
      while (ucp_worker_progress(worker_) > 0) {
      }
      if (to_recv_.load(std::memory_order_acquire) == 2) {
        static size_t received{0};
        nb::gil_scoped_acquire acquire;
        debug_print("Worker got something to recv. Total: {}", ++received);
        auto [recv_future_obj, buf_ptr, buf_size, tag, tag_mask] = recv_args_;
        recv_args_ = {};
        to_recv_.store(0, std::memory_order_release);
        auto recv_future = nb::inst_ptr<ServerRecvFuture>(recv_future_obj);
        ucp_tag_recv_info_t tag_info{};
        ucp_request_param_t param{.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                                                  UCP_OP_ATTR_FIELD_USER_DATA |
                                                  UCP_OP_ATTR_FIELD_RECV_INFO,
                                  .cb{.recv = recv_cb},
                                  .user_data = recv_future,
                                  .recv_info{
                                      .tag_info = &tag_info,
                                  }};
        auto status =
            ucp_tag_recv_nbx(worker_, buf_ptr, buf_size, tag, tag_mask, &param);
        if (status == NULL) {
          // immediately done
          recv_future->sender_tag_ = tag_info.sender_tag;
          recv_future->length_ = tag_info.length;
          recv_future->set_result(UCS_OK);
        } else if (UCS_PTR_IS_ERR(status)) {
          recv_future->set_result(UCS_PTR_STATUS(status));
        } else {
          nb::gil_scoped_acquire acquire;
          recv_future->req_ = status;
          recv_reqs_.insert(recv_future_obj);
        }
      }
    }
    // detect closed, cancel all outgoing reqs
    debug_print("Detect close in worker thread.");
    debug_print("Hangling requests: {}", recv_reqs_.size());

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
    while (ucp_worker_progress(worker_) > 0) {
    }

    {
      // additional safe guard to clean ref counts
      nb::gil_scoped_acquire acquire;
      recv_reqs_.clear();
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
  std::atomic<uint8_t> to_recv_{0};
  std::tuple<nb::object, uint8_t *, size_t, uint64_t, uint64_t> recv_args_{};
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
  auto const &recv_ref = std::get<0>(w->recv_args_);
  nb::handle recv_ref_obj = nb::find(recv_ref);
  Py_VISIT(recv_ref_obj.ptr());
  return 0;
}
int server_wrapper_tp_clear(PyObject *self) {
  auto *w = nb::inst_ptr<Server>(self);
  w->recv_reqs_.clear();
  w->recv_args_ = {};
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
  auto const &todo_ref = std::get<0>(w->todo_content_);
  nb::handle todo_obj = nb::find(todo_ref);
  Py_VISIT(todo_obj.ptr());
  return 0;
}
int client_wrapper_tp_clear(PyObject *self) {
  auto *w = nb::inst_ptr<Client>(self);
  w->todo_content_ = {};
  w->sends_.clear();
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

  nb::class_<Client>(m, "Client", nb::type_slots(client_wrapper_slots))
      .def(nb::init<std::string_view, uint64_t>(), "addr"_a, "port"_a)
      .def("send", &Client::send, "buffer"_a, "tag"_a, "done_callback"_a,
           "fail_callback"_a);

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
           "done_callback"_a, "fail_callback"_a);
}
