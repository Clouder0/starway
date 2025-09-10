// #include "ucp/api/ucp_def.h"
// #include <arpa/inet.h>
// #include <array>
// #include <atomic>
// #include <cassert>
// #include <format>
// #include <memory>
// #include <nanobind/nanobind.h>
// #include <nanobind/ndarray.h>
// #include <nanobind/stl/bind_vector.h>
// #include <nanobind/stl/function.h>
// #include <nanobind/stl/set.h>
// #include <nanobind/stl/shared_ptr.h>
// #include <nanobind/stl/string.h>
// #include <nanobind/stl/string_view.h>
// #include <nanobind/stl/tuple.h>
// #include <nanobind/stl/vector.h>
// #include <optional>
// #include <set>
// #include <thread>
// #include <ucp/api/ucp.h>
// #include <ucp/api/ucp_compat.h>
// #include <ucs/type/status.h>
// #include <ucs/type/thread_mode.h>
// #include <utility>

// namespace nb = nanobind;
// using namespace nb::literals;

// #ifdef NDEBUG
// // --- RELEASE MODE ---
// // In release mode (when NDEBUG is defined), this macro expands to nothing.
// // The compiler will see an empty statement, and importantly, the arguments
// // passed to the macro will NEVER be evaluated.
// #define debug_print(...)                                                       \
//   do {                                                                         \
//   } while (0)

// #else
// // --- DEBUG MODE ---
// // In debug mode, the macro expands to a std::println call to stderr.
// // We use stderr for debug messages to separate them from normal program output
// // (stdout). The __VA_ARGS__ preprocessor token forwards all arguments to
// // std::println.
// #include <format>
// #include <iostream>
// #define debug_print(...)                                                       \
//   do {                                                                         \
//     std::cout << std::format(__VA_ARGS__) << "\n";                             \
//   } while (0)
// #endif // NDEBUG

// inline void ucp_check_status(ucs_status_t status, std::string_view msg) {
//   if (status != UCS_OK) {
//     throw std::runtime_error(
//         "UCP error: " + std::string(ucs_status_string(status)) + " - " +
//         std::string(msg));
//   }
// }

// ClientSendFuture::ClientSendFuture(Client *client, void *req,
//                                    UniRef<nb::object> auto done_callback,
//                                    UniRef<nb::object> auto fail_callback)
//     : client_(client), req_(req),
//       done_callback_(std::forward<decltype(done_callback)>(done_callback)),
//       fail_callback_(std::forward<decltype(fail_callback)>(fail_callback)) {}
// void ClientSendFuture::set_result(ucs_status_t result) {
//   done_status_ = result;
//   status_.store(1, std::memory_order_release);
//   if (done_status_ == UCS_OK) {
//     nb::handle obj = nb::find(this);
//     debug_print("Client Send Future done.");
//     done_callback_(obj);
//   } else {
//     nb::handle obj = nb::find(this);
//     debug_print("Client Send Future failed with {}.",
//                 ucs_status_string(result));
//     fail_callback_(obj);
//   }
// }
// [[nodiscard]] bool ClientSendFuture::done() const {
//   return status_.load(std::memory_order_acquire) == 1;
// }
// [[nodiscard]] const char *ClientSendFuture::excpetion() const {
//   return ucs_status_string(done_status_);
// }

//   struct ClientRecvFuture {
//     ClientRecvFuture(Client *client, nb::object done_callback,
//                      nb::object fail_callback)
//         : done_callback_(done_callback), fail_callback_(fail_callback),
//           client_(client) {}
//     auto done() const { return done_.load(std::memory_order_acquire); }
//     auto exception() const { return ucs_status_string(done_status_); }
//     auto set_result(ucs_status_t status) {
//       done_status_ = status;
//       done_.store(true, std::memory_order_release);
//       auto self = nb::find(this);
//       assert(self.is_valid());
//       if (status == UCS_OK) {
//         done_callback_(self);
//       } else {
//         fail_callback_(self);
//       }
//     }
//     auto info() const { return std::make_tuple(sender_tag_, length_); }
//     void wait() {
//       while (!done_.load(std::memory_order_acquire)) {
//         std::this_thread::yield();
//       }
//     }

//     friend struct Client;
//     std::atomic<bool> done_{false};
//     ucs_status_t done_status_{UCS_OK};
//     nb::object done_callback_;
//     nb::object fail_callback_;
//     void *req_;
//     Client *client_;
//     uint64_t sender_tag_{};
//     size_t length_{};
//   };

// struct ClientSendPack {
//   nb::object send_future;
//   uint64_t tag;
//   size_t buf_size;
//   uint8_t *buf_ptr;
// };
// struct ClientRecvPack {
//   nb::object recv_future;
//   uint64_t tag;
//   uint64_t tag_mask;
//   size_t buf_size;
//   uint8_t *buf_ptr;
// };


// struct Client {
//   Client(std::string_view addr, uint64_t port)
//       : worker_thread_([this, addr, port]() { working_thread(addr, port); }) {
//     // holding GIL to wait for init
//     while (closed_.load(std::memory_order_acquire)) {
//       std::this_thread::yield();
//     }
//     debug_print("Client: init done.");
//     if (init_state_ != UCS_OK) [[unlikely]] {
//       // don't throw first, it would cause the thread to join and wait for close
//       // instead, set close first
//       debug_print("Client: init failed! reason: {}",
//                   ucs_status_string(init_state_));
//       {
//         debug_print("Client: failed init wait for close...");
//         nb::gil_scoped_release release;
//         closed_.store(true, std::memory_order_release);
//         worker_thread_.join();
//         debug_print("Client: init wait close done.");
//       }
//       throw std::runtime_error(std::format("Client: Init error: {}",
//                                            ucs_status_string(init_state_)));
//     }
//   }
//   ~Client() {
//     nb::gil_scoped_release release;
//     debug_print("Client: Start to destroy Client.");
//     closed_.store(true, std::memory_order_release);
//     // wait for it to close
//     worker_thread_.join();
//   }
//   void connect(nb::object connect_done_cb) {
//     closed_.store(false, std::memory_order_release);
//   }
//   auto send(nb::ndarray<uint8_t, nb::ndim<1>, nb::device::cpu> &buffer,
//             uint64_t tag, nb::object done_callback, nb::object fail_callback) {
//     auto buf_ptr = buffer.data();
//     auto buf_size = buffer.size();
//     auto send_future_obj = nb::cast(
//         new ClientSendFuture(this, nullptr, done_callback, fail_callback));
//     {
//       nb::gil_scoped_release release;
//       send_q_.wait_emplace([&](auto &dst) {
//         nb::gil_scoped_acquire acquire;
//         dst.emplace(send_future_obj, tag, buf_size, buf_ptr);
//       });
//       return send_future_obj;
//     }
//   }
//   auto recv(nb::ndarray<uint8_t, nb::ndim<1>, nb::device::cpu> &buffer,
//             uint64_t tag, uint64_t tag_mask, nb::object done_callback,
//             nb::object fail_callback) {
//     auto buf_ptr = buffer.data();
//     auto buf_size = buffer.size();
//     auto recv_future = new ClientRecvFuture(this, done_callback, fail_callback);
//     auto recv_future_obj = nb::cast(recv_future);
//     {
//       nb::gil_scoped_release release;
//       recv_q_.wait_emplace([&](auto &dst) {
//         nb::gil_scoped_acquire acquire;
//         dst.emplace(recv_future_obj, tag, tag_mask, buf_size, buf_ptr);
//       });
//       return recv_future_obj;
//     }
//   }
//   void init_context() {
//     ucp_params_t params{.field_mask = UCP_PARAM_FIELD_FEATURES |
//                                       UCP_PARAM_FIELD_ESTIMATED_NUM_EPS,
//                         .features = UCP_FEATURE_TAG,
//                         .estimated_num_eps = 1};
//     ucp_check_status(ucp_init(&params, NULL, &context_),
//                      "Failed to init UCP context");
//   }
//   void init_worker() {
//     ucp_worker_params_t worker_params{
//         .field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE,
//         .thread_mode = UCS_THREAD_MODE_MULTI,
//     };
//     ucp_check_status(ucp_worker_create(context_, &worker_params, &worker_),
//                      "Failed to create UCP worker");
//   }
//   void init_sock_ep(std::string_view addr, uint64_t port) {
//     struct sockaddr_in connect_addr{
//         .sin_family = AF_INET,
//         .sin_port = htons(static_cast<uint16_t>(port)),
//         .sin_addr = {inet_addr(addr.data())},
//     };
//     ucp_ep_params_t ep_params{
//         .field_mask = UCP_EP_PARAM_FIELD_FLAGS | UCP_EP_PARAM_FIELD_SOCK_ADDR |
//                       UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE,
//         .err_mode = UCP_ERR_HANDLING_MODE_NONE,
//         .flags = UCP_EP_PARAMS_FLAGS_CLIENT_SERVER,
//         .sockaddr =
//             {
//                 .addr = reinterpret_cast<struct sockaddr *>(&connect_addr),
//                 .addrlen = sizeof(connect_addr),
//             },
//     };
//     ucp_check_status(ucp_ep_create(worker_, &ep_params, &ep_),
//                      "Failed to create UCP endpoint");
//     // ensure connection has been established when init
//     debug_print("Client: init ep start flushing to ensure connection.");
//     ucp_request_param_t flush_params{};
//     auto status = ucp_ep_flush_nbx(ep_, &flush_params);
//     if (UCS_PTR_STATUS(status) == UCS_OK) {
//       debug_print("Client: init ep flush done");
//       init_state_ = UCS_OK;
//       return;
//     } else if (UCS_PTR_IS_ERR(status)) {
//       init_state_ = UCS_PTR_STATUS(status);
//       debug_print("Client: init ep flush failed: {}",
//                   ucs_status_string(init_state_));
//       // don't throw here, as it cannot be captured by nanobind in another
//       // thread throw std::runtime_error( "Failed to flush UCP endpoint at init
//       // ep: " + std::string(ucs_status_string(UCS_PTR_STATUS(status))));
//     } else {
//       while (ucp_request_check_status(status) == UCS_INPROGRESS) {
//         ucp_worker_progress(worker_);
//       }
//       auto final_status = ucp_request_check_status(status);
//       if (final_status != UCS_OK) {
//         init_state_ = final_status;
//         debug_print("Client: init ep flush failed: {}",
//                     ucs_status_string(final_status));
//         // throw std::runtime_error("Failed to flush UCP endpoint at init ep: "
//         // +
//         //  std::string(ucs_status_string(final_status)));
//       }
//       debug_print("Client: init ep flush done");
//       ucp_request_free(status);
//     }
//   }
//   auto evaluate_perf(size_t msg_size) {
//     perf_msg_size_.store(msg_size, std::memory_order_release);
//     while (perf_msg_size_.load(std::memory_order_acquire) > 0) {
//       std::this_thread::yield();
//     }
//     return perf_result_;
//   }
//   void working_thread(std::string_view addr, uint64_t port) {
//     init_context();
//     init_worker();
//     init_sock_ep(addr, port);
//     closed_.store(false, std::memory_order_release);
//     while (!closed_.load(std::memory_order_acquire)) {
//       ucp_worker_progress(worker_);
//       send_q_.try_consume([this](auto &&src) {
//         nb::gil_scoped_acquire acquire;
//         ClientSendPack const &pack = src.value();
//         auto send_future = nb::inst_ptr<ClientSendFuture>(pack.send_future);
//         ucp_request_param_t send_param{.op_attr_mask =
//                                            UCP_OP_ATTR_FIELD_CALLBACK |
//                                            UCP_OP_ATTR_FIELD_USER_DATA,
//                                        .cb{.send = send_cb},
//                                        .user_data = send_future};
//         auto *req = ucp_tag_send_nbx(ep_, pack.buf_ptr, pack.buf_size, pack.tag,
//                                      &send_param);
//         if (UCS_PTR_STATUS(req) == UCS_OK) {
//           send_future->set_result(UCS_OK);
//         } else if (UCS_PTR_IS_ERR(req)) {
//           send_future->set_result(UCS_PTR_STATUS(req));
//         } else {
//           send_future->req_ = req;
//           sends_.insert(std::move(pack.send_future));
//         }
//         src.reset();
//       });
//       recv_q_.try_consume([this](auto &&src) {
//         nb::gil_scoped_acquire acquire;
//         ClientRecvPack const &pack = src.value();
//         auto recv_future = nb::inst_ptr<ClientRecvFuture>(pack.recv_future);
//         ucp_tag_recv_info_t tag_info{};
//         ucp_request_param_t param{.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
//                                                   UCP_OP_ATTR_FIELD_USER_DATA |
//                                                   UCP_OP_ATTR_FIELD_RECV_INFO,
//                                   .cb{.recv = recv_cb},
//                                   .user_data = recv_future,
//                                   .recv_info{
//                                       .tag_info = &tag_info,
//                                   }};
//         auto req = ucp_tag_recv_nbx(worker_, pack.buf_ptr, pack.buf_size,
//                                     pack.tag, pack.tag_mask, &param);
//         if (req == NULL) {
//           recv_future->sender_tag_ = tag_info.sender_tag;
//           recv_future->length_ = tag_info.length;
//           recv_future->set_result(UCS_OK);
//         } else if (UCS_PTR_IS_ERR(req)) {
//           recv_future->set_result(UCS_PTR_STATUS(req));
//         } else {
//           recv_future->req_ = req;
//           recv_reqs_.insert(std::move(pack.recv_future));
//         }
//         src.reset();
//       });
//       {
//         auto msg_size = perf_msg_size_.load(std::memory_order_acquire);
//         if (msg_size != 0) {
//           debug_print("Client: start perf evaluation with msg size {}...",
//                       msg_size);
//           ucp_ep_evaluate_perf_param_t params{
//               .field_mask = UCP_EP_PERF_PARAM_FIELD_MESSAGE_SIZE,
//               .message_size = msg_size};
//           ucp_ep_evaluate_perf_attr_t out{
//               .field_mask = UCP_EP_PERF_ATTR_FIELD_ESTIMATED_TIME};
//           auto status = ucp_ep_evaluate_perf(ep_, &params, &out);
//           if (status != UCS_OK) [[unlikely]] {
//             throw std::runtime_error(ucs_status_string(status));
//           }
//           perf_result_ = out.estimated_time;
//           perf_msg_size_.store(0, std::memory_order_release);
//         }
//       }
//     }
//     debug_print("Client: Start closing flush...");
//     // while (ucp_worker_progress(worker_) > 0) {
//     // debug_print("Client made some progress");
//     // }

//     // debug_print("Client: Start cancelling all requests...");
//     // {
//     //   nb::gil_scoped_acquire acquire;
//     //   for (auto const &_req : sends_) {
//     //     debug_print("Client: start cancel one send.");
//     //     auto req = nb::inst_ptr<ClientSendFuture>(_req);
//     //     if (ucp_request_check_status(req->req_) == UCS_INPROGRESS) {
//     //       debug_print("Client: cancel Some send request.");
//     //       ucp_request_cancel(worker_, req->req_);
//     //       debug_print("Client: Some send request cancelled.");
//     //     }
//     //     debug_print("Client: cancel one send done.");
//     //   }
//     //   for (auto const &_req : recv_reqs_) {
//     //     debug_print("Client: start cancel one recv.");
//     //     auto req = nb::inst_ptr<ClientRecvFuture>(_req);
//     //     if (ucp_request_check_status(req->req_) == UCS_INPROGRESS) {
//     //       ucp_request_cancel(worker_, req->req_);
//     //       debug_print("Client: Some request cancelled.");
//     //     }
//     //     debug_print("Client: cancel one recv done.");
//     //   }
//     // }
//     // debug_print("Client: all cancel done.");
//     while (ucp_worker_progress(worker_) > 0) {
//       debug_print("Client: Client made some progress");
//     }

//     // {
//     //   nb::gil_scoped_acquire acquire;
//     //   sends_.clear();
//     //   recv_reqs_.clear();
//     // }
//     ucp_request_param_t flush_params{};
//     auto flush_status = ucp_ep_flush_nbx(ep_, &flush_params);
//     if (UCS_PTR_STATUS(flush_status) == UCS_OK) {
//       debug_print("Client: Flush done!");
//     } else if (UCS_PTR_IS_ERR(flush_status)) {
//       if (UCS_PTR_STATUS(flush_status) != UCS_ERR_CONNECTION_RESET) {
//         // just ignore CONN RESET, which indicates remote end has been closed
//         debug_print("Client: Flushed failed! {}",
//                     ucs_status_string(UCS_PTR_STATUS(flush_status)));
//       } else {
//         debug_print("Client: Flush done with expected ERR: {}",
//                     ucs_status_string(UCS_PTR_STATUS(flush_status)));
//       }
//     } else {
//       ucs_status_t flush_req_status{};
//       do {
//         ucp_worker_progress(worker_);
//         flush_req_status = ucp_request_check_status(flush_status);
//       } while (flush_req_status == UCS_INPROGRESS);
//       debug_print("Client: Flush request done.");
//       ucp_request_free(flush_status);
//     }

//     debug_print("Client: Start sending close...");
//     ucp_request_param_t close_param{};
//     auto status = ucp_ep_close_nbx(ep_, &close_param);
//     if (UCS_PTR_STATUS(status) == UCS_OK) {
//       debug_print("Client: Close done.");
//     } else if (UCS_PTR_IS_ERR(status)) {
//       if (UCS_PTR_STATUS(status) != UCS_ERR_CONNECTION_RESET &&
//           UCS_PTR_STATUS(status) != UCS_ERR_UNREACHABLE &&
//           UCS_PTR_STATUS(status) != UCS_ERR_NOT_CONNECTED) {
//         // just ignore CONN RESET, which indicates remote end has been closed
//         debug_print("Client: Close failed: {}",
//                     ucs_status_string(UCS_PTR_STATUS(status)));
//       } else {
//         debug_print("Client: Close done with expected ERR: {}",
//                     ucs_status_string(UCS_PTR_STATUS(status)));
//       }
//     } else {
//       debug_print("Client: async Close, waiting...");
//       while (ucp_request_check_status(status) == UCS_INPROGRESS) {
//         while (ucp_worker_progress(worker_) > 0) {
//         }
//       }
//       debug_print("Client: Close request done.");
//       ucp_request_free(status);
//     }
//     debug_print("Client: start destroy worker");
//     ucp_worker_destroy(worker_);
//     debug_print("Client: Worker thread exit done.");
//     ucp_cleanup(context_);
//     debug_print("Client: Cleanup done.");
//     closed_.store(false, std::memory_order_release);
//   }
//   static void recv_cb(void *request, ucs_status_t status,
//                       ucp_tag_recv_info_t const *info, void *args) {
//     auto *recv_future = reinterpret_cast<ClientRecvFuture *>(args);
//     {
//       nb::gil_scoped_acquire acquire;
//       auto obj = nb::find(recv_future);
//       assert(obj.is_valid());
//       recv_future->sender_tag_ = info->sender_tag;
//       recv_future->length_ = info->length;
//       assert(recv_future->req_ == request);
//       recv_future->req_ = NULL;
//       recv_future->set_result(status);
//       recv_future->client_->recv_reqs_.erase(obj);
//     }
//     ucp_request_free(request);
//   }
//   static void send_cb(void *req, ucs_status_t status, void *user_data) {
//     auto *future = reinterpret_cast<ClientSendFuture *>(user_data);
//     {
//       nb::gil_scoped_acquire acquire;
//       nb::object obj = nb::find(future);
//       assert(obj.is_valid());
//       future->set_result(status);
//       future->client()->sends_.erase(obj);
//     }
//     // free the request
//     ucp_request_free(req);
//   }

//   std::jthread worker_thread_;
//   ucp_context_h context_;
//   ucp_worker_h worker_;
//   ucp_ep_h ep_;
//   ucs_status_t init_state_;

//   std::atomic<bool> closed_{true};
//   Channel<ClientSendPack> send_q_{};
//   Channel<ClientRecvPack> recv_q_{};
//   std::atomic<size_t> perf_msg_size_{0};
//   double perf_result_{0};

//   std::set<nb::object,
//            decltype([](nb::object const &lhs, nb::object const &rhs) {
//              return lhs.ptr() < rhs.ptr();
//            })>
//       sends_{};
//   std::set<nb::object,
//            decltype([](nb::object const &lhs, nb::object const &rhs) {
//              return lhs.ptr() < rhs.ptr();
//            })>
//       recv_reqs_{};
// };

// // Server
// struct Server;
// struct ServerSendFuture {
//   ServerSendFuture(Server *server, void *req, nb::object done_callback,
//                    nb::object fail_callback)
//       : server_(server), req_(req), done_callback_(done_callback),
//         fail_callback_(fail_callback) {}
//   ServerSendFuture(ServerSendFuture const &) = delete;
//   auto operator=(ServerSendFuture const &) -> ServerSendFuture & = delete;
//   [[nodiscard]] auto done() const noexcept -> bool { return done_; }
//   auto exception() {
//     if (done_status_ == UCS_OK) {
//       return "";
//     }
//     return ucs_status_string(done_status_);
//   }
//   void set_result(ucs_status_t status) {
//     done_status_ = status;
//     done_.store(true, std::memory_order_release);
//     if (done_status_ == UCS_OK) {
//       nb::handle obj = nb::find(this);
//       debug_print("Done obj name {}, valid {}", nb::inst_name(obj).c_str(),
//                   obj.is_valid());
//       done_callback_(obj);
//     } else {
//       nb::handle obj = nb::find(this);
//       assert(obj.is_valid());
//       fail_callback_(obj);
//     }
//   }
//   void wait() {
//     while (!done_.load(std::memory_order_acquire)) {
//       std::this_thread::yield();
//     }
//   }
//   auto server() const { return server_; }

//   std::atomic<bool> done_{false};
//   Server *server_;
//   void *req_;
//   ucs_status_t done_status_{UCS_OK};
//   nb::object done_callback_, fail_callback_;
// };
// struct ServerRecvFuture {
//   ServerRecvFuture(Server *server, nb::object done_callback,
//                    nb::object fail_callback)
//       : done_callback_(done_callback), fail_callback_(fail_callback),
//         server_(server) {}
//   auto done() const { return done_.load(std::memory_order_acquire); }
//   auto exception() const { return ucs_status_string(done_status_); }
//   auto set_result(ucs_status_t status) {
//     done_status_ = status;
//     done_.store(true, std::memory_order_release);
//     auto self = nb::find(this);
//     assert(self.is_valid());
//     if (status == UCS_OK) {
//       done_callback_(self);
//     } else {
//       fail_callback_(self);
//     }
//   }
//   auto info() const { return std::make_tuple(sender_tag_, length_); }
//   void wait() {
//     while (!done_.load(std::memory_order_acquire)) {
//       std::this_thread::yield();
//     }
//   }

//   friend struct Server;
//   std::atomic<bool> done_{false};
//   ucs_status_t done_status_{UCS_OK};
//   nb::object done_callback_;
//   nb::object fail_callback_;
//   void *req_;
//   Server *server_;
//   uint64_t sender_tag_{};
//   size_t length_{};
// };
// struct ServerRecvPack {
//   nb::object recv_future;
//   uint64_t tag, tag_mask;
//   size_t buf_size;
//   uint8_t *buf_ptr;
// };
// struct ServerSendPack {
//   nb::object send_future;
//   ucp_ep_h dst_ep;
//   uint64_t tag;
//   size_t buf_size;
//   uint8_t *buf_ptr;
// };
// struct ServerEndpoint {
//   ucp_ep_h ep;
//   std::string name;
//   std::string local_addr;
//   uint16_t local_port;
//   std::string remote_addr;
//   uint16_t remote_port;
//   size_t num_transports;
//   std::array<ucp_transport_entry_t, 8> transports;
//   auto view_transports() const {
//     std::vector<std::tuple<std::string, std::string>> res{};
//     res.reserve(num_transports);
//     for (size_t i = 0; i < num_transports; i++) {
//       res.emplace_back(transports[i].device_name, transports[i].transport_name);
//     }
//     return res;
//   }
//   auto operator<=>(ServerEndpoint const &rhs) const { return ep <=> rhs.ep; }
// };
// struct Server {
//   void init_context() {
//     ucp_params_t params{
//         .field_mask = UCP_PARAM_FIELD_FEATURES,
//         .features = UCP_FEATURE_TAG,
//     };
//     ucp_check_status(ucp_init(&params, NULL, &context_),
//                      "Failed to init UCP context");
//   }
//   void init_worker() {
//     ucp_worker_params_t worker_params{
//         .field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE,
//         .thread_mode = UCS_THREAD_MODE_MULTI,
//     };
//     ucp_check_status(ucp_worker_create(context_, &worker_params, &worker_),
//                      "Failed to create UCP worker");
//   }
//   static void accept_cb(ucp_ep_h ep, void *arg) {
//     auto *cur = reinterpret_cast<Server *>(arg);
//     ServerEndpoint endpoint{};
//     ucp_ep_attr_t attrs{
//         .field_mask =
//             UCP_EP_ATTR_FIELD_NAME | UCP_EP_ATTR_FIELD_LOCAL_SOCKADDR |
//             UCP_EP_ATTR_FIELD_REMOTE_SOCKADDR | UCP_EP_ATTR_FIELD_TRANSPORTS,
//         .transports{.entries = endpoint.transports.data(),
//                     .num_entries = endpoint.transports.size(),
//                     .entry_size = sizeof(ucp_transport_entry_t)}};
//     auto status = ucp_ep_query(ep, &attrs);
//     if (status != UCS_OK) {
//       throw std::runtime_error(
//           std::format("Server: ERR when querying ep on connection: {}",
//                       ucs_status_string(status)));
//     }
//     sockaddr_in *local_addr =
//         reinterpret_cast<sockaddr_in *>(&attrs.local_sockaddr);
//     sockaddr_in *remote_addr =
//         reinterpret_cast<sockaddr_in *>(&attrs.remote_sockaddr);
//     endpoint.ep = ep;
//     endpoint.name = attrs.name;
//     endpoint.local_addr = inet_ntoa(local_addr->sin_addr);
//     endpoint.local_port = ntohs(local_addr->sin_port);
//     endpoint.remote_addr = inet_ntoa(remote_addr->sin_addr);
//     endpoint.remote_port = ntohs(remote_addr->sin_port);
//     endpoint.num_transports = attrs.transports.num_entries;
//     auto [endpoint_it, _] = cur->connected_.emplace(std::move(endpoint));
//     auto &endpoint_ref = *endpoint_it;
//     {
//       nb::gil_scoped_acquire acquire;
//       if (cur->connect_callback_.is_valid()) {
//         cur->connect_callback_(nb::find(cur), endpoint_ref);
//       }
//     }
//   }
//   void init_listener(std::string_view addr, uint64_t port) {
//     struct sockaddr_in listen_addr{
//         .sin_family = AF_INET,
//         .sin_port = htons(static_cast<uint16_t>(port)),
//         .sin_addr = {inet_addr(addr.data())},
//     };

//     ucp_listener_params_t params{
//         .field_mask = UCP_LISTENER_PARAM_FIELD_SOCK_ADDR |
//                       UCP_LISTENER_PARAM_FIELD_ACCEPT_HANDLER,
//         .sockaddr{
//             .addr = reinterpret_cast<struct sockaddr *>(&listen_addr),
//             .addrlen = sizeof(listen_addr),
//         },
//         .accept_handler{.cb = accept_cb, .arg = this}};
//     ucp_check_status(ucp_listener_create(worker_, &params, &listener_),
//                      "Failed to create listener.");
//   }
//   auto recv(nb::ndarray<uint8_t, nb::ndim<1>, nb::device::cpu> &buffer,
//             uint64_t tag, uint64_t tag_mask, nb::object done_callback,
//             nb::object fail_callback) {
//     auto buf_ptr = buffer.data();
//     auto buf_size = buffer.size();
//     auto recv_future = new ServerRecvFuture(this, done_callback, fail_callback);
//     auto recv_future_obj = nb::cast(recv_future);
//     {
//       nb::gil_scoped_release release;
//       recv_q_.wait_emplace([&](auto &dst) {
//         nb::gil_scoped_acquire acquire;
//         dst.emplace(recv_future_obj, tag, tag_mask, buf_size, buf_ptr);
//       });
//       return recv_future_obj;
//     }
//   }
//   auto list_clients() const -> std::set<ServerEndpoint> const & {
//     return connected_;
//   }
//   auto send(ServerEndpoint const &client_ep,
//             nb::ndarray<uint8_t, nb::ndim<1>, nb::device::cpu> &buffer,
//             uint64_t tag, nb::object done_callback, nb::object fail_callback) {
//     auto buf_ptr = buffer.data();
//     auto buf_size = buffer.size();
//     auto send_future_obj = nb::cast(
//         new ServerSendFuture(this, nullptr, done_callback, fail_callback));
//     {
//       nb::gil_scoped_release release;
//       auto ep = client_ep.ep;
//       send_q_.wait_emplace([&](auto &dst) {
//         nb::gil_scoped_acquire acquire;
//         dst.emplace(send_future_obj, ep, tag, buf_size, buf_ptr);
//       });
//       return send_future_obj;
//     }
//   }
//   Server(std::string_view addr, uint64_t port, nb::object connect_callback)
//       : worker_thread_(
//             [this, addr, port]() { this->working_thread(addr, port); }),
//         connect_callback_(std::move(connect_callback)) {}

//   void close(nb::object close_cb) {
//     close_callback_ = close_cb;
//     closed_.store(true, std::memory_order_release);
//     debug_print("Server: Send close signal.");
//   }

//   ~Server() {
//     if (!closed_.load(std::memory_order_acquire)) {
//       debug_print("Server: Not closed yet! Try close, but may incur problem.");
//       nb::gil_scoped_release release;
//       closed_.store(true, std::memory_order_release);
//       worker_thread_.join();
//     }
//     debug_print("Server: dtor main done.");
//   }
//   static void send_cb(void *req, ucs_status_t status, void *user_data) {
//     auto *future = reinterpret_cast<ServerSendFuture *>(user_data);
//     {
//       nb::gil_scoped_acquire acquire;
//       nb::object obj = nb::find(future);
//       assert(obj.is_valid());
//       future->set_result(status);
//       future->server()->send_reqs_.erase(obj);
//     }
//     // free the request
//     ucp_request_free(req);
//   }
//   static void recv_cb(void *request, ucs_status_t status,
//                       ucp_tag_recv_info_t const *info, void *args) {
//     auto *recv_future = reinterpret_cast<ServerRecvFuture *>(args);
//     {
//       nb::gil_scoped_acquire acquire;
//       auto obj = nb::find(recv_future);
//       assert(obj.is_valid());
//       recv_future->sender_tag_ = info->sender_tag;
//       recv_future->length_ = info->length;
//       assert(recv_future->req_ == request);
//       recv_future->req_ = NULL;
//       recv_future->set_result(status);
//       recv_future->server_->recv_reqs_.erase(obj);
//     }
//     ucp_request_free(request);
//   }
//   auto evaluate_perf(ServerEndpoint const &client_ep, size_t msg_size) {
//     perf_client_ = client_ep.ep;
//     perf_msg_size_.store(msg_size, std::memory_order_release);
//     while (perf_msg_size_.load(std::memory_order_acquire) > 0) {
//       std::this_thread::yield();
//     }
//     return perf_result_;
//   }

//   void working_thread(std::string_view addr, uint64_t port) {
//     init_context();
//     init_worker();
//     init_listener(addr, port);
//     closed_.store(false, std::memory_order_release);
//     while (!closed_.load(std::memory_order_acquire)) {
//       ucp_worker_progress(worker_);
//       recv_q_.try_consume([&](auto &&src) {
//         nb::gil_scoped_acquire acquire;
//         auto const &pack = src.value();
//         auto recv_future = nb::inst_ptr<ServerRecvFuture>(pack.recv_future);
//         ucp_tag_recv_info_t tag_info{};
//         ucp_request_param_t param{.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
//                                                   UCP_OP_ATTR_FIELD_USER_DATA |
//                                                   UCP_OP_ATTR_FIELD_RECV_INFO,
//                                   .cb{.recv = recv_cb},
//                                   .user_data = recv_future,
//                                   .recv_info{
//                                       .tag_info = &tag_info,
//                                   }};
//         auto status = ucp_tag_recv_nbx(worker_, pack.buf_ptr, pack.buf_size,
//                                        pack.tag, pack.tag_mask, &param);
//         if (status == NULL) {
//           recv_future->sender_tag_ = tag_info.sender_tag;
//           recv_future->length_ = tag_info.length;
//           recv_future->set_result(UCS_OK);
//         } else if (UCS_PTR_IS_ERR(status)) {
//           recv_future->set_result(UCS_PTR_STATUS(status));
//         } else {
//           recv_future->req_ = status;
//           recv_reqs_.insert(pack.recv_future);
//         }
//         src.reset();
//       });

//       send_q_.try_consume([&](auto &&src) {
//         nb::gil_scoped_acquire acquire;
//         ServerSendPack const &pack = src.value();
//         auto send_future = nb::inst_ptr<ServerSendFuture>(pack.send_future);

//         ucp_request_param_t send_param{.op_attr_mask =
//                                            UCP_OP_ATTR_FIELD_CALLBACK |
//                                            UCP_OP_ATTR_FIELD_USER_DATA,
//                                        .cb{.send = send_cb},
//                                        .user_data = send_future};
//         auto *req = ucp_tag_send_nbx(pack.dst_ep, pack.buf_ptr, pack.buf_size,
//                                      pack.tag, &send_param);
//         if (UCS_PTR_STATUS(req) == UCS_OK) {
//           send_future->set_result(UCS_OK);
//         } else if (UCS_PTR_IS_ERR(req)) {
//           send_future->set_result(UCS_PTR_STATUS(req));
//         } else {
//           send_future->req_ = req;
//           send_reqs_.insert(std::move(pack.send_future));
//         }
//         src.reset();
//       });
//       {
//         auto msg_size = perf_msg_size_.load(std::memory_order_acquire);
//         if (msg_size != 0) {
//           debug_print("Server: start perf evaluation...");
//           ucp_ep_evaluate_perf_param_t params{
//               .field_mask = UCP_EP_PERF_PARAM_FIELD_MESSAGE_SIZE,
//               .message_size = msg_size};
//           ucp_ep_evaluate_perf_attr_t out{
//               .field_mask = UCP_EP_PERF_ATTR_FIELD_ESTIMATED_TIME};
//           auto status = ucp_ep_evaluate_perf(perf_client_, &params, &out);
//           if (status != UCS_OK) [[unlikely]] {
//             throw std::runtime_error(ucs_status_string(status));
//           }
//           perf_result_ = out.estimated_time;
//           perf_msg_size_.store(0, std::memory_order_release);
//         }
//       }
//     }
//     // detect closed, cancel all outgoing reqs
//     debug_print("Server: Detect close in worker thread.");
//     debug_print("Server: Hangling recv requests: {}", recv_reqs_.size());
//     debug_print("Server: Hangling send requests: {}", send_reqs_.size());

//     // {
//     //   nb::gil_scoped_acquire acquire;
//     //   for (auto _req : recv_reqs_) {
//     //     auto req = nb::inst_ptr<ServerRecvFuture>(_req);
//     //     if (ucp_request_check_status(req->req_) == UCS_INPROGRESS) {
//     //       ucp_request_cancel(worker_, req->req_);
//     //       debug_print("Server: Some recv request cancelled.");
//     //     }
//     //   }
//     //   for (auto _req : send_reqs_) {
//     //     auto req = nb::inst_ptr<ServerSendFuture>(_req);
//     //     if (ucp_request_check_status(req->req_) == UCS_INPROGRESS) {
//     //       ucp_request_cancel(worker_, req->req_);
//     //       debug_print("Server: Some send request cancelled.");
//     //     }
//     //   }
//     // }

//     // while (ucp_worker_progress(worker_) > 0) {
//     //   debug_print("Server: cancel in progress...");
//     // }

//     // {
//     //   // additional safe guard to clean ref counts
//     //   nb::gil_scoped_acquire acquire;
//     //   recv_reqs_.clear();
//     //   send_reqs_.clear();
//     // }

//     // first stop listening
//     debug_print("Server: start stop listener.");
//     ucp_listener_destroy(listener_);
//     debug_print("Server: done stop listener.");
//     // close all eps
//     for (auto const &client : connected_) {
//       ucp_request_param_t param{};
//       auto status = ucp_ep_close_nbx(client.ep, &param);
//       if (UCS_PTR_STATUS(status) == UCS_OK) {
//         debug_print("Server: Close some ep.");
//         continue;
//       } else if (UCS_PTR_IS_ERR(status)) {
//         if (UCS_PTR_STATUS(status) == UCS_ERR_CONNECTION_RESET) {
//           debug_print("Server: Some ep has been conn_reset.");
//           continue;
//         }
//         debug_print("Server: Error when trying to close ep: {}",
//                     ucs_status_string(UCS_PTR_STATUS(status)));
//       } else {
//         while (ucp_request_check_status(status) == UCS_INPROGRESS) {
//           while (ucp_worker_progress(worker_) > 0) {
//           }
//         }
//         debug_print("Server: Wait close some ep.");
//         ucp_request_free(status);
//       }
//     }
//     ucp_worker_destroy(worker_);
//     ucp_cleanup(context_);
//     debug_print("Server: cleanup done!");
//     {
//       nb::gil_scoped_acquire acquire;
//       if(close_callback_.is_valid()) {
//         debug_print("Server: call close callback.");
//         close_callback_(nb::find(this));
//       }
//     }
//   }

//   std::atomic<bool> closed_{true};
//   std::jthread worker_thread_;
//   ucp_context_h context_;
//   ucp_worker_h worker_;
//   ucp_listener_h listener_;
//   nb::object connect_callback_;
//   nb::object close_callback_;
//   std::set<ServerEndpoint> connected_;
//   std::set<nb::object,
//            decltype([](nb::object const &lhs, nb::object const &rhs) {
//              return lhs.ptr() < rhs.ptr();
//            })>
//       recv_reqs_;
//   std::set<nb::object,
//            decltype([](nb::object const &lhs, nb::object const &rhs) {
//              return lhs.ptr() < rhs.ptr();
//            })>
//       send_reqs_;
//   Channel<ServerRecvPack> recv_q_;
//   Channel<ServerSendPack> send_q_;
//   std::atomic<size_t> perf_msg_size_;
//   ucp_ep_h perf_client_;
//   double perf_result_;
// };

// int client_send_future_wrapper_tp_traverse(PyObject *self, visitproc visit,
//                                            void *arg) {
// // On Python 3.9+, we must traverse the implicit dependency
// // of an object on its associated type object.
// #if PY_VERSION_HEX >= 0x03090000
//   Py_VISIT(Py_TYPE(self));
// #endif

//   // The tp_traverse method may be called after __new__ but before or during
//   // __init__, before the C++ constructor has been completed. We must not
//   // inspect the C++ state if the constructor has not yet completed.
//   if (!nb::inst_ready(self)) {
//     return 0;
//   }

//   // Get the C++ object associated with 'self' (this always succeeds)
//   ClientSendFuture *w = nb::inst_ptr<ClientSendFuture>(self);

//   // If w->value has an associated Python object, return it.
//   // If not, value.ptr() will equal NULL, which is also fine.
//   nb::handle value = nb::find(w->done_callback_);

//   // Inform the Python GC about the instance
//   Py_VISIT(value.ptr());

//   nb::handle value2 = nb::find(w->fail_callback_);
//   Py_VISIT(value2.ptr());

//   return 0;
// }

// int client_send_future_wrapper_tp_clear(PyObject *self) {
//   // Get the C++ object associated with 'self' (this always succeeds)
//   ClientSendFuture *w = nb::inst_ptr<ClientSendFuture>(self);

//   // Break the reference cycle!
//   w->done_callback_ = {};
//   w->fail_callback_ = {};

//   return 0;
// }

// // Table of custom type slots we want to install
// PyType_Slot client_send_future_wrapper_slots[] = {
//     {Py_tp_traverse, (void *)client_send_future_wrapper_tp_traverse},
//     {Py_tp_clear, (void *)client_send_future_wrapper_tp_clear},
//     {0, 0}};

// int client_recv_future_wrapper_tp_traverse(PyObject *self, visitproc visit,
//                                            void *arg) {
// #if PY_VERSION_HEX >= 0x03090000
//   Py_VISIT(Py_TYPE(self));
// #endif
//   if (!nb::inst_ready(self)) {
//     return 0;
//   }
//   ClientRecvFuture *w = nb::inst_ptr<ClientRecvFuture>(self);
//   nb::handle value = nb::find(w->done_callback_);
//   Py_VISIT(value.ptr());
//   nb::handle value2 = nb::find(w->fail_callback_);
//   Py_VISIT(value2.ptr());
//   return 0;
// }
// int client_recv_future_wrapper_tp_clear(PyObject *self) {
//   ClientRecvFuture *w = nb::inst_ptr<ClientRecvFuture>(self);
//   w->done_callback_ = {};
//   w->fail_callback_ = {};
//   return 0;
// }
// PyType_Slot client_recv_future_wrapper_slots[] = {
//     {Py_tp_traverse, (void *)client_recv_future_wrapper_tp_traverse},
//     {Py_tp_clear, (void *)client_recv_future_wrapper_tp_clear},
//     {0, 0}};

// int server_send_future_wrapper_tp_traverse(PyObject *self, visitproc visit,
//                                            void *arg) {
// #if PY_VERSION_HEX >= 0x03090000
//   Py_VISIT(Py_TYPE(self));
// #endif
//   if (!nb::inst_ready(self)) {
//     return 0;
//   }
//   ServerSendFuture *w = nb::inst_ptr<ServerSendFuture>(self);
//   nb::handle value = nb::find(w->done_callback_);
//   Py_VISIT(value.ptr());
//   nb::handle value2 = nb::find(w->fail_callback_);
//   Py_VISIT(value2.ptr());
//   return 0;
// }
// int server_send_future_wrapper_tp_clear(PyObject *self) {
//   ServerSendFuture *w = nb::inst_ptr<ServerSendFuture>(self);
//   w->done_callback_ = {};
//   w->fail_callback_ = {};
//   return 0;
// }
// PyType_Slot server_send_future_wrapper_slots[] = {
//     {Py_tp_traverse, (void *)server_send_future_wrapper_tp_traverse},
//     {Py_tp_clear, (void *)server_send_future_wrapper_tp_clear},
//     {0, 0}};

// int server_recv_future_wrapper_tp_traverse(PyObject *self, visitproc visit,
//                                            void *arg) {
// #if PY_VERSION_HEX >= 0x03090000
//   Py_VISIT(Py_TYPE(self));
// #endif
//   if (!nb::inst_ready(self)) {
//     return 0;
//   }
//   ServerRecvFuture *w = nb::inst_ptr<ServerRecvFuture>(self);
//   nb::handle value = nb::find(w->done_callback_);
//   Py_VISIT(value.ptr());
//   nb::handle value2 = nb::find(w->fail_callback_);
//   Py_VISIT(value2.ptr());
//   return 0;
// }
// int server_recv_future_wrapper_tp_clear(PyObject *self) {
//   ServerRecvFuture *w = nb::inst_ptr<ServerRecvFuture>(self);
//   w->done_callback_ = {};
//   w->fail_callback_ = {};
//   return 0;
// }
// PyType_Slot server_recv_future_wrapper_slots[] = {
//     {Py_tp_traverse, (void *)server_recv_future_wrapper_tp_traverse},
//     {Py_tp_clear, (void *)server_recv_future_wrapper_tp_clear},
//     {0, 0}};

// int server_wrapper_tp_traverse(PyObject *self, visitproc visit, void *arg) {
// #if PY_VERSION_HEX >= 0x03090000
//   Py_VISIT(Py_TYPE(self));
// #endif
//   if (!nb::inst_ready(self)) {
//     return 0;
//   }
//   auto *w = nb::inst_ptr<Server>(self);
//   for (auto req : w->recv_reqs_) {
//     Py_VISIT(req.ptr());
//   }
//   for (auto req : w->send_reqs_) {
//     Py_VISIT(req.ptr());
//   }
//   if (w->recv_q_.data.has_value()) {
//     Py_VISIT(w->recv_q_.data->recv_future.ptr());
//   }
//   if (w->send_q_.data.has_value()) {
//     Py_VISIT(w->send_q_.data->send_future.ptr());
//   }
//   Py_VISIT(w->connect_callback_.ptr());
//   return 0;
// }
// int server_wrapper_tp_clear(PyObject *self) {
//   auto *w = nb::inst_ptr<Server>(self);
//   w->recv_reqs_.clear();
//   w->send_reqs_.clear();
//   w->recv_q_.data.reset();
//   w->send_q_.data.reset();
//   w->recv_q_.status.store(0, std::memory_order_release);
//   w->send_q_.status.store(0, std::memory_order_release);
//   w->connect_callback_.release();
//   return 0;
// }
// PyType_Slot server_wrapper_slots[] = {
//     {Py_tp_traverse, (void *)server_wrapper_tp_traverse},
//     {Py_tp_clear, (void *)server_wrapper_tp_clear},
//     {0, 0}};
// int client_wrapper_tp_traverse(PyObject *self, visitproc visit, void *arg) {
// #if PY_VERSION_HEX >= 0x03090000
//   Py_VISIT(Py_TYPE(self));
// #endif
//   if (!nb::inst_ready(self)) {
//     return 0;
//   }
//   auto *w = nb::inst_ptr<Client>(self);
//   for (auto const &send : w->sends_) {
//     Py_VISIT(send.ptr());
//   }
//   for (auto const &recv : w->recv_reqs_) {
//     Py_VISIT(recv.ptr());
//   }
//   if (w->send_q_.data.has_value()) {
//     Py_VISIT(w->send_q_.data.value().send_future.ptr());
//   }
//   if (w->recv_q_.data.has_value()) {
//     Py_VISIT(w->recv_q_.data.value().recv_future.ptr());
//   }
//   return 0;
// }
// int client_wrapper_tp_clear(PyObject *self) {
//   auto *w = nb::inst_ptr<Client>(self);
//   w->sends_.clear();
//   w->recv_reqs_.clear();
//   w->send_q_.data.reset();
//   w->send_q_.status.store(0, std::memory_order_release);
//   w->recv_q_.data.reset();
//   w->recv_q_.status.store(0, std::memory_order_release);
//   return 0;
// }
// PyType_Slot client_wrapper_slots[] = {
//     {Py_tp_traverse, (void *)client_wrapper_tp_traverse},
//     {Py_tp_clear, (void *)client_wrapper_tp_clear},
//     {0, 0}};

// NB_MODULE(_bindings, m) {
//   nb::class_<ClientSendFuture>(m, "ClientSendFuture",
//                                nb::type_slots(client_send_future_wrapper_slots))
//       .def("done", &ClientSendFuture::done)
//       .def("wait", &ClientSendFuture::wait,
//            nb::call_guard<nb::gil_scoped_release>())
//       .def("exception", &ClientSendFuture::exception);

//   nb::class_<ClientRecvFuture>(m, "ClientRecvFuture",
//                                nb::type_slots(client_recv_future_wrapper_slots))
//       .def("done", &ClientRecvFuture::done)
//       .def("exception", &ClientRecvFuture::exception)
//       .def("info", &ClientRecvFuture::info)
//       .def("wait", &ClientRecvFuture::wait,
//            nb::call_guard<nb::gil_scoped_release>());

//   nb::class_<Client>(m, "Client", nb::type_slots(client_wrapper_slots))
//       .def(nb::init<std::string_view, uint64_t>(), "addr"_a, "port"_a)
//       .def("send", &Client::send, "buffer"_a, "tag"_a, "done_callback"_a,
//            "fail_callback"_a,
//            nb::sig(
//                "def send(self, buffer: Annotated[NDArray[numpy.uint8], "
//                "dict(shape=(None,), device='cpu')], tag: int, done_callback: "
//                "Callable[[ClientSendFuture], None], fail_callback: "
//                "Callable[[ClientSendFuture], None]) -> ClientSendFuture"))
//       .def("recv", &Client::recv, "buffer"_a, "tag"_a, "tag_mask"_a,
//            "done_callback"_a, "fail_callback"_a,
//            nb::sig("def recv(self, buffer: Annotated[NDArray[numpy.uint8], "
//                    "dict(shape=(None,), device='cpu')], tag: int, tag_mask: "
//                    "int, done_callback: Callable[[ClientRecvFuture], None], "
//                    "fail_callback: Callable[[ClientRecvFuture], None]) -> "
//                    "ClientRecvFuture"))
//       .def("evaluate_perf", &Client::evaluate_perf, "msg_size"_a,
//            nb::call_guard<nb::gil_scoped_release>());

//   nb::class_<ServerSendFuture>(m, "ServerSendFuture",
//                                nb::type_slots(server_send_future_wrapper_slots))
//       .def("done", &ServerSendFuture::done)
//       .def("wait", &ServerSendFuture::wait,
//            nb::call_guard<nb::gil_scoped_release>())
//       .def("exception", &ServerSendFuture::exception);

//   nb::class_<ServerRecvFuture>(m, "ServerRecvFuture",
//                                nb::type_slots(server_recv_future_wrapper_slots))
//       .def("done", &ServerRecvFuture::done)
//       .def("exception", &ServerRecvFuture::exception)
//       .def("info", &ServerRecvFuture::info)
//       .def("wait", &ServerRecvFuture::wait,
//            nb::call_guard<nb::gil_scoped_release>());

//   nb::class_<ServerEndpoint>(m, "ServerEndpoint")
//       .def(nb::init<>())
//       .def_ro("name", &ServerEndpoint::name)
//       .def_ro("local_addr", &ServerEndpoint::local_addr)
//       .def_ro("local_port", &ServerEndpoint::local_port)
//       .def_ro("remote_addr", &ServerEndpoint::remote_addr)
//       .def_ro("remote_port", &ServerEndpoint::remote_port)
//       .def("view_transports", &ServerEndpoint::view_transports);

//   nb::class_<Server>(m, "Server", nb::type_slots(server_wrapper_slots))
//       .def(nb::init<std::string_view, uint64_t, nb::object>(), "addr"_a,
//            "port"_a, "connect_callback"_a,
//            nb::sig("def __init__(self, addr: str, port: int, connect_callback: "
//                    "Callable[[Server, ServerEndpoint], None])"))
//       .def("recv", &Server::recv, "buffer"_a, "tag"_a, "tag_mask"_a,
//            "done_callback"_a, "fail_callback"_a,
//            nb::sig("def recv(self, buffer: Annotated[NDArray[numpy.uint8], "
//                    "dict(shape=(None,), device='cpu')], tag: int, tag_mask: "
//                    "int, done_callback: Callable[[ServerRecvFuture], None], "
//                    "fail_callback: Callable[[ServerRecvFuture], None]) -> "
//                    "ServerRecvFuture"))
//       .def("list_clients", &Server::list_clients,
//            nb::rv_policy::reference_internal)
//       .def("send", &Server::send, "client_ep"_a, "buffer"_a, "tag"_a,
//            "done_callback"_a, "fail_callback"_a,
//            nb::sig("def send(self, client_ep: ServerEndpoint, buffer:  "
//                    "Annotated[NDArray[numpy.uint8], dict(shape=(None,), "
//                    "device='cpu')], tag: int, done_callback: "
//                    "Callable[[ServerSendFuture], None], fail_callback: "
//                    "Callable[[ServerSendFuture], None]) -> ServerSendFuture"))
//       .def("evaluate_perf", &Server::evaluate_perf, "client_ep"_a, "msg_size"_a,
//            nb::call_guard<nb::gil_scoped_release>());
//   m.def("ucp_get_version", []() { return ucp_get_version_string(); });
// }
