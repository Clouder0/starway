#include <arpa/inet.h>
#include <cstddef>
#include <cstring>
#include <netinet/in.h>
#include <print>
#include <thread>
#include <ucp/api/ucp.h>
#include <ucp/api/ucp_compat.h>
#include <ucs/type/status.h>
#include <vector>
using namespace std::chrono_literals;

int64_t request_done = 0;

typedef struct ucx_server_ctx {
  ucp_worker_h worker;
  ucp_listener_h listener;
  ucp_ep_h ep;
} ucx_server_ctx_t;

void server_conn_handler(ucp_conn_request *conn_req, void *arg) {
  std::println("New connection established.");
  auto *ctx = reinterpret_cast<ucx_server_ctx_t *>(arg);
  ucp_ep_params_t ep_params{.field_mask = UCP_EP_PARAM_FIELD_CONN_REQUEST,
                            .conn_request = conn_req};
  auto status = ucp_ep_create(ctx->worker, &ep_params, &ctx->ep);
  if (status != UCS_OK) {
    std::println(
        "Failed to create endpoint from connection request. Status: {}",
        ucs_status_string(status));
  } else {
    std::println("Endpoint created successfully from connection request.");
  }
}

void custom_recv_callback(void *request, ucs_status_t status,
                          const ucp_tag_recv_info *info, void *user_data) {
  if (status == UCS_OK) {
    std::println("Message received successfully. User data: {}",
                 *(int64_t *)user_data);
    ++request_done;
  } else {
    std::println("Failed to receive message. Status: {}, User data: {}",
                 ucs_status_string(status), *(int64_t *)user_data);
  }
}

static std::string
sockaddr_get_ip_str(const struct sockaddr_storage *sock_addr) {
  char buf[50];
  struct sockaddr_in addr_in;
  struct sockaddr_in6 addr_in6;

  switch (sock_addr->ss_family) {
  case AF_INET:
    memcpy(&addr_in, sock_addr, sizeof(struct sockaddr_in));
    inet_ntop(AF_INET, &addr_in.sin_addr, buf, 50);
    return std::string(buf);
  case AF_INET6:
    memcpy(&addr_in6, sock_addr, sizeof(struct sockaddr_in6));
    inet_ntop(AF_INET6, &addr_in6.sin6_addr, buf, 50);
    return std::string(buf);
  default:
    return "Invalid address family";
  }
}

static std::string
sockaddr_get_port_str(const struct sockaddr_storage *sock_addr) {
  char buf[50];
  struct sockaddr_in addr_in;
  struct sockaddr_in6 addr_in6;

  switch (sock_addr->ss_family) {
  case AF_INET:
    memcpy(&addr_in, sock_addr, sizeof(struct sockaddr_in));
    snprintf(buf, 50, "%d", ntohs(addr_in.sin_port));
    return std::string(buf);
  case AF_INET6:
    memcpy(&addr_in6, sock_addr, sizeof(struct sockaddr_in6));
    snprintf(buf, 50, "%d", ntohs(addr_in6.sin6_port));
    return std::string(buf);
  default:
    return "Invalid address family";
  }
}

char message[50] = {0};
int main() {

  // init context
  ucp_context_h ucp_context;
  ucp_params_t ucp_params;
  ucp_params.field_mask = UCP_PARAM_FIELD_FEATURES;
  ucp_params.features = UCP_FEATURE_TAG | UCP_FEATURE_WAKEUP;
  ucp_init(&ucp_params, NULL, &ucp_context);

  // init worker
  ucp_worker_h ucp_worker;
  ucp_worker_params_t worker_params;
  worker_params.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
  worker_params.thread_mode = UCS_THREAD_MODE_SINGLE;
  ucp_worker_create(ucp_context, &worker_params, &ucp_worker);

  // create server listener and accept incoming conns
  ucx_server_ctx_t server_ctx = {.worker = ucp_worker};
  struct sockaddr_in listen_addr{
      .sin_family = AF_INET,
      .sin_port = htons(12345),
      .sin_addr = {inet_addr("127.0.0.1")},
  };
  ucp_listener_params_t listen_params{
      .field_mask = UCP_LISTENER_PARAM_FIELD_SOCK_ADDR |
                    UCP_LISTENER_PARAM_FIELD_CONN_HANDLER,
      .sockaddr{
          .addr = reinterpret_cast<struct sockaddr *>(&listen_addr),
          .addrlen = sizeof(listen_addr),
      },
      .conn_handler{
          .cb = server_conn_handler,
          .arg = &server_ctx,
      }};
  ucp_listener_create(ucp_worker, &listen_params, &server_ctx.listener);
  ucp_listener_attr_t attr{
      .field_mask = UCP_LISTENER_ATTR_FIELD_SOCKADDR,
  };
  ucp_listener_query(server_ctx.listener, &attr);
  std::println("Server is listening on IP {} port {}",
               sockaddr_get_ip_str(&attr.sockaddr),
               sockaddr_get_port_str(&attr.sockaddr));

  // recv message, sequentially
  int64_t some_custom_user_data_when_callback = 42;
  ucp_request_param_t recv_param{
      .op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_USER_DATA,
      .cb{.recv = custom_recv_callback},
      .user_data = &some_custom_user_data_when_callback};

  std::println("Testing: sequential recv");
  for (int i = 0; i < 10; ++i) {
    std::this_thread::sleep_for(1ms);
    auto status = ucp_tag_recv_nbx(ucp_worker, message, sizeof(message), 123, 0,
                                   &recv_param);
    if (status == NULL) {
      std::println("Message received immediately.");
      std::println("Request done: {}", request_done);
    } else if (UCS_PTR_IS_ERR(status)) {
      std::println("Failed to recv message. Status: {}",
                   ucs_status_string(UCS_PTR_STATUS(status)));
    } else {
      std::println("Recv request is in progress.");
      // wait for request to be done
      while (ucp_request_check_status(status) == UCS_INPROGRESS) {
        while (ucp_worker_progress(ucp_worker) > 0) {
          std::println("Worker made progress.");
        }
      }
      std::println("Request done: {}", request_done);
      ucp_request_free(status);
    }
  }

  // recv message, concurrently
  std::println("Testing: concurrent recv");
  request_done = 0;
  std::vector<void *> reqs;
  std::vector<int64_t> user_datas;
  user_datas.reserve(100);
  reqs.reserve(100);
  for (int i = 0; i < 10; ++i) {
    user_datas.emplace_back(i);
  }
  for (int i = 0; i < 10; ++i) {
    ucp_request_param_t param{.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                                              UCP_OP_ATTR_FIELD_USER_DATA,
                              .cb{.recv = custom_recv_callback},
                              .user_data = user_datas.data() + i};
    auto status =
        ucp_tag_recv_nbx(ucp_worker, message, sizeof(message), i, 0xFFFF, &param);
    if (status == NULL) {
      ++request_done;
      std::println("Request {} done immediately.", i);
    } else if (UCS_PTR_IS_ERR(status)) {
      std::println("Failed to recv message. Status: {}",
                   ucs_status_string(UCS_PTR_STATUS(status)));
    } else {
      reqs.emplace_back(status);
    }
  }
  while (request_done < reqs.size()) {
    ucp_worker_progress(ucp_worker);
  }
  for (auto req : reqs) {
    ucp_request_free(req);
  }

  // close endpoint
  ucp_request_param_t close_param{};
  auto close_req = ucp_ep_close_nbx(server_ctx.ep, &close_param);
  if (close_req != NULL) {
    if (UCS_PTR_IS_ERR(close_req)) {
      std::println("Failed to close endpoint. Status: {}",
                   ucs_status_string(UCS_PTR_STATUS(close_req)));
    } else {
      std::println("Closing endpoint...");
      while (ucp_request_check_status(close_req) == UCS_INPROGRESS) {
        ucp_worker_progress(ucp_worker);
      }
      ucp_request_free(close_req);
    }
  }
  ucp_listener_destroy(server_ctx.listener);
  ucp_worker_destroy(ucp_worker);
  ucp_cleanup(ucp_context);
  return 0;
}