#include <arpa/inet.h>
#include <chrono>
#include <cstring>
#include <netinet/in.h>
#include <print>
#include <ucp/api/ucp.h>
#include <ucs/type/status.h>

int64_t request_done = 0;

void custom_send_callback(void *request, ucs_status_t status, void *user_data) {
  if (status == UCS_OK) {
    std::println("Message sent successfully. User data: {}",
                 *(int64_t *)user_data);
    ++request_done;
  } else {
    std::println("Failed to send message. Status: {}, User data: {}",
                 ucs_status_string(status), *(int64_t *)user_data);
  }
}
char message[50] = {0};
int main() {
  std::memset(message, 0x3f, sizeof(message));
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

  // create client endpoint and connect
  struct sockaddr_in connect_addr{
      .sin_family = AF_INET,
      .sin_port = htons(12345),
      .sin_addr = {inet_addr("127.0.0.1")},
  };
  ucp_ep_params_t ep_params;
  ucp_ep_h ucp_ep;
  ep_params.field_mask =
      UCP_EP_PARAM_FIELD_FLAGS | UCP_EP_PARAM_FIELD_SOCK_ADDR;
  ep_params.flags = UCP_EP_PARAMS_FLAGS_CLIENT_SERVER;
  ep_params.sockaddr.addr = reinterpret_cast<struct sockaddr *>(&connect_addr);
  ep_params.sockaddr.addrlen = sizeof(connect_addr);
  ucp_ep_create(ucp_worker, &ep_params, &ucp_ep);

  // send message
  int64_t some_custom_user_data_when_callback = 42;
  ucp_request_param_t send_param{
      .op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_USER_DATA,
      .cb{.send = custom_send_callback},
      .user_data = &some_custom_user_data_when_callback};
  // sequential send
  std::println("Testing: sequential send");
  std::vector<std::chrono::nanoseconds> durations;
  for (int i = 0; i < 10; ++i) {
    auto t0 = std::chrono::high_resolution_clock::now();
    message[0] = 'A' + (i % 26);
    auto status =
        ucp_tag_send_nbx(ucp_ep, message, sizeof(message), 123, &send_param);
    if (reinterpret_cast<int64_t>(status) == UCS_OK) {
      std::println("Message sent immediately.");
      std::println("Request done: {}", request_done);
    } else if (UCS_PTR_IS_ERR(status)) {
      std::println("Failed to send message. Status: {}",
                   ucs_status_string(UCS_PTR_STATUS(status)));
    } else {
      std::println("Send request is in progress.");
      // wait for request to be done
      while (ucp_request_check_status(status) == UCS_INPROGRESS) {
        if (ucp_worker_progress(ucp_worker) > 0) {
          std::println("Worker made progress.");
        }
      }
      std::println("Request done: {}", request_done);
      ucp_request_free(status);
    }
    auto t1 = std::chrono::high_resolution_clock::now();
    durations.emplace_back(
        std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0));
  }
  for (const auto &d : durations) {
    std::println("Duration: {} ns", d.count());
  }

  std::println("Testing: concurrent send");
  durations.clear();
  std::vector<void *> requests;
  requests.reserve(100);
  request_done = 0;
  auto t0 = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < 10; ++i) {
    message[0] = 'A' + (i % 26);
    auto status =
        ucp_tag_send_nbx(ucp_ep, message, sizeof(message), i, &send_param);

    if (reinterpret_cast<int64_t>(status) == UCS_OK) {
      ++request_done;
    } else if (UCS_PTR_IS_ERR(status)) {
      std::println("Failed to send message. Status: {}",
                   ucs_status_string(UCS_PTR_STATUS(status)));
    } else {
      requests.emplace_back(status);
    }
  }
  while (request_done < 10) {
    ucp_worker_progress(ucp_worker);
  }
  for (auto req : requests) {
    ucp_request_free(req);
  }
  auto t1 = std::chrono::high_resolution_clock::now();
  std::println("Request done: {}", request_done);
  std::println(
      "Takes: {} ns",
      std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
  ucp_request_param_t close_param{};
  auto close_req = ucp_ep_close_nbx(ucp_ep, &close_param);
  if (close_req != NULL) {
    if (UCS_PTR_IS_ERR(close_req)) {
      std::println("Failed to close endpoint. Status: {}",
                   ucs_status_string(UCS_PTR_STATUS(close_req)));
    } else {
      while (ucp_request_check_status(close_req) == UCS_INPROGRESS) {
        ucp_worker_progress(ucp_worker);
      }
      ucp_request_free(close_req);
      std::println("Endpoint closed successfully.");
    }
  }
  ucp_worker_destroy(ucp_worker);
  ucp_cleanup(ucp_context);
  return 0;
}