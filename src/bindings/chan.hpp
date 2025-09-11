#pragma once
#include <algorithm>
#include <atomic>
#include <thread>
#include <tuple>
#include <utility>

template <class... Args> inline consteval static auto type_max_size() noexcept {
  return std::ranges::max({sizeof(Args)...});
}
template <class O, class... Ts> inline consteval static bool one_of() noexcept {
  return (std::is_same_v<O, Ts> || ...);
}

template <class... Args> struct MultiChannel {
  constexpr static auto max_size = type_max_size<Args...>();
  using Types = std::tuple<Args...>;

  // helper to get type tag
  void visit(int64_t type_idx, auto &&visitor) {
    [&]<size_t... Is>(std::index_sequence<Is...> &&) {
      ((Is == type_idx &&
        (std::forward<decltype(visitor)>(visitor)(
             reinterpret_cast<std::tuple_element_t<Is, Types> *>(data_.data())),
         true)),
       ...);
    }(std::make_index_sequence<std::tuple_size_v<Types>>());
  }
  template <class T>
  constexpr static int64_t index_of()
    requires(one_of<T, Args...>())
  {
    int64_t res = -1;
    [&]<size_t... Is>(std::index_sequence<Is...> &&) {
      ((std::is_same_v<T, std::tuple_element_t<Is, Types>> && (res = Is)), ...);
    }(std::make_index_sequence<std::tuple_size_v<Types>>());
    return res;
  }

public:
  [[nodiscard]] auto full() const {
    return status.load(std::memory_order_acquire) >= 0;
  }

  // consumer should take charge of calling dtor,
  // as sometimes it would require GIL in reader func
  auto try_consume(auto &&reader) {
    auto cur = status.load(std::memory_order_acquire);
    if (cur < 0) {
      return false;
    }
    visit(cur, [&](auto const &src) {
      std::forward<decltype(reader)>(reader)(src);
    });
    status.store(-1, std::memory_order_release);
    return true;
  }

  // writer should take charge of calling ctor,
  // as sometimes it would require GIL in writer func
  template <class T>
  void wait_emplace(auto &&writer)
    requires(one_of<T, Args...>()) &&
            (requires(decltype(writer) &&w, T *ptr) { w(ptr); })
  {
    // optimistic
    int64_t expected{-1};
    for (;;) {
      if (status.compare_exchange_weak(expected, -2,
                                       std::memory_order_acq_rel)) {
        writer(reinterpret_cast<T *>(data_.data()));
        status.store(index_of<T>(), std::memory_order_release);
        return;
      }
      while (status.load(std::memory_order_acquire) != -1) {
        std::this_thread::yield();
      }
    }
  }
  std::atomic<int64_t> status{-1}; // -1: spare, -2: loading, >= 0: type index
  std::array<std::byte, max_size> data_;
};

template <class T> struct Channel {
  bool wait_emplace(auto &&writer)
    requires requires(decltype(writer) &&w, T *ptr) { w(ptr); }
  {
    uint8_t expected{0};
    for (;;) {
      if (status.compare_exchange_weak(expected, 1,
                                       std::memory_order_acq_rel)) {
        writer(&data_);
        status.store(2, std::memory_order_release);
        return true;
      }
      while (status.load(std::memory_order_acquire) != 0 &&
             status.load(std::memory_order_acquire) != 3) {
        std::this_thread::yield();
      }
      if (status.load(std::memory_order_acquire) == 3) {
        // pipe closed
        return false;
      }
    }
  }
  bool try_consume(auto &&reader)
    requires requires(decltype(reader) &&r, T *ptr) { r(ptr); }
  {
    if (status.load(std::memory_order_acquire) != 2) {
      return false;
    }
    reader(&data_);
    status.store(0, std::memory_order_release);
    return true;
  }
  void close() { status.store(3, std::memory_order_release); }

  std::atomic<uint8_t> status{0}; // 0: spare, 1: loading, 2: ready 3: closed
  T data_{};
};