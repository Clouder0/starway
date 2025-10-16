# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Starway is an ultra-fast communication library for Python that provides zero-copy, RDMA-enabled, full-duplex asynchronous communication. It wraps OpenUCX via C++20 nanobind extensions to deliver high-performance networking with an easy-to-use Python asyncio API.

## Core Architecture

### Three-Layer Design

1. **Python API Layer** (`src/starway/__init__.py`):
   - Exposes `Client` and `Server` classes with async/await interface
   - Wraps C++ `_Client` and `_Server` from `_bindings` module
   - Handles asyncio integration via callbacks and futures
   - Manages UCX library loading (system vs wheel package via `STARWAY_USE_SYSTEM_UCX`)

2. **C++ Bindings Layer** (`src/bindings/main.{cpp,hpp}`):
   - Implements core UCX functionality in C++20
   - Key classes: `Context`, `Server`, `Client`, `ServerEndpoint`
   - Uses nanobind for Python interop
   - Manages UCX workers, endpoints, and async operations

3. **UCX Layer**:
   - OpenUCX provides RDMA transport and zero-copy operations
   - Dynamically linked via system libraries or `libucx-cu12` wheel

### Async Pattern: Python asyncio ↔ C++ UCX

The async bridge works by:
- Python methods (e.g., `asend()`, `arecv()`) create `asyncio.Future` objects
- C++ callbacks are registered with UCX operations
- On completion, C++ calls Python callbacks via `loop.call_soon_threadsafe()`
- This allows UCX's native async to integrate with Python's event loop

Example flow:
```
Client.asend() → creates Future → _client.send(done_cb, fail_cb)
→ UCX async send → on complete: done_cb()
→ loop.call_soon_threadsafe(future.set_result)
```

## Build and Development Commands

### Setup
```bash
# Install dependencies (Python, nanobind, test tools)
uv sync --group dev --group test

# Editable install (triggers scikit-build to compile C++ bindings)
uv run python -m pip install -e .
```

### C++ Development
```bash
# Configure CMake with GCC debug preset
uv run cmake --preset gcc-debug

# Build C++ bindings directly (faster iteration when changing C++ only)
uv run cmake --build --preset gcc-debug
```

### Testing
```bash
# Run full test suite (asyncio-based integration tests)
uv run pytest tests/test_basic.py -vv

# Run specific test
uv run pytest tests/test_basic.py::test_name -vv

# For RDMA/flush testing, set UCX transport
UCS_TLS=tcp uv run pytest tests/test_basic.py -vv
```

### Building Distributions
```bash
# Build source and wheel distributions
uv run python -m build
```

## Key Design Decisions

### UCX Library Loading Strategy
- Controlled by `STARWAY_USE_SYSTEM_UCX` env var (defaults to "true")
- Fallback chain: system → wheel → error
- Allows flexibility for different deployment scenarios (HPC clusters vs PyPI installs)

### Full-Duplex Communication
- Both `Server` and `Client` can send/recv simultaneously
- Uses UCX tags for message matching (tag + tag_mask)
- `ServerEndpoint` represents individual connected clients on server side

### Zero-Copy Support
- Operates directly on NumPy array buffers (`NDArray[np.uint8]`)
- No intermediate copies between Python and UCX
- Requires pre-allocated receive buffers

### Flush Semantics
- `flush()` / `aflush()`: Flush all pending operations for all endpoints
- `flush_ep()` / `aflush_ep()`: Flush specific client endpoint
- Critical for ensuring data delivery before cleanup or synchronization

## Important File Locations

- `src/starway/__init__.py` - Python API, async wrappers, UCX loading logic
- `src/bindings/main.cpp` - C++ implementation (keep aligned with main.hpp)
- `src/bindings/main.hpp` - C++ header declarations
- `tests/test_basic.py` - Integration tests covering bidirectional messaging
- `CMakeLists.txt` - Build configuration for nanobind extension
- `pyproject.toml` - Python packaging, scikit-build config, cibuildwheel settings

## Testing Patterns

- Use `gen_server_client` context manager for setup in new tests
- Structure tests as async coroutines (`async def test_*`)
- Cover both client→server and server→client paths
- Use buffers large enough to test zero-copy behavior
- Set `UCS_TLS=tcp` for flush-related testing

## Commit Convention

Follow Conventional Commits pattern:
- `feat(Client): add new feature`
- `fix(Server): resolve bug`
- `chore: maintenance task`
- Keep subject ≤72 characters, imperative mood
