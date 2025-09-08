# Starway

Starway aims to be an ultra-fast communication library, which features:

1. Zero Copy, supports sending from pointer and receiving into pre-allocated buffer.
2. RDMA support, by utilizing OpenMPI/OpenUCX for transportation.
3. Ease of use, generally should work out of the box, and don't require much configuration efforts.

Current Python alternatives are lacking core features:

1. ZeroMQ, no support for RDMA.
2. MPI, hard to use, hard to setup environment properly.
