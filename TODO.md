# BorgFS - TODO List

This document outlines the planned features, known limitations, and areas for improvement in the BorgFS project. Contributions and pull requests addressing these points are highly welcome!

## High Priority / Architecture Improvements

* **Refactor Write Buffer Mechanism:** Currently, the entire file buffer is held in RAM (`file_ctx_t -> data`) before a commit. While this is protected against overflow in the `borgfs_write` function, it is highly inefficient for very large files. This needs to be optimized (e.g., streaming chunks directly to disk instead of hoarding them in memory).
* **Implement Asynchronous I/O (AIO):** The storage backend currently uses synchronous I/O. Introducing AIO will significantly improve read/write performance, especially on HDDs.
* **Codebase Modularization:** The `main.c` file has grown quite large. The codebase needs to be split into smaller, logical C files (e.g., `fuse_ops.c`, `storage.c`, `cdc.c`) and compiled into separate object files via the `Makefile`.

## Future Enhancements

* **Garbage Collection (GC):** Improve and finalize the offline/online garbage collection logic for orphaned chunks.
* **Extended FUSE Operations:** Implement proper handling for symlinks, hardlinks, and extended attributes (xattr).
* **Unit Testing:** Expand the `test/` directory with comprehensive unit tests for core components (like the FastCDC implementation and chunk addressing).

