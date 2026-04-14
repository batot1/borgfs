# BorgFS

BorgFS is a modern, deduplicating file system. 
"BorgFS was born out of frustration with the poor deduplication performance of existing file systems like ZFS and BTRFS. By leveraging the FastCDC algorithm for content-defined chunking, BorgFS offers a lightweight, highly efficient alternative built from the ground up for massive data deduplication, complete with built-in garbage collection and data scrubbing mechanisms."

## Project Structure

Here is an overview of the repository's structure and core components:

```text
borgfs/
 ├─ docs/                  # Documentation, PGP public keys, and test outputs (e.g., dd, iso)
 ├─ include/               # C header files
 ├─ test/                  # Testing scripts and suites
 ├─ tools/                 # Additional helper utilities
 ├─ bench_crc_blake.c      # Benchmark for CRC and Blake3 hashing
 ├─ main.c                 # Main file system implementation
 ├─ gc.c                   # Garbage collection logic
 ├─ scrub.c                # Data scrubbing and integrity checking
 └─ Makefile               # Build automations
```

## Building and Installation

To build the project, simply run:

```bash
make
```

## Contributing

We welcome contributions! Please see our [CONTRIBUTING.md](CONTRIBUTING.md) for details on how to submit pull requests.

**Important:** All contributors must agree to the Contributor License Agreement ([CLA.md](CLA.md)) to maintain the project's dual-licensing model. Every commit must be signed-off.

## License

This project is open source and licensed under the **GNU AGPLv3** (or later). See the [LICENSE.md](LICENSE.md) file for details.

### Commercial Licensing

For organizations unable to adopt AGPL terms, a commercial license is available. See [COMMERCIAl-LICENSE-OFFER.md](COMMERCIAl-LICENSE-OFFER.md) for a summary of terms and contact information.

