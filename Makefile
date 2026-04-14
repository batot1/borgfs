# Dependencies (Debian/Ubuntu)
# sudo apt install libfuse3-dev libssl-dev

CC ?= gcc
CFLAGS ?= -O2 -Wall -Wextra

# Default path to BLAKE3 (users can override this by running: make BLAKE3_DIR=/path/to/blake3)
BLAKE3_DIR ?= ../BLAKE3/c

INCLUDES := -I$(BLAKE3_DIR)
LIBS := $(shell pkg-config --cflags --libs fuse3) $(BLAKE3_DIR)/libblake3.a -lcrypto

TARGET = borgfs

.PHONY: all clean

all: $(TARGET)

$(TARGET): main.c
	$(CC) $(CFLAGS) $< $(INCLUDES) $(LIBS) -o $@

# Additional: FastCDC variant
#borgfs-fastcdc: borgfs_fuse_fastcdc.c
#	$(CC) $(CFLAGS) $< $(INCLUDES) $(LIBS) -o $@

clean:
	rm -f $(TARGET) borgfs
