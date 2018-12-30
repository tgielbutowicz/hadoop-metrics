package utils;

public enum FileSize {
    BYTES_IN_4_MB(4194304L),
    BYTES_IN_16_MB(16777216L),
    BYTES_IN_32_MB(33554432L),
    BYTES_IN_64_MB(67108864L);

    long bytes;

    FileSize(long bytes) {
        this.bytes = bytes;
    }

    public long getBytes() {
        return bytes;
    }
}
