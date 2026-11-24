package io.github.embeddedbench.db;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class PgLiteWireBridgeTest {
    @Test
    void fragmentedAndPipelinedTcpReadsAreReassembledIntoExactlyOneFrontendFrame() throws Exception {
        byte[] first = "SELECT 1;\0".getBytes(StandardCharsets.UTF_8);
        byte[] second = "SELECT 2;\0".getBytes(StandardCharsets.UTF_8);
        byte[] pipeline = ByteBuffer.allocate(8 + first.length + second.length)
                .putInt(first.length + 4).put(first).putInt(second.length + 4).put(second).array();
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(pipeline) {
            @Override
            public synchronized int read(byte[] bytes, int offset, int length) {
                return super.read(bytes, offset, Math.min(length, 1));
            }
        })) {
            assertArrayEquals(ByteBuffer.allocate(first.length + 5).put((byte) 'Q')
                    .putInt(first.length + 4).put(first).array(), PgLiteWireBridge.readMessage(input, 'Q'));
            assertArrayEquals(ByteBuffer.allocate(second.length + 5).put((byte) 'Q')
                    .putInt(second.length + 4).put(second).array(), PgLiteWireBridge.readMessage(input, 'Q'));
            assertEquals(-1, input.read());
        }
    }

    @Test
    void startupMessageHasNoTypeByteAndMalformedOrTruncatedFramesFailBeforeExecution() throws Exception {
        byte[] startup = ByteBuffer.allocate(8).putInt(8).putInt(196608).array();
        assertArrayEquals(startup, PgLiteWireBridge.readMessage(
                new DataInputStream(new ByteArrayInputStream(startup)), -1));
        assertThrows(EOFException.class, () -> PgLiteWireBridge.readMessage(new DataInputStream(
                new ByteArrayInputStream(ByteBuffer.allocate(4).putInt(12).array())), 'Q'));
        for (int length : new int[]{-1, 3, 64 * 1024 * 1024 + 1}) {
            assertThrows(IOException.class, () -> PgLiteWireBridge.readMessage(new DataInputStream(
                    new ByteArrayInputStream(ByteBuffer.allocate(4).putInt(length).array())), 'Q'));
        }
    }
}
