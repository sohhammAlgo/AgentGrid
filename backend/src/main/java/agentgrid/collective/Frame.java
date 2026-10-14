package agentgrid.collective;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * One collective message on the wire, big-endian and length-prefixed:
 * opId (8) | op (1) | epoch (8) | srcRank (4) | dstRank (4) | seq (4) | payloadLength (4) | payload.
 *
 * srcRank CONTROL is the control plane telling a rank to take part in operation opId (payload:
 * JSON parameters); the rank answers on the same connection with dstRank CONTROL (payload: JSON
 * result). Rank-to-rank frames carry the data itself: BCAST the raw bytes, SCATTER the chunk as
 * int32s, GATHER an 8-byte chunk tag and then the chunk. seq is the round the sender sent it in.
 */
record Frame(long opId, byte op, long epoch, int srcRank, int dstRank, int seq, byte[] payload) {

    static final byte BCAST = 1;
    static final byte SCATTER = 2;
    static final byte GATHER = 3;
    /** Local step of the scatter-compute-gather demo: each rank squares its chunk (no messages). */
    static final byte COMPUTE = 4;

    static final int CONTROL = -1;

    /** Far above any legal payload (1000 ints, a 1024-character message, JSON around them). */
    static final int MAX_PAYLOAD = 1 << 20;

    void write(DataOutputStream out) throws IOException {
        out.writeLong(opId);
        out.writeByte(op);
        out.writeLong(epoch);
        out.writeInt(srcRank);
        out.writeInt(dstRank);
        out.writeInt(seq);
        out.writeInt(payload.length);
        out.write(payload);
        out.flush();
    }

    /** The next frame; EOFException at a clean end of stream, IOException for bytes that are not a frame. */
    static Frame read(DataInputStream in) throws IOException {
        long opId = in.readLong();
        byte op = in.readByte();
        long epoch = in.readLong();
        int src = in.readInt();
        int dst = in.readInt();
        int seq = in.readInt();
        int length = in.readInt();
        if (op < BCAST || op > COMPUTE || length < 0 || length > MAX_PAYLOAD) {
            throw new IOException("not a collective frame (op " + op + ", payload length " + length + ")");
        }
        byte[] payload = new byte[length];
        in.readFully(payload);
        return new Frame(opId, op, epoch, src, dst, seq, payload);
    }

    static String name(byte op) {
        return switch (op) {
            case BCAST -> "BCAST";
            case SCATTER -> "SCATTER";
            case GATHER -> "GATHER";
            default -> "COMPUTE";
        };
    }

    static byte[] ints(int[] values) {
        ByteBuffer b = ByteBuffer.allocate(values.length * 4);
        for (int v : values) {
            b.putInt(v);
        }
        return b.array();
    }

    static int[] ints(byte[] bytes, int offset) {
        ByteBuffer b = ByteBuffer.wrap(bytes, offset, bytes.length - offset);
        int[] out = new int[b.remaining() / 4];
        for (int i = 0; i < out.length; i++) {
            out[i] = b.getInt();
        }
        return out;
    }
}
