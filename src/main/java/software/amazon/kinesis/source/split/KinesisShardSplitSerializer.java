package software.amazon.kinesis.source.split;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.kinesis.shaded.com.amazonaws.services.kinesis.model.HashKeyRange;
import org.apache.flink.kinesis.shaded.com.amazonaws.services.kinesis.model.SequenceNumberRange;
import org.apache.flink.kinesis.shaded.com.amazonaws.services.kinesis.model.Shard;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class KinesisShardSplitSerializer implements SimpleVersionedSerializer<KinesisShardSplit> {

    private static final int CURRENT_VERSION = 0;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(KinesisShardSplit split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {
            out.writeUTF(split.getShardHandle().getStreamName());
            out.writeUTF(split.getShardHandle().getShard().getShardId());
            out.writeUTF(split.getShardHandle().getShard().getHashKeyRange().getStartingHashKey());
            out.writeUTF(split.getShardHandle().getShard().getHashKeyRange().getEndingHashKey());
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public KinesisShardSplit deserialize(int i, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             DataInputStream in = new DataInputStream(bais)) {
            return KinesisShardSplit.builder()
                    .shardHandle(new StreamShardHandle(in.readUTF(), new Shard()
                            .withShardId(in.readUTF())
                            .withHashKeyRange(new HashKeyRange()
                                    .withStartingHashKey(in.readUTF())
                                    .withEndingHashKey(in.readUTF()))))
                    .build();
        }
    }
}
