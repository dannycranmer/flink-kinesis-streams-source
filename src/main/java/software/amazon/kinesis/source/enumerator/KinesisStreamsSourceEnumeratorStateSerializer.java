package software.amazon.kinesis.source.enumerator;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import software.amazon.kinesis.source.split.KinesisShardSplit;

import java.io.IOException;

public class KinesisStreamsSourceEnumeratorStateSerializer implements SimpleVersionedSerializer<KinesisStreamsSourceEnumeratorState> {

    private static final int CURRENT_VERSION = 0;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(KinesisStreamsSourceEnumeratorState kinesisStreamsSourceEnumeratorState) throws IOException {
        return new byte[0];
    }

    @Override
    public KinesisStreamsSourceEnumeratorState deserialize(int i, byte[] bytes) throws IOException {
        return null;
    }
}
