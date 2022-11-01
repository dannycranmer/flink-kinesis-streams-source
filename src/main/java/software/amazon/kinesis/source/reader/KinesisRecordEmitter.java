package software.amazon.kinesis.source.reader;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.kinesis.shaded.com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;

import lombok.Builder;
import software.amazon.kinesis.source.split.KinesisShardSplitState;

@Builder
public class KinesisRecordEmitter<T> implements RecordEmitter<UserRecord, T, KinesisShardSplitState> {

    private final DeserializationSchema<T> deserializationSchema;

    @Override
    public void emitRecord(
            UserRecord userRecord,
            SourceOutput<T> sourceOutput,
            KinesisShardSplitState kinesisShardSplitState) throws Exception {
        sourceOutput.collect(deserializationSchema.deserialize(userRecord.getData().array()));
    }
}
