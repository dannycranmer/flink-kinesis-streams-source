package software.amazon.kinesis.source.split;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class KinesisShardSplit implements SourceSplit {

    private final StreamShardHandle shardHandle;

    @Override
    public String splitId() {
        return shardHandle.getShard().getShardId();
    }

    @Override
    public String toString() {
        return "KinesisShardSplit{" +
            "shardHandle=" + shardHandle +
            '}';
    }
}
