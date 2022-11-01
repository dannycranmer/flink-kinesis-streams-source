package software.amazon.kinesis.source.reader;

import org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordBatch;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordPublisher;
import org.apache.flink.streaming.connectors.kinesis.model.SequenceNumber;

import lombok.Builder;
import lombok.Getter;
import lombok.SneakyThrows;
import software.amazon.kinesis.source.split.KinesisShardSplit;

import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber.SENTINEL_LATEST_SEQUENCE_NUM;

public class KinesisShardSplitConsumer {

    @Getter
    private final KinesisShardSplit split;
    private final RecordPublisher recordPublisher;
    private SequenceNumber latestSequenceNumber = SENTINEL_LATEST_SEQUENCE_NUM.get();

    @Builder
    public KinesisShardSplitConsumer(
            final KinesisShardSplit split,
            final RecordPublisher recordPublisher) {
        this.split = split;
        this.recordPublisher = recordPublisher;
    }

    @SneakyThrows
    public RecordBatch getNextBatch() {
        // For now this is blocking, and should not be used with EFO
        final AtomicReference<RecordBatch> recordBatchReference = new AtomicReference<>();

        recordPublisher.run(recordBatch -> {
            recordBatchReference.set(recordBatch);

            if (!recordBatch.getDeaggregatedRecords().isEmpty()) {
                latestSequenceNumber = new SequenceNumber(recordBatch
                        .getDeaggregatedRecords()
                        .get(recordBatch.getDeaggregatedRecordSize() - 1)
                        .getSequenceNumber());
            }

            return latestSequenceNumber;
        });

        return recordBatchReference.get();
    }

}
