package software.amazon.kinesis.source.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.kinesis.shaded.com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordBatch;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordPublisher;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.polling.PollingRecordPublisherFactory;
import org.apache.flink.streaming.connectors.kinesis.model.StartingPosition;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxy;

import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.source.split.KinesisShardSplit;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static java.util.Collections.emptySet;
import static org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber.SENTINEL_LATEST_SEQUENCE_NUM;

@Slf4j
@Builder
public class KinesisShardSplitReader implements SplitReader<UserRecord, KinesisShardSplit> {

    private static final PollingRecordPublisherFactory PUBLISHER_FACTORY = new PollingRecordPublisherFactory(
            consumerConfig -> new KinesisProxy(consumerConfig) {});

    private final MetricGroup metricGroup;
    private final Properties consumerConfig;
    private final Deque<KinesisShardSplitConsumer> roundRobinQueue = new ArrayDeque<>();
    private final Set<String> finishedSplits = new HashSet<>();

    @Override
    @SneakyThrows
    public RecordsWithSplitIds<UserRecord> fetch() throws IOException {
        KinesisShardSplitConsumer consumer = roundRobinQueue.poll();

        if (consumer.isShardComplete()) {
            log.info("Marking shard " + consumer.getSplit().splitId() + " as finished");
            finishedSplits.add(consumer.getSplit().splitId());
        } else {
            roundRobinQueue.add(consumer);
        }

        System.out.println("Consuming from Shard: " + consumer.getSplit().splitId());

        return new KinesisShardSplitRecords(consumer.getSplit(), consumer.getNextBatch());
    }

    @Override
    @SneakyThrows
    public void handleSplitsChanges(SplitsChange<KinesisShardSplit> splitsChange) {
        log.info("handleSplitsChanges()");

        for (KinesisShardSplit split : splitsChange.splits()) {
            final StartingPosition startingPosition = StartingPosition.continueFromSequenceNumber(SENTINEL_LATEST_SEQUENCE_NUM.get());
            final RecordPublisher recordPublisher = PUBLISHER_FACTORY.create(startingPosition, consumerConfig, metricGroup, split.getShardHandle());

            roundRobinQueue.add(KinesisShardSplitConsumer.builder()
                    .recordPublisher(recordPublisher)
                    .split(split)
                    .build());
        }
    }

    @Override
    public void wakeUp() {
        log.info("wakeUp()");
    }

    @Override
    public void close() throws Exception {
        log.info("close()");
    }

    /**
     * We can use this class to decide when we need to consume child shards in the future (I think).
     */
    private static class KinesisShardSplitRecords implements RecordsWithSplitIds<UserRecord> {

        private final KinesisShardSplit split;
        private final Iterator<UserRecord> recordIterator;

        public KinesisShardSplitRecords(final KinesisShardSplit split, final RecordBatch recordBatch) {
            // This does not support aggregated records
            this.split = split;
            this.recordIterator = Optional.ofNullable(recordBatch)
                    .map(RecordBatch::getDeaggregatedRecords)
                    .orElse(Collections.emptyList())
                    .iterator();
        }

        @Nullable
        @Override
        public String nextSplit() {
            log.info("nextRecordFromSplit");
            return recordIterator.hasNext() ? split.splitId() : null;
        }

        @Nullable
        @Override
        public UserRecord nextRecordFromSplit() {
            return recordIterator.hasNext() ? recordIterator.next() : null;
        }

        @Override
        public Set<String> finishedSplits() {
            log.info("finishedSplits");
            // We need to return shards here that are finished
            return emptySet();
        }
    }
}
