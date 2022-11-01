package software.amazon.kinesis.source.enumerator;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.streaming.connectors.kinesis.proxy.GetShardListResult;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxy;

import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.source.split.KinesisShardSplit;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static java.util.Collections.singletonMap;

@Slf4j
@Builder
public class KinesisStreamsSourceEnumerator implements
        SplitEnumerator<KinesisShardSplit, KinesisStreamsSourceEnumeratorState> {

    private final String streamName;
    private final Properties consumerConfig;
    private final SplitEnumeratorContext<KinesisShardSplit> context;

    @Override
    public void start() {
        log.info("Starting KinesisStreamsSourceEnumerator");

        context.callAsync(this::discoverShards, (a, b) -> {});
    }

    @Override
    public void handleSplitRequest(int i, @Nullable String s) {

    }

    @Override
    public void addSplitsBack(List<KinesisShardSplit> list, int i) {

    }

    @Override
    public void addReader(int i) {

    }

    @Override
    public KinesisStreamsSourceEnumeratorState snapshotState(long l) throws Exception {
        return null;
    }

    @Override
    public void close() throws IOException {
        log.info("Closing KinesisStreamsSourceEnumerator");
    }

    @SneakyThrows
    private boolean discoverShards() {
        KinesisProxy kinesisProxy = new KinesisProxy(consumerConfig) {
            // Dodgy hack to call a protected constructor
        };

        GetShardListResult shardList = kinesisProxy.getShardList(singletonMap(streamName, null));

        // Because we like skew, all splits are going to subTask 0. Should probably improve this.
        final int subTaskId = 0;

        shardList.getRetrievedShardListOfStream(streamName)
                .forEach(shard -> {
                    log.info("Found new source split: " + shard.getShard().getShardId());
                    context.assignSplit(KinesisShardSplit.builder()
                            .shardHandle(shard)
                            .build(), subTaskId);
                });

        // Not sure what/why we need to return something here
        return false;
    }
}
