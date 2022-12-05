package software.amazon.kinesis.source.enumerator;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.streaming.connectors.kinesis.proxy.GetShardListResult;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxy;

import org.apache.flink.shaded.guava30.com.google.common.collect.Sets;

import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.source.split.KinesisShardSplit;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.singletonMap;

@Slf4j
@Builder
public class KinesisStreamsSourceEnumerator implements
    SplitEnumerator<KinesisShardSplit, KinesisStreamsSourceEnumeratorState> {

    private final String streamName;
    private final Properties consumerConfig;
    private final SplitEnumeratorContext<KinesisShardSplit> context;
    private final Set<String> assignedSplits = new HashSet<>();

    @Override
    public void start() {
        log.info("Starting KinesisStreamsSourceEnumerator");

        context.callAsync(this::discoverShards, (splits, err) -> assignSplits(splits), 0L, 60000L);
    }

    @Override
    public void handleSplitRequest(int i, @Nullable String s) {
        log.info("handleSplitRequest");

    }

    @Override
    public void addSplitsBack(List<KinesisShardSplit> list, int i) {
        log.info("addSplitsBack");
        assignedSplits.removeAll(list.stream().map(KinesisShardSplit::splitId).collect(Collectors.toSet()));
    }

    @Override
    public void addReader(int i) {
        log.info("addReader");
    }

    @Override
    public KinesisStreamsSourceEnumeratorState snapshotState(long l) throws Exception {
        return KinesisStreamsSourceEnumeratorState.builder()
            .assignedSplits(assignedSplits)
            .build();
    }

    @Override
    public void close() throws IOException {
        log.info("Closing KinesisStreamsSourceEnumerator");
    }

    @SneakyThrows
    private Set<KinesisShardSplit> discoverShards() {
        KinesisProxy kinesisProxy = new KinesisProxy(consumerConfig) {
            // Dodgy hack to call a protected constructor
        };

        GetShardListResult shardList = kinesisProxy.getShardList(singletonMap(streamName, null));

        return shardList.getRetrievedShardListOfStream(streamName)
            .stream()
            .map(shard -> {
                log.info("Found new source split: " + shard.getShard().getShardId());
                return KinesisShardSplit.builder().shardHandle(shard).build();
            })
            .collect(Collectors.toSet());
    }

    private void assignSplits(Set<KinesisShardSplit> discoveredSplits) {
        int numReaders = context.currentParallelism();

        getUnassignedSplits(discoveredSplits)
            .forEach(split -> {
                int splitOwner = getSplitOwner(split, numReaders);
                log.info("Assigning split {} to owner {}", split.splitId(), splitOwner);
                context.assignSplit(split, splitOwner);
                assignedSplits.add(split.splitId());
            });
    }

    private Set<KinesisShardSplit> getUnassignedSplits(Set<KinesisShardSplit> discoveredSplits) {
        return discoveredSplits.stream()
            .filter(split -> !assignedSplits.contains(split.splitId()))
            .collect(Collectors.toSet());
    }

    private int getSplitOwner(KinesisShardSplit split, int numReaders) {
        return split.splitId().hashCode() % numReaders;
    }
}
