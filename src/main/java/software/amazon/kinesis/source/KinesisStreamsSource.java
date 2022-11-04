package software.amazon.kinesis.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.kinesis.shaded.com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;

import lombok.Builder;
import software.amazon.kinesis.source.enumerator.KinesisStreamsSourceEnumerator;
import software.amazon.kinesis.source.enumerator.KinesisStreamsSourceEnumeratorState;
import software.amazon.kinesis.source.enumerator.KinesisStreamsSourceEnumeratorStateSerializer;
import software.amazon.kinesis.source.reader.KinesisRecordEmitter;
import software.amazon.kinesis.source.reader.KinesisStreamsSourceReader;
import software.amazon.kinesis.source.reader.KinesisShardSplitReader;
import software.amazon.kinesis.source.split.KinesisShardSplit;
import software.amazon.kinesis.source.split.KinesisShardSplitSerializer;

import java.util.Properties;
import java.util.function.Supplier;

@Builder
public class KinesisStreamsSource<T> implements Source<T, KinesisShardSplit, KinesisStreamsSourceEnumeratorState> {

    private static final long serialVersionUID = 1L;

    private final DeserializationSchema<T> deserializationSchema;
    private final Properties consumerConfig;
    private final String streamName;

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<T, KinesisShardSplit> createReader(
            SourceReaderContext sourceReaderContext) throws Exception {

        FutureCompletingBlockingQueue<RecordsWithSplitIds<UserRecord>>
                elementsQueue = new FutureCompletingBlockingQueue<>();

        Supplier<KinesisShardSplitReader> splitReaderSupplier =
                () -> KinesisShardSplitReader.builder()
                        .consumerConfig(consumerConfig)
                        .metricGroup(sourceReaderContext.metricGroup())
                        .build();

        KinesisRecordEmitter<T> recordEmitter = KinesisRecordEmitter.<T>builder()
                .deserializationSchema(deserializationSchema)
                .build();

        return new KinesisStreamsSourceReader<>(
                elementsQueue,
                new SingleThreadFetcherManager<UserRecord, KinesisShardSplit>(elementsQueue, splitReaderSupplier::get),
                recordEmitter,
                toConfiguration(consumerConfig),
                sourceReaderContext);
    }

    @Override
    public SplitEnumerator<KinesisShardSplit, KinesisStreamsSourceEnumeratorState> createEnumerator(SplitEnumeratorContext<KinesisShardSplit> splitEnumeratorContext) throws Exception {
        return restoreEnumerator(splitEnumeratorContext, null);
    }

    @Override
    public SplitEnumerator<KinesisShardSplit, KinesisStreamsSourceEnumeratorState> restoreEnumerator(SplitEnumeratorContext<KinesisShardSplit> splitEnumeratorContext, KinesisStreamsSourceEnumeratorState kinesisSourceEnumState) throws Exception {
        return KinesisStreamsSourceEnumerator.builder()
                .streamName(streamName)
                .consumerConfig(consumerConfig)
                .context(splitEnumeratorContext)
                .build();
    }

    @Override
    public SimpleVersionedSerializer<KinesisShardSplit> getSplitSerializer() {
        return new KinesisShardSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<KinesisStreamsSourceEnumeratorState> getEnumeratorCheckpointSerializer() {
        return new KinesisStreamsSourceEnumeratorStateSerializer();
    }

    private Configuration toConfiguration(Properties props) {
        Configuration config = new Configuration();
        props.stringPropertyNames().forEach(key -> config.setString(key, props.getProperty(key)));
        return config;
    }
}
