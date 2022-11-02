package software.amazon.kinesis.source.reader;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.kinesis.shaded.com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;

import software.amazon.kinesis.source.split.KinesisShardSplit;
import software.amazon.kinesis.source.split.KinesisShardSplitState;

import java.util.Map;

@Slf4j
public class KinesisStreamsSourceReader<T> extends SingleThreadMultiplexSourceReaderBase<
        UserRecord, T, KinesisShardSplit, KinesisShardSplitState> {

    public KinesisStreamsSourceReader(FutureCompletingBlockingQueue<RecordsWithSplitIds<UserRecord>> elementsQueue, SingleThreadFetcherManager<UserRecord, KinesisShardSplit> splitFetcherManager, RecordEmitter<UserRecord, T, KinesisShardSplitState> recordEmitter, Configuration config, SourceReaderContext context) {
        super(elementsQueue, splitFetcherManager, recordEmitter, config, context);
    }

    @Override
    protected void onSplitFinished(Map<String, KinesisShardSplitState> map) {
        log.info("onSplitFinished");
    }

    @Override
    protected KinesisShardSplitState initializedState(KinesisShardSplit kinesisShardSplit) {
        return null;
    }

    @Override
    protected KinesisShardSplit toSplitType(String s, KinesisShardSplitState kinesisShardSplitState) {
        return kinesisShardSplitState.getKinesisShardSplit();
    }
}
