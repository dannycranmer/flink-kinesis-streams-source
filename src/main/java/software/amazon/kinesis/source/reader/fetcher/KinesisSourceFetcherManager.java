package software.amazon.kinesis.source.reader.fetcher;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.kinesis.shaded.com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;

import software.amazon.kinesis.source.split.KinesisShardSplit;

import java.util.function.Supplier;

public class KinesisSourceFetcherManager extends SingleThreadFetcherManager<UserRecord, KinesisShardSplit> {

    public KinesisSourceFetcherManager(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<UserRecord>> elementsQueue,
            Supplier<SplitReader<UserRecord, KinesisShardSplit>> splitReaderSupplier) {
        super(elementsQueue, splitReaderSupplier);
    }



}
