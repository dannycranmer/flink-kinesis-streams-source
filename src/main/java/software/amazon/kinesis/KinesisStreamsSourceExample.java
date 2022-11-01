package software.amazon.kinesis;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;

import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.source.KinesisStreamsSource;

import java.util.Properties;

@Slf4j
public class KinesisStreamsSourceExample {
    private static final String STREAM = "stream";
    private static final String REGION = "us-east-1";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties consumerConfig = new Properties();
        consumerConfig.setProperty(AWSConfigConstants.AWS_REGION, REGION);

        KinesisStreamsSource<String> source = KinesisStreamsSource.<String>builder()
                .streamName(STREAM)
                .consumerConfig(consumerConfig)
                .deserializationSchema(new SimpleStringSchema())
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "kds-source")
                .returns(String.class)
                .print();

        env.execute("Kinesis Streams Source Example");
    }
}
