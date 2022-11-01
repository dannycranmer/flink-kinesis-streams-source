package software.amazon.kinesis.source.split;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class KinesisShardSplitState {

    private final KinesisShardSplit kinesisShardSplit;
}
