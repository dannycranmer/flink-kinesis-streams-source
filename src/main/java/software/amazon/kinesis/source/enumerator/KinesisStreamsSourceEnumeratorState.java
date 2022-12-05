package software.amazon.kinesis.source.enumerator;

import lombok.Builder;

import java.util.Set;

@Builder
public class KinesisStreamsSourceEnumeratorState {
    private final Set<String> assignedSplits;
}
