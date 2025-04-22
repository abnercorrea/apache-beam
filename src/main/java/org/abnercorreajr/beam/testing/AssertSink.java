package org.abnercorreajr.beam.testing;

import org.abnercorreajr.beam.transforms.LogElements;
import org.abnercorreajr.beam.transforms.PrintSchema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;


@SuppressWarnings("NullableProblems")
public class AssertSink<T> extends PTransform<PCollection<T>, PDone> {

    private final AssertAction<T> assertAction;

    public AssertSink(AssertAction<T> assertAction) {
        this.assertAction = assertAction;
    }

    @Override
    public PDone expand(PCollection<T> input) {
        input
            .apply(PrintSchema.create())
            .apply(LogElements.toSysOut());

        AssertRunner.run(assertAction, input);

        return PDone.in(input.getPipeline());
    }
}
