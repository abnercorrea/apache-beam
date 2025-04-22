package org.abnercorreajr.beam.testing;

import org.abnercorreajr.beam.transforms.LogElements;
import org.abnercorreajr.beam.transforms.PrintSchema;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResultFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;


@SuppressWarnings("NullableProblems")
public class BigQueryAssertSink<T> extends PTransform<PCollection<T>, WriteResult> {

    private final AssertAction<T> assertAction;

    public BigQueryAssertSink(AssertAction<T> assertAction) {
        this.assertAction = assertAction;
    }

    @Override
    public WriteResult expand(PCollection<T> input) {
        input
            .apply(PrintSchema.create())
            .apply(LogElements.toSysOut());

        AssertRunner.run(assertAction, input);

        return WriteResultFactory.empty(input.getPipeline());
    }

}
