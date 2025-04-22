package org.abnercorreajr.beam.testing;

import org.abnercorreajr.beam.transforms.PrintSchema;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;


@SuppressWarnings("NullableProblems")
public class IOSinkFactory implements SinkFactory {
    @Override
    public <T> PTransform<PCollection<T>, PDone> create(String sinkId, PTransform<PCollection<T>, PDone> sink) {
        return new PTransform<>() {
            @Override
            public PDone expand(PCollection<T> input) {
                return input
                    .apply(PrintSchema.create())
                    .apply(sink);
            }
        };
    }

    @Override
    public <T> PTransform<PCollection<T>, WriteResult> createForBigQuery(String sinkId, BigQueryIO.Write<T> sink) {
        return new PTransform<>() {
            @Override
            public WriteResult expand(PCollection<T> input) {
                return input
                    .apply(PrintSchema.create())
                    .apply(sink);
            }
        };
    }
}
