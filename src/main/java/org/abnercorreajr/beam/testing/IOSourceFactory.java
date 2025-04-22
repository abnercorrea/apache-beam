package org.abnercorreajr.beam.testing;

import org.abnercorreajr.beam.transforms.PrintSchema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

@SuppressWarnings("NullableProblems")
public class IOSourceFactory implements SourceFactory {

    public <T> PTransform<PBegin, PCollection<T>> create(String sourceId, PTransform<PBegin, PCollection<T>> source) {
        return new PTransform<PBegin, PCollection<T>>() {
            @Override
            public PCollection<T> expand(PBegin input) {
                return input
                    .apply(source)
                    .apply(PrintSchema.create());
            }
        };
    }
}
