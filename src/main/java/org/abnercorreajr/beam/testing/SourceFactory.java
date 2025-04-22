package org.abnercorreajr.beam.testing;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;


@SuppressWarnings("NullableProblems")
public interface SourceFactory {

    public <T> PTransform<PBegin, PCollection<T>> create(String sourceId, PTransform<PBegin, PCollection<T>> source);
}

