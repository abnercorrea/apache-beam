package org.abnercorreajr.beam.testing;

import org.apache.beam.sdk.values.PCollection;

public interface AssertAction<T> {
    public void runAsserts(PCollection<T> pipelineOutput);
}
