package org.abnercorreajr.beam.testing;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

@SuppressWarnings("NullableProblems")
public class TestSinkFactory implements SinkFactory {

    private final Map<String, AssertAction<?>> assertActions;

    public TestSinkFactory() {
        assertActions = new HashMap<>();
    }

    @Override
    public <T> PTransform<PCollection<T>, PDone> create(String sinkId, PTransform<PCollection<T>, PDone> sink) {
        return new AssertSink<>(getAssert(sinkId, sink.getName()));
    }

    @Override
    public <T> PTransform<PCollection<T>, WriteResult> createForBigQuery(String sinkId, BigQueryIO.Write<T> sink) {
        return new BigQueryAssertSink<>(getAssert(sinkId, sink.getName()));
    }

    private <T> AssertAction<T> getAssert(String sinkId, String transformName) {
        @SuppressWarnings("unchecked")
        AssertAction<T> assertAction = (AssertAction<T>) assertActions.get(sinkId);

        checkNotNull(assertAction, "Couldn't find a assert with id \"%s\" for transform \"%s\"", sinkId, transformName);

        return assertAction;
    }

    public void addAssert(String sinkId, AssertAction<?> assertAction) {
        assertActions.put(sinkId, assertAction);
    }

}
