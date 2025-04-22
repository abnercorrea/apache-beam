package org.abnercorreajr.beam.testing;

import com.google.api.services.bigquery.model.TableRow;
import org.abnercorreajr.beam.transforms.LogElements;
import org.abnercorreajr.beam.transforms.PrintSchema;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

@SuppressWarnings("NullableProblems")
public class TestSourceFactory implements SourceFactory {

    private final Map<String, PTransform<PBegin, ?>> sourceMap;

    public TestSourceFactory() {
        this.sourceMap = new HashMap<>();
    }

    public <T> void addSource(String sourceId, PTransform<PBegin, PCollection<T>> source) {
        sourceMap.put(sourceId, source);
    }

    public void addBigQuerySource(String sourceId, Create.Values<TableRow> source, Schema schema) {
        sourceMap.put(sourceId, source.withSchema(
            schema,
            TypeDescriptor.of(TableRow.class),
            (TableRow tr) -> BigQueryUtils.toBeamRow(schema, tr),
            BigQueryUtils::toTableRow
        ));
    }

    @SuppressWarnings("unchecked")
    public <T> PTransform<PBegin, PCollection<T>> create(String sourceId, PTransform<PBegin, PCollection<T>> source) {
        PTransform<PBegin, PCollection<T>> testSource = (PTransform<PBegin, PCollection<T>>) sourceMap.get(sourceId);

        checkNotNull(testSource, "Couldn't find a test source with id \"%s\" for transform \"%s\"", sourceId, source.getName());

        return new PTransform<>() {
            @Override
            public PCollection<T> expand(PBegin input) {
                return input
                    .apply("TestSource", testSource)
                    .apply("TestSourceSchema", PrintSchema.create())
                    .apply("LogTestSource", LogElements.toSysOut());
            }
        };
    }

}
