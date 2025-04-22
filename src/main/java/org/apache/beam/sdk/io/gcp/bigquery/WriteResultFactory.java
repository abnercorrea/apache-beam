package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

public class WriteResultFactory {

    public static WriteResult empty(Pipeline pipeline) {
        PCollection<TableRow> empty = pipeline.apply(Create.empty(TypeDescriptor.of(TableRow.class)));

        return WriteResult.in(
            pipeline,
            null,
            empty,
            empty,
            null,
            null,
            null,
            null,
            null,
            empty
        );
    }
}
