package org.abnercorreajr.beam.transforms;

import org.abnercorreajr.beam.fn.RowFieldFn;
import org.abnercorreajr.beam.schema.RowTransformer;
import org.abnercorreajr.beam.schema.SchemaUpdater;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

import static com.google.common.base.Preconditions.checkArgument;

@SuppressWarnings("NullableProblems")
public class TransformRows<T> extends PTransform<PCollection<? extends T>, PCollection<Row>> {

    private final SchemaUpdater schemaUpdater;
    private final RowTransformer rowTransformer;

    private TransformRows() {
        this.schemaUpdater = new SchemaUpdater();
        this.rowTransformer = new RowTransformer();
    }

    public static <T> TransformRows<T> create() {
        return new TransformRows<>();
    }

    public <FieldT> TransformRows<T> withField(String name, TypeDescriptor<FieldT> fieldType, RowFieldFn<FieldT> fn) {
        return withField(name, fieldType, fn, true);
    }

    public <FieldT> TransformRows<T> withField(String name, TypeDescriptor<FieldT> fieldType, RowFieldFn<FieldT> fn, boolean nullable) {
        schemaUpdater.upsert(name, fieldType, nullable);
        rowTransformer.registerProvider(name, fieldType, fn, nullable);

        return this;
    }

    public TransformRows<T> withFieldRenamed(String currentName, String newName) {
        schemaUpdater.rename(currentName, newName);
        rowTransformer.renameProvider(currentName, newName);

        return this;
    }

    public TransformRows<T> drop(String... fieldNames) {
        schemaUpdater.drop(fieldNames);
        rowTransformer.dropProviders(fieldNames);

        return this;
    }

    @Override
    public PCollection<Row> expand(PCollection<? extends T> input) {
        checkArgument(input.hasSchema(), "Input PCollection must have a schema.");

        final Schema outputSchema = schemaUpdater.applyUpdates(input.getSchema());
        final RowCoder outputCoder = RowCoder.of(outputSchema);

        return input.apply(
            ParDo.of(new DoFn<T, Row>() {
                /**
                 * Since every schema can be represented by a Row type, Beam will automatically convert elements to Row.
                 */
                @ProcessElement
                public void processElement(@Element Row inputRow, OutputReceiver<Row> r) {
                    Row output = rowTransformer.transform(inputRow, outputSchema);

                    r.output(output);
                }
            }))
            .setCoder(outputCoder);
    }
}
