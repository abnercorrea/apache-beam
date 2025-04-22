package org.abnercorreajr.beam.transforms;

import org.abnercorreajr.beam.util.SchemaUtils;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("NullableProblems")
public class PrintSchema<T> extends PTransform<PCollection<T>, PCollection<T>> {

    private static final Logger LOG = LoggerFactory.getLogger(PrintSchema.class);

    public static <T> PrintSchema<T> create() {
        return new PrintSchema<>();
    }

    @Override
    public PCollection<T> expand(PCollection<T> input) {
        if (input.hasSchema()) {
            LOG.info(
                "Schema:\n\n{}\n\nJava code to generate schema:\nlowerCamelCase:\n{}\n\nsnake_case:\n{}\n",
                input.getSchema(),
                SchemaUtils.generateSchemaCode(input.getSchema()),
                SchemaUtils.generateSchemaCode(input.getSchema().toSnakeCase())
            );
        }
        else {
            LOG.warn("PCollection does NOT have a schema.");
        }

        return input;
    }
}
