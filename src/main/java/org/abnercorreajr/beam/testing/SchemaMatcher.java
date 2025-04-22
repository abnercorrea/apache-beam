package org.abnercorreajr.beam.testing;

import org.abnercorreajr.beam.util.SchemaUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;

public class SchemaMatcher extends BaseMatcher<Schema> {
    private final Schema expectedSchema;

    public SchemaMatcher(Schema expectedSchema) {
        this.expectedSchema = expectedSchema;
    }

    @Override
    public boolean matches(Object actual) {
        return actual instanceof Schema
            && expectedSchema.equivalent((Schema) actual);
    }

    @Override
    public void describeTo(Description description) {
        description
            .appendText("\nBeam Schema:\n")
            .appendValue(expectedSchema);
    }

    @Override
    public void describeMismatch(Object item, Description description) {
        Schema actual = (Schema) item;

        description
            .appendText("\nActual Schema was: \n")
            .appendValue(actual)
            .appendText("\nJava code to create actual Schema: \n")
            .appendText(SchemaUtils.generateSchemaCode(actual));
    }

}
