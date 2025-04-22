package org.abnercorreajr.beam.transforms;

import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

@SuppressWarnings("NullableProblems")
public class JsonToRow {
    public static <T> PTransform<PCollection<String>, PCollection<Row>> withJavaFieldSchema(Class<T> javaFieldClass) {
        Schema schema = new JavaFieldSchema().schemaFor(TypeDescriptor.of(javaFieldClass));

        return org.apache.beam.sdk.transforms.JsonToRow.withSchema(schema);
    }

    public static <T> PTransform<PCollection<String>, PCollection<Row>> withJavaBeamSchema(Class<T> javaBeanClass) {
        Schema schema = new JavaBeanSchema().schemaFor(TypeDescriptor.of(javaBeanClass));

        return org.apache.beam.sdk.transforms.JsonToRow.withSchema(schema);
    }
}
