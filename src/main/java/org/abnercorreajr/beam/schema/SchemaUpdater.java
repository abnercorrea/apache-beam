package org.abnercorreajr.beam.schema;

import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.StaticSchemaInference;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.OptionalInt;
import java.util.stream.IntStream;

public class SchemaUpdater implements Serializable {

    List<Update> updates = new LinkedList<>();

    public SchemaUpdater upsert(String fieldName, TypeDescriptor<?> type, boolean nullable) {
        Schema.FieldType fieldType = schemaTypeFrom(type).withNullable(nullable);

        updates.add(new Upsert(fieldName, fieldType));
        return this;
    }

    public SchemaUpdater rename(String fieldName, String newName) {
        updates.add(new Rename(fieldName, newName));
        return this;
    }

    public SchemaUpdater drop(String... fieldNames) {
        for (String fieldName : fieldNames) {
            updates.add(new Drop(fieldName));
        }
        return this;
    }

    private Schema.FieldType schemaTypeFrom(TypeDescriptor<?> td) {
        Schema.FieldType fieldType = StaticSchemaInference.fieldFromType(td, JavaFieldSchema.JavaFieldTypeSupplier.INSTANCE);

        return fieldType;
    }

    public Schema applyUpdates(Schema schema) {
        List<Schema.Field> schemaFields = new LinkedList<>(schema.getFields());

        for (Update update : updates) {
            update.apply(schemaFields);
        }

        return Schema.builder().addFields(schemaFields).build();
    }

    public abstract class Update implements Serializable {
        protected final String fieldName;

        protected Update(String fieldName) {
            this.fieldName = fieldName;
        }

        public abstract void apply(List<Schema.Field> schemaFields);

        public OptionalInt findField(List<Schema.Field> schemaFields) {
            return IntStream
                .range(0, schemaFields.size())
                .filter(i -> schemaFields.get(i).getName().equals(fieldName))
                .findFirst();
        }
    }

    public class Upsert extends Update {
        private final Schema.FieldType fieldType;

        protected Upsert(String fieldName, Schema.FieldType fieldType) {
            super(fieldName);

            this.fieldType = fieldType;
        }

        @Override
        public void apply(List<Schema.Field> schemaFields) {
            OptionalInt index = findField(schemaFields);

            // Update
            if (index.isPresent()) {
                Schema.Field field = schemaFields.remove(index.getAsInt());
                schemaFields.add(index.getAsInt(), field.toBuilder().setType(fieldType).build());
            }
            // Insert
            else {
                // New fields always nullable
                schemaFields.add(Schema.Field.nullable(fieldName, fieldType));
            }
        }
    }

    public class Rename extends Update {
        private final String newName;

        protected Rename(String fieldName, String newName) {
            super(fieldName);

            this.newName = newName;
        }

        @Override
        public void apply(List<Schema.Field> schemaFields) {
            int index = findField(schemaFields)
                .orElseThrow(() -> new IllegalArgumentException(String.format("Field %s not found !", fieldName)));

            Schema.Field field = schemaFields.remove(index);
            schemaFields.add(index, field.toBuilder().setName(newName).build());
        }
    }

    public class Drop extends Update {
        protected Drop(String fieldName) {
            super(fieldName);
        }

        @Override
        public void apply(List<Schema.Field> schemaFields) {
            int index = findField(schemaFields)
                .orElseThrow(() -> new IllegalArgumentException(String.format("Field %s not found !", fieldName)));

            schemaFields.remove(index);
        }
    }
}
