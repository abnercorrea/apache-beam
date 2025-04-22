package org.abnercorreajr.beam.schema;

import com.google.auto.value.AutoValue;
import org.abnercorreajr.beam.fn.RowFieldFn;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class RowTransformer implements Serializable {

    private final Map<String, ValueProvider<?>> valueProviders = new HashMap<>();

    public <T> RowTransformer registerProvider(String fieldName, TypeDescriptor<T> fieldType, RowFieldFn<T> fn, boolean nullable) {
        valueProviders.put(fieldName,
            FnValueProvider
                .<T>builder()
                .setFieldName(fieldName)
                .setType(fieldType)
                .setFn(fn)
                .setNullable(nullable)
                .build());

        return this;
    }

    public RowTransformer renameProvider(String currentName, String newName) {
        if (valueProviders.containsKey(currentName)) {
            valueProviders.put(newName, valueProviders.remove(currentName));
        }
        else {
            valueProviders.put(newName, new RenameValueProvider(currentName));
        }

        return this;
    }

    public RowTransformer dropProviders(String[] fieldNames) {
        Arrays.stream(fieldNames).forEach(valueProviders::remove);

        return this;
    }

    public Row transform(Row inputRow, Schema outputSchema) {
        Row.Builder builder = Row.withSchema(outputSchema);

        // Using .attachValues(), since .withFieldValue() & .build() does not work! (IllegalStateException)
        Object[] values = new Object[outputSchema.getFieldCount()];

        for (int i = 0; i < outputSchema.getFieldCount(); i++) {
            String fieldName = outputSchema.getField(i).getName();

            if (valueProviders.containsKey(fieldName)) {
                values[i] = valueProviders.get(fieldName).fieldValue(inputRow);
            }
            else {
                values[i] = inputRow.getValue(fieldName);
            }
        }

        // Using .attachValues(), since .withFieldValue() & .build() does not work! (IllegalStateException)
        Row output = builder.attachValues(values);

        return output;
    }

    public interface ValueProvider<T> extends Serializable {
        public T fieldValue(Row row);
    }

    @AutoValue
    static abstract class FnValueProvider<T> implements ValueProvider<T> {
        abstract String getFieldName();
        abstract RowFieldFn<T> getFn();
        abstract TypeDescriptor<T> getType();
        abstract Boolean getNullable();

        public static <T> Builder<T> builder() {
            return new AutoValue_RowTransformer_FnValueProvider.Builder<>();
        }

        @AutoValue.Builder
        public abstract static class Builder<T> {
            abstract FnValueProvider<T> build();

            abstract Builder<T> setFieldName(String fieldName);

            abstract Builder<T> setFn(RowFieldFn<T> fn);

            abstract Builder<T> setType(TypeDescriptor<T> type);

            abstract Builder<T> setNullable(Boolean ignoreNull);
        }

        public T fieldValue(Row row) {
            try {
                return getFn().apply(row);
            }
            catch (NullPointerException e) {
                // If not permissive to nulls, rethrows exception.
                if (!getNullable()) {
                    throw e;
                }

                // If permissive to nulls, on NullPointerException, sets field value to null.
                return null;
            }
        }
    }

    static class RenameValueProvider implements ValueProvider<Object> {
        private final String originalFieldName;

        RenameValueProvider(String originalFieldName) {
            this.originalFieldName = originalFieldName;
        }

        @Override
        public Object fieldValue(Row row) {
            return row.getValue(originalFieldName);
        }
    }
}
