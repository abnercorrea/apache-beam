package org.abnercorreajr.beam.util;

import org.apache.beam.sdk.schemas.Schema;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

public class SchemaUtils {


    //  Schema inputSchema = Schema.builder()
    //      .addFields(
    //          Schema.Field.of("x", Schema.FieldType.STRING),
    //          Schema.Field.nullable("y", Schema.FieldType.STRING))
    //      .build();
    public static String generateSchemaCode(Schema schema) {
        StringBuilder builder = new StringBuilder();

        builder.append("Schema schema = Schema.builder()");
        builder.append(System.lineSeparator());
        builder.append("\t.addFields(");
        builder.append(System.lineSeparator());

        for (int i = 0; i < schema.getFieldCount(); i++) {
            Schema.Field field = schema.getField(i);

            String builderMethod = (field.getType().getNullable()) ? "nullable" : "of";
            String eol = (i == schema.getFieldCount() - 1) ? ")" : ",";

            builder.append("\t\tSchema.Field.");
            builder.append(builderMethod);
            builder.append("(\"");
            builder.append(field.getName());
            builder.append("\", ");
            builder.append(generateFieldTypeCode(field.getType()));
            builder.append(")");
            builder.append(eol);
            builder.append(System.lineSeparator());
        }

        builder.append("\t.build();");

        return builder.toString();
    }

    public static String generateFieldTypeCode(Schema.FieldType type) {
        StringBuilder builder = new StringBuilder();

        switch (type.getTypeName()) {
            case ROW:
                String rowSchemaCode = generateSchemaCode(type.getRowSchema())
                    .replaceAll("\n", "")
                    .replaceAll("\t", "");

                builder.append("Schema.FieldType.row(");
                builder.append(rowSchemaCode);
                builder.append(")");
                break;
            case ARRAY:
                builder.append("Schema.FieldType.array(");
                builder.append(generateFieldTypeCode(type.getCollectionElementType()));
                builder.append(")");
                break;
            case ITERABLE:
                builder.append("Schema.FieldType.iterable(");
                builder.append(generateFieldTypeCode(type.getCollectionElementType()));
                builder.append(")");
                break;
            case MAP:
                builder.append("Schema.FieldType.map(");
                builder.append(generateFieldTypeCode(type.getMapKeyType()));
                builder.append(", ");
                builder.append(generateFieldTypeCode(type.getMapValueType()));
                builder.append(")");
                break;
            case LOGICAL_TYPE:
                builder.append("Schema.FieldType.logicalType(");
                builder.append(type.getLogicalType());
                builder.append(")");
                break;
            default:
                if (type.getTypeName().isPrimitiveType()) {
                    builder.append("Schema.FieldType.");
                    builder.append(type.getTypeName());
                }
        }

        return builder.toString();
    }
}

