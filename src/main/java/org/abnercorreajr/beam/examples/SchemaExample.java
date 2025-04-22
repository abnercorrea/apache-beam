package org.abnercorreajr.beam.examples;

import org.abnercorreajr.beam.pipelines.PipelineDef;
import org.abnercorreajr.beam.transforms.LogElements;
import org.abnercorreajr.beam.transforms.PrintSchema;
import org.abnercorreajr.beam.util.SchemaUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldName;
import org.apache.beam.sdk.schemas.transforms.AddFields;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.schemas.transforms.DropFields;
import org.apache.beam.sdk.schemas.transforms.RenameFields;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.DateTime;

public class SchemaExample implements PipelineDef {

    @BuildPipeline
    public void buildPipeline(Pipeline pipeline) throws NoSuchSchemaException, CannotProvideCoderException {
        //Schema schema = pipeline.getSchemaRegistry().getSchema(Person.class);
        //Coder<Person> coder = pipeline.getCoderRegistry().getCoder(Person.class);

        PCollection<Person> person = pipeline
            .apply("CreateInput", GenerateSequence.from(0).to(10))
            .apply("ToPerson",
                MapElements.into(TypeDescriptor.of(Person.class)).via(
                    (Long i) -> Person.create(String.format("Person %d", i), i.intValue())
                )
            )
            .apply(PrintSchema.create())
            .apply(LogElements.toSysOut())
        ;

        PCollection<Row> personRows = person
            .apply("AddFields",
                AddFields.<Person>create()
                    .field("phone", Schema.FieldType.STRING)
                    .field("dateOfBirth", Schema.FieldType.DATETIME)
            )
            .apply(PrintSchema.create())
            .apply(LogElements.toSysOut())
            ;

        PCollection<Row> personExtRows = personRows
            .apply("PopulatePerson",
                MapElements.into(TypeDescriptor.of(Row.class)).via(
                    (Row r) -> Row.fromRow(r)
                        .withFieldValue("phone", "123-456-7890" + r.getInt32("age"))
                        .withFieldValue("dateOfBirth", DateTime.now())
                        .build()
                )
            )
            .setCoder(personRows.getCoder())
            .apply(LogElements.toSysOut())
            ;

        PCollection<Person> personFromRow = personExtRows
            // Removing added fields so schema matches Person's
            .apply(DropFields.fields("phone", "dateOfBirth"))
            // Converts Row -> Person
            .apply(Convert.fromRows(Person.class))
            .apply(PrintSchema.create())
            .apply(LogElements.toSysOut());

        personFromRow
            // Renaming field
            .apply(RenameFields.<Person>create().rename("firstName", "fullName"))
            .apply(PrintSchema.create())
            .apply(LogElements.toSysOut());

        pipeline
            .apply("CreateInput2", GenerateSequence.from(0).to(10))
            .apply(LogElements.toSysOut());

    }

    @DefaultSchema(JavaFieldSchema.class)
    public static class Person {
        // CAREFUL! JavaFieldSchema requires the fields to be accessible (public or package)
        @SchemaFieldName("firstName")
        public String name;
        public Integer age;

        private Person(String name, Integer age) {
            this.name = name;
            this.age = age;
        }

        @SchemaCreate
        public static Person create(String name, Integer age) {
            return new Person(name, age);
        }
    }

    public static void main(String[] args) throws NoSuchSchemaException {
        // Coder<Person> coder = CoderRegistry.createDefault().getCoder(Person.class);

        Schema schema = SchemaRegistry.createDefault().getSchema(Person.class);
        System.out.println(SchemaUtils.generateSchemaCode(schema));

        new SchemaExample().run(args);
    }
}
