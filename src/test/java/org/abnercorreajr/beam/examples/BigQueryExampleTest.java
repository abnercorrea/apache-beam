package org.abnercorreajr.beam.examples;

import com.google.api.services.bigquery.model.TableRow;
import org.abnercorreajr.beam.testing.*;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesTestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

import static org.abnercorreajr.beam.examples.BigQueryExample.Options;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(JUnit4.class)
public class BigQueryExampleTest {

    @Rule
    public final transient TestPipeline testPipeline1 = TestPipeline.fromOptions(new TestCase1().getOptions());

    @Test
    @Category({NeedsRunner.class, UsesTestStream.class})
    public void testCase1() {
        //AssertRunner.disable();
        TestCaseRunner.of(BigQueryExample.class, testPipeline1, new TestCase1()).run();
    }

    /**
     *
     */
    public static class TestCase1 implements PipelineTestCase {

        @Override
        public Options getOptions() {
            Options options = PipelineOptionsFactory.as(Options.class);

            options.setInputTable(DummyOptions.PUBLIC_COVID19_TABLE);
            options.setOutputTable(DummyOptions.PUBLIC_COVID19_TABLE);

            return options;
        }


        @Override
        public TestSourceFactory buildTestSources() {
            TestSourceFactory sourceFactory = new TestSourceFactory();

            Schema schema = Schema.builder()
                .addFields(
                    Schema.Field.nullable("key", Schema.FieldType.INT64),
                    Schema.Field.nullable("a", Schema.FieldType.INT64),
                    Schema.Field.nullable("b", Schema.FieldType.STRING))
                .build();

            Create.Values<TableRow> bqIn = Create.of(
                new TableRow().set("key", 1).set("a", 1).set("b", "X"),
                new TableRow().set("key", 2).set("a", 2).set("b", "Y"),
                new TableRow().set("key", 3).set("a", 3).set("b", "Z")
            );

            sourceFactory.addBigQuerySource("bq-in", bqIn, schema);

            return sourceFactory;
        }

        @Override
        public TestSinkFactory buildAsserts() {
            TestSinkFactory sinkFactory = new TestSinkFactory();

            AssertAction<Row> bqOut = new AssertAction<>() {
                @Override
                public void runAsserts(PCollection<Row> pipelineOutput) {
                    Schema expectedSchema = Schema.builder()
                        .addFields(
                            Schema.Field.nullable("a", Schema.FieldType.INT64),
                            Schema.Field.nullable("name", Schema.FieldType.STRING),
                            Schema.Field.nullable("aIsEven", Schema.FieldType.BOOLEAN),
                            Schema.Field.nullable("c", Schema.FieldType.INT64),
                            Schema.Field.nullable("doubleC", Schema.FieldType.INT64),
                    		Schema.Field.nullable("numbers", Schema.FieldType.array(Schema.FieldType.INT64)))
                        .build();

                    assertThat(pipelineOutput.getSchema(), new SchemaMatcher(expectedSchema));

                    PAssert.that(pipelineOutput).containsInAnyOrder(
                        Row.withSchema(expectedSchema).attachValues(1L, "X 1", false, 1L, 2L, List.of(1L, 1L)),
                        Row.withSchema(expectedSchema).attachValues(2L, "Y 2", true, 2L, 4L, List.of(2L, 2L)),
                        Row.withSchema(expectedSchema).attachValues(3L, "Z 3", false, 3L, 6L, List.of(3L, 3L))
                    );
                }
            };

            sinkFactory.addAssert("bq-out", bqOut);

            return sinkFactory;
        }
    }

}