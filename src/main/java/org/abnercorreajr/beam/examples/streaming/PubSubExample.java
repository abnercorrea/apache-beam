package org.abnercorreajr.beam.examples.streaming;

import org.abnercorreajr.beam.fn.MaxFn;
import org.abnercorreajr.beam.fn.MinFn;
import org.abnercorreajr.beam.pipelines.PipelineDef;
import org.abnercorreajr.beam.testing.SinkFactory;
import org.abnercorreajr.beam.testing.SourceFactory;
import org.abnercorreajr.beam.transforms.JsonToRow;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;

public class PubSubExample implements PipelineDef {

    @OptionsDef
    public interface Options extends DataflowPipelineOptions, StreamingOptions {
        @Validation.Required
        String getInputTopic();
        void setInputTopic(String inputTopic);

        @Validation.Required
        String getOutputTopic();
        void setOutputTopic(String outputTopic);

        @Description("Window size in SECONDS")
        @Default.Integer(10)
        int getWindowSize();
        void setWindowSize(int windowSize);

        @Description("Allowed lateness in SECONDS")
        @Default.Integer(30)
        int getAllowedLateness();
        void setAllowedLateness(int allowedLateness);
    }

    @BuildPipeline
    public void buildPipeline(Pipeline pipeline, Options options, SourceFactory sourceFactory, SinkFactory sinkFactory) {
        // Get a schema that was registered using @DefaultSchema
        // Schema schema = pipeline.getSchemaRegistry().getSchema(InputMsg.class);
        // Schema schema = SchemaRegistry.createDefault().getSchema(InputMsg.class);

        pipeline
            .apply("ReadFromPubSub", sourceFactory.create("pubsub-in",
                PubsubIO.readStrings().fromTopic(options.getInputTopic()).withTimestampAttribute("timestamp"))
            )
            //.apply("ParseJson", ParseJson.to(InputMsg.class))
            .apply("ParseJson", JsonToRow.withJavaFieldSchema(InputMsg.class))
            .apply("FixedWindows", Window
                .<Row>into(FixedWindows.of(Duration.standardSeconds(options.getWindowSize())))
                .triggering(
                    AfterWatermark
                        .pastEndOfWindow()
                        .withLateFirings(AfterPane.elementCountAtLeast(1))
                )
                .withAllowedLateness(Duration.standardSeconds(options.getAllowedLateness()))
                .accumulatingFiredPanes()
                .withTimestampCombiner(TimestampCombiner.END_OF_WINDOW)
            )
            .apply("AggByKey", Group
                .<Row>byFieldNames("key")
                .aggregateField("value", Count.combineFn(), "count")
                .aggregateField("value", Sum.ofIntegers(), "value_sum")
                .aggregateField("value", Min.ofIntegers(), "min_val")
                .aggregateField("value", Max.ofIntegers(), "max_val")
                .aggregateField("timestamp", MinFn.ofStrings(), "min_ts")
                .aggregateField("timestamp", MaxFn.ofStrings(), "max_ts")
            )
            .apply("FlattenSchema", Select.fieldNames("key.key", "value.count", "value.value_sum", "value.min_val", "value.max_val", "value.min_ts", "value.max_ts"))
            .apply("AddWindowInterval", ParDo.of(new AddWindowIntervalFn()))
            .setRowSchema(outputSchema)
            .apply("ToJson", ToJson.of())
            .apply("WriteToPubSub", sinkFactory.create("pubsub-out",
                PubsubIO.writeStrings().to(options.getOutputTopic()).withTimestampAttribute("timestamp"))
            )
        ;
    }

    /** There is also {@link JavaBeanSchema} **/
    @DefaultSchema(JavaFieldSchema.class)
    public static class InputMsg {
        // CAREFUL! JavaFieldSchema requires the fields to be accessible (public or package)
        String key;
        Integer value;
        String timestamp;
    }

    static class AddWindowIntervalFn extends DoFn<Row, Row> {
        @ProcessElement
        public void processElement(ProcessContext c, IntervalWindow w) {
            Row input = c.element();

            Row output = Row
                .withSchema(outputSchema)
                // CAREFUL with .addValues() since the values in the input must be in the same order as in the row schema.
                .addValues(input.getValues())
                .addValue(w.start().toDateTime())
                .addValue(w.end().toDateTime())
                .build();

            c.output(output);
        }
    }

    private static final Schema outputSchema = Schema
        .builder()
        .addStringField("key")
        .addInt64Field("count")
        .addInt32Field("value_sum")
        .addInt32Field("min_val")
        .addInt32Field("max_val")
        .addStringField("min_ts")
        .addStringField("max_ts")
        .addDateTimeField("window_start")
        .addDateTimeField("window_end")
        .build();

}
