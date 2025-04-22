package org.abnercorreajr.beam.examples.streaming;

import org.abnercorreajr.beam.testing.*;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.abnercorreajr.beam.examples.streaming.PubSubExample.Options;


@RunWith(JUnit4.class)
public class PubSubExampleTest {

    @Rule
    public final transient TestPipeline testPipeline1 = TestPipeline.fromOptions(new TestCase1().getOptions());

    @Test
    @Category({NeedsRunner.class, UsesTestStream.class})
    public void testCase1() {
        TestCaseRunner.of(PubSubExample.class, testPipeline1, new TestCase1()).run();
    }

    @Rule
    public final transient TestPipeline testPipeline2 = TestPipeline.fromOptions(new TestCase2().getOptions());

    @Test
    @Category({NeedsRunner.class, UsesTestStream.class})
    public void testCase2() {
        TestCaseRunner.of(PubSubExample.class, testPipeline2, new TestCase2()).run();
    }

    /**
     *
     */
    public static class TestCase1 implements PipelineTestCase {

        @Override
        public Options getOptions() {
            Options options = PipelineOptionsFactory.as(Options.class);

            options.setWindowSize(10);
            options.setAllowedLateness(30);
            options.setInputTopic(DummyOptions.PUBLIC_TAXI_RIDES_TOPIC);
            options.setOutputTopic(DummyOptions.PUBLIC_TAXI_RIDES_TOPIC);

            return options;
        }

        @Override
        public TestSourceFactory buildTestSources() {
            TestSourceFactory sourceFactory = new TestSourceFactory();

            Instant start = new Instant(0);
            long windowSizeMs = getOptions().getWindowSize() * 1000L;

            TestStream<String> pubSubIn = TestStream
                .create(StringUtf8Coder.of())
                .advanceWatermarkTo(start)
                .addElements(TimestampedValue.of("{\"key\": \"A\", \"value\": 0, \"timestamp\": \"1970/01/01 00:00:00\"}", start))
                .addElements(TimestampedValue.of("{\"key\": \"B\", \"value\": 0, \"timestamp\": \"1970/01/01 00:00:00\"}", start))
                .addElements(TimestampedValue.of("{\"key\": \"A\", \"value\": 1, \"timestamp\": \"1970/01/01 00:00:01\"}", start))
                .addElements(TimestampedValue.of("{\"key\": \"B\", \"value\": 1, \"timestamp\": \"1970/01/01 00:00:01\"}", start))
                // Advance watermark past the first window end.
                .advanceWatermarkTo(start.plus(windowSizeMs))
                .addElements(TimestampedValue.of("{\"key\": \"A\", \"value\": 2, \"timestamp\": \"1970/01/01 00:00:10\"}", start.plus(windowSizeMs)))
                .addElements(TimestampedValue.of("{\"key\": \"B\", \"value\": 2, \"timestamp\": \"1970/01/01 00:00:10\"}", start.plus(windowSizeMs)))
                .advanceWatermarkToInfinity();

            sourceFactory.addSource("pubsub-in", pubSubIn);

            return sourceFactory;
        }

        @Override
        public TestSinkFactory buildAsserts() {
            TestSinkFactory sinkFactory = new TestSinkFactory();

            AssertAction<String> pubSubOut = new AssertAction<>() {
                @Override
                public void runAsserts(PCollection<String> pipelineOutput) {
                    PAssert.that(pipelineOutput).containsInAnyOrder(
                        "{\"key\":\"A\",\"count\":2,\"value_sum\":1,\"min_val\":0,\"max_val\":1,\"min_ts\":\"1970/01/01 00:00:00\",\"max_ts\":\"1970/01/01 00:00:01\",\"window_start\":\"1970-01-01T00:00:00.000Z\",\"window_end\":\"1970-01-01T00:00:10.000Z\"}",
                        "{\"key\":\"B\",\"count\":2,\"value_sum\":1,\"min_val\":0,\"max_val\":1,\"min_ts\":\"1970/01/01 00:00:00\",\"max_ts\":\"1970/01/01 00:00:01\",\"window_start\":\"1970-01-01T00:00:00.000Z\",\"window_end\":\"1970-01-01T00:00:10.000Z\"}",
                        "{\"key\":\"A\",\"count\":1,\"value_sum\":2,\"min_val\":2,\"max_val\":2,\"min_ts\":\"1970/01/01 00:00:10\",\"max_ts\":\"1970/01/01 00:00:10\",\"window_start\":\"1970-01-01T00:00:10.000Z\",\"window_end\":\"1970-01-01T00:00:20.000Z\"}",
                        "{\"key\":\"B\",\"count\":1,\"value_sum\":2,\"min_val\":2,\"max_val\":2,\"min_ts\":\"1970/01/01 00:00:10\",\"max_ts\":\"1970/01/01 00:00:10\",\"window_start\":\"1970-01-01T00:00:10.000Z\",\"window_end\":\"1970-01-01T00:00:20.000Z\"}"
                    );
                }
            };

            sinkFactory.addAssert("pubsub-out", pubSubOut);

            return sinkFactory;
        }
    }

    /**
     *
     */
    public static class TestCase2 implements PipelineTestCase {

        @Override
        public Options getOptions() {
            Options options = PipelineOptionsFactory.as(Options.class);

            options.setWindowSize(10);
            options.setAllowedLateness(30);
            options.setInputTopic(DummyOptions.PUBLIC_TAXI_RIDES_TOPIC);
            options.setOutputTopic(DummyOptions.PUBLIC_TAXI_RIDES_TOPIC);

            return options;
        }

        @Override
        public TestSourceFactory buildTestSources() {
            TestSourceFactory sourceFactory = new TestSourceFactory();

            Instant start = new Instant(0);
            long windowSizeMs = getOptions().getWindowSize() * 1000L;

            TestStream<String> pubSubIn = TestStream
                .create(StringUtf8Coder.of())
                .advanceWatermarkTo(start)
                .addElements(TimestampedValue.of("{\"key\": \"A\", \"value\": 0, \"timestamp\": \"1970/01/01 00:00:00\"}", start))
                .addElements(TimestampedValue.of("{\"key\": \"B\", \"value\": 0, \"timestamp\": \"1970/01/01 00:00:00\"}", start))
                .addElements(TimestampedValue.of("{\"key\": \"A\", \"value\": 1, \"timestamp\": \"1970/01/01 00:00:01\"}", start))
                .addElements(TimestampedValue.of("{\"key\": \"B\", \"value\": 1, \"timestamp\": \"1970/01/01 00:00:01\"}", start))
                .advanceWatermarkToInfinity();

            sourceFactory.addSource("pubsub-in", pubSubIn);

            return sourceFactory;
        }

        @Override
        public TestSinkFactory buildAsserts() {
            TestSinkFactory sinkFactory = new TestSinkFactory();

            AssertAction<String> pubSubOut = new AssertAction<>() {
                @Override
                public void runAsserts(PCollection<String> pipelineOutput) {
                    PAssert.that(pipelineOutput).containsInAnyOrder(
                        "{\"key\":\"A\",\"count\":2,\"value_sum\":1,\"min_val\":0,\"max_val\":1,\"min_ts\":\"1970/01/01 00:00:00\",\"max_ts\":\"1970/01/01 00:00:01\",\"window_start\":\"1970-01-01T00:00:00.000Z\",\"window_end\":\"1970-01-01T00:00:10.000Z\"}",
                        "{\"key\":\"B\",\"count\":2,\"value_sum\":1,\"min_val\":0,\"max_val\":1,\"min_ts\":\"1970/01/01 00:00:00\",\"max_ts\":\"1970/01/01 00:00:01\",\"window_start\":\"1970-01-01T00:00:00.000Z\",\"window_end\":\"1970-01-01T00:00:10.000Z\"}"
                    );
                }
            };

            sinkFactory.addAssert("pubsub-out", pubSubOut);

            return sinkFactory;
        }
    }
}