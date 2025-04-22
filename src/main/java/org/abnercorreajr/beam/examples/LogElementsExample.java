/*

# Maven print classpath:

mvn dependency:build-classpath -DincludeScope=runtime -q -Dmdep.outputFile=/dev/stdout

# Maven run pipeline - Dataflow:

mvn clean compile -e exec:java \
    -Dexec.mainClass=org.abnercorreajr.beam.examples.LogElementsExample \
    -Dexec.args="\
        --runner=DataflowRunner \
        --project=pde-cert-441703 \
        --stagingLocation="gs://abnercorreajr/dataflow/staging/java/2_63" \
        --tempLocation="gs://abnercorreajr/dataflow/temp/java" \
        --region=us-east1 \
        --windowSize=5 \
    "

    --workerMachineType=e2-standard-2 \

# Maven run pipeline - Local:

mvn clean compile -e exec:java \
    -Dexec.mainClass=org.abnercorreajr.beam.examples.LogElementsExample \
    -Dexec.args="\
        --runner=DirectRunner \
        --windowSize=5 \
    "

# Running with Java from the "beam-pipelines" dir:

PIPELINE_JAR="$(pwd)/target/abnercorreajr-beam-1.0-SNAPSHOT.jar"
"${HOME}"/.jdks/corretto-11.0.26/bin/java \
    -Dfile.encoding=UTF-8 \
    -classpath "${PIPELINE_JAR}":"$(mvn dependency:build-classpath -DincludeScope=runtime -q -Dmdep.outputFile=/dev/stdout)" \
    org.abnercorreajr.beam.examples.LogElementsExample \
        --runner=DirectRunner \
        --windowSize=5

 */
package org.abnercorreajr.beam.examples;

import org.abnercorreajr.beam.pipelines.PipelineDef;
import org.abnercorreajr.beam.transforms.LogElements;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.event.Level;


public class LogElementsExample implements PipelineDef {

    @OptionsDef
    public interface Options extends ApplicationNameOptions {
        @Default.Integer(10)
        int getWindowSize();
        void setWindowSize(int windowSize);
    }

    @BuildPipeline
    public void buildPipeline(Pipeline pipeline, Options options) {
        PCollection<Long> source = pipeline
            .apply("Create", GenerateSequence.from(0).to(20).withTimestampFn(Instant::ofEpochSecond))
            .setCoder(VarLongCoder.of())
            .apply("FixedWindows", Window.into(FixedWindows.of(Duration.standardSeconds(options.getWindowSize()))))
            // Logs the source panes (not triggered, timing = UNKNOWN)
            .apply("LogSysOut", LogElements.toSysOut());

        PCollection<Long> triggered = source
            // With the aggregation (group by), the trigger will fire panes with aggregated results.
            // This is a "passthrough" (in = out) / dummy agg, added just to allow the trigger to fire
            .apply(WithKeys.of(1))
            .apply(GroupByKey.create())
            .apply(Values.create())
            .apply(Flatten.iterables())
            // Logs the triggered panes
            .apply("TriggeredWithLogger", LogElements.withLevel(Level.INFO));

        PCollection<KV<String, Long>> sum = source
            .apply("AddEvenOddKey", WithKeys.of(i -> (i % 2 == 0) ? "Even" : "Odd"))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))
            // There are 2 ways to define the MapValues (and WithKeys) fn: (and avoid a Coder error at runtime)
            // - use a lambda and the .setCoder() method. (like done here)
            // - use an instance of SerializableFunction (instead of lambda)
            .apply("Times10", MapValues.into(TypeDescriptors.longs()).via(i -> i * 10))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))
            .apply(Sum.longsPerKey())
            .apply("SumToSysOut", LogElements.withLevel(Level.INFO));
    }

}


