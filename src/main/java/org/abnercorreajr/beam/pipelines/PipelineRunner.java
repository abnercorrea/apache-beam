package org.abnercorreajr.beam.pipelines;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Supports running a pipeline from code or using the CLI.
 *
 * Code example:
 *
 * <pre>{@code
 *      PipelineRunner.of(MyPipeline.class).run(args);
 * }</pre>
 *
 * In the above example, MyPipeline implements {@link PipelineDef}
 *
 * Or
 */
public class PipelineRunner {

    private static final Logger LOG = LoggerFactory.getLogger(PipelineRunner.class);

    private final PipelineBuilder pipelineBuilder;

    public PipelineRunner(PipelineBuilder pipelineBuilder) {
        this.pipelineBuilder = pipelineBuilder;
    }

    public static void main(String[] args) {
        String pipelineClassName = args[0];
        String[] pipelineArgs = Arrays.stream(args, 1, args.length).toArray(String[]::new);

        LOG.info("\nPipeline: {}\nargs: {}", pipelineClassName, pipelineArgs);

        PipelineRunner.of(pipelineClassName).run(pipelineArgs);
    }

    public static PipelineRunner of(String pipelineClassName) {
        return new PipelineRunner(PipelineBuilder.of(pipelineClassName));
    }

    public static PipelineRunner of(Class<? extends PipelineDef> pipelineClass) {
        return new PipelineRunner(PipelineBuilder.of(pipelineClass));
    }

    public static PipelineRunner of(PipelineBuilder pipelineBuilder) {
        return new PipelineRunner(pipelineBuilder);
    }

    public void run(PipelineOptions options) {
        pipelineBuilder.withOptions(options);

        run();
    }

    public void run(String[] args) {
        pipelineBuilder.withOptionsFromArgs(args);

        run();
    }

    public void run() {
        Pipeline pipeline = pipelineBuilder.build();

        LOG.info("Running pipeline with options:\n{}", pipeline.getOptions());

        PipelineResult result = pipeline.run();

        try {
            result.waitUntilFinish();
        }
        catch (UnsupportedOperationException e) {
            LOG.warn("Pipeline result does not support waitUntilFinish()");
        }

        LOG.info("Finished running pipeline with state: {}", result.getState());
    }
}
