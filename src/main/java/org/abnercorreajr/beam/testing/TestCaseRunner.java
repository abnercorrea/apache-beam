package org.abnercorreajr.beam.testing;

import org.abnercorreajr.beam.pipelines.PipelineBuilder;
import org.abnercorreajr.beam.pipelines.PipelineDef;
import org.abnercorreajr.beam.pipelines.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestCaseRunner {

    private static final Logger LOG = LoggerFactory.getLogger(TestCaseRunner.class);

    private final PipelineBuilder pipelineBuilder;

    private TestCaseRunner(PipelineBuilder pipelineBuilder) {
        this.pipelineBuilder = pipelineBuilder;
    }

    public static <T extends PipelineOptions>
    TestCaseRunner of(Class<? extends PipelineDef> defClass, TestPipeline testPipeline, PipelineTestCase testCase) {
        LOG.info("Creating pipeline test case runner for:\nPipeline: {}\nTest Case: {}", defClass, testCase.getClass());

        PipelineBuilder builder = PipelineBuilder
            .of(defClass)
            .withOptions(testCase.getOptions())
            .withPipeline(testPipeline)
            .withSourceFactory(testCase.buildTestSources())
            .withSinkFactory(testCase.buildAsserts());

        return new TestCaseRunner(builder);
    }

    public void run() {
        PipelineRunner.of(pipelineBuilder).run();
    }

}
