package org.abnercorreajr.beam.testing;

import org.apache.beam.sdk.options.PipelineOptions;

public interface PipelineTestCase {

    /**
     * @return
     */
    public PipelineOptions getOptions();

    /**
     * @return
     */
    public TestSourceFactory buildTestSources();

    /**
     * @return
     */
    public TestSinkFactory buildAsserts();

}
