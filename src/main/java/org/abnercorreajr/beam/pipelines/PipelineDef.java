package org.abnercorreajr.beam.pipelines;

import org.apache.beam.sdk.options.PipelineOptions;

import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

public interface PipelineDef extends Serializable {

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public @interface BuildPipeline {}

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface OptionsDef {}

    // run() delegates to PipelineRunner
    default void run() { PipelineRunner.of(getClass()).run(); }
    default void run(String[] args) { PipelineRunner.of(getClass()).run(args); }
    default void run(PipelineOptions options) { PipelineRunner.of(getClass()).run(options); }
}
