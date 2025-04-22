package org.abnercorreajr.beam.pipelines;

import org.abnercorreajr.beam.testing.IOSinkFactory;
import org.abnercorreajr.beam.testing.IOSourceFactory;
import org.abnercorreajr.beam.testing.SinkFactory;
import org.abnercorreajr.beam.testing.SourceFactory;
import org.abnercorreajr.util.ReflectUtil;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Optional;

import static org.abnercorreajr.beam.pipelines.PipelineDef.BuildPipeline;
import static org.abnercorreajr.beam.pipelines.PipelineDef.OptionsDef;


public class PipelineBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(PipelineBuilder.class);

    private final Class<? extends PipelineDef> defClass;
    private final Class<? extends PipelineOptions> optionsClass;

    private Optional<PipelineOptions> options = Optional.empty();
    private Optional<Pipeline> pipeline = Optional.empty();
    private SourceFactory sourceFactory = new IOSourceFactory();
    private SinkFactory sinkFactory = new IOSinkFactory();

    protected PipelineBuilder(Class<? extends PipelineDef> defClass) {
        this.defClass = defClass;
        // Gets Options class ref from pipeline definition
        this.optionsClass = findOptionsClass(defClass);
    }

    public static PipelineBuilder of(String defClassName) {
        Class<PipelineDef> defClass = ReflectUtil.classFor(defClassName);

        return of(defClass);
    }

    public static PipelineBuilder of(Class<? extends PipelineDef> defClass) {
        return new PipelineBuilder(defClass);
    }

    public PipelineBuilder withOptionsFromArgs(String[] args) {
        // Creates pipeline options from input args
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(optionsClass);

        return withOptions(options);
    }

    public PipelineBuilder withOptions(PipelineOptions options) {
        this.options = Optional.of(options);

        return this;
    }

    public PipelineBuilder withPipeline(Pipeline pipeline) {
        this.pipeline = Optional.of(pipeline);

        return this;
    }

    public PipelineBuilder withSourceFactory(SourceFactory sourceFactory) {
        this.sourceFactory = sourceFactory;

        return this;
    }

    public PipelineBuilder withSinkFactory(SinkFactory sinkFactory) {
        this.sinkFactory = sinkFactory;

        return this;
    }

    public Pipeline build() {
        PipelineOptions options = this.options.orElse(PipelineOptionsFactory.as(optionsClass));
        Pipeline pipeline = this.pipeline.orElse(Pipeline.create(options));

        LOG.info("Building pipeline for definition: {}\nUsing options:\n{}", defClass, options);

        BuildPipelineInvoker.of(defClass).invoke(pipeline, options, sourceFactory, sinkFactory);

        return pipeline;
    }

    @SuppressWarnings("unchecked")
    private static <T extends PipelineOptions> Class<T> findOptionsClass(Class<? extends PipelineDef> defClass) {
        for (Class<?> clazz : defClass.getDeclaredClasses()) {
            if (clazz.isAnnotationPresent(OptionsDef.class)) {
                return (Class<T>) clazz;
            }
        }

        return (Class<T>) PipelineOptions.class;
    }

    /**
     *
     */
    static class BuildPipelineInvoker {
        private final Method buildPipeline;

        private static final int MAX_PARAMETER_COUNT = 4;

        private BuildPipelineInvoker(Method buildPipeline) {
            this.buildPipeline = buildPipeline;
        }

        static BuildPipelineInvoker of(Class<? extends PipelineDef> defClass) {
            Method buildPipeline = findBuildPipeline(defClass);

            return new BuildPipelineInvoker(buildPipeline);
        }

        /**
         * Finds method annotated with {@link BuildPipeline} in the provided class.
         * @param defClass
         * @return
         */
        private static Method findBuildPipeline(Class<? extends PipelineDef> defClass) {
            for (Method method : defClass.getDeclaredMethods()) {
                if (method.isAnnotationPresent(BuildPipeline.class)) {
                    method.setAccessible(true);  // in case method is private
                    return method;
                }
            }

            throw new IllegalArgumentException(String.format("Class %s does not define a method annotated with %s", defClass, BuildPipeline.class));
        }

        /**
         * Builds array of arguments to be used to invoke the method annotated with {@link BuildPipeline}
         *
         * The arguments used and their order depend on the signature of the method
         *
         * @param pipeline
         * @param options
         * @param sourceFactory
         * @param sinkFactory
         * @return
         */
        public Object[] buildArgs(Pipeline pipeline, PipelineOptions options, SourceFactory sourceFactory, SinkFactory sinkFactory) {
            int paramCount = buildPipeline.getParameterCount();

            if (paramCount > MAX_PARAMETER_COUNT) {
                throw new IllegalArgumentException(String.format("Method %s has %d parameters (max = %d).", buildPipeline.getName(), paramCount, MAX_PARAMETER_COUNT));
            }

            Object[] args = new Object[paramCount];

            Class<?>[] paramTypes = buildPipeline.getParameterTypes();

            boolean hasPipelineParam = false;

            for (int i = 0; i < paramCount; i++) {
                if (paramTypes[i].isInstance(pipeline)) {
                    args[i] = pipeline;
                    hasPipelineParam = true;
                } else if (paramTypes[i].isInstance(options)) {
                    args[i] = options;
                } else if (paramTypes[i].isInstance(sourceFactory)) {
                    args[i] = sourceFactory;
                } else if (paramTypes[i].isInstance(sinkFactory)) {
                    args[i] = sinkFactory;
                } else {
                    throw new IllegalArgumentException(String.format("Method %s has parameter %s with unexpected type (%s).", buildPipeline.getName(), buildPipeline.getParameters()[i].getName(), paramTypes[i]));
                }
            }

            if (!hasPipelineParam) {
                throw new IllegalArgumentException(String.format("Method %s does not have a parameter that is a subtype of (%s).", buildPipeline.getName(), Pipeline.class));
            }

            return args;
        }

        /**
         * Invokes the method annotated with {@link BuildPipeline}
         *
         * @param pipeline
         * @param options
         * @param sourceFactory
         * @param sinkFactory
         */
        public void invoke(Pipeline pipeline, PipelineOptions options, SourceFactory sourceFactory, SinkFactory sinkFactory) {
            try {
                Object[] args = buildArgs(pipeline, options, sourceFactory, sinkFactory);

                PipelineDef pipelineDef = (PipelineDef) ReflectUtil.newInstance(buildPipeline.getDeclaringClass());

                buildPipeline.invoke(pipelineDef, args);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
