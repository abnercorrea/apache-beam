package org.abnercorreajr.beam.examples;

import org.abnercorreajr.beam.pipelines.PipelineDef;
import org.abnercorreajr.beam.transforms.LogElements;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.Arrays;
import java.util.List;

public class SideInputsExample implements PipelineDef {

    @BuildPipeline
    public void buildPipeline(Pipeline pipeline) {
        pipeline
            .apply(GenerateSequence.from(30).to(70))
            .apply(new FilterAboveAvg(10, "A", "D"))
            .apply(LogElements.toSysOut());
    }

    @SuppressWarnings("NullableProblems")
    static class FilterAboveAvg extends PTransform<PCollection<Long>, PCollection<KV<String, Long>>> {

        private final int factor;
        private final List<String> lettersToExclude;

        public FilterAboveAvg(int factor, String... lettersToExclude) {
            this.factor = factor;
            this.lettersToExclude = Arrays.asList(lettersToExclude);
        }

        @Override
        public PCollection<KV<String, Long>> expand(PCollection<Long> input) {
            Pipeline pipeline = input.getPipeline();

            PCollection<Long> numbers = pipeline.apply(GenerateSequence.from(0).to(100));
            PCollection<String> letters = pipeline.apply(Create.of("A", "B", "C", "D", "E"));

            // Singleton side input
            PCollectionView<Double> avgView = numbers.apply(Mean.globally()).apply(View.asSingleton());
            // List side input
            PCollectionView<List<String>> lettersView = letters.apply(View.asList());

            return input.apply(
                ParDo.of(
                    new DoFn<Long, KV<String, Long>>() {
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                            Double avg = c.sideInput(avgView);
                            List<String> letterList = c.sideInput(lettersView);

                            if (c.element() >= avg) {
                                for (String l: letterList) {
                                    if (!lettersToExclude.contains(l)) {
                                        c.output(KV.of(l, c.element() * factor));
                                    }
                                }
                            }
                        }
                    }
                ).withSideInputs(avgView, lettersView)
            );
        }
    }

    public static void main(String[] args) {
        new SideInputsExample().run(args);
    }
}
