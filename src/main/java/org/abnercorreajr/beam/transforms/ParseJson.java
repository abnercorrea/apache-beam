package org.abnercorreajr.beam.transforms;

import com.google.gson.Gson;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

@SuppressWarnings("NullableProblems")
public class ParseJson<T> extends PTransform<PCollection<String>, PCollection<T>> {

    private final Class<T> parsedClass;

    private ParseJson(Class<T> parsedClass) {
        this.parsedClass = parsedClass;
    }

    @Override
    public PCollection<T> expand(PCollection<String> input) {
        return input
            .apply(ParDo.of(
                // Parses json using Gson
                new DoFn<String, T>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) throws Exception {
                        Gson gson = new Gson();
                        T parsed = gson.fromJson(c.element(), parsedClass);
                        c.output(parsed);
                    }
                }
            ))
            .setTypeDescriptor(TypeDescriptor.of(parsedClass));
    }

    public static <T> ParseJson<T> to(Class<T> parsedClass) {
        return new ParseJson<>(parsedClass);
    }
}

