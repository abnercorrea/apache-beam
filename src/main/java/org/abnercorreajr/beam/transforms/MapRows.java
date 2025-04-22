package org.abnercorreajr.beam.transforms;

import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

@SuppressWarnings("NullableProblems")
public class MapRows extends PTransform<PCollection<Row>, PCollection<Row>> {

    private final MapElements<Row, Row> sdkMap;

    private MapRows(MapElements<Row, Row> sdkMap) {
        this.sdkMap = sdkMap;
    }

    public static MapRows via(
      final InferableFunction<Row, Row> fn) {
        return new MapRows(MapElements.via(fn));
    }

    /** Binary compatibility adapter for {@link #via(InferableFunction)}. */
    public static MapRows via(
      final SimpleFunction<Row, Row> fn) {
        return new MapRows(MapElements.via(fn));
    }

    public static MapRows via(ProcessFunction<Row, Row> fn) {
        return new MapRows(MapElements.into(TypeDescriptor.of(Row.class)).via(fn));
    }

    /** Binary compatibility adapter for {@link #via(ProcessFunction)}. */
    public static MapRows via(
      SerializableFunction<Row, Row> fn) {
        return via((ProcessFunction<Row, Row>) fn);
    }

    /** Like {@link #via(ProcessFunction)}, but supports access to context, such as side inputs. */
    public static MapRows via(Contextful<Contextful.Fn<Row, Row>> fn) {
        return new MapRows(MapElements.into(TypeDescriptor.of(Row.class)).via(fn));
    }

    @Override
    public PCollection<Row> expand(PCollection<Row> input) {
        return input.apply("MapRows", sdkMap).setCoder(input.getCoder());
    }
}
