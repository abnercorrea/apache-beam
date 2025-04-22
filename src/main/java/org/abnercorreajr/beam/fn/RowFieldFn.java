package org.abnercorreajr.beam.fn;

import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;

import javax.annotation.Nonnull;

@FunctionalInterface
public interface RowFieldFn<OutputT> extends SerializableFunction<Row, OutputT> {

  @Override
  OutputT apply(@Nonnull Row input);

}
