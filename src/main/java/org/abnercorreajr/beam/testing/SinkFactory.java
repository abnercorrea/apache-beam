/**
 * Why can't static methods be abstract in Java?
 * Because
 * "abstract" means: "Implements no functionality",
 * and
 * "static" means: "There is functionality even if you don't have an object instance".
 * And that's a logical contradiction.
 *
 * https://stackoverflow.com/a/370967
 */
package org.abnercorreajr.beam.testing;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;


@SuppressWarnings("NullableProblems")
public interface SinkFactory {

    public <T> PTransform<PCollection<T>, PDone> create(String sinkId, PTransform<PCollection<T>, PDone> sink);

    public <T> PTransform<PCollection<T>, WriteResult> createForBigQuery(String sinkId, BigQueryIO.Write<T> sink);

}
