/*

CREATE TABLE beam_pipelines.example_in AS
SELECT * FROM
UNNEST([
  STRUCT<`key` INT64, a INT64, b STRING>
  (1, 1, "X"),
  (2, 2, "Y"),
  (3, 3, "Z")
]);

 */
package org.abnercorreajr.beam.examples;

import org.abnercorreajr.beam.fn.RowFieldFn;
import org.abnercorreajr.beam.pipelines.PipelineDef;
import org.abnercorreajr.beam.testing.SinkFactory;
import org.abnercorreajr.beam.testing.SourceFactory;
import org.abnercorreajr.beam.transforms.TransformRows;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.List;

public class BigQueryExample implements PipelineDef {

    @OptionsDef
    public interface Options extends PipelineOptions {
        @Description("BigQuery input table in the format project-id.dataset.table-name")
        @Validation.Required
        String getInputTable();
        void setInputTable(String InputTable);

        @Description("BigQuery output table in the format project-id.dataset.table-name")
        @Validation.Required
        String getOutputTable();
        void setOutputTable(String OutputTable);
    }

    @BuildPipeline
    public void buildPipeline(Pipeline pipeline, Options options, SourceFactory sourceFactory, SinkFactory sinkFactory) {
        RowFieldFn<Long> generateC = r -> (r.getInt64("a") + r.getInt64("key")) / 2;

        pipeline
            .apply(sourceFactory.create("bq-in",
                BigQueryIO
                    .readTableRowsWithSchema()
                    .from(options.getInputTable())))
            .apply(
                TransformRows.create()
                    .withField("a", TypeDescriptors.longs(), r -> r.getInt64("a"))
                    .withField("b", TypeDescriptors.strings(), r -> r.getString("b") + " " + r.getInt64("a"))
                    .withField("aIsEven", TypeDescriptors.booleans(), r -> r.getInt64("a") % 2 == 0)
                    .withField("c", TypeDescriptors.longs(), generateC)
                    .withField("d", TypeDescriptors.longs(), r -> 2 * generateC.apply(r))
                    .withField("numbers", TypeDescriptors.lists(TypeDescriptors.longs()), r -> List.of(r.getInt64("key"), r.getInt64("a")))
                    .withFieldRenamed("b", "name")
                    .withFieldRenamed("d", "doubleC")
                    .drop("key"))
            .apply(sinkFactory.createForBigQuery("bq-out",
                BigQueryIO
                    .<Row>write()
                    .to(options.getOutputTable())
                    .useBeamSchema()
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)));
    }

    public static void main(String[] args) {
        new BigQueryExample().run(args);
    }
}
