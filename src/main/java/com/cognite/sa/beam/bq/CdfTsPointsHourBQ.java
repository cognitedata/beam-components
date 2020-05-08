package com.cognite.sa.beam.bq;

import avro.shaded.com.google.common.collect.ImmutableList;
import com.cognite.beam.io.CogniteIO;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.dto.TimeseriesMetadata;
import com.cognite.beam.io.dto.TimeseriesPoint;
import com.cognite.beam.io.servicesV1.RequestParameters;
import com.google.api.services.bigquery.model.*;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

/**
 * This pipeline reads TS data points along a rolling time window from the specified cdp instance and writes
 * them to a target BigQuery table.
 *
 * The job is designed as a batch job which will trucate and write to BQ. That is, it will do a full update
 * with each execution.
 *
 * This job is prepared to be deployed as a template on GCP (Dataflow) + can be executed directly on any runner.
 */
public class CdfTsPointsHourBQ {
    // The log to output status messages to.
    private static final Logger LOG = LoggerFactory.getLogger(CdfTsPointsHourBQ.class);

    /* BQ output schema */
    private static final TableSchema tsPointAggSchemaBQ = new TableSchema().setFields(ImmutableList.of(
            new TableFieldSchema().setName("id").setType("INT64"),
            new TableFieldSchema().setName("external_id").setType("STRING"),
            new TableFieldSchema().setName("timestamp").setType("TIMESTAMP"),
            new TableFieldSchema().setName("average").setType("FLOAT64"),
            new TableFieldSchema().setName("max").setType("FLOAT64"),
            new TableFieldSchema().setName("min").setType("FLOAT64"),
            new TableFieldSchema().setName("count").setType("INT64"),
            new TableFieldSchema().setName("sum").setType("FLOAT64"),
            new TableFieldSchema().setName("interpolation").setType("FLOAT64"),
            new TableFieldSchema().setName("step_interpolation").setType("FLOAT64"),
            new TableFieldSchema().setName("continuous_variance").setType("FLOAT64"),
            new TableFieldSchema().setName("discrete_variance").setType("FLOAT64"),
            new TableFieldSchema().setName("total_variation").setType("FLOAT64"),
            new TableFieldSchema().setName("row_updated_time").setType("TIMESTAMP")
    ));


    /**
     * Custom options for this pipeline.
     */
    public interface CdfTsPointsAggBqOptions extends PipelineOptions {
        /**
         * Specify the Cdf config file.
         */
        @Description("The cdf config file. The name should be in the format of gs://<bucket>/folder.")
        @Validation.Required
        ValueProvider<String> getCdfConfigFile();
        void setCdfConfigFile(ValueProvider<String> value);

        /**
         * Specify delta read override.
         *
         * Set to <code>true</code> for complete reads.
         */
        @Description("Full read flag. Set to true for full read, false for delta read.")
        @Default.Boolean(false)
        ValueProvider<Boolean> getFullRead();
        void setFullRead(ValueProvider<Boolean> value);

        /**
         * Specify the BQ table for the main output.
         */
        @Description("The BQ table to write to. The name should be in the format of <project-id>:<dataset>.<table>.")
        @Validation.Required
        ValueProvider<String> getOutputMainTable();
        void setOutputMainTable(ValueProvider<String> value);

        /**
         * Specify the BQ temp location.
         */
        @Description("The BQ temp storage location. Used for temp staging of writes to BQ. "
                + "The name should be in the format of gs://<bucket>/folder.")
        @Validation.Required
        ValueProvider<String> getBqTempStorage();
        void setBqTempStorage(ValueProvider<String> value);
    }

    /**
     * Setup the main pipeline structure and run it.
     * @param options
     */
    private static PipelineResult runCdfTsPointsAggBQ(CdfTsPointsAggBqOptions options) throws IOException {
        Pipeline p = Pipeline.create(options);

        // Read ts headers.
        PCollection<TimeseriesMetadata> tsHeaders = p
                .apply("Read cdf TS headers", CogniteIO.readTimeseriesMetadata()
                        .withProjectConfigFile(options.getCdfConfigFile())
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("CdfTsPointsHourBQ"))
                ).apply("Filter out TS w/ security categories", Filter.by(
                        tsHeader -> tsHeader.getSecurityCategoriesList().isEmpty()
                                && !tsHeader.getName().getValue().startsWith("SRE-cognite-sre-Timeseries")
                                && !tsHeader.getIsString()
                ));

        // Read ts points for all headers
        PCollection<TimeseriesPoint> tsPoints = tsHeaders.apply("Build ts points request", MapElements
                .into(TypeDescriptor.of(RequestParameters.class))
                .via((TimeseriesMetadata header) -> {
                    // set time window to the past 1 - 2 days
                    Instant fromTime = Instant.now().truncatedTo(ChronoUnit.DAYS)
                            .minus(4, ChronoUnit.DAYS);

                    return RequestParameters.create()
                            .withItems(ImmutableList.of(ImmutableMap.of("id", header.getId().getValue())))
                            .withRootParameter("start", fromTime.toEpochMilli())
                            .withRootParameter("limit", 10000)
                            .withRootParameter("aggregates", ImmutableList.of("average", "max", "min", "count", "sum",
                                    "interpolation", "stepInterpolation", "interpolation", "continuousVariance",
                                    "discreteVariance", "totalVariation"))
                            .withRootParameter("granularity", "1h");
                }))
                .apply("Read ts points", CogniteIO.readAllTimeseriesPoints()
                        .withProjectConfigFile(options.getCdfConfigFile())
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("CdfTsPointsBQ")));

        // Write to BQ
        tsPoints.apply("Write output to BQ", BigQueryIO.<TimeseriesPoint>write()
                .to(options.getOutputMainTable())
                .withSchema(tsPointAggSchemaBQ)
                .withFormatFunction((TimeseriesPoint element) -> {
                    DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;
                    return new TableRow()
                            .set("id", element.getId())
                            .set("external_id", element.hasExternalId() ? element.getExternalId().getValue() : null)
                            .set("timestamp", formatter.format(Instant.ofEpochMilli(element.getTimestamp())))
                            .set("average", element.getValueAggregates().hasAverage() ?
                                    element.getValueAggregates().getAverage().getValue() : null)
                            .set("average", element.getValueAggregates().hasAverage() ?
                                    element.getValueAggregates().getAverage().getValue() : null)
                            .set("max", element.getValueAggregates().hasMax() ?
                                    element.getValueAggregates().getMax().getValue() : null)
                            .set("min", element.getValueAggregates().hasMin() ?
                                    element.getValueAggregates().getMin().getValue() : null)
                            .set("count", element.getValueAggregates().hasCount() ?
                                    element.getValueAggregates().getCount().getValue() : null)
                            .set("sum", element.getValueAggregates().hasSum() ?
                                    element.getValueAggregates().getSum().getValue() : null)
                            .set("interpolation", element.getValueAggregates().hasInterpolation() ?
                                    element.getValueAggregates().getInterpolation().getValue() : null)
                            .set("step_interpolation", element.getValueAggregates().hasStepInterpolation() ?
                                    element.getValueAggregates().getStepInterpolation().getValue() : null)
                            .set("continuous_variance", element.getValueAggregates().hasContinuousVariance() ?
                                    element.getValueAggregates().getContinuousVariance().getValue() : null)
                            .set("discrete_variance", element.getValueAggregates().hasDiscreteVariance() ?
                                    element.getValueAggregates().getDiscreteVariance().getValue() : null)
                            .set("total_variation", element.getValueAggregates().hasTotalVariation() ?
                                    element.getValueAggregates().getTotalVariation().getValue() : null)
                            .set("row_updated_time", formatter.format(Instant.now()));
                })
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                .withTimePartitioning(new TimePartitioning().setField("timestamp"))
                .withClustering(new Clustering().setFields(ImmutableList.of("external_id", "id")))
                .optimizedWrites()
                .withCustomGcsTempLocation(options.getBqTempStorage()));

        return p.run();
    }

    /**
     * Read the pipeline options from args and run the pipeline.
     * @param args
     */
    public static void main(String[] args) throws IOException{
        CdfTsPointsAggBqOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(CdfTsPointsAggBqOptions.class);
        runCdfTsPointsAggBQ(options);
    }
}
