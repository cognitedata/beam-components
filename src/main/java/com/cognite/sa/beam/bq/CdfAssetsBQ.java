package com.cognite.sa.beam.bq;

import avro.shaded.com.google.common.collect.ImmutableList;
import com.cognite.beam.io.CogniteIO;
import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.dto.Asset;
import com.google.api.services.bigquery.model.TableFieldSchema;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.values.PCollection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This pipeline reads all assets from the specified cdp instance and writes them to a target BigQuery table.
 *
 * The job is designed as a batch job which will truncate and write to BQ. It has built in delta read support
 * and can be configured (via a parameter) to do delta or full read.
 *
 * This job is prepared to be deployed as a template on GCP (Dataflow) + can be executed directly on any runner.
 */
public class CdfAssetsBQ {
    // The log to output status messages to.
    private static final Logger LOG = LoggerFactory.getLogger(CdfAssetsBQ.class);
    private static final String appIdentifier = "CdfAssetsBQ";

    /* BQ output schema */
    private static final TableSchema assetSchemaBQ = new TableSchema().setFields(ImmutableList.of(
            new TableFieldSchema().setName("id").setType("INT64"),
            new TableFieldSchema().setName("external_id").setType("STRING"),
            new TableFieldSchema().setName("name").setType("STRING"),
            new TableFieldSchema().setName("parent_id").setType("INT64"),
            new TableFieldSchema().setName("description").setType("STRING"),
            new TableFieldSchema().setName("root_id").setType("INT64"),
            new TableFieldSchema().setName("source").setType("STRING"),
            new TableFieldSchema().setName("created_time").setType("TIMESTAMP"),
            new TableFieldSchema().setName("last_updated_time").setType("TIMESTAMP"),
            new TableFieldSchema().setName("metadata").setType("RECORD").setMode("REPEATED").setFields(ImmutableList.of(
                    new TableFieldSchema().setName("key").setType("STRING"),
                    new TableFieldSchema().setName("value").setType("STRING")
            )),
            new TableFieldSchema().setName("row_updated_time").setType("TIMESTAMP")
    ));


    /**
     * Custom options for this pipeline.
     */
    public interface CdfAssetsBqOptions extends PipelineOptions {
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
        @Validation.Required
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
    private static PipelineResult runCdfAssetsBQ(CdfAssetsBqOptions options) throws IOException {
        Pipeline p = Pipeline.create(options);

        // Read and parse the main input.
        PCollection<Asset> mainInput = p.apply("Read cdf assets", CogniteIO.readAssets()
                .withProjectConfigFile(options.getCdfConfigFile())
                .withHints(Hints.create()
                        .withReadShards(100))
                .withReaderConfig(ReaderConfig.create()
                        .withAppIdentifier(appIdentifier)
                        .enableDeltaRead("system.bq-delta")
                        .withDeltaIdentifier("assets")
                        .withDeltaOffset(Duration.ofMinutes(10))
                        .withFullReadOverride(options.getFullRead()))
        );

        // Write to BQ
        mainInput.apply("Write output to BQ", BigQueryIO.<Asset>write()
                .to(options.getOutputMainTable())
                .withSchema(assetSchemaBQ)
                .withFormatFunction((Asset element) -> {
                    List<TableRow> metadata = new ArrayList<>();
                    DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;

                    for (Map.Entry<String, String> mElement : element.getMetadataMap().entrySet()) {
                        metadata.add(new TableRow()
                                .set("key", mElement.getKey())
                                .set("value", mElement.getValue()));
                    }

                    return new TableRow()
                            .set("id", element.hasId() ? element.getId().getValue() : null)
                            .set("external_id", element.hasExternalId() ? element.getExternalId().getValue() : null)
                            .set("name", element.getName())
                            .set("parent_id", element.hasParentId() ? element.getParentId().getValue() : null)
                            .set("description", element.hasDescription() ? element.getDescription().getValue() : null)
                            .set("root_id", element.hasRootId() ? element.getRootId().getValue() : null)
                            .set("source", element.hasSource() ? element.getSource().getValue() : null)
                            .set("created_time", element.hasCreatedTime() ? formatter.format(Instant.ofEpochMilli(element.getCreatedTime().getValue())) : null)
                            .set("last_updated_time", element.hasLastUpdatedTime() ? formatter.format(Instant.ofEpochMilli(element.getLastUpdatedTime().getValue())) : null)
                            .set("metadata", metadata)
                            .set("row_updated_time", formatter.format(Instant.now()));
                })
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                .optimizedWrites()
                .withCustomGcsTempLocation(options.getBqTempStorage()));

        return p.run();
    }

    /**
     * Read the pipeline options from args and run the pipeline.
     * @param args
     */
    public static void main(String[] args) throws IOException{
        CdfAssetsBqOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(CdfAssetsBqOptions.class);
        runCdfAssetsBQ(options);
    }
}
