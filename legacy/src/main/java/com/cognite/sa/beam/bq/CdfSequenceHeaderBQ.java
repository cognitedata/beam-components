package com.cognite.sa.beam.bq;

/*
This pipeline reads all sequence headers/metadata ( and maybe columns metadata) from the specified cdp instance and writes
them to the target BigQuery table.

The job is designed as a batch job which will truncate and write to BigQuery. It has built in delta read support
and can be configured (via a parameter) to do delta or full read.

This job is prepared as a template on GCP (Dataflow) + can be executed directly on any runner.
 */


import com.cognite.beam.io.CogniteIO;
import com.cognite.beam.io.config.GcpSecretConfig;
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.client.dto.SequenceColumn;
import com.cognite.client.dto.SequenceMetadata;
import com.cognite.client.servicesV1.parser.SequenceParser;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
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


public class CdfSequenceHeaderBQ {
    // Setup logging
    private static final Logger LOG = LoggerFactory.getLogger(CdfSequenceHeaderBQ.class);
    private static final String appIdentifier = "CdfSequenceHeaderBQ";

    /* BQ output schema */
    private static final TableSchema SequenceHeaderSchemaBQ = new TableSchema().setFields(ImmutableList.of(
            new TableFieldSchema().setName("id").setType("INT64"),
            new TableFieldSchema().setName("name").setType("STRING"),
            new TableFieldSchema().setName("description").setType("STRING"),
            new TableFieldSchema().setName("asset_id").setType("INT64"),
            new TableFieldSchema().setName("external_id").setType("STRING"),
            new TableFieldSchema().setName("metadata").setType("RECORD").setMode("REPEATED").setFields(ImmutableList.of(
                    new TableFieldSchema().setName("key").setType("STRING"),
                    new TableFieldSchema().setName("value").setType("STRING")
            )),
            new TableFieldSchema().setName("columns").setType("RECORD").setMode("REPEATED").setFields(ImmutableList.of(
                    new TableFieldSchema().setName("name").setType("STRING"),
                    new TableFieldSchema().setName("external_id").setType("STRING"),
                    new TableFieldSchema().setName("value_type").setType("STRING")
            )),
            new TableFieldSchema().setName("created_time").setType("TIMESTAMP"),
            new TableFieldSchema().setName("last_updated_time").setType("TIMESTAMP"),
            new TableFieldSchema().setName("data_set_id").setType("INT64"),
            new TableFieldSchema().setName("row_updated_time").setType("TIMESTAMP")
    ));

    /*
    Custom options for this pipeline
     */
    public interface CdfSequenceHeaderBqOptions extends PipelineOptions {

        @Description("The GCP secret holding the source API key. The reference should be <projectId>.<secretId>")
        @Validation.Required
        ValueProvider<String> getCdfSecret();
        void setCdfSecret(ValueProvider<String> value);

        @Description("The CDF host name. The default value is https://api.cognitedata.com")
        @Validation.Required
        ValueProvider<String> getCdfHost();
        void setCdfHost(ValueProvider<String> value);

        @Description("Full read flag. Set to true for full read, false for delta read")
        @Validation.Required
        ValueProvider<Boolean> getFullRead();
        void setFullRead(ValueProvider<Boolean> value);

        @Description("The BQ table to write to. The name should be in the format <projectId>:<dataset>.<table>")
        @Validation.Required
        ValueProvider<String> getOutputMainTable();
        void setOutputMainTable(ValueProvider<String> value);

        @Description("The BQ temp storage location. Used for temp staging of writes to BQ. "
                + "The name should be in the format of gs://<bucket>/folder.")
        @Validation.Required
        ValueProvider<String> getBqTempStorage();
        void setBqTempStorage(ValueProvider<String> value);
    }

    /*
    Setup the main pipeline structure and run it
     */
    private static PipelineResult runCdfSequenceHeaderBQ(CdfSequenceHeaderBqOptions options) {

        GcpSecretConfig secretConfig = GcpSecretConfig.of(
                ValueProvider.NestedValueProvider.of(options.getCdfSecret(), secret -> secret.split("\\.")[0]),
                ValueProvider.NestedValueProvider.of(options.getCdfSecret(), secret -> secret.split("\\.")[1]));
        ProjectConfig projectConfig = ProjectConfig.create()
                .withApiKeyFromGcpSecret(secretConfig)
                .withHost(options.getCdfHost());

        Pipeline p = Pipeline.create(options);

        // Read and parse the main input
        PCollection<SequenceMetadata> sequenceHeaders = p.apply("Read CDF Sequence headers", CogniteIO.readSequencesMetadata()
                .withProjectConfig(projectConfig)
                .withReaderConfig(ReaderConfig.create()
                        .withAppIdentifier(appIdentifier)
                        .enableDeltaRead("system.bq-delta")
                        .withDeltaIdentifier("seq-header")
                        .withDeltaOffset(Duration.ofMinutes(10))
                        .withFullReadOverride(options.getFullRead())));

        // Write to BQ
        sequenceHeaders.apply("Write output to BQ", BigQueryIO.<SequenceMetadata>write()
                .to(options.getOutputMainTable())
                .withSchema(SequenceHeaderSchemaBQ)
                .withFormatFunction((SequenceMetadata element) -> {
                    List<TableRow> metadata = new ArrayList<>();
                    List<TableRow> columns = new ArrayList<>();
                    DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;

                    for (Map.Entry<String, String> mElement : element.getMetadataMap().entrySet()) {
                        metadata.add(new TableRow()
                                .set("key", mElement.getKey())
                                .set("value", mElement.getValue()));
                    }

                    for (SequenceColumn col : element.getColumnsList()) {
                        columns.add(new TableRow()
                                .set("name", col.hasName() ? col.getName().getValue() : null)
                                .set("external_id", col.getExternalId())
                                .set("value_type", SequenceParser.toString(col.getValueType())));
                    }

                    return new TableRow()
                            .set("id", element.hasId() ? element.getId().getValue() : null)
                            .set("name", element.hasName() ? element.getName().getValue() : null)
                            .set("description", element.hasDescription() ? element.getDescription().getValue() : null)
                            .set("asset_id", element.hasAssetId() ? element.getAssetId().getValue() : null)
                            .set("external_id", element.hasExternalId() ? element.getExternalId().getValue() : null)
                            .set("metadata", metadata)
                            .set("columns", columns)
                            .set("created_time", element.hasCreatedTime() ?
                                    formatter.format(Instant.ofEpochMilli(element.getCreatedTime().getValue())) : null)
                            .set("last_updated_time", element.hasLastUpdatedTime() ?
                                    formatter.format(Instant.ofEpochMilli(element.getLastUpdatedTime().getValue())) : null)
                            .set("data_set_id", element.hasDataSetId() ? element.getDataSetId().getValue() : null)
                            .set("row_updated_time", formatter.format(Instant.now()));
                })
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                .optimizedWrites()
                .withCustomGcsTempLocation(options.getBqTempStorage()));

        return p.run();
    }

    public static void main(String[] args) throws IOException {
        CdfSequenceHeaderBqOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(CdfSequenceHeaderBqOptions.class);
        runCdfSequenceHeaderBQ(options);
    }

}
