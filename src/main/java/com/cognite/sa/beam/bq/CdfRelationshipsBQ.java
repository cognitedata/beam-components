package com.cognite.sa.beam.bq;

import avro.shaded.com.google.common.collect.ImmutableList;
import com.cognite.beam.io.CogniteIO;
import com.cognite.beam.io.config.GcpSecretConfig;
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.dto.Relationship;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableBiMap;
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

/**
 * This pipeline reads all relationships from the specified cdp instance and writes them to a target BigQuery table.
 *
 * The job is designed as a batch job which will truncate and write to BQ. It has built in delta read support
 * and can be configured (via a parameter) to do delta or full read.
 *
 * This job is prepared to be deployed as a template on GCP (Dataflow) + can be executed directly on any runner.
 */
public class CdfRelationshipsBQ {
    // The log to output status messages to.
    private static final Logger LOG = LoggerFactory.getLogger(CdfRelationshipsBQ.class);
    private static final String appIdentifier = "CdfEventsBQ";

    /* BQ output schema */
    private static final TableSchema relationshipSchemaBQ = new TableSchema().setFields(ImmutableList.of(
            new TableFieldSchema().setName("external_id").setType("STRING"),
            new TableFieldSchema().setName("source_external_id").setType("STRING"),
            new TableFieldSchema().setName("source_type").setType("STRING"),
            new TableFieldSchema().setName("target_external_id").setType("STRING"),
            new TableFieldSchema().setName("target_type").setType("STRING"),
            new TableFieldSchema().setName("start_time").setType("TIMESTAMP"),
            new TableFieldSchema().setName("end_time").setType("TIMESTAMP"),
            new TableFieldSchema().setName("confidence").setType("FLOAT64"),
            new TableFieldSchema().setName("data_set").setType("STRING"),
            new TableFieldSchema().setName("relationship_type").setType("STRING"),
            new TableFieldSchema().setName("created_time").setType("TIMESTAMP"),
            new TableFieldSchema().setName("last_updated_time").setType("TIMESTAMP"),
            new TableFieldSchema().setName("row_updated_time").setType("TIMESTAMP")
    ));

    /* Temporary maps. Can be removed when the Beam connector 0.9.4 is released. */
    private static final ImmutableBiMap<String, Relationship.Reference.ResourceType> resourceTypeMap = ImmutableBiMap
            .<String, Relationship.Reference.ResourceType>builder()
            .put("asset", Relationship.Reference.ResourceType.ASSET)
            .put("timeSeries", Relationship.Reference.ResourceType.TIME_SERIES)
            .put("file", Relationship.Reference.ResourceType.FILE)
            .put("threeD", Relationship.Reference.ResourceType.THREE_D)
            .put("threeDRevision", Relationship.Reference.ResourceType.THREE_D_REVISION)
            .put("event", Relationship.Reference.ResourceType.EVENT)
            .put("sequence", Relationship.Reference.ResourceType.SEQUENCE)
            .build();

    private static final ImmutableBiMap<String, Relationship.RelationshipType> relationshipTypeMap = ImmutableBiMap
            .<String, Relationship.RelationshipType>builder()
            .put("flowsTo", Relationship.RelationshipType.FLOWS_TO)
            .put("belongsTo", Relationship.RelationshipType.BELONGS_TO)
            .put("isParentOf", Relationship.RelationshipType.IS_PARENT_OF)
            .put("implements", Relationship.RelationshipType.IMPLEMENTS)
            .build();


    /**
     * Custom options for this pipeline.
     */
    public interface CdfRelationshipsBqOptions extends PipelineOptions {
        // The options below can be used for file-based secrets handling.
        /**
         * Specify the Cdf config file.
         */
        /*
        @Description("The cdf config file. The name should be in the format of gs://<bucket>/folder.")
        @Validation.Required
        ValueProvider<String> getCdfConfigFile();
        void setCdfConfigFile(ValueProvider<String> value);

         */

        @Description("The GCP secret holding the source api key. The reference should be <projectId>.<secretId>.")
        @Validation.Required
        ValueProvider<String> getCdfSecret();
        void setCdfSecret(ValueProvider<String> value);

        @Description("The CDF host name. The default value is https://api.cognitedata.com.")
        @Default.String("https://api.cognitedata.com")
        ValueProvider<String> getCdfHost();
        void setCdfHost(ValueProvider<String> value);

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
    private static PipelineResult runCdfRelationshipsBQ(CdfRelationshipsBqOptions options) {
        /*
        Build the project configuration (CDF tenant and api key) based on:
        - api key from Secret Manager
        - CDF api host
         */
        GcpSecretConfig secretConfig = GcpSecretConfig.of(
                ValueProvider.NestedValueProvider.of(options.getCdfSecret(), secret -> secret.split("\\.")[0]),
                ValueProvider.NestedValueProvider.of(options.getCdfSecret(), secret -> secret.split("\\.")[1]));
        ProjectConfig projectConfig = ProjectConfig.create()
                .withApiKeyFromGcpSecret(secretConfig)
                .withHost(options.getCdfHost());

        Pipeline p = Pipeline.create(options);

        // Read and parse the main input.
        PCollection<Relationship> mainInput = p.apply("Read relationships", CogniteIO.readRelationships()
                .withProjectConfig(projectConfig)
                .withReaderConfig(ReaderConfig.create()
                        .withAppIdentifier(appIdentifier)
                        .enableDeltaRead("system.bq-delta")
                        .withDeltaIdentifier("relationships")
                        .withDeltaOffset(Duration.ofMinutes(10))
                        .withFullReadOverride(options.getFullRead()))
        );

        // Write to BQ
        mainInput.apply("Write output to BQ", BigQueryIO.<Relationship>write()
                .to(options.getOutputMainTable())
                .withSchema(relationshipSchemaBQ)
                .withFormatFunction((Relationship element) -> {
                    DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;

                    return new TableRow()
                            .set("external_id", element.getExternalId())
                            .set("source_external_id", element.hasSource() ?
                                    element.getSource().getResourceExternalId() : null)
                            .set("source_type", element.hasSource() ?
                                    resourceTypeMap.inverse().get(element.getSource().getResourceType()) : null)
                            .set("target_external_id", element.hasTarget() ?
                                    element.getTarget().getResourceExternalId() : null)
                            .set("target_type", element.hasTarget() ?
                                    resourceTypeMap.inverse().get(element.getTarget().getResourceType()) : null)
                            .set("start_time", element.hasStartTime() ? formatter.format(Instant.ofEpochMilli(element.getStartTime().getValue())) : null)
                            .set("end_time", element.hasEndTime() ? formatter.format(Instant.ofEpochMilli(element.getEndTime().getValue())) : null)
                            .set("confidence", element.getConfidence())
                            .set("data_set", element.getDataSet())
                            .set("relationship_type", relationshipTypeMap.inverse().get(element.getRelationshipType()))
                            .set("created_time", element.hasCreatedTime() ? formatter.format(Instant.ofEpochMilli(element.getCreatedTime().getValue())) : null)
                            .set("last_updated_time", element.hasLastUpdatedTime() ? formatter.format(Instant.ofEpochMilli(element.getLastUpdatedTime().getValue())) : null)
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
        CdfRelationshipsBqOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(CdfRelationshipsBqOptions.class);
        runCdfRelationshipsBQ(options);
    }
}
