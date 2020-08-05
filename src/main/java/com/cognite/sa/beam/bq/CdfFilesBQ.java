/*
 * Copyright 2020 Cognite AS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cognite.sa.beam.bq;

import avro.shaded.com.google.common.collect.ImmutableList;
import com.cognite.beam.io.CogniteIO;
import com.cognite.beam.io.config.GcpSecretConfig;
import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.dto.FileMetadata;
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
 * This pipeline reads all file headers from the specified cdp instance and writes them to a target BigQuery table.
 *
 * The job is designed as a batch job which will truncate and write to BQ. It has built in delta read support
 * and can be configured (via a parameter) to do delta or full read.
 *
 * This job is prepared to be deployed as a template on GCP (Dataflow) + can be executed directly on any runner.
 */
public class CdfFilesBQ {
    // The log to output status messages to.
    private static final Logger LOG = LoggerFactory.getLogger(CdfFilesBQ.class);
    private static final String appIdentifier = "CdfFilesBQ";

    /* BQ output schema */
    private static final TableSchema FileMetaSchemaBQ = new TableSchema().setFields(ImmutableList.of(
            new TableFieldSchema().setName("id").setType("INT64"),
            new TableFieldSchema().setName("external_id").setType("STRING"),
            new TableFieldSchema().setName("source_created_time").setType("TIMESTAMP"),
            new TableFieldSchema().setName("source_modified_time").setType("TIMESTAMP"),
            new TableFieldSchema().setName("name").setType("STRING"),
            new TableFieldSchema().setName("mime_type").setType("STRING"),
            new TableFieldSchema().setName("source").setType("STRING"),
            new TableFieldSchema().setName("asset_ids").setType("INT64").setMode("REPEATED"),
            new TableFieldSchema().setName("security_categories").setType("INT64").setMode("REPEATED"),
            new TableFieldSchema().setName("uploaded").setType("BOOL"),
            new TableFieldSchema().setName("uploaded_time").setType("TIMESTAMP"),
            new TableFieldSchema().setName("created_time").setType("TIMESTAMP"),
            new TableFieldSchema().setName("last_updated_time").setType("TIMESTAMP"),
            new TableFieldSchema().setName("data_set_id").setType("INT64"),
            new TableFieldSchema().setName("metadata").setType("RECORD").setMode("REPEATED").setFields(ImmutableList.of(
                    new TableFieldSchema().setName("key").setType("STRING"),
                    new TableFieldSchema().setName("value").setType("STRING")
            )),
            new TableFieldSchema().setName("row_updated_time").setType("TIMESTAMP")
    ));

    /**
     * Custom options for this pipeline.
     */
    public interface CdfFilesMetaBqOptions extends PipelineOptions {
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
    private static PipelineResult runCdfFilesMetaBq(CdfFilesMetaBqOptions options) {
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
        PCollection<FileMetadata> mainInput = p.apply("Read cdf Files", CogniteIO.readFilesMetadata()
                .withProjectConfig(projectConfig)
                .withHints(Hints.create()
                        .withReadShards(1000))
                .withReaderConfig(ReaderConfig.create()
                        .withAppIdentifier(appIdentifier)
                        .enableDeltaRead("system.bq-delta")
                        .withDeltaIdentifier("files-header")
                        .withDeltaOffset(Duration.ofMinutes(10))
                        .withFullReadOverride(options.getFullRead()))
        );

        // Write to BQ
        mainInput.apply("Write output to BQ", BigQueryIO.<FileMetadata>write()
                .to(options.getOutputMainTable())
                .withSchema(FileMetaSchemaBQ)
                .withFormatFunction((FileMetadata element) -> {
                    List<Long> assetIds = element.getAssetIdsList();
                    List<Long> securityCategories = element.getSecurityCategoriesList();
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
                            .set("source_created_time", element.hasSourceCreatedTime() ?
                                    formatter.format(Instant.ofEpochMilli(element.getSourceCreatedTime().getValue())) : null)
                            .set("source_modified_time", element.hasSourceModifiedTime() ?
                                    formatter.format(Instant.ofEpochMilli(element.getSourceModifiedTime().getValue())) : null)
                            .set("name", element.hasName() ? element.getName().getValue() : null)
                            .set("mime_type", element.hasMimeType() ? element.getMimeType().getValue() : null)
                            .set("source", element.hasSource() ? element.getSource().getValue() : null)
                            .set("asset_ids", assetIds)
                            .set("security_categories", assetIds)
                            .set("uploaded", element.getUploaded())
                            .set("uploaded_time", element.hasUploadedTime() ?
                                    formatter.format(Instant.ofEpochMilli(element.getUploadedTime().getValue())) : null)
                            .set("created_time", element.hasCreatedTime() ?
                                    formatter.format(Instant.ofEpochMilli(element.getCreatedTime().getValue())) : null)
                            .set("last_updated_time", element.hasLastUpdatedTime() ?
                                    formatter.format(Instant.ofEpochMilli(element.getLastUpdatedTime().getValue())) : null)
                            .set("data_set_id", element.hasDataSetId() ? element.getDataSetId().getValue() : null)
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
        CdfFilesMetaBqOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(CdfFilesMetaBqOptions.class);
        runCdfFilesMetaBq(options);
    }
}
