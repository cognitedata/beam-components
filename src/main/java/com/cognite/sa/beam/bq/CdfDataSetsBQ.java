/*
 * Copyright 2021 Cognite AS
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
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.client.dto.DataSet;

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
public class CdfDataSetsBQ {
    // The log to output status messages to
    private static final Logger LOG = LoggerFactory.getLogger(CdfDataSetsBQ.class);
    private static final String appIdentifier = "CdfDataSetsBQ";

    /* BQ output schema */
    private static final TableSchema dataSetSchemaBQ = new TableSchema().setFields(ImmutableList.of(
            new TableFieldSchema().setName("id").setType("INT64"),
            new TableFieldSchema().setName("external_id").setType("STRING"),
            new TableFieldSchema().setName("name").setType("STRING"),
            new TableFieldSchema().setName("description").setType("STRING"),
            new TableFieldSchema().setName("metadata").setType("RECORD").setMode("REPEATED").setFields(ImmutableList.of(
                    new TableFieldSchema().setName("key").setType("STRING"),
                    new TableFieldSchema().setName("value").setType("STRING")
            )),
            new TableFieldSchema().setName("write_protected").setType("BOOL"),
            new TableFieldSchema().setName("created_time").setType("TIMESTAMP"),
            new TableFieldSchema().setName("last_updated_time").setType("TIMESTAMP"),
            new TableFieldSchema().setName("row_updated_time").setType("TIMESTAMP")
    ));

    /**
     * Custom options for this pipeline.
     */
    public interface CdfDataSetsBqOptions extends PipelineOptions {

        @Description("The GCP secret holding the source api key. The reference should be <projectId>.<secretId>.")
        @Validation.Required
        ValueProvider<String> getCdfSecret();
        void setCdfSecret(ValueProvider<String> value);

        @Description("The CDF host name. The default values is https://api.cognitedata.com.")
        @Default.String("https://api.cognitedata.com")
        ValueProvider<String> getCdfHost();
        void setCdfHost(ValueProvider<String> value);

        @Description("The BQ table to write to. The name should be in the format of <projectId>:<dataset>.<table>.")
        @Validation.Required
        ValueProvider<String> getOutputMainTable();
        void setOutputMainTable(ValueProvider<String> value);

        @Description("The BQ temp storage location. Used for temp staging of writes to BQ." +
                "The name should be in the format of gs://<bucket>/folder.")
        @Validation.Required
        ValueProvider<String> getBqTempStorage();
        void setBqTempStorage(ValueProvider<String> value);
    }


    private static PipelineResult runCdfDataSetsBq(CdfDataSetsBqOptions options) {

        GcpSecretConfig secretConfig = GcpSecretConfig.of(
                ValueProvider.NestedValueProvider.of(options.getCdfSecret(), secret -> secret.split("\\.")[0]),
                ValueProvider.NestedValueProvider.of(options.getCdfSecret(), secret -> secret.split("\\.")[1]));

        ProjectConfig projectConfig = ProjectConfig.create()
                .withApiKeyFromGcpSecret(secretConfig)
                .withHost(options.getCdfHost());

        Pipeline p = Pipeline.create(options);

        PCollection<DataSet> mainInput = p.apply("Read CDF data sets", CogniteIO.readDataSets()
                .withProjectConfig(projectConfig)
                .withReaderConfig(ReaderConfig.create()
                        .withAppIdentifier(appIdentifier)
                )
        );

        mainInput.apply("Write output to BQ", BigQueryIO.<DataSet>write()
                .to(options.getOutputMainTable())
                .withSchema(dataSetSchemaBQ)
                .withFormatFunction((DataSet element ) -> {
                    List<TableRow> metadata = new ArrayList<>();
                    DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;

                    for (Map.Entry<String, String> mElement : element.getMetadataMap().entrySet()) {
                        metadata.add(new TableRow()
                                .set("key", mElement.getKey())
                                .set("value", mElement.getValue()));
                    }

                    return new TableRow()
                            .set("id", element.hasId() ? element.getId() : null)
                            .set("external_id", element.hasExternalId() ? element.getExternalId() : null)
                            .set("name", element.getName())
                            .set("description", element.hasDescription() ? element.getDescription() : null)
                            .set("metadata", metadata)
                            .set("write_protected", element.hasWriteProtected() ? element.getWriteProtected() : null)
                            .set("created_time", element.hasCreatedTime() ? formatter.format(Instant.ofEpochMilli(element.getCreatedTime())) : null)
                            .set("last_updated_time", element.hasLastUpdatedTime() ? formatter.format(Instant.ofEpochMilli(element.getLastUpdatedTime())) : null)
                            .set("row_updated_time", formatter.format(Instant.now()));

                })
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                .optimizedWrites()
                .withCustomGcsTempLocation(options.getBqTempStorage()));

        return p.run();

    }

    public static void main(String[] args) throws IOException {
        CdfDataSetsBqOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(CdfDataSetsBqOptions.class);
        runCdfDataSetsBq(options);
    }
}
