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
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.client.dto.TimeseriesMetadata;
import com.cognite.client.dto.TimeseriesPoint;
import com.cognite.beam.io.RequestParameters;
import com.cognite.beam.io.transform.BreakFusion;
import com.cognite.beam.io.transform.GroupIntoBatches;
import com.google.api.services.bigquery.model.*;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * This pipeline reads TS data points along a rolling time window from the specified cdp instance and writes
 * them to a target BigQuery table.
 *
 * The job is designed as a batch job which will trucate and write to BQ. That is, it will do a full update
 * with each execution.
 *
 * This job is prepared to be deployed as a template on GCP (Dataflow) + can be executed directly on any runner.
 */
public class CdfTsPointsBQ {
    // The log to output status messages to.
    private static final Logger LOG = LoggerFactory.getLogger(CdfTsPointsBQ.class);

    /* BQ output schema */
    private static final TableSchema tsPointSchemaBQ = new TableSchema().setFields(ImmutableList.of(
            new TableFieldSchema().setName("id").setType("INT64"),
            new TableFieldSchema().setName("external_id").setType("STRING"),
            new TableFieldSchema().setName("timestamp").setType("TIMESTAMP"),
            new TableFieldSchema().setName("value").setType("FLOAT64"),
            new TableFieldSchema().setName("value_string").setType("STRING"),
            new TableFieldSchema().setName("row_updated_time").setType("TIMESTAMP")
    ));


    /**
     * Custom options for this pipeline.
     */
    public interface CdfTsPointsBqOptions extends PipelineOptions {
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
    private static PipelineResult runCdfTsPointsBQ(CdfTsPointsBqOptions options) {
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

        // Read ts headers.
        PCollection<TimeseriesMetadata> tsHeaders = p
                .apply("Read cdf TS headers", CogniteIO.readTimeseriesMetadata()
                        .withProjectConfig(projectConfig)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("CdfTsPointsBQ"))
                ).apply("Filter out TS w/ security categories", Filter.by(
                        tsHeader -> tsHeader.getSecurityCategoriesList().isEmpty()
                                && !tsHeader.getName().startsWith("SRE-cognite-sre-Timeseries")
                ));

        // Read ts points for all headers
        PCollection<TimeseriesPoint> tsPoints = tsHeaders
                .apply("Add key", WithKeys.of(ThreadLocalRandom.current().nextInt(20)))
                .apply("Batch TS items", GroupIntoBatches.<Integer, TimeseriesMetadata>of(
                        KvCoder.of(VarIntCoder.of(), ProtoCoder.of(TimeseriesMetadata.class)))
                        .withMaxBatchSize(20))
                .apply("Remove key", Values.create())
                .apply("Break fusion", BreakFusion.create())
                .apply("Build ts points request", ParDo.of(new DoFn<Iterable<TimeseriesMetadata>, RequestParameters>() {
                    @ProcessElement
                    public void processElement(@Element Iterable<TimeseriesMetadata> input,
                                               OutputReceiver<RequestParameters> out,
                                               ProcessContext context) {
                        List<Map<String, Object>> items = new ArrayList<>();
                        for (TimeseriesMetadata ts : input) {
                            items.add(ImmutableMap.of("id", ts.getId()));
                        }

                        // set time window to the past 1 - 2 hours
                        Instant fromTime = Instant.now().truncatedTo(ChronoUnit.HOURS)
                                .minus(1, ChronoUnit.HOURS)
                                .minus(10, ChronoUnit.MINUTES);

                        Instant toTime = Instant.now();


                        out.output(RequestParameters.create()
                                .withItems(items)
                                .withRootParameter("start", fromTime.toEpochMilli())
                                .withRootParameter("end", toTime.toEpochMilli())
                                .withRootParameter("limit", 100000));
                    }
                }))
                .apply("Read ts points", CogniteIO.readAllTimeseriesPoints()
                        .withProjectConfig(projectConfig)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("CdfTsPointsBQ")
                                //.enableDeltaRead("system.bq-delta")
                                //.withDeltaIdentifier("ts-points")
                                //.withDeltaOffset(Duration.ofMinutes(2))
                                //.withFullReadOverride(options.getFullRead())
                        ));

        // Write to BQ
        tsPoints.apply("Write output to BQ", BigQueryIO.<TimeseriesPoint>write()
                .to(options.getOutputMainTable())
                .withSchema(tsPointSchemaBQ)
                .withFormatFunction((TimeseriesPoint element) -> {
                    DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;
                    return new TableRow()
                            .set("id", element.getId())
                            .set("external_id", element.hasExternalId() ? element.getExternalId() : null)
                            .set("timestamp", formatter.format(Instant.ofEpochMilli(element.getTimestamp())))
                            .set("value", element.getDatapointTypeCase() == TimeseriesPoint.DatapointTypeCase.VALUE_NUM ?
                                    element.getValueNum() : null)
                            .set("value_string", element.getDatapointTypeCase() == TimeseriesPoint.DatapointTypeCase.VALUE_STRING ?
                                    element.getValueString() : null)
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
        CdfTsPointsBqOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(CdfTsPointsBqOptions.class);
        runCdfTsPointsBQ(options);
    }
}
