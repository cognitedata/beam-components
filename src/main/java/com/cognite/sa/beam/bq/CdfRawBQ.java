package com.cognite.sa.beam.bq;

import avro.shaded.com.google.common.collect.ImmutableList;
import com.cognite.beam.io.CogniteIO;
import com.cognite.beam.io.config.GcpSecretConfig;
import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.dto.RawRow;
import com.cognite.beam.io.dto.RawTable;
import com.cognite.beam.io.servicesV1.RequestParameters;
import com.cognite.beam.io.transform.BreakFusion;
import com.google.api.services.bigquery.model.*;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This pipeline reads all raw tables from the specified cdp instance and writes them to a target BigQuery table.
 *
 * The job is designed as a batch job which will truncate and write to BQ. It has built in delta read support
 * and can be configured (via a parameter) to do delta or full read.
 *
 * This job is prepared to be deployed as a template on GCP (Dataflow) + can be executed directly on any runner.
 */
public class CdfRawBQ {
    // The log to output status messages to.
    private static final Logger LOG = LoggerFactory.getLogger(CdfRawBQ.class);
    private static final String appIdentifier = "CdfRawBQ";

    /* BQ output schema */
    private static final TableSchema rawSchemaBQ = new TableSchema().setFields(ImmutableList.of(
            new TableFieldSchema().setName("db_name").setType("STRING"),
            new TableFieldSchema().setName("table_name").setType("STRING"),
            new TableFieldSchema().setName("row_key").setType("STRING"),
            new TableFieldSchema().setName("last_updated_time").setType("TIMESTAMP"),
            new TableFieldSchema().setName("columns").setType("RECORD").setMode("REPEATED").setFields(ImmutableList.of(
                    new TableFieldSchema().setName("key").setType("STRING"),
                    new TableFieldSchema().setName("value").setType("STRING")
            )),
            new TableFieldSchema().setName("row_updated_time").setType("TIMESTAMP")
    ));

    /**
     * Custom options for this pipeline.
     */
    public interface CdfRawBqOptions extends PipelineOptions {
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
        //@Default.Boolean(false)
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
    private static PipelineResult runCdfRawBQ(CdfRawBqOptions options) {
        GcpSecretConfig secretConfig = GcpSecretConfig.of(
                ValueProvider.NestedValueProvider.of(options.getCdfSecret(), secret -> secret.split("\\.")[0]),
                ValueProvider.NestedValueProvider.of(options.getCdfSecret(), secret -> secret.split("\\.")[1]));
        ProjectConfig projectConfig = ProjectConfig.create()
                .withApiKeyFromGcpSecret(secretConfig)
                .withHost(options.getCdfHost());

        Pipeline p = Pipeline.create(options);

        // Read all raw db and table names.
        PCollection<RawTable> rawTables = p
                .apply("Read cdf raw db names", CogniteIO.readRawDatabase()
                        .withProjectConfig(projectConfig)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)))
                .apply("Read raw table names", CogniteIO.readAllRawTable()
                        .withProjectConfig(projectConfig)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)))
                .apply("Break fusion", BreakFusion.<RawTable>create());

        // Read all rows
        PCollection<RawRow> rows = rawTables
                .apply("Map to read row requests", MapElements
                        .into(TypeDescriptor.of(RequestParameters.class))
                        .via((RawTable input) -> {
                            int limit = 5000;
                            if (input.getDbName().equalsIgnoreCase("EDM")
                                    && input.getTableName().equalsIgnoreCase("CD_ATTACHMENT")) {
                                limit = 500;
                            }

                            return RequestParameters.create()
                                    .withDbName(input.getDbName())
                                    .withTableName(input.getTableName())
                                    .withRootParameter("limit", limit)
                                    .withRootParameter("maxLastUpdatedTime", System.currentTimeMillis());
                        }
                        ))
                .apply("Read cdf raw rows", CogniteIO.readAllRawRow()
                        .withProjectConfig(projectConfig)
                        .withHints(Hints.create()
                                .withReadShards(20))
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)
                                .enableDeltaRead("system.bq-delta")
                                .withDeltaIdentifier("raw")
                                .withDeltaOffset(Duration.ofMinutes(1))
                                .withFullReadOverride(options.getFullRead())));

        // Write rows to BQ
        rows.apply("Write output to BQ", BigQueryIO.<RawRow>write()
                .to(options.getOutputMainTable())
                .withSchema(rawSchemaBQ)
                .withFormatFunction((RawRow element) -> {
                    List<TableRow> columns = new ArrayList<>();
                    DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;

                    try {
                        if (element.hasColumns()) {
                            for (Map.Entry<String, Value> mElement : element.getColumns().getFieldsMap().entrySet()) {
                                TableRow tempColumn = new TableRow();
                                String tempValue = "";
                                if (mElement.getValue().getKindCase() == Value.KindCase.STRING_VALUE) {
                                    tempValue = mElement.getValue().getStringValue();
                                } else {
                                    tempValue = JsonFormat.printer().print(mElement.getValue());
                                }

                                tempColumn.set("key", mElement.getKey());
                                tempColumn.set("value", tempValue);
                                columns.add(tempColumn);
                            }
                        }
                    } catch (Exception e) {
                        LOG.error("Error when parsing row from Raw", e);
                        throw new RuntimeException(e);
                    }

                    return new TableRow()
                            .set("db_name", element.getDbName())
                            .set("table_name", element.getTableName())
                            .set("row_key", element.getKey())
                            .set("last_updated_time", formatter.format(Instant.ofEpochMilli(
                                    element.getLastUpdatedTime().getValue())))
                            .set("columns", columns)
                            .set("row_updated_time", formatter.format(Instant.now()));
                })
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                .withTimePartitioning(new TimePartitioning().setField("last_updated_time"))
                .withClustering(new Clustering().setFields(ImmutableList.of("db_name", "table_name")))
                .optimizedWrites()
                .withCustomGcsTempLocation(options.getBqTempStorage()));

        return p.run();
    }

    /**
     * Read the pipeline options from args and run the pipeline.
     * @param args
     */
    public static void main(String[] args) throws Exception {
        CdfRawBqOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(CdfRawBqOptions.class);
        runCdfRawBQ(options);
    }
}
