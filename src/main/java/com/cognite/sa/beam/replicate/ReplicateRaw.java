package com.cognite.sa.beam.replicate;

import com.cognite.beam.io.CogniteIO;
import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.config.WriterConfig;
import com.cognite.beam.io.dto.RawRow;
import com.cognite.beam.io.dto.RawTable;
import com.cognite.beam.io.servicesV1.RequestParameters;
import com.cognite.beam.io.transform.BreakFusion;
import com.cognite.beam.io.transform.toml.ReadTomlFile;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tomlj.*;

import java.util.ArrayList;
import java.util.List;
/**
 * This pipeline reads all raw tables from the specified cdp instance and writes them to a target BigQuery table.
 *
 * The job is designed as a batch job which will trucate and write to BQ. That is, it will do a full update
 * with each execution.
 *
 * This job is prepared to be deployed as a template on GCP (Dataflow) + can be executed directly on any runner.
 */
public class ReplicateRaw {
    // The log to output status messages to.
    private static final Logger LOG = LoggerFactory.getLogger(ReplicateRaw.class);

    /**
     * Parses the toml configuration entry and extracts the dbNames whitelist entry.
     */
    private static class ParseDbNameWhitelistFn extends DoFn<String, List<String>> {
        @ProcessElement
        public void processElement(@Element String tomlString,
                                   OutputReceiver<List<String>> outputReceiver) throws Exception {
            List<String> outputList = new ArrayList<>(20);
            LOG.debug("Received TOML string. Size: {}", tomlString.length());
            LOG.debug("Parsing TOML string");
            TomlParseResult parseResult = Toml.parse(tomlString);
            LOG.debug("Finish parsing toml string");

            if (!parseResult.isArray("whitelist.dbName")) {
                LOG.warn("dbName is not defined as an array: {}", parseResult.toString());
            }

            TomlArray dbNameArray = parseResult.getArrayOrEmpty("whitelist.dbName");
            if (dbNameArray.isEmpty()) {
                LOG.warn("Cannot find a dbName array under the whitelist section: {}", parseResult.toString());
                outputReceiver.output(outputList);
                return;
            }

            if (dbNameArray.containsStrings()) {
                for (int i = 0; i < dbNameArray.size(); i++) {
                    outputList.add(dbNameArray.getString(i));
                }
                outputReceiver.output(outputList);
            } else {
                LOG.warn("dbName config entry does not contain string values: {}", dbNameArray.toString());
                outputReceiver.output(outputList);
            }
        }
    }

    /**
     * Filters the main input (String) based on the side input (List<String>)
     */
    private static class FilterInputFn extends DoFn<String, String> {
        private final PCollectionView<List<String>> stringListView;

        FilterInputFn(PCollectionView<List<String>> pCollectionView) {
            stringListView = pCollectionView;
        }

        @ProcessElement
        public void processElement(@Element String input,
                                   OutputReceiver<String> outputReceiver,
                                   ProcessContext context) {
            List<String> whitelist = context.sideInput(stringListView);
            if (whitelist.contains("*")) {
                // no filter
                outputReceiver.output(input);
                return;
            }
            if (whitelist.contains(input)) {
                outputReceiver.output(input);
            }
        }
    }

    /**
     * Custom options for this pipeline.
     */
    public interface ReplicateRawOptions extends PipelineOptions {
        @Description("The cdf source config file."
                + "The name should be in the format of "
                + "gs://<bucket>/folder.")
        @Validation.Required
        ValueProvider<String> getCdfInputConfigFile();
        void setCdfInputConfigFile(ValueProvider<String> value);

        @Description("The cdf target config file."
                + "The name should be in the format of "
                + "gs://<bucket>/folder.")
        @Validation.Required
        ValueProvider<String> getCdfOutputConfigFile();
        void setCdfOutputConfigFile(ValueProvider<String> value);

        @Description("The job config file."
                + "The name should be in the format of "
                + "gs://<bucket>/folder.")
        @Validation.Required
        ValueProvider<String> getJobConfigFile();
        void setJobConfigFile(ValueProvider<String> value);
    }

    /**
     * Setup the main pipeline structure and run it.
     * @param options
     */
    private static PipelineResult runReplicateRaw(ReplicateRawOptions options) throws Exception {
        String appIdentifier = "Replicate_Raw";
        Pipeline p = Pipeline.create(options);

        // Read the job config file
        PCollection<String> jobConfig = p
                .apply("Read job config file", ReadTomlFile.from(options.getJobConfigFile()))
                .apply("Remove key", Values.create());

        // Parse dbName whitelist to side input
        PCollectionView<List<String>> dbNameWhitelistView = jobConfig
                .apply("Extract dbName whitelist", ParDo.of(new ParseDbNameWhitelistFn()))
                .apply("To singleton view", View.asSingleton());

        // Read all raw db and table names.
        PCollection<RawTable> rawTables = p
                .apply("Read cdf raw db names", CogniteIO.readRawDatabase()
                        .withProjectConfigFile(options.getCdfInputConfigFile())
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)))
                .apply("Filter db names", ParDo.of(new FilterInputFn(dbNameWhitelistView))
                        .withSideInputs(dbNameWhitelistView))
                .apply("Read raw table names", CogniteIO.readAllRawTable()
                        .withProjectConfigFile(options.getCdfInputConfigFile())
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)))
                .apply("Break fusion", BreakFusion.<RawTable>create());

        // Read all rows
        PCollection<RawRow> rows = rawTables
                .apply("Map to read row requests", MapElements
                        .into(TypeDescriptor.of(RequestParameters.class))
                        .via((RawTable input) ->
                                RequestParameters.create()
                                        .withDbName(input.getDbName())
                                        .withTableName(input.getTableName())
                                        .withRootParameter("limit", 5000)
                        ))
                .apply("Read cdf raw rows", CogniteIO.readAllRawRow()
                        .withProjectConfigFile(options.getCdfInputConfigFile())
                        .withHints(Hints.create()
                                .withReadShards(4))
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)));

        // Write rows to target CDF
        rows.apply("Write raw rows to target CDF", CogniteIO.writeRawRow()
                .withProjectConfigFile(options.getCdfOutputConfigFile())
                .withHints(Hints.create()
                        .withWriteShards(10))
                .withWriterConfig(WriterConfig.create()
                        .withAppIdentifier(appIdentifier)));

        return p.run();
    }

    /**
     * Read the pipeline options from args and run the pipeline.
     * @param args
     */
    public static void main(String[] args) throws Exception {
        ReplicateRawOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(ReplicateRawOptions.class);
        runReplicateRaw(options);
    }
}
