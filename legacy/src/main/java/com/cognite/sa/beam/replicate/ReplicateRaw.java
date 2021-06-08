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

package com.cognite.sa.beam.replicate;

import com.cognite.beam.io.CogniteIO;
import com.cognite.beam.io.config.*;
import com.cognite.client.dto.RawRow;
import com.cognite.client.dto.RawTable;
import com.cognite.beam.io.RequestParameters;
import com.cognite.beam.io.transform.BreakFusion;
import com.cognite.beam.io.transform.toml.ReadTomlStringArray;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
/**
 * This pipeline reads raw tables from the specified cdp instance and writes them to a target CDF instance.
 *
 * This job is prepared to be deployed as a template on GCP (Dataflow) + can be executed directly on any runner.
 */
public class ReplicateRaw {
    // The log to output status messages to.
    private static final Logger LOG = LoggerFactory.getLogger(ReplicateRaw.class);
    private static final String appIdentifier = "Replicate_Raw";


    /**
     * Filters the main input (String) based on the side input (List<String>)
     */
    private static class FilterInputFn extends DoFn<String, String> {
        private final PCollectionView<List<String>> allowListView;
        private final PCollectionView<List<String>> denyListView;

        FilterInputFn(PCollectionView<List<String>> allowListView,
                      PCollectionView<List<String>> denyListView) {
            this.allowListView = allowListView;
            this.denyListView = denyListView;
        }

        @ProcessElement
        public void processElement(@Element String input,
                                   OutputReceiver<String> outputReceiver,
                                   ProcessContext context) {
            List<String> allowList = context.sideInput(allowListView);
            List<String> denyList = context.sideInput(denyListView);

            // check against deny list entries
            if (!denyList.isEmpty()) {
                if (denyList.contains(input)) {
                    LOG.debug("Deny list match. DB {} will be skipped.", input);
                    return;
                }
            }

            if (allowList.contains("*")) {
                // no filter
                outputReceiver.output(input);
                return;
            }
            if (allowList.contains(input)) {
                LOG.debug("Allow list match. DB {} will be included.", input);
                outputReceiver.output(input);
            }
        }
    }

    /**
     * Custom options for this pipeline.
     */
    public interface ReplicateRawOptions extends PipelineOptions {
        // The options below can be used for file-based secrets handling.
        /*
        @Description("The cdf source config file.The name should be in the format of gs://<bucket>/folder.")
        @Validation.Required
        ValueProvider<String> getCdfInputConfigFile();
        void setCdfInputConfigFile(ValueProvider<String> value);

        @Description("The cdf target config file. The name should be in the format of gs://<bucket>/folder.")
        @Validation.Required
        ValueProvider<String> getCdfOutputConfigFile();
        void setCdfOutputConfigFile(ValueProvider<String> value);
*/
        @Description("The GCP secret holding the source api key. The reference should be <projectId>.<secretId>.")
        @Validation.Required
        ValueProvider<String> getCdfInputSecret();
        void setCdfInputSecret(ValueProvider<String> value);

        @Description("The CDF source host name. The default value is https://api.cognitedata.com.")
        @Default.String("https://api.cognitedata.com")
        ValueProvider<String> getCdfInputHost();
        void setCdfInputHost(ValueProvider<String> value);

        @Description("The GCP secret holding the target api key. The reference should be <projectId>.<secretId>.")
        @Validation.Required
        ValueProvider<String> getCdfOutputSecret();
        void setCdfOutputSecret(ValueProvider<String> value);

        @Description("The CDF target host name. The default value is https://api.cognitedata.com.")
        @Default.String("https://api.cognitedata.com")
        ValueProvider<String> getCdfOutputHost();
        void setCdfOutputHost(ValueProvider<String> value);

        @Description("The job config file. The name should be in the format of gs://<bucket>/folder.")
        @Validation.Required
        ValueProvider<String> getJobConfigFile();
        void setJobConfigFile(ValueProvider<String> value);
    }

    /**
     * Setup the main pipeline structure and run it.
     * @param options
     */
    private static PipelineResult runReplicateRaw(ReplicateRawOptions options) throws Exception {
        /*
        Build the project configuration (CDF tenant and api key) based on:
        - api key from Secret Manager
        - CDF api host
         */
        GcpSecretConfig sourceSecret = GcpSecretConfig.of(
                ValueProvider.NestedValueProvider.of(options.getCdfInputSecret(), secret -> secret.split("\\.")[0]),
                ValueProvider.NestedValueProvider.of(options.getCdfInputSecret(), secret -> secret.split("\\.")[1]));
        ProjectConfig sourceConfig = ProjectConfig.create()
                .withApiKeyFromGcpSecret(sourceSecret)
                .withHost(options.getCdfInputHost());
        GcpSecretConfig targetSecret = GcpSecretConfig.of(
                ValueProvider.NestedValueProvider.of(options.getCdfOutputSecret(), secret -> secret.split("\\.")[0]),
                ValueProvider.NestedValueProvider.of(options.getCdfOutputSecret(), secret -> secret.split("\\.")[1]));
        ProjectConfig targetConfig = ProjectConfig.create()
                .withApiKeyFromGcpSecret(targetSecret)
                .withHost(options.getCdfOutputHost());

        Pipeline p = Pipeline.create(options);

        // Parse dbName allow and deny lists to side input
        PCollectionView<List<String>> denyListDbName = p
                .apply("Read dbName allow list", ReadTomlStringArray.from(options.getJobConfigFile())
                        .withArrayKey("denyList.dbNames"))
                .apply("Log dbName deny", MapElements.into(TypeDescriptors.strings())
                        .via(expression -> {
                            LOG.info("Registered db name: {}", expression);
                            return expression;
                        }))
                .apply("To view", View.asList());

        PCollectionView<List<String>> allowListDbName = p
                .apply("Read dbName allow list", ReadTomlStringArray.from(options.getJobConfigFile())
                        .withArrayKey("allowList.dbNames"))
                .apply("Log dbName allow", MapElements.into(TypeDescriptors.strings())
                        .via(expression -> {
                            LOG.info("Registered db name: {}", expression);
                            return expression;
                        }))
                .apply("To view", View.asList());

        // Read all raw db and table names.
        PCollection<RawTable> rawTables = p
                .apply("Read cdf raw db names", CogniteIO.readRawDatabase()
                        .withProjectConfig(sourceConfig)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)))
                .apply("Filter db names", ParDo.of(new FilterInputFn(allowListDbName, denyListDbName))
                        .withSideInputs(allowListDbName, denyListDbName))
                .apply("Read raw table names", CogniteIO.readAllRawTable()
                        .withProjectConfig(sourceConfig)
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
                                        .withRootParameter("limit", 2000)
                        ))
                .apply("Read cdf raw rows", CogniteIO.readAllRawRow()
                        .withProjectConfig(sourceConfig)
                        .withHints(Hints.create()
                                .withReadShards(4))
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)));

        // Write rows to target CDF
        rows.apply("Write raw rows to target CDF", CogniteIO.writeRawRow()
                .withProjectConfig(targetConfig)
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
