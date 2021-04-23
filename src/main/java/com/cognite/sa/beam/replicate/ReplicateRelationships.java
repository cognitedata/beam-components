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
import com.cognite.beam.io.RequestParameters;
import com.cognite.beam.io.config.*;
import com.cognite.beam.io.transform.toml.ReadTomlStringArray;
import com.cognite.beam.io.transform.toml.ReadTomlStringMap;
import com.cognite.client.config.UpsertMode;
import com.cognite.client.dto.DataSet;
import com.cognite.client.dto.Relationship;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Int64Value;
import com.google.protobuf.StringValue;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This pipeline reads relationships from the specified cdf instance and writes them to a target cdf.
 *
 * The relationships can be linked to resources at the target based on externalId mapping from the source.
 *
 * This job is prepared to be deployed as a template on GCP (Dataflow) + can be executed directly on any runner.
 */
public class ReplicateRelationships {
    // The log to output status messages to.
    private static final Logger LOG = LoggerFactory.getLogger(ReplicateRelationships.class);
    private static final String appIdentifier = "Replicate_Relationshisp";
    private static final String dataSetConfigKey = "enableDataSetMapping";

    /**
     * Create target event:
     *         - Remove system fields (id, created date, last updated date)
     *         - Translate the asset id links
     *         - Map data set ids
     */
    private static class PrepareRelationships extends DoFn<Relationship, Relationship> {
        PCollectionView<Map<String, String>> configMap;
        PCollectionView<Map<Long, String>> sourceDataSetsIdMapView;
        PCollectionView<Map<String, Long>> targetDataSetsExtIdMapView;

        final Counter dataSetMapCounter = Metrics.counter(ReplicateRelationships.PrepareRelationships.class,
                "Map data set");

        PrepareRelationships(PCollectionView<Map<String, String>> configMap,
                             PCollectionView<Map<Long, String>> sourceDataSetsIdMap,
                             PCollectionView<Map<String, Long>> targetDataSetsExtIdMap) {
            this.configMap = configMap;
            this.sourceDataSetsIdMapView = sourceDataSetsIdMap;
            this.targetDataSetsExtIdMapView = targetDataSetsExtIdMap;
        }

        @ProcessElement
        public void processElement(@Element Relationship input,
                                   OutputReceiver<Relationship> out,
                                   ProcessContext context) {
            Map<Long, String> sourceDataSetsIdMap = context.sideInput(sourceDataSetsIdMapView);
            Map<String, Long> targetDataSetsExtIdMap = context.sideInput(targetDataSetsExtIdMapView);
            Map<String, String> config = context.sideInput(configMap);

            Relationship.Builder builder = input.toBuilder()
                    .clearCreatedTime()
                    .clearLastUpdatedTime()
                    .clearDataSetId();

            // Map data set if enabled and it is available in the target
            if (config.getOrDefault(dataSetConfigKey, "no").equalsIgnoreCase("yes")
                    && input.hasDataSetId()) {
                String targetDataSetExtId = sourceDataSetsIdMap.getOrDefault(
                        input.getDataSetId().getValue(), String.valueOf(input.getDataSetId().getValue()));
                if (targetDataSetsExtIdMap.containsKey(targetDataSetExtId)) {
                    builder.setDataSetId(Int64Value.of(targetDataSetsExtIdMap.get(targetDataSetExtId)));
                    dataSetMapCounter.inc();
                }
            }

            out.output(builder.build());
        }
    }

    /**
     * Custom options for this pipeline.
     */
    public interface ReplicateRelationshipsOptions extends PipelineOptions {
        @Description("The GCP secret holding the source api key. The reference should be <projectId>.<secretId>")
        @Validation.Required
        ValueProvider<String> getCdfInputSecret();
        void setCdfInputSecret(ValueProvider<String> value);

        @Description("The CDF source host name. The default value is https://api.cognitedata.com")
        @Default.String("https://api.cognitedata.com")
        ValueProvider<String> getCdfInputHost();
        void setCdfInputHost(ValueProvider<String> value);

        @Description("The GCP secret holding the target api key. The reference should be <projectId>.<secretId>")
        @Validation.Required
        ValueProvider<String> getCdfOutputSecret();
        void setCdfOutputSecret(ValueProvider<String> value);

        @Description("The CDF source host name. The default value is https://api.cognitedata.com")
        @Default.String("https://api.cognitedata.com")
        ValueProvider<String> getCdfOutputHost();
        void setCdfOutputHost(ValueProvider<String> value);

        /**
         * Specify the job configuration file.
         */
        @Description("The job config file. The name should be in the format of gs://<bucket>/folder.")
        @Validation.Required
        ValueProvider<String> getJobConfigFile();
        void setJobConfigFile(ValueProvider<String> value);
    }

    /**
     * Setup the main pipeline structure and run it:
     * - Read the config settings
     * - Read the datasets from both source and target (in order to map the relationships to the proper dataset)
     * - Replicate the relationships
     *
     * @param options
     * @return
     * @throws IOException
     */
    private static PipelineResult runReplicateRelationships(ReplicateRelationshipsOptions options) throws IOException {
        /*
        Build the project confiduration (CDF tenant and api key) based on:
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

        /*
        Read the job config file and parse into views.
        Config maps are published as views so they can be used by the transforms as side inputs.
         */
        PCollectionView<Map<String, String>> configMap = p
                .apply("Read confgi map", ReadTomlStringMap.from(options.getJobConfigFile())
                        .withMapKey("config"))
                .apply("Log config", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                        .via((KV<String, String> config) -> {
                            LOG.info("Config entry: {}", config);
                            return config;
                        }))
                .apply("To map view", View.asMap());

        PCollectionView<List<String>> allowListDataSet = p
                .apply("Read config allow list", ReadTomlStringArray.from(options.getJobConfigFile())
                        .withArrayKey("allowList.dataSetExternalId"))
                .apply("Log data set extId", MapElements.into(TypeDescriptors.strings())
                        .via(extId -> {
                            LOG.info("Registered dataSetExternalId: {}", extId);
                            return extId;
                        }))
                .apply("To view", View.asList());

        /*
        Read the data sets from source and target. Will use these to map items from source data set to
        a target data set.
         */
        PCollectionView<Map<Long, String>> sourceDataSetsIdMap = p
                .apply("Read source data sets", CogniteIO.readDataSets()
                        .withProjectConfig(sourceConfig)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)
                                .enableMetrics(false)))
                .apply("Select id + externalId", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.longs(), TypeDescriptors.strings()))
                        .via((DataSet dataSet) -> {
                            LOG.info("Source data set - id: {}, extId: {}, name: {}",
                                    dataSet.getId().getValue(),
                                    dataSet.getExternalId().getValue(),
                                    dataSet.getName().getValue());
                            return KV.of(dataSet.getId().getValue(), dataSet.getExternalId().getValue());
                        }))
                .apply("Max per key", Max.perKey())
                .apply("To map view", View.asMap());

        PCollectionView<Map<String, Long>> targetDataSetsExtIdMap = p
                .apply("Read target data sets", CogniteIO.readDataSets()
                        .withProjectConfig(targetConfig)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)
                                .enableMetrics(false)))
                .apply("Select externalId + id", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
                        .via((DataSet dataSet) -> {
                            LOG.info("Target data set - id: {}, extId: {}, name: {}",
                                    dataSet.getId().getValue(),
                                    dataSet.getExternalId().getValue(),
                                    dataSet.getName().getValue());
                            return KV.of(dataSet.getExternalId().getValue(), dataSet.getId().getValue());
                        }))
                .apply("Max per key", Max.perKey())
                .apply("To map view", View.asMap());

        /*
        Read relationships from source and parse them:
        - Filter on data set external id
        - Remove system fields (created data, last updated time)
        - Translate data set id link

        Write the prepared events to target
         */
        PCollection<Relationship> relationshipPCollection = p
                .apply("Build basic query", Create.of(RequestParameters.create()))
                .apply("Add data set filter", ParDo.of(new DoFn<RequestParameters, RequestParameters>() {
                    @ProcessElement
                    public void processElement(@Element RequestParameters input,
                                               OutputReceiver<RequestParameters> out,
                                               ProcessContext context) {
                        List<String> allowList = context.sideInput(allowListDataSet);
                        List<Map<String, String>> datasetExternalIds = new ArrayList<>();
                        LOG.info("Data set whiteList contains {} entries", allowList.size());

                        // Build the list of data set external id filters
                        for (String extId : allowList) {
                            datasetExternalIds.add(ImmutableMap.of("externalId", extId));
                        }

                        if (datasetExternalIds.isEmpty() || allowList.contains("*")) {
                            LOG.info("Will not filter data set external id");
                            out.output(input);
                        } else {
                            LOG.info("Add filter on {} data set external ids.", datasetExternalIds.size());
                            out.output(input
                                    .withFilterParameter("dataSetIds", datasetExternalIds));
                        }
                    }
                }).withSideInputs(allowListDataSet))
                .apply("Read source relationships", CogniteIO.readAllRelationships()
                        .withProjectConfig(sourceConfig)
                        .withHints(Hints.create())
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)))
                .apply("Process relationships", ParDo.of(new PrepareRelationships(
                        configMap, sourceDataSetsIdMap, targetDataSetsExtIdMap))
                        .withSideInputs(configMap, sourceDataSetsIdMap, targetDataSetsExtIdMap))
                .apply("Write target relationships", CogniteIO.writeRelationships()
                        .withProjectConfig(targetConfig)
                        .withHints(Hints.create()
                                .withWriteShards(20))
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier(appIdentifier)
                                .withUpsertMode(UpsertMode.REPLACE)));


        return p.run();
    }

    /**
     * Read the pipeline options from args and run the pipeline.
     * @param args
     */
    public static void main(String[] args) throws IOException {
        ReplicateRelationshipsOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(ReplicateRelationshipsOptions.class);
        runReplicateRelationships(options);
    }
}
