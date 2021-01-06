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
import com.cognite.client.config.UpsertMode;
import com.cognite.beam.io.dto.*;
import com.cognite.beam.io.RequestParameters;
import com.cognite.beam.io.transform.toml.ReadTomlStringArray;
import com.cognite.beam.io.transform.toml.ReadTomlStringMap;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Int64Value;
import com.google.protobuf.StringValue;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReplicateSequences {
    // The log to output status messages to.
    private static final Logger LOG = LoggerFactory.getLogger(ReplicateSequences.class);

    // Pipeline configuration
    private static final String appIdentifier = "Replicate_Sequences";
    private static final String contextualizationConfigKey = "enableContextualization";
    private static final String dataSetConfigKey = "enableDataSetMapping";
    private static final String sequenceHeaderConfigKey = "sequenceHeaders";
    private static final String sequenceRowsConfigKey = "sequenceRows";

    /**
     * Create target sequence header
     *     - Remove system fields (id, created time, last updated time)
     *     - Translate the asset id links
     *     - If the headers does not have externalId, us the source's id
     *     - Map data set ids
     */
    private static class PrepareSequenceHeader extends DoFn<SequenceMetadata, SequenceMetadata> {
        PCollectionView<Map<String, String>> configMap;
        PCollectionView<Map<Long, String>> sourceAssetsIdMapView;
        PCollectionView<Map<String, Long>> targetAssetsIdMapView;
        PCollectionView<Map<Long, String>> sourceDataSetsIdMapView;
        PCollectionView<Map<String, Long>> targetDataSetsIdMapView;

        final Counter missingExtCounter = Metrics.counter(PrepareSequenceHeader.class,
                "No external id");
        final Counter assetMapCounter = Metrics.counter(PrepareSequenceHeader.class,
                "Map asset ids");
        final Counter dataSetMapCounter = Metrics.counter(PrepareSequenceHeader.class,
                "Map data set");

        PrepareSequenceHeader(PCollectionView<Map<String, String>> configMap,
                              PCollectionView<Map<Long, String>> sourceAssetsIdMapView,
                              PCollectionView<Map<String, Long>> targetAssetsIdMapView,
                              PCollectionView<Map<Long, String>> sourceDataSetsIdMapView,
                              PCollectionView<Map<String, Long>> targetDataSetsIdMapView) {
            this.configMap = configMap;
            this.sourceAssetsIdMapView = sourceAssetsIdMapView;
            this.targetAssetsIdMapView = targetAssetsIdMapView;
            this.sourceDataSetsIdMapView = sourceDataSetsIdMapView;
            this.targetDataSetsIdMapView = targetDataSetsIdMapView;
        }

        @ProcessElement
        public void processElement(@Element SequenceMetadata input,
                                   OutputReceiver<SequenceMetadata> out,
                                   ProcessContext context) {
            Map<Long, String> sourceAssetsIdMap = context.sideInput(sourceAssetsIdMapView);
            Map<String, Long> targetAssetsIdMap = context.sideInput(targetAssetsIdMapView);
            Map<Long, String> sourceDataSetsIdMap = context.sideInput(sourceDataSetsIdMapView);
            Map<String, Long> targetDataSetsIdMap = context.sideInput((targetDataSetsIdMapView));
            Map<String, String> config = context.sideInput(configMap);

            SequenceMetadata.Builder builder = input.toBuilder()
                    .clearAssetId()
                    .clearCreatedTime()
                    .clearLastUpdatedTime()
                    .clearId()
                    .clearDataSetId();

            if (!input.hasExternalId()) {
                builder.setExternalId(StringValue.of(String.valueOf(input.getId().getValue())));
                missingExtCounter.inc();
            }

            // add asset link if enabled and it is available in target
            if (config.getOrDefault(contextualizationConfigKey, "no").equalsIgnoreCase("yes")
                    && input.hasAssetId()) {
                // if the source asset has an externalId use it--if not, use the asset internal id
                String targetAssetExtId = sourceAssetsIdMap.getOrDefault(input.getAssetId().getValue(),
                        String.valueOf(input.getAssetId().getValue()));

                if (targetAssetsIdMap.containsKey(targetAssetExtId)) {
                    builder.setAssetId(Int64Value.of(targetAssetsIdMap.get(targetAssetExtId)));
                    assetMapCounter.inc();
                }
            }

            // map data set if enabled and it is available in the target
            if (config.getOrDefault(dataSetConfigKey, "no").equalsIgnoreCase("yes")
                    && input.hasDataSetId()) {
                String targetDataSetExtId = sourceDataSetsIdMap.getOrDefault(
                        input.getDataSetId().getValue(), String.valueOf(input.getDataSetId().getValue()));

                if (targetDataSetsIdMap.containsKey(targetDataSetExtId)) {
                    builder.setDataSetId(Int64Value.of(targetDataSetsIdMap.get(targetDataSetExtId)));
                    dataSetMapCounter.inc();
                }
            }

            LOG.info("Data set map count: {}", dataSetMapCounter.toString());
            LOG.info("Asset map count: {}", assetMapCounter.toString());

            out.output(builder.build());
        }
    }

    public interface ReplicateSequenceOptions extends PipelineOptions {

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

        /**
         * Specify the job configuration file.
         */
        @Description("The job config file. The name should be in the format of gs://<bucket>/folder/file.toml.")
        @Validation.Required
        ValueProvider<String> getJobConfigFile();
        void setJobConfigFile(ValueProvider<String> value);

        /**
         * Specify delta read override.
         *
         * Set to {@code true} for full reads.
         */
        @Description("Full read flag. Set to true for full read, false for delta read.")
        @Default.Boolean(false)
        ValueProvider<Boolean> getFullRead();
        void setFullRead(ValueProvider<Boolean> value);
    }

    private static PipelineResult runReplicateSequences(ReplicateSequenceOptions options) throws IOException {
        /*
        Build the project configuration (CDF tenant and api key) based on:
        - API key from GCP Secret Manager
        - CDF API host
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
        Config maps are published as views so that they can be used by the transforms as side inputs.
         */

        PCollectionView<Map<String, String>> configMap = p
                .apply("Read config map", ReadTomlStringMap.from(options.getJobConfigFile())
                        .withMapKey("config"))
                .apply("Log config", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                        .via((KV<String, String> config) -> {
                            LOG.info("Config entry: {}", config);
                            return config;
                        })
                )
                .apply("To map view", View.asMap());

        PCollectionView<List<String>> allowListDataSet = p
                .apply("Read config allow list", ReadTomlStringArray.from(options.getJobConfigFile())
                        .withArrayKey("allowList.dataSetExternalId"))
                .apply("Log data set extId", MapElements
                        .into(TypeDescriptors.strings())
                        .via(expression -> {
                            LOG.info("Registered dataSetExternalId: {}", expression);
                            return expression;
                        }))
                .apply("To list view", View.asList());

        PCollectionView<List<String>> sequenceAllowList = p
                .apply("Read sequence allow list", ReadTomlStringArray.from(options.getJobConfigFile())
                        .withArrayKey("allowList.sequenceExternalId"))
                .apply("Log sequence allow", MapElements.into(TypeDescriptors.strings())
                        .via(expression -> {
                            LOG.info("Registered sequence extId allow: {}", expression);
                            return expression;
                        }))
                .apply("To list view", View.asList());

        PCollectionView<List<String>> sequenceDenyList = p
                .apply("Read sequence deny list", ReadTomlStringArray.from(options.getJobConfigFile())
                        .withArrayKey("denyList.sequenceExternalId"))
                .apply("Log ts deny", MapElements.into(TypeDescriptors.strings())
                        .via(expression -> {
                            LOG.info("Registered sequence extId deny: {}", expression);
                            return expression;
                        }))
                .apply("To list view", View.asList());


        /*
        Read the asset hierarchies from source and target. Shave off the metadata and use id + externalId
        as the basis for mapping Files to Assets. Project the asset collections as views so that they can be
        used as side inputs to the main File transform.
         */
        PCollectionView<Map<Long, String>> sourceAssetsIdMap = p
                .apply("Read source assets", CogniteIO.readAssets()
                        .withProjectConfig(sourceConfig)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)
                                .enableMetrics(false)))
                .apply("Extract id + externalId", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.longs(), TypeDescriptors.strings()))
                        .via((Asset asset) -> KV.of(asset.getId().getValue(), asset.getExternalId().getValue())))
                .apply("Max per key", Max.perKey())
                .apply("To map view", View.asMap());

        PCollectionView<Map<String, Long>> targetAssetsExtIdMap = p
                .apply("Read target assets", CogniteIO.readAssets()
                        .withProjectConfig(targetConfig)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)
                                .enableMetrics(false)))
                .apply("Extract externalId + id", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
                        .via((Asset asset) -> KV.of(asset.getExternalId().getValue(), asset.getId().getValue())))
                .apply("Max per key", Max.perKey())
                .apply("To map view", View.asMap());


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
                            LOG.info("Source dataset - id: {}, extId: {}, name: {}",
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
                                .withAppIdentifier(appIdentifier).enableMetrics(false)))
                .apply("Select external id + id", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
                        .via((DataSet dataSet) -> {
                            LOG.info("Target dataset - id: {}, extId: {}, name: {}",
                                    dataSet.getId().getValue(),
                                    dataSet.getExternalId().getValue(),
                                    dataSet.getName().getValue());
                            return KV.of(dataSet.getExternalId().getValue(), dataSet.getId().getValue());
                        }))
                .apply("Max per key", Max.perKey())
                .apply("To map view", View.asMap());

        PCollection<SequenceMetadata> sequenceHeaders = p
                .apply("Build basic query", Create.of(RequestParameters.create()))
                .apply("Add dataset filter", ParDo.of(new DoFn<RequestParameters, RequestParameters>() {
                    @ProcessElement
                    public void processElement(@Element RequestParameters input,
                                               OutputReceiver<RequestParameters> out,
                                               ProcessContext context) {
                        List<String> allowList = context.sideInput(allowListDataSet);
                        List<Map<String, String>> datasetExternalIds = new ArrayList<>();
                        LOG.info("Data set whitelist contains {} entries", allowList.size());

                        //Build the list of data set external id filters
                        for (String extId : allowList) {
                            datasetExternalIds.add(ImmutableMap.of("externalId", extId));
                        }

                        if (datasetExternalIds.isEmpty() || allowList.contains("*")) {
                            LOG.info("Will not filter on data set external id");
                            out.output(input);
                        } else {
                            LOG.info("Add filter on {} data set external ids.", datasetExternalIds.size());
                            out.output(input
                                    .withFilterParameter("dataSetIds", datasetExternalIds));
                        }
                    }
                }).withSideInputs(allowListDataSet))
                .apply("Read CDF Sequence headers", CogniteIO.readAllSequencesMetadata()
                        .withProjectConfig(sourceConfig)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)))
                .apply("Filter sequences", ParDo.of(new DoFn<SequenceMetadata, SequenceMetadata>() {
                    @ProcessElement
                    public void processElement(@Element SequenceMetadata input,
                                               OutputReceiver<SequenceMetadata> out,
                                               ProcessContext context) {
                        List<String> denyList = context.sideInput(sequenceDenyList);
                        List<String> allowList = context.sideInput(sequenceAllowList);

                        // Check for deny list match
                        if (!denyList.isEmpty() && input.hasExternalId()) {
                            if (denyList.contains(input.getExternalId().getValue())) {
                                LOG.debug("Deny list match {}. Sequence will be dropped.", input.getExternalId().getValue());
                                return;
                            }
                        }

                        // Check for allow list match
                        if (allowList.contains("*")) {
                            LOG.info("Allow all sequences");
                            out.output(input);
                            return;
                        }
                        if (allowList.contains(input.getExternalId().getValue())) {
                            LOG.debug("Allow list match {}. Sequence will be included.", input.getExternalId().getValue());
                            out.output(input);
                        }
                    }
                }).withSideInputs(sequenceDenyList, sequenceAllowList));


        /*
        Create target Sequence Header and write:
        - Check if config includes Sequence Headers
        - Remove system fields (id, created date, last updated date)
        - Translate the asset id links
         */
        PCollection<SequenceMetadata> output = sequenceHeaders
                .apply("Include Sequence headers?", ParDo.of(new DoFn<SequenceMetadata, SequenceMetadata>() {
                    @ProcessElement
                    public void processElement(@Element SequenceMetadata input,
                                               OutputReceiver<SequenceMetadata> out,
                                               ProcessContext context) {
                        Map<String, String> config = context.sideInput(configMap);
                        if (config.getOrDefault(sequenceHeaderConfigKey, "no").equalsIgnoreCase("yes")) {
                            out.output(input);
                        }
                    }
                }).withSideInputs(configMap))
                .apply("Process Sequence Headers", ParDo.of(new PrepareSequenceHeader(configMap, sourceAssetsIdMap,
                        targetAssetsExtIdMap, sourceDataSetsIdMap, targetDataSetsExtIdMap))
                        .withSideInputs(configMap, sourceAssetsIdMap, targetAssetsExtIdMap,
                                sourceDataSetsIdMap, targetDataSetsExtIdMap))
                .apply("Write Sequence Headers", CogniteIO.writeSequencesMetadata()
                        .withProjectConfig(targetConfig)
                        .withHints(Hints.create()
                                .withWriteShards(20))
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier(appIdentifier)
                                .withUpsertMode(UpsertMode.REPLACE)));


        /*
        Read Sequence Rows for all headers
        - Batch headers
        - Check if config include rows
        - Build the request to read the rows for each batch of headers.
        - Process all read requests
         */
        PCollection<SequenceBody> sequenceBodies = sequenceHeaders
                .apply("Map to read Sequence rows requests", MapElements
                        .into(TypeDescriptor.of(RequestParameters.class))
                        .via((SequenceMetadata input) -> {
                            int limit = 10000;
                            Map<String, Object> requestParameters = new HashMap<String, Object>();
                            requestParameters.put("id", input.getId().getValue());
                            return RequestParameters.create()
                                    .withRequestParameters(requestParameters)
                                    .withRootParameter("limit", limit);

                        })
                )
                .apply("Read Sequence rows", CogniteIO.readAllSequenceRows()
                        .withProjectConfig(sourceConfig)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)
                        ));

        /*
        Write the Sequence Rows to the CDF target
        - Process each row and convert into a row post object
        - Write the row
         */
        sequenceBodies
                .apply("Wait on: Write Sequence Headers", Wait.on(output))
                .apply("Write Sequence Rows", CogniteIO.writeSequenceRows()
                        .withProjectConfig(targetConfig)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier(appIdentifier)
                                .withUpsertMode(UpsertMode.REPLACE)));

        return p.run();
    }

    public static void main(String[] args) throws IOException {
        ReplicateSequenceOptions options = PipelineOptionsFactory
                .fromArgs(args).withValidation()
                .as(ReplicateSequenceOptions.class);
        runReplicateSequences(options);
    }
}
