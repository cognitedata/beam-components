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
import com.cognite.client.dto.Asset;
import com.cognite.client.dto.DataSet;
import com.cognite.client.dto.FileMetadata;
import com.cognite.beam.io.RequestParameters;
import com.cognite.beam.io.transform.toml.ReadTomlStringArray;
import com.cognite.beam.io.transform.toml.ReadTomlStringMap;
import com.cognite.client.config.UpsertMode;
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
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ReplicateFilesMetadata {
    // The log to output status messages to.
    private static final Logger LOG = LoggerFactory.getLogger(ReplicateFilesMetadata.class);
    private static final String appIdentifier = "Replicate_Files";
    private static final String contextualizationConfigKey = "enableContextualization";
    private static final String dataSetConfigKey = "enableDataSetMapping";

    private static class PrepareFilesMetadata extends DoFn<FileMetadata, FileMetadata> {
        PCollectionView<Map<String, String>> configMap;
        PCollectionView<Map<Long, String>> sourceAssetsIdMapView;
        PCollectionView<Map<String, Long>> targetAssetsExtIdMapView;
        PCollectionView<Map<Long, String>> sourceDataSetsIdMapView;
        PCollectionView<Map<String, Long>> targetDataSetsExtIdMapView;

        final Counter missingExtIdCounter = Metrics.counter(PrepareFilesMetadata.class,
                "No external id");
        final Counter assetMapCounter = Metrics.counter(PrepareFilesMetadata.class,
                "Asset ids mapped");
        final Counter noAssetMapCounter = Metrics.counter(PrepareFilesMetadata.class,
                "Asset ids not mapped");
        final Counter dataSetMapCounter = Metrics.counter(PrepareFilesMetadata.class,
                "Data set mapped");

        public PrepareFilesMetadata(PCollectionView<Map<String, String>> configMap,
                            PCollectionView<Map<Long, String>> sourceAssetsIdMapView,
                            PCollectionView<Map<String, Long>> targetAssetsExtIdMapView,
                            PCollectionView<Map<Long, String>> sourceDataSetsIdMapView,
                            PCollectionView<Map<String, Long>> targetDataSetsExtIdMapView) {
            this.configMap = configMap;
            this.sourceAssetsIdMapView = sourceAssetsIdMapView;
            this.targetAssetsExtIdMapView = targetAssetsExtIdMapView;
            this.sourceDataSetsIdMapView = sourceDataSetsIdMapView;
            this.targetDataSetsExtIdMapView = targetDataSetsExtIdMapView;
        }

        @ProcessElement
        public void processElement(@Element FileMetadata input,
                                   OutputReceiver<FileMetadata> out,
                                   ProcessContext context) {
            Map<String, String> config = context.sideInput(configMap);
            Map<Long, String> sourceAssetsIdMap = context.sideInput(sourceAssetsIdMapView);
            Map<String, Long> targetAssetsExtIdMap = context.sideInput(targetAssetsExtIdMapView);
            Map<Long, String> sourceDataSetsIdMap = context.sideInput(sourceDataSetsIdMapView);
            Map<String, Long> targetDataSetsExtIdMap = context.sideInput(targetDataSetsExtIdMapView);

            FileMetadata.Builder fileMetadataBuilder = input.toBuilder()
                    .clearId()
                    .clearAssetIds()
                    .clearCreatedTime()
                    .clearLastUpdatedTime()
                    .clearDataSetId();

            if (!fileMetadataBuilder.hasExternalId()) {
                fileMetadataBuilder.setExternalId(StringValue.of(String.valueOf(fileMetadataBuilder.getId().getValue())));
                missingExtIdCounter.inc();
            }

            // Add asset link if enabled and it is available on the target
            if (config.getOrDefault(contextualizationConfigKey, "no").equalsIgnoreCase("yes")
                    && fileMetadataBuilder.getAssetIdsCount() > 0) {
                for (Long assetId : fileMetadataBuilder.getAssetIdsList()) {
                    // If the source asset has an externalId, use it -- if not, use the asset internal id
                    String targetAssetExtId = sourceAssetsIdMap.getOrDefault(assetId, String.valueOf(assetId));

                    if (targetAssetsExtIdMap.containsKey(targetAssetExtId)) {
                        fileMetadataBuilder.addAssetIds(targetAssetsExtIdMap.get(targetAssetExtId));
                        assetMapCounter.inc();
                    } else {
                        noAssetMapCounter.inc();
                        LOG.warn("Could not map asset linke for source asset externalId = [{}]",
                                targetAssetExtId);
                    }
                }
            }

            // Map data set if enabled and it is available on the target
            if(config.getOrDefault(dataSetConfigKey, "no").equalsIgnoreCase("yes")
                    && fileMetadataBuilder.hasDataSetId()) {
                String targetDataSetExtId = sourceDataSetsIdMap.getOrDefault(
                        fileMetadataBuilder.getDataSetId().getValue(),
                        String.valueOf(fileMetadataBuilder.getDataSetId().getValue()));

                if (targetDataSetsExtIdMap.containsKey(targetDataSetExtId)) {
                    fileMetadataBuilder.setDataSetId(Int64Value.of(targetDataSetsExtIdMap.get(targetDataSetExtId)));
                    dataSetMapCounter.inc();
                }
            }

            out.output(fileMetadataBuilder.build());
        }
    }

    public interface ReplicateFilesMetadataOptions extends PipelineOptions {

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

        @Description("The source delta read identifier. The default value is 'files-metadata-replicator'. Use a descriptive identifier.")
        @Default.String("files-metadata-replicator")
        ValueProvider<String> getDeltaIdentifier();
        void setDeltaIdentifier(ValueProvider<String> value);
    }

    private static PipelineResult runReplicateFiles(ReplicateFilesMetadataOptions options) throws IOException {
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

        /*
        Read files from source and parse them:
        - Filter on data set external id
        - Remove system fields (id, created date, last updated date
        - Translate the asset id links

        Then write the prepared files to target.
         */
        p
                .apply("Build basic query", Create.of(RequestParameters.create()))
                .apply("Add dataset and metadata filters", ParDo.of(new DoFn<RequestParameters, RequestParameters>() {
                    @ProcessElement
                    public void processElement(@Element RequestParameters input,
                                               OutputReceiver<RequestParameters> out,
                                               ProcessContext context) {
                        List<String> allowList = context.sideInput(allowListDataSet);
                        List<Map<String, String>> dataSetExternalIds = new ArrayList<>();
                        LOG.info("Data set whitelist contains {} entries", allowList.size());

                        // Build the list of data set external id filters
                        for (String extId : allowList) {
                            dataSetExternalIds.add(ImmutableMap.of("externalId", extId));
                        }

                        // Add filter for max lastUpdatedTime
                        RequestParameters req = input
                                .withFilterParameter("lastUpdatedTime", ImmutableMap.of(
                                        "max", System.currentTimeMillis()));

                        if (dataSetExternalIds.isEmpty() || allowList.contains("*")) {
                            LOG.info("Will not filter on data set external id ");
                            out.output(req);
                        } else {
                            LOG.info("Add filter on {} data set external ids.", dataSetExternalIds.size());
                            out.output(req.withFilterParameter("dataSetIds", dataSetExternalIds));
                        }
                    }
                }).withSideInputs(allowListDataSet))
                .apply("Read source files metadata", CogniteIO.readAllFilesMetadata()
                        .withProjectConfig(sourceConfig)
                        .withHints(Hints.create())
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)
                                .withDeltaIdentifier(options.getDeltaIdentifier())
                                .withDeltaOffset(Duration.ofMinutes(30))
                                .withFullReadOverride(options.getFullRead())))
                .apply("Process files metadata", ParDo.of(new PrepareFilesMetadata(configMap,
                        sourceAssetsIdMap,
                        targetAssetsExtIdMap,
                        sourceDataSetsIdMap,
                        targetDataSetsExtIdMap
                ))
                        .withSideInputs(configMap,
                                sourceAssetsIdMap,
                                targetAssetsExtIdMap,
                                sourceDataSetsIdMap,
                                targetDataSetsExtIdMap))
                .apply("Write target files metadata", CogniteIO.writeFilesMetadata()
                        .withProjectConfig(targetConfig)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier(appIdentifier)
                                .withUpsertMode(UpsertMode.REPLACE)));

        return p.run();
    }

    public static void main(String[] args) throws IOException {
        ReplicateFilesMetadataOptions options = PipelineOptionsFactory
                .fromArgs(args).withValidation()
                .as(ReplicateFilesMetadataOptions.class);
        runReplicateFiles(options);
    }
}
