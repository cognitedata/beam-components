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
import com.cognite.client.dto.*;
import com.cognite.beam.io.RequestParameters;
import com.cognite.beam.io.transform.toml.ReadTomlStringArray;
import com.cognite.beam.io.transform.toml.ReadTomlStringMap;
import com.cognite.client.config.UpsertMode;
import com.google.common.collect.ImmutableMap;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ReplicateFiles {
    // The log to output status messages to.
    private static final Logger LOG = LoggerFactory.getLogger(ReplicateFiles.class);
    private static final String appIdentifier = "Replicate_Files";
    private static final String contextualizationConfigKey = "enableContextualization";
    private static final String dataSetConfigKey = "enableDataSetMapping";
    private static final String labelsConfigKey = "ignoreInvalidLabels";
    private static final String forwardSlashSubString = "/";
    private static final String backSlashSubString = "\\";

    private static class PrepareFiles extends DoFn<List<FileContainer>, Iterable<FileContainer>> {
        PCollectionView<Map<String, String>> configMap;
        PCollectionView<Map<Long, String>> sourceAssetsIdMapView;
        PCollectionView<Map<String, Long>> targetAssetsExtIdMapView;
        PCollectionView<Map<Long, String>> sourceDataSetsIdMapView;
        PCollectionView<Map<String, Long>> targetDataSetsExtIdMapView;
        PCollectionView<List<String>> targetLabelExtIdListView;

        final Counter missingExtIdCounter = Metrics.counter(PrepareFiles.class,
                "No external id");
        final Counter assetMapCounter = Metrics.counter(PrepareFiles.class,
                "Asset ids mapped");
        final Counter noAssetMapCounter = Metrics.counter(PrepareFiles.class,
                "Asset ids not mapped");
        final Counter dataSetMapCounter = Metrics.counter(PrepareFiles.class,
                "Data set mapped");
        final Counter invalidLabelCounter = Metrics.counter(PrepareFiles.class,
                "Removed labels");

        public PrepareFiles(PCollectionView<Map<String, String>> configMap,
                            PCollectionView<Map<Long, String>> sourceAssetsIdMapView,
                            PCollectionView<Map<String, Long>> targetAssetsExtIdMapView,
                            PCollectionView<Map<Long, String>> sourceDataSetsIdMapView,
                            PCollectionView<Map<String, Long>> targetDataSetsExtIdMapView,
                            PCollectionView<List<String>> targetLabelExtIdListView) {
            this.configMap = configMap;
            this.sourceAssetsIdMapView = sourceAssetsIdMapView;
            this.targetAssetsExtIdMapView = targetAssetsExtIdMapView;
            this.sourceDataSetsIdMapView = sourceDataSetsIdMapView;
            this.targetDataSetsExtIdMapView = targetDataSetsExtIdMapView;
            this.targetLabelExtIdListView = targetLabelExtIdListView;
        }

        @ProcessElement
        public void processElement(@Element List<FileContainer> input,
                                   OutputReceiver<Iterable<FileContainer>> out,
                                   ProcessContext context) {
            Map<String, String> config = context.sideInput(configMap);
            Map<Long, String> sourceAssetsIdMap = context.sideInput(sourceAssetsIdMapView);
            Map<String, Long> targetAssetsExtIdMap = context.sideInput(targetAssetsExtIdMapView);
            Map<Long, String> sourceDataSetsIdMap = context.sideInput(sourceDataSetsIdMapView);
            Map<String, Long> targetDataSetsExtIdMap = context.sideInput(targetDataSetsExtIdMapView);
            List<String> targetLabelsExtIdList = context.sideInput(targetLabelExtIdListView);

            List<FileContainer> outputList = new ArrayList<>();
            for (FileContainer fileContainer : input) {
                FileMetadata fileMetadata = fileContainer.getFileMetadata();
                FileMetadata.Builder fileMetadataBuilder = fileMetadata.toBuilder()
                        .clearId()
                        .clearAssetIds()
                        .clearCreatedTime()
                        .clearLastUpdatedTime()
                        .clearDataSetId();

                if (!fileMetadata.hasExternalId()) {
                    fileMetadataBuilder.setExternalId(String.valueOf(fileMetadata.getId()));
                    missingExtIdCounter.inc();
                }

                // Add asset link if enabled and if it is available on the target
                if (config.getOrDefault(contextualizationConfigKey, "no").equalsIgnoreCase("yes")
                        && fileMetadata.getAssetIdsCount() > 0) {
                    for (Long assetId : fileMetadata.getAssetIdsList()) {
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

                // Map data set if enabled, and if it is available on the target
                if(config.getOrDefault(dataSetConfigKey, "no").equalsIgnoreCase("yes")
                        && fileMetadata.hasDataSetId()) {
                    String targetDataSetExtId = sourceDataSetsIdMap.getOrDefault(
                            fileMetadata.getDataSetId(),
                            String.valueOf(fileMetadata.getDataSetId()));

                    if (targetDataSetsExtIdMap.containsKey(targetDataSetExtId)) {
                        fileMetadataBuilder.setDataSetId(targetDataSetsExtIdMap.get(targetDataSetExtId));
                        dataSetMapCounter.inc();
                    }
                }

                // Remove invalid labels if enabled
                if (config.getOrDefault(labelsConfigKey, "no").equalsIgnoreCase("yes")
                        && fileMetadataBuilder.getLabelsCount() > 0) {
                    List<String> validLabels = fileMetadataBuilder.getLabelsList().stream()
                            .filter(label -> targetLabelsExtIdList.contains(label))
                            .collect(Collectors.toList());
                    List<String> invalidLabels = fileMetadataBuilder.getLabelsList().stream()
                            .filter(label -> !targetLabelsExtIdList.contains(label))
                            .collect(Collectors.toList());

                    fileMetadataBuilder
                            .clearLabels()
                            .addAllLabels(validLabels);

                    // log invalid labels...
                    for (String label : invalidLabels) {
                        LOG.warn("Found invalid label extId reference {} in file extId {}",
                                label,
                                fileMetadataBuilder.getExternalId());
                        invalidLabelCounter.inc();
                    }
                }

                outputList.add(
                        fileContainer.toBuilder()
                                .setFileMetadata(fileMetadataBuilder)
                                .build()
                );
            }

            out.output(outputList);
        }
    }

    /**
     * Replaces forward and backslash characters ("/" and "\") with dash ("-").
     */
    private static class CleanFileNames extends DoFn<List<FileContainer>, List<FileContainer>> {
        final Counter forwardSlashCounter = Metrics.counter(CleanFileNames.class,
                "Forward slash in name");
        final Counter backSlashCounter = Metrics.counter(CleanFileNames.class,
                "Backward slash in name");
        final Counter cleanedNameCounter = Metrics.counter(CleanFileNames.class,
                "Cleaned file name");

        @ProcessElement
        public void processElement(@Element List<FileContainer> input,
                                   OutputReceiver<List<FileContainer>> out) {
            List<FileContainer> outputList = new ArrayList<>();
            for (FileContainer fileContainer : input) {
                FileMetadata fileMetadata = fileContainer.getFileMetadata();
                String originalFileName = fileMetadata.getName();
                String cleanedFileName = originalFileName;

                FileMetadata.Builder fileMetadataBuilder = fileMetadata.toBuilder();

                if (cleanedFileName.contains(forwardSlashSubString)) {
                    cleanedFileName = cleanedFileName.replace(forwardSlashSubString, "-");
                    forwardSlashCounter.inc();
                }

                if (cleanedFileName.contains(backSlashSubString)) {
                    cleanedFileName = cleanedFileName.replace(backSlashSubString, "-");
                    backSlashCounter.inc();
                }

                if (!cleanedFileName.equals(originalFileName)) {
                    fileMetadataBuilder.setName(cleanedFileName);
                    fileMetadataBuilder.putMetadata("originalFileName", originalFileName);
                    LOG.info("Changed file name from '{}' to '{}'", originalFileName, cleanedFileName);
                    cleanedNameCounter.inc();
                }

                outputList.add(fileContainer.toBuilder()
                        .setFileMetadata(fileMetadataBuilder)
                        .build());
            }

            out.output(outputList);
        }
    }

    public interface ReplicateFilesOptions extends PipelineOptions {

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

        @Description("Temp storage for large files. The name should be in the format of gs://<bucket>/folder/.")
        @Validation.Required
        ValueProvider<String> getTempStorageUri();
        void setTempStorageUri(ValueProvider<String> value);
    }

    private static PipelineResult runReplicateFiles(ReplicateFilesOptions options) throws IOException {
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
                                .withAppIdentifier(appIdentifier)))
                .apply("Extract id + externalId", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.longs(), TypeDescriptors.strings()))
                        .via((Asset asset) -> KV.of(asset.getId(), asset.getExternalId())))
                .apply("Max per key", Max.perKey())
                .apply("To map view", View.asMap());

        PCollectionView<Map<String, Long>> targetAssetsExtIdMap = p
                .apply("Read target assets", CogniteIO.readAssets()
                        .withProjectConfig(targetConfig)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)))
                .apply("Extract externalId + id", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
                        .via((Asset asset) -> KV.of(asset.getExternalId(), asset.getId())))
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
                                .withAppIdentifier(appIdentifier)))
                .apply("Select id + externalId", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.longs(), TypeDescriptors.strings()))
                        .via((DataSet dataSet) -> {
                            LOG.info("Source dataset - id: {}, extId: {}, name: {}",
                                    dataSet.getId(),
                                    dataSet.getExternalId(),
                                    dataSet.getName());
                            return KV.of(dataSet.getId(), dataSet.getExternalId());
                        }))
                .apply("Max per key", Max.perKey())
                .apply("To map view", View.asMap());

        PCollectionView<Map<String, Long>> targetDataSetsExtIdMap = p
                .apply("Read target data sets", CogniteIO.readDataSets()
                        .withProjectConfig(targetConfig)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)))
                .apply("Select external id + id", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
                        .via((DataSet dataSet) -> {
                            LOG.info("Target dataset - id: {}, extId: {}, name: {}",
                                    dataSet.getId(),
                                    dataSet.getExternalId(),
                                    dataSet.getName());
                            return KV.of(dataSet.getExternalId(), dataSet.getId());
                        }))
                .apply("Max per key", Max.perKey())
                .apply("To map view", View.asMap());

        /*
        Read labels from the destination CDF. Will use this list to determine which labels to remove from the files
        if "ignoreUnknownLabels = yes"
         */
        PCollectionView<List<String>> targetLabelExtIdList = p
                .apply("Read target labels", CogniteIO.readLabels()
                        .withProjectConfig(targetConfig)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)))
                .apply("Select extId", MapElements.into(TypeDescriptors.strings())
                        .via(Label::getExternalId))
                .apply("To list view", View.asList());

        /*
        Read files from source and parse them:
        - Filter on data set external id
        - Remove system fields (id, created date, last updated date
        - Translate the asset id links

        Then write the prepared files to target.
         */
        PCollection<FileMetadata> fileContainerPCollection = p
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
                                        "max", System.currentTimeMillis()
                                ))
                                ;

                        if (dataSetExternalIds.isEmpty() || allowList.contains("*")) {
                            LOG.info("Will not filter on data set external id ");
                            out.output(req);
                        } else {
                            LOG.info("Add filter on {} data set external ids.", dataSetExternalIds.size());
                            out.output(req.withFilterParameter("dataSetIds", dataSetExternalIds));
                        }
                    }
                }).withSideInputs(allowListDataSet))
                .apply("Read source files", CogniteIO.readAllDirectFiles()
                        .withProjectConfig(sourceConfig)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier))
                        .withTempStorageURI(options.getTempStorageUri())
                        .enableForceTempStorage(true)
                )
                .apply("Clean file names", ParDo.of(new CleanFileNames()))
                .apply("Process files", ParDo.of(new PrepareFiles(configMap,
                        sourceAssetsIdMap,
                        targetAssetsExtIdMap,
                        sourceDataSetsIdMap,
                        targetDataSetsExtIdMap,
                        targetLabelExtIdList
                ))
                        .withSideInputs(configMap,
                                sourceAssetsIdMap,
                                targetAssetsExtIdMap,
                                sourceDataSetsIdMap,
                                targetDataSetsExtIdMap,
                                targetLabelExtIdList))
                .apply("Write target files", CogniteIO.writeDirectFiles()
                        .withProjectConfig(targetConfig)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier(appIdentifier)
                                .withUpsertMode(UpsertMode.REPLACE)));

        return p.run();
    }

    public static void main(String[] args) throws IOException {
        ReplicateFilesOptions options = PipelineOptionsFactory
                .fromArgs(args).withValidation()
                .as(ReplicateFilesOptions.class);
        runReplicateFiles(options);
    }
}
