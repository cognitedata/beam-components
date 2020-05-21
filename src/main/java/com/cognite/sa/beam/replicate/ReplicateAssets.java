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
import com.cognite.beam.io.dto.Asset;
import com.cognite.beam.io.dto.DataSet;
import com.cognite.beam.io.dto.Event;
import com.cognite.beam.io.servicesV1.RequestParameters;
import com.cognite.beam.io.transform.toml.ReadTomlStringArray;
import com.cognite.beam.io.transform.toml.ReadTomlStringMap;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.StringValue;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This pipeline reads assets from the specified cdf instance and writes them to a target cdf.
 *
 * The assets will be synchronized via change detection between source and target. That is, the source will be considered
 * the "master" dataset and the target cdf instance will be updated to match it via upserts and deletes of assets.
 *
 * This job is prepared to be deployed as a template on GCP (Dataflow) + can be executed directly on any runner.
 */
public class ReplicateAssets {
    // The log to output status messages to.
    private static final Logger LOG = LoggerFactory.getLogger(ReplicateAssets.class);
    private static final String appIdentifier = "Replicate_Assets";

    /**
     * Create target asset:
     *         - Remove system fields (id, created date, last updated date, parentId)
     *         - Translate the asset id links to externalParentId
     *         - Set the key to the rootAssetExternalId
     */
    private static class PrepareAssets extends DoFn<Asset, KV<String, Asset>> {
        PCollectionView<Map<Long, String>> sourceAssetsIdMapView;

        PrepareAssets(PCollectionView<Map<Long, String>> sourceAssetsIdMap) {
            this.sourceAssetsIdMapView = sourceAssetsIdMap;
        }

        @ProcessElement
        public void processElement(@Element Asset input,
                                   OutputReceiver<KV<String, Asset>> out,
                                   ProcessContext context) {
            Preconditions.checkArgument(input.hasExternalId(), "Source assets must have an externalId.");
            Preconditions.checkArgument(input.hasRootId(), "Source assets must have a rootId");
            Map<Long, String> sourceAssetsIdMap = context.sideInput(sourceAssetsIdMapView);

            Asset.Builder builder = Asset.newBuilder(input);
            builder.clearCreatedTime();
            builder.clearLastUpdatedTime();
            builder.clearId();
            builder.clearParentId();

            if (sourceAssetsIdMap.containsKey(input.getParentId().getValue())) {
                builder.setParentExternalId(StringValue.of(sourceAssetsIdMap.get(input.getParentId().getValue())));
            }

            out.output(KV.of(sourceAssetsIdMap.getOrDefault(input.getRootId().getValue(),
                    ""), builder.build()));
        }
    }

    /**
     * Custom options for this pipeline.
     */
    public interface ReplicateAssetsOptions extends PipelineOptions {
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
     * - Read the assets from both source and target (in order to map the events' assets links)
     * - Replicate the events.
     *
     * @param options
     */
    private static PipelineResult runReplicateAssets(ReplicateAssetsOptions options) throws IOException {
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

       /*
        Read the job config file and parse the allow and deny list into views.
        Config maps are published as views so they can be used by the transforms as side inputs.
         */
        PCollectionView<Map<String, String>> configMap = p
                .apply("Read config map", ReadTomlStringMap.from(options.getJobConfigFile())
                        .withMapKey("config"))
                .apply("to map view", View.asMap());

        PCollectionView<List<String>> assetDenyList = p
                .apply("Read asset deny list", ReadTomlStringArray.from(options.getJobConfigFile())
                        .withArrayKey("denyList.rootAssetExternalId"))
                .apply("To view", View.asList());

        PCollectionView<List<String>> assetAllowList = p
                .apply("Read asset allow list", ReadTomlStringArray.from(options.getJobConfigFile())
                        .withArrayKey("allowList.rootAssetExternalId"))
                .apply("To view", View.asList());

        PCollectionView<List<String>> allowListDataSet = p
                .apply("Read data set allow list", ReadTomlStringArray.from(options.getJobConfigFile())
                        .withArrayKey("allowList.dataSetExternalId"))
                .apply("Log data set extId", MapElements.into(TypeDescriptors.strings())
                        .via(expression -> {
                            LOG.info("Registered dataSetExternalId: {}", expression);
                            return expression;
                        }))
                .apply("To view", View.asList());

        /*
        Read the asset hierarchies from source. Shave off the metadata and use id + externalId
        as the basis for mapping assets ids and externalIds. Project the resulting asset collections as views so
        they can be used as side inputs to the main Asset transform.
         */
        PCollectionView<Map<Long, String>> sourceAssetsIdMap = p
                .apply("Read source assets", CogniteIO.readAssets()
                        .withProjectConfig(sourceConfig)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)))
                .apply("Extract id + externalId", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.longs(), TypeDescriptors.strings()))
                        .via((Asset asset) -> KV.of(asset.getId().getValue(), asset.getExternalId().getValue())))
                .apply("Max per key", Max.perKey())
                .apply("To map view", View.asMap());

        /*
        Read the data sets from source and target. Will use these to map items from source data set to
        a target data set.
         */
        PCollectionView<Map<Long, String>> sourceDataSetsIdMap = p
                .apply("Read source assets", CogniteIO.readDataSets()
                        .withProjectConfig(sourceConfig)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)))
                .apply("Select id + externalId", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.longs(), TypeDescriptors.strings()))
                        .via((DataSet dataSet) -> KV.of(dataSet.getId().getValue(), dataSet.getExternalId().getValue())))
                .apply("Max per key", Max.perKey())
                .apply("To map view", View.asMap());

        PCollectionView<Map<Long, String>> targetDataSetsIdMap = p
                .apply("Read source assets", CogniteIO.readDataSets()
                        .withProjectConfig(targetConfig)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)))
                .apply("Select id + externalId", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.longs(), TypeDescriptors.strings()))
                        .via((DataSet dataSet) -> KV.of(dataSet.getId().getValue(), dataSet.getExternalId().getValue())))
                .apply("Max per key", Max.perKey())
                .apply("To map view", View.asMap());

        /*
        Read assets from source and parse them:
        - Filter on data set external id
        - Remove system fields (id, created date, last updated date, parentId)
        - Translate the parentId to parentExternalId
        - Set key to root asset externalId

        Filter the assets based on root asset externalId

        Write the prepared events to target.
         */
        PCollectionTuple assetsPCollectionTuple = p
                .apply("Build basic query", Create.of(RequestParameters.create()))
                .apply("Add dataset filter", ParDo.of(new DoFn<RequestParameters, RequestParameters>() {
                    @ProcessElement
                    public void processElement(@Element RequestParameters input,
                                               OutputReceiver<RequestParameters> out,
                                               ProcessContext context) {
                        List<String> allowList = context.sideInput(allowListDataSet);
                        List<Map<String, String>> datasetExternalIds = new ArrayList<>();
                        LOG.info("Data set allow list contains {} entries", allowList.size());

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
                .apply("Read source assets", CogniteIO.readAllAssets()
                        .withProjectConfig(sourceConfig)
                        .withHints(Hints.create()
                                .withReadShards(100))
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)))
                .apply("Process assets", ParDo.of(new PrepareAssets(sourceAssetsIdMap))
                        .withSideInputs(sourceAssetsIdMap))
                .apply("Filter assets", ParDo.of(new DoFn<KV<String, Asset>, KV<String, Asset>>() {
                    @ProcessElement
                    public void processElement(@Element KV<String, Asset> input,
                                               OutputReceiver<KV<String, Asset>> out,
                                               ProcessContext context) {
                        List<String> denyList = context.sideInput(assetDenyList);
                        List<String> allowList = context.sideInput(assetAllowList);

                        if (!denyList.isEmpty()) {
                            if (denyList.contains(input.getKey())) {
                                LOG.debug("Deny list match root externalId {}. Asset [{}] will be dropped.",
                                        input.getKey(), input.getValue().getExternalId().getValue());
                                return;
                            }
                        }

                        if (allowList.contains("*")) {
                            out.output(input);
                        } else if (allowList.contains(input.getKey())) {
                            LOG.debug("Allow list match root externalId {}. Asset [{}] will be included.",
                                    input.getKey(), input.getValue().getExternalId().getValue());
                            out.output(input);
                        }
                    }
                }).withSideInputs(assetDenyList, assetAllowList))
                .apply("Synchronize target assets", CogniteIO.synchronizeHierarchies()
                        .withProjectConfig(targetConfig)
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier(appIdentifier)));

        return p.run();
    }

    /**
     * Read the pipeline options from args and run the pipeline.
     * @param args
     */
    public static void main(String[] args) throws IOException{
        ReplicateAssetsOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(ReplicateAssetsOptions.class);
        runReplicateAssets(options);
    }
}
