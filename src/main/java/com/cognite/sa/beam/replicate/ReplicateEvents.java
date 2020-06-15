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
import com.cognite.beam.io.dto.*;
import com.cognite.beam.io.servicesV1.RequestParameters;
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

/**
 * This pipeline reads events from the specified cdf instance and writes them to a target cdf.
 *
 * The events can be linked to assets at the target based on externalId mapping from the assets in the source.
 *
 * This job is prepared to be deployed as a template on GCP (Dataflow) + can be executed directly on any runner.
 */
public class ReplicateEvents {
    // The log to output status messages to.
    private static final Logger LOG = LoggerFactory.getLogger(ReplicateEvents.class);
    private static final String appIdentifier = "Replicate_Events";
    private static final String contextualizationConfigKey = "enableContextualization";
    private static final String dataSetConfigKey = "enableDataSetMapping";

    /**
     * Create target event:
     *         - Remove system fields (id, created date, last updated date)
     *         - Translate the asset id links
     *         - If the headers does not have externalId, use the source's id
     *         - Map data set ids
     */
    private static class PrepareEvents extends DoFn<Event, Event> {
        PCollectionView<Map<String, String>> configMap;
        PCollectionView<Map<Long, String>> sourceAssetsIdMapView;
        PCollectionView<Map<String, Long>> targetAssetsIdMapView;
        PCollectionView<Map<Long, String>> sourceDataSetsIdMapView;
        PCollectionView<Map<String, Long>> targetDataSetsExtIdMapView;

        final Counter missingExtCounter = Metrics.counter(ReplicateEvents.PrepareEvents.class,
                "No external id");
        final Counter assetMapCounter = Metrics.counter(ReplicateEvents.PrepareEvents.class,
                "Map asset ids");
        final Counter dataSetMapCounter = Metrics.counter(ReplicateEvents.PrepareEvents.class,
                "Map data set");

        PrepareEvents(PCollectionView<Map<String, String>> configMap,
                      PCollectionView<Map<Long, String>> sourceAssetsIdMap,
                      PCollectionView<Map<String, Long>> targetAssetsIdMap,
                      PCollectionView<Map<Long, String>> sourceDataSetsIdMap,
                      PCollectionView<Map<String, Long>> targetDataSetsExtIdMap) {
            this.configMap = configMap;
            this.sourceAssetsIdMapView = sourceAssetsIdMap;
            this.targetAssetsIdMapView = targetAssetsIdMap;
            this.sourceDataSetsIdMapView = sourceDataSetsIdMap;
            this.targetDataSetsExtIdMapView = targetDataSetsExtIdMap;
        }

        @ProcessElement
        public void processElement(@Element Event input,
                                   OutputReceiver<Event> out,
                                   ProcessContext context) {
            Map<Long, String> sourceAssetsIdMap = context.sideInput(sourceAssetsIdMapView);
            Map<String, Long> targetAssetsIdMap = context.sideInput(targetAssetsIdMapView);
            Map<Long, String> sourceDataSetsIdMap = context.sideInput(sourceDataSetsIdMapView);
            Map<String, Long> targetDataSetsExtIdMap = context.sideInput(targetDataSetsExtIdMapView);
            Map<String, String> config = context.sideInput(configMap);

            Event.Builder builder = input.toBuilder()
                    .clearAssetIds()
                    .clearCreatedTime()
                    .clearLastUpdatedTime()
                    .clearId()
                    .clearDataSetId();

            if (!input.hasExternalId()) {
                builder.setExternalId(StringValue.of(String.valueOf(input.getId().getValue())));
                missingExtCounter.inc();
            }

            // add asset link if enabled and it is available in the target
            if (config.getOrDefault(contextualizationConfigKey, "no").equalsIgnoreCase("yes")
                    && input.getAssetIdsCount() > 0) {
                for (long assetId : input.getAssetIdsList()) {
                    // if the source asset has an externalId use it--if not, use the asset internal id
                    String targetAssetExtId = sourceAssetsIdMap.getOrDefault(assetId, String.valueOf(assetId));

                    if (targetAssetsIdMap.containsKey(targetAssetExtId)) {
                        builder.addAssetIds(targetAssetsIdMap.get(targetAssetExtId));
                    }
                }
                if (builder.getAssetIdsCount() > 0) assetMapCounter.inc();
            }

            // map data set if enabled and it is available in the target
            if (config.getOrDefault(dataSetConfigKey, "no").equalsIgnoreCase("yes")
                    && input.hasDataSetId()) {
                String targetDataSetExtId = sourceDataSetsIdMap.getOrDefault(
                        input.getDataSetId().getValue(), String.valueOf(input.getDataSetId().getValue()));
                if (targetDataSetsExtIdMap.containsKey(targetDataSetExtId)) {
                    builder.setDataSetId(Int64Value.of(targetDataSetsExtIdMap.get(targetDataSetExtId)));
                    dataSetMapCounter.inc();
                }
            }

            // Check for constraint violations and correct
            if (!builder.hasStartTime()) {
                builder.setStartTime(Int64Value.of(0L));
                String message = "startTime was not set. Setting startTime to 0L.";
                builder.putMetadata("constraintViolationStartTime_a", message);
                LOG.warn(message + " Event externalId: {}", builder.getExternalId().getValue());
            }

            if (!builder.hasEndTime()) {
                builder.setEndTime(Int64Value.of(builder.getStartTime().getValue()));
                String message = "endTime was not set. Set endTime to be equal to startTime";
                builder.putMetadata("constraintViolationEndTime_a", message);
                LOG.warn(message + " Event externalId: {}", builder.getExternalId().getValue());
            }

            if (builder.getStartTime().getValue() < 0) {
                builder.setStartTime(Int64Value.of(0L));
                String message = "startTime was < 0. Setting startTime to 0L.";
                builder.putMetadata("constraintViolationStartTime_b", message);
                LOG.warn(message + " Event externalId: {}", builder.getExternalId().getValue());
            }

            if (builder.getEndTime().getValue() < 0) {
                builder.setEndTime(Int64Value.of(builder.getStartTime().getValue()));
                String message = "endTime was < 0. Set endTime to be equal to startTime";
                builder.putMetadata("constraintViolationEndTime_b", message);
                LOG.warn(message + " Event externalId: {}", builder.getExternalId().getValue());
            }

            if (builder.getStartTime().getValue() > builder.getEndTime().getValue()) {
                builder.setStartTime(Int64Value.of(builder.getEndTime().getValue()));
                String message = "startTime was greater than endTime. Changed startTime to be equal to endTime.";
                builder.putMetadata("constraintViolationStartTime_b", message);
                LOG.warn(message + " Event externalId: {}", builder.getExternalId().getValue());
            }

            if (builder.getDescription().getValue().length() > 500) {
                builder.setDescription(StringValue.of(builder.getDescription().getValue().substring(0, 500)));
                String message = "Description length was >500 characters. Truncated the description to the first 500 characters.";
                builder.putMetadata("constraintViolationDescription", message);
                LOG.warn(message + " Event externalId: {}", builder.getExternalId().getValue());
            }

            out.output(builder.build());
        }
    }

    /**
     * Custom options for this pipeline.
     */
    public interface ReplicateEventsOptions extends PipelineOptions {
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
    private static PipelineResult runReplicateEvents(ReplicateEventsOptions options) throws IOException {
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
        Read the job config file and parse into views.
        Config maps are published as views so they can be used by the transforms as side inputs.
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
                .apply("to map view", View.asMap());

        PCollectionView<List<String>> allowListDataSet = p
                .apply("Read config allow list", ReadTomlStringArray.from(options.getJobConfigFile())
                        .withArrayKey("allowList.dataSetExternalId"))
                .apply("Log data set extId", MapElements.into(TypeDescriptors.strings())
                        .via(expression -> {
                            LOG.info("Registered dataSetExternalId: {}", expression);
                            return expression;
                        }))
                .apply("To view", View.asList());

        /*
        Read the asset hierarchies from source and target. Shave off the metadata and use id + externalId
        as the basis for mapping TS to assets. Project the resulting asset collections as views so they can be
        used as side inputs to the main Event transform.
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

        PCollectionView<Map<String, Long>> targetAssetsIdMap = p
                .apply("Read target assets", CogniteIO.readAssets()
                        .withProjectConfig(targetConfig)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)))
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
                                .withAppIdentifier(appIdentifier)))
                .apply("Select id + externalId", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.longs(), TypeDescriptors.strings()))
                        .via((DataSet dataSet) -> {
                                    LOG.info("Source dataset - id: {}, extId: {}, name: {}",
                                            dataSet.getId().getValue(),
                                            dataSet.getExternalId().getValue(),
                                            dataSet.getName().getValue());
                                    return KV.of(dataSet.getId().getValue(), dataSet.getExternalId().getValue());
                                }
                        ))
                .apply("Max per key", Max.perKey())
                .apply("To map view", View.asMap());

        PCollectionView<Map<String, Long>> targetDataSetsExtIdMap = p
                .apply("Read target data sets", CogniteIO.readDataSets()
                        .withProjectConfig(targetConfig)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)))
                .apply("Select externalId + id", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
                        .via((DataSet dataSet) -> {
                                    LOG.info("Target dataset - id: {}, extId: {}, name: {}",
                                            dataSet.getId().getValue(),
                                            dataSet.getExternalId().getValue(),
                                            dataSet.getName().getValue());
                                    return KV.of(dataSet.getExternalId().getValue(), dataSet.getId().getValue());
                                }
                        ))
                .apply("Max per key", Max.perKey())
                .apply("To map view", View.asMap());

        /*
        Read events from source and parse them:
        - Filter on data set external id
        - Remove system fields (id, created date, last updated date)
        - Translate the asset id links (to be done)

        Write the prepared events to target.
         */
        PCollection<Event> eventPCollection = p
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
                .apply("Read source events", CogniteIO.readAllEvents()
                        .withProjectConfig(sourceConfig)
                        .withHints(Hints.create()
                                .withReadShards(1000))
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)))
                .apply("Process events", ParDo.of(new PrepareEvents(configMap, sourceAssetsIdMap,
                        targetAssetsIdMap, sourceDataSetsIdMap, targetDataSetsExtIdMap))
                        .withSideInputs(configMap, sourceAssetsIdMap, targetAssetsIdMap,
                                sourceDataSetsIdMap, targetDataSetsExtIdMap))
                .apply("Write target events", CogniteIO.writeEvents()
                        .withProjectConfig(targetConfig)
                        .withHints(Hints.create()
                                .withWriteShards(20))
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier(appIdentifier)));

        return p.run();
    }

    /**
     * Read the pipeline options from args and run the pipeline.
     * @param args
     */
    public static void main(String[] args) throws IOException{
        ReplicateEventsOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(ReplicateEventsOptions.class);
        runReplicateEvents(options);
    }
}
