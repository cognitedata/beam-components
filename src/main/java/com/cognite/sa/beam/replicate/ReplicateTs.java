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
import com.cognite.beam.io.transform.BreakFusion;
import com.cognite.beam.io.transform.GroupIntoBatches;
import com.cognite.beam.io.transform.toml.ReadTomlStringArray;
import com.cognite.beam.io.transform.toml.ReadTomlStringMap;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Int64Value;
import com.google.protobuf.StringValue;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;

/**
 * This pipeline reads TS headers and data points along a rolling time window from the specified cdf instance and writes
 * them to a target cdf.
 *
 * This job is prepared to be deployed as a template on GCP (Dataflow) + can be executed directly on any runner.
 */
public class ReplicateTs {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicateTs.class);

    // Pipeline configuration
    private static final String appIdentifier = "Replicate_TimeSeries";
    private static final String tsHeaderConfigKey = "tsHeaders";
    private static final String tsPointsConfigKey = "tsPoints";
    private static final String tsPointsWindowConfigKey = "tsPointsWindowDays";
    private static final String contextualizationConfigKey = "enableContextualization";

    /**
     * Create target ts point:
     *         - If the data point does not have externalId, use the source's id
     */
    private static class PrepareTsPoint extends DoFn<TimeseriesPoint, TimeseriesPointPost> {
        @ProcessElement
        public void processElement(@Element TimeseriesPoint input,
                                   OutputReceiver<TimeseriesPointPost> out) {

            TimeseriesPointPost.Builder builder = TimeseriesPointPost.newBuilder()
                    .setTimestamp(input.getTimestamp());
            if (input.hasExternalId()) {
                builder.setExternalId(input.getExternalId().getValue());
            } else {
                builder.setExternalId(String.valueOf(input.getId()));
            }

            if (input.getDatapointTypeCase() == TimeseriesPoint.DatapointTypeCase.VALUE_STRING) {
                builder.setValueString(input.getValueString());
            } else {
                builder.setValueNum(input.getValueNum());
            }

            out.output(builder.build());
        }
    }

    /**
     * Create target ts header:
     *         - Remove system fields (id, created date, last updated date, security categories)
     *         - Translate the asset id links
     *         - If the headers does not have externalId, use the source's id
     */
    private static class PrepareTsHeader extends DoFn<TimeseriesMetadata, TimeseriesMetadata> {
        PCollectionView<Map<String, String>> configMap;
        PCollectionView<Map<Long, String>> sourceAssetsIdMapView;
        PCollectionView<Map<String, Long>> targetAssetsIdMapView;

        PrepareTsHeader(PCollectionView<Map<String, String>> configMap,
                        PCollectionView<Map<Long, String>> sourceAssetsIdMap,
                        PCollectionView<Map<String, Long>> targetAssetsIdMapView) {
            this.configMap = configMap;
            this.sourceAssetsIdMapView = sourceAssetsIdMap;
            this.targetAssetsIdMapView = targetAssetsIdMapView;
        }

        @ProcessElement
        public void processElement(@Element TimeseriesMetadata input,
                                   OutputReceiver<TimeseriesMetadata> out,
                                   ProcessContext context) {
            Map<Long, String> sourceAssetsIdMap = context.sideInput(sourceAssetsIdMapView);
            Map<String, Long> targetAssetsIdMap = context.sideInput(targetAssetsIdMapView);
            Map<String, String> config = context.sideInput(configMap);

            TimeseriesMetadata.Builder builder = TimeseriesMetadata.newBuilder(input);
            builder.clearAssetId();
            builder.clearCreatedTime();
            builder.clearLastUpdatedTime();
            builder.clearId();
            builder.clearSecurityCategories();

            if (!input.hasExternalId()) {
                builder.setExternalId(StringValue.of(String.valueOf(input.getId().getValue())));
            }

            // add asset link if enabled and it is available in the target
            if (config.getOrDefault(contextualizationConfigKey, "no").equalsIgnoreCase("yes")
                    && input.hasAssetId()) {
                // if the source asset has an externalId use it--if not, use the asset internal id
                String targetAssetExtId = sourceAssetsIdMap.getOrDefault(input.getAssetId().getValue(),
                        String.valueOf(input.getAssetId().getValue()));

                if (targetAssetsIdMap.containsKey(targetAssetExtId)) {
                    builder.setAssetId(Int64Value.of(targetAssetsIdMap.get(targetAssetExtId)));
                }
            }
            out.output(builder.build());
        }
    }

    /**
     * Custom options for this pipeline.
     */
    public interface ReplicateTsOptions extends PipelineOptions {
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
     * Setup the main pipeline structure and run it.
     * @param options
     */
    private static PipelineResult runReplicateTs(ReplicateTsOptions options) {
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
        Read the job config file and parse out the allow and deny list.
        Both lists are published as views so they can be used by the transforms as side inputs.
         */
        PCollectionView<List<String>> tsDenyList = p
                .apply("Read ts deny list", ReadTomlStringArray.from(options.getJobConfigFile())
                        .withArrayKey("denyList.tsExternalId"))
                .apply("To view", View.asList());

        PCollectionView<List<String>> tsDenyListRegEx = p
                .apply("Read ts regEx deny list", ReadTomlStringArray.from(options.getJobConfigFile())
                        .withArrayKey("denyList.tsExternalIdRegEx"))
                .apply("Log deny regEx", MapElements.into(TypeDescriptors.strings())
                        .via(expression -> {
                            LOG.info("Registered regex: {}", expression);
                            return expression;
                        }))
                .apply("To view", View.asList());

        PCollectionView<List<String>> tsAllowList = p
                .apply("Read ts allow list", ReadTomlStringArray.from(options.getJobConfigFile())
                        .withArrayKey("allowList.tsExternalId"))
                .apply("To view", View.asList());

        PCollectionView<List<String>> tsAllowListRegEx = p
                .apply("Read ts regEx allow list", ReadTomlStringArray.from(options.getJobConfigFile())
                        .withArrayKey("allowList.tsExternalIdRegEx"))
                .apply("Log allow regEx", MapElements.into(TypeDescriptors.strings())
                        .via(expression -> {
                            LOG.info("Registered regex: {}", expression);
                            return expression;
                        }))
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

        PCollectionView<Map<String, String>> configMap = p
                .apply("Read config map", ReadTomlStringMap.from(options.getJobConfigFile())
                        .withMapKey("config"))
                .apply("to map view", View.asMap());

        /*
        Read the asset hierarchies from source and target. Shave off the metadata and use id + externalId
        as the basis for mapping TS to assets. Project the resulting asset collections as views so they can be
        used as side inputs to the main TS header transform.
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
                        .via((DataSet dataSet) -> KV.of(dataSet.getId().getValue(), dataSet.getExternalId().getValue())))
                .apply("Max per key", Max.perKey())
                .apply("To map view", View.asMap());

        PCollectionView<Map<String, Long>> targetDataSetsExtIdMap = p
                .apply("Read target data sets", CogniteIO.readDataSets()
                        .withProjectConfig(targetConfig)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)))
                .apply("Select externalId + id", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
                        .via((DataSet dataSet) -> KV.of(dataSet.getExternalId().getValue(), dataSet.getId().getValue())))
                .apply("Max per key", Max.perKey())
                .apply("To map view", View.asMap());

        /*
        Read, parse and filter the TS headers.
        - Filter on data set external id
        - Filter on security categories
        - Filter on the externalId white- blacklists
        The TS is filtered in two steps: 1) on security categories and 2) against the blacklist
        and whitelist on externalId.
         */
        PCollection<TimeseriesMetadata> tsHeaders = p
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
                .apply("Read Ts headers", CogniteIO.readAllTimeseriesMetadata()
                        .withProjectConfig(sourceConfig)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)))
                .apply("Filter out TS w/ security categories", Filter.by(
                        tsHeader -> tsHeader.getSecurityCategoriesList().isEmpty()
                                && !tsHeader.getName().getValue().startsWith("SRE-cognite-sre-Timeseries")
                ))
                .apply("Filter ts", ParDo.of(new DoFn<TimeseriesMetadata, TimeseriesMetadata>() {
                    @ProcessElement
                    public void processElement(@Element TimeseriesMetadata input,
                                               OutputReceiver<TimeseriesMetadata> out,
                                               ProcessContext context) {
                        List<String> blacklist = context.sideInput(tsDenyList);
                        List<String> whitelist = context.sideInput(tsAllowList);
                        List<String> blacklistRegEx = context.sideInput(tsDenyListRegEx);
                        List<String> whitelistRegEx = context.sideInput(tsAllowListRegEx);

                        // Check for blacklist match
                        if (!blacklist.isEmpty() && input.hasExternalId()) {
                            if (blacklist.contains(input.getExternalId().getValue())) {
                                LOG.debug("Deny list match {}. TS will be dropped.", input.getExternalId().getValue());
                                return;
                            }
                        }

                        if (!blacklistRegEx.isEmpty() && input.hasExternalId()) {
                            for (String regExString : blacklistRegEx) {
                                if (Pattern.matches(regExString, input.getExternalId().getValue())) {
                                    LOG.debug("Deny list regEx {} match externalId {}. TS will be dropped.",
                                            regExString,
                                            input.getExternalId().getValue());
                                    return;
                                }
                            }
                        }

                        // Check for whitelist match
                        if (whitelist.contains("*")) {
                            out.output(input);
                            return;
                        }

                        if (whitelist.contains(input.getExternalId().getValue())) {
                            LOG.debug("Allow list match {}. TS will be included.", input.getExternalId().getValue());
                            out.output(input);
                            return;
                        }

                        if (!whitelistRegEx.isEmpty() && input.hasExternalId()) {
                            for (String regExString : whitelistRegEx) {
                                if (Pattern.matches(regExString, input.getExternalId().getValue())) {
                                    LOG.debug("Allow list regEx {} match externalId {}. TS will be included.",
                                            regExString,
                                            input.getExternalId().getValue());
                                }
                                out.output(input);
                                return;
                            }
                        }
                    }
                }).withSideInputs(tsDenyList, tsAllowList, tsDenyListRegEx, tsAllowListRegEx));

        /*
        Create target ts header and write:
        - Check if config includes TS headers
        - Remove system fields (id, created date, last updated date, security categories)
        - Translate the asset id links
         */
        PCollection<TimeseriesMetadata> output = tsHeaders
                .apply("Include TS headers?", ParDo.of(new DoFn<TimeseriesMetadata, TimeseriesMetadata>() {
                    @ProcessElement
                    public void processElement(@Element TimeseriesMetadata input,
                                               OutputReceiver<TimeseriesMetadata> out,
                                               ProcessContext context) {
                        Map<String, String> config = context.sideInput(configMap);
                        if (config.getOrDefault(tsHeaderConfigKey, "no").equalsIgnoreCase("yes")) {
                            out.output(input);
                        }
                    }
                }).withSideInputs(configMap))
                .apply("Process TS headers", ParDo.of(new PrepareTsHeader(configMap, sourceAssetsIdMap, targetAssetsIdMap))
                        .withSideInputs(configMap, sourceAssetsIdMap, targetAssetsIdMap))
                .apply("Write TS headers", CogniteIO.writeTimeseriesMetadata()
                        .withProjectConfig(targetConfig)
                        .withHints(Hints.create()
                                .withWriteShards(20))
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier(appIdentifier)));

        /*
         Read ts points for all headers.
         - Batch headers
         - Check if config includes ts points
         - Build the request to read the datapoints for each batch of headers. The time window is specified here.
         - Process all read requests.
         */
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
                        Map<String, String> config = context.sideInput(configMap);
                        List<Map<String, Object>> items = new ArrayList<>();
                        for (TimeseriesMetadata ts : input) {
                            items.add(ImmutableMap.of("id", ts.getId().getValue()));
                        }

                        Instant fromTime = Instant.now().truncatedTo(ChronoUnit.DAYS); //default
                        int windowDays = Integer.valueOf(config.getOrDefault(tsPointsWindowConfigKey, "1"));
                        //int windowDays = 1;

                        if (windowDays < 0) {
                            // Run full history for negative time windows.
                            fromTime = Instant.ofEpochMilli(31536000000L); // Jan 1st, 1971
                        } else {
                            fromTime = Instant.now().truncatedTo(ChronoUnit.DAYS)
                                    .minus(windowDays, ChronoUnit.DAYS)
                                    .minus(1, ChronoUnit.HOURS);
                        }

                        Instant toTime = Instant.now()
                                //.truncatedTo(ChronoUnit.DAYS)
                                //.minus(8, ChronoUnit.DAYS)
                                ;

                        if (config.getOrDefault(tsPointsConfigKey, "no").equalsIgnoreCase("yes")) {
                            out.output(RequestParameters.create()
                                    .withItems(items)
                                    .withRootParameter("start", fromTime.toEpochMilli())
                                    .withRootParameter("end", toTime.toEpochMilli())
                                    .withRootParameter("limit", 100000));
                        }
                    }
                }).withSideInputs(configMap))
                .apply("Read ts points", CogniteIO.readAllTimeseriesPoints()
                        .withProjectConfig(sourceConfig)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)));

        /*
        Write the ts points to the cdf target.
        - Process each data point and convert it into a datapoint post object.
        - Write the datapoint.
         */
        tsPoints
                .apply("Build TS point post object", ParDo.of(new PrepareTsPoint()))
                .apply("Write ts points", CogniteIO.writeTimeseriesPoints()
                        .withProjectConfig(targetConfig)
                        .withHints(Hints.create()
                                .withWriteTsPointsUpdateFrequency(UpdateFrequency.SECOND)
                                .withWriteShards(2)
                                .withWriteMaxBatchLatency(java.time.Duration.ofMinutes(5)))
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier(appIdentifier)));

        return p.run();
    }

    /**
     * Read the pipeline options from args and run the pipeline.
     * @param args
     */
    public static void main(String[] args) throws IOException {
        ReplicateTsOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(ReplicateTsOptions.class);
        runReplicateTs(options);
    }
}
