package com.cognite.sa.beam.replicate;

import com.cognite.beam.io.CogniteIO;
import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.config.UpdateFrequency;
import com.cognite.beam.io.config.WriterConfig;
import com.cognite.beam.io.dto.Asset;
import com.cognite.beam.io.dto.TimeseriesMetadata;
import com.cognite.beam.io.dto.TimeseriesPoint;
import com.cognite.beam.io.dto.TimeseriesPointPost;
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
        /**
         * Specify the source Cdf config file.
         */
        @Description("The cdf config file. The name should be in the format of gs://<bucket>/folder.")
        @Validation.Required
        ValueProvider<String> getCdfSourceConfigFile();
        void setCdfSourceConfigFile(ValueProvider<String> value);

        /**
         * Specify the target Cdf config file.
         */
        @Description("The cdf config file. The name should be in the format of gs://<bucket>/folder.")
        @Validation.Required
        ValueProvider<String> getCdfTargetConfigFile();
        void setCdfTargetConfigFile(ValueProvider<String> value);

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
    private static PipelineResult runReplicateTs(ReplicateTsOptions options) throws IOException {
        Pipeline p = Pipeline.create(options);

       /*
        Read the job config file and parse out the blacklist and whitelist.
        Both lists are published as views so they can be used by the transforms as side inputs.
         */
        PCollectionView<List<String>> tsBlacklist = p
                .apply("Read config blacklist", ReadTomlStringArray.from(options.getJobConfigFile())
                        .withArrayKey("blacklist.tsExternalId"))
                .apply("To view", View.asList());

        PCollectionView<List<String>> tsWhitelist = p
                .apply("Read config whitelist", ReadTomlStringArray.from(options.getJobConfigFile())
                        .withArrayKey("whitelist.tsExternalId"))
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
                        .withProjectConfigFile(options.getCdfSourceConfigFile())
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)))
                .apply("Extract id + externalId", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.longs(), TypeDescriptors.strings()))
                        .via((Asset asset) -> KV.of(asset.getId().getValue(), asset.getExternalId().getValue())))
                .apply("Max per key", Max.perKey())
                .apply("To map view", View.asMap());

        PCollectionView<Map<String, Long>> targetAssetsIdMap = p
                .apply("Read target assets", CogniteIO.readAssets()
                        .withProjectConfigFile(options.getCdfTargetConfigFile())
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)))
                .apply("Extract externalId + id", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
                        .via((Asset asset) -> KV.of(asset.getExternalId().getValue(), asset.getId().getValue())))
                .apply("Max per key", Max.perKey())
                .apply("To map view", View.asMap());

        /*
        Read, parse and filter the TS headers.
        The TS is filtered in two steps: 1) on security categories and 2) against the blacklist
        and whitelist on externalId.
         */
        PCollection<TimeseriesMetadata> tsHeaders = p
                .apply("Read Ts headers", CogniteIO.readTimeseriesMetadata()
                        .withProjectConfigFile(options.getCdfSourceConfigFile())
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
                        List<String> blacklist = context.sideInput(tsBlacklist);
                        List<String> whitelist = context.sideInput(tsWhitelist);

                        if (!blacklist.isEmpty() && input.hasExternalId()) {
                            if (blacklist.contains(input.getExternalId().getValue())) {
                                LOG.info("Blacklist match {}. TS will be dropped.", input.getExternalId().getValue());
                                return;
                            }
                        }

                        if (whitelist.contains("*")) {
                            out.output(input);
                        } else if (whitelist.contains(input.getExternalId().getValue())) {
                            LOG.info("Whitelist match {}. TS will be included.", input.getExternalId().getValue());
                            out.output(input);
                        }
                    }
                }).withSideInputs(tsBlacklist, tsWhitelist));

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
                        .withProjectConfigFile(options.getCdfTargetConfigFile())
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

                        int windowDays = Integer.valueOf(config.getOrDefault(tsPointsWindowConfigKey, "1"));
                        //int windowDays = 1;
                        Instant fromTime = Instant.now().truncatedTo(ChronoUnit.DAYS)
                                .minus(windowDays, ChronoUnit.DAYS)
                                .minus(1, ChronoUnit.HOURS);

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
                        .withProjectConfigFile(options.getCdfSourceConfigFile())
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
                        .withProjectConfigFile(options.getCdfTargetConfigFile())
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
    public static void main(String[] args) throws IOException{
        ReplicateTsOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(ReplicateTsOptions.class);
        runReplicateTs(options);
    }
}
