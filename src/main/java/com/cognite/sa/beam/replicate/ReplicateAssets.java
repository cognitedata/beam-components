package com.cognite.sa.beam.replicate;

import com.cognite.beam.io.CogniteIO;
import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.config.WriterConfig;
import com.cognite.beam.io.dto.Asset;
import com.cognite.beam.io.dto.Event;
import com.cognite.beam.io.transform.toml.ReadTomlStringArray;
import com.cognite.beam.io.transform.toml.ReadTomlStringMap;
import com.google.common.base.Preconditions;
import com.google.protobuf.StringValue;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
     * Setup the main pipeline structure and run it:
     * - Read the config settings
     * - Read the assets from both source and target (in order to map the events' assets links)
     * - Replicate the events.
     *
     * @param options
     */
    private static PipelineResult runReplicateAssets(ReplicateAssetsOptions options) throws IOException {
        Pipeline p = Pipeline.create(options);

       /*
        Read the job config file and parse the whitelist and blacklist into views.
        Config maps are published as views so they can be used by the transforms as side inputs.
         */
        PCollectionView<List<String>> assetBlacklist = p
                .apply("Read config blacklist", ReadTomlStringArray.from(options.getJobConfigFile())
                        .withArrayKey("blacklist.rootAssetExternalId"))
                .apply("To view", View.asList());

        PCollectionView<List<String>> assetWhitelist = p
                .apply("Read config whitelist", ReadTomlStringArray.from(options.getJobConfigFile())
                        .withArrayKey("whitelist.rootAssetExternalId"))
                .apply("To view", View.asList());

        /*
        Read the asset hierarchies from source. Shave off the metadata and use id + externalId
        as the basis for mapping assets ids and externalIds. Project the resulting asset collections as views so
        they can be used as side inputs to the main Asset transform.
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

        /*
        Read assets from source and parse them:
        - Remove system fields (id, created date, last updated date, parentId)
        - Translate the parentId to parentExternalId
        - Set key to root asset externalId

        Filter the assets based on root asset externalId

        Write the prepared events to target.
         */
        PCollectionTuple assetsPCollectionTuple = p
                .apply("Read source assets", CogniteIO.readAssets()
                        .withProjectConfigFile(options.getCdfSourceConfigFile())
                        .withHints(Hints.create()
                                .withReadShards(1000))
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)))
                .apply("Process assets", ParDo.of(new PrepareAssets(sourceAssetsIdMap))
                        .withSideInputs(sourceAssetsIdMap))
                .apply("Filter assets", ParDo.of(new DoFn<KV<String, Asset>, KV<String, Asset>>() {
                    @ProcessElement
                    public void processElement(@Element KV<String, Asset> input,
                                               OutputReceiver<KV<String, Asset>> out,
                                               ProcessContext context) {
                        List<String> blacklist = context.sideInput(assetBlacklist);
                        List<String> whitelist = context.sideInput(assetWhitelist);

                        if (!blacklist.isEmpty()) {
                            if (blacklist.contains(input.getKey())) {
                                LOG.info("Blacklist match root externalId {}. Asset [{}] will be dropped.",
                                        input.getKey(), input.getValue().getExternalId().getValue());
                                return;
                            }
                        }

                        if (whitelist.contains("*")) {
                            out.output(input);
                        } else if (whitelist.contains(input.getKey())) {
                            LOG.info("Whitelist match root externalId {}. Asset [{}] will be included.",
                                    input.getKey(), input.getValue().getExternalId().getValue());
                            out.output(input);
                        }
                    }
                }).withSideInputs(assetBlacklist, assetWhitelist))
                .apply("Synchronize target assets", CogniteIO.synchronizeHierarchies()
                        .withProjectConfigFile(options.getCdfTargetConfigFile())
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
