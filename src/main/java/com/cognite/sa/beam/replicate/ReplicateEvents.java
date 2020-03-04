package com.cognite.sa.beam.replicate;

import com.cognite.beam.io.CogniteIO;
import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.config.WriterConfig;
import com.cognite.beam.io.dto.*;
import com.cognite.beam.io.transform.toml.ReadTomlStringMap;
import com.google.protobuf.Int64Value;
import com.google.protobuf.StringValue;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

    /**
     * Create target event:
     *         - Remove system fields (id, created date, last updated date)
     *         - Translate the asset id links
     *         - If the headers does not have externalId, use the source's id
     */
    private static class PrepareEvents extends DoFn<Event, Event> {
        PCollectionView<Map<String, String>> configMap;
        PCollectionView<Map<Long, String>> sourceAssetsIdMapView;
        PCollectionView<Map<String, Long>> targetAssetsIdMapView;

        PrepareEvents(PCollectionView<Map<String, String>> configMap,
                      PCollectionView<Map<Long, String>> sourceAssetsIdMap,
                      PCollectionView<Map<String, Long>> targetAssetsIdMapView) {
            this.configMap = configMap;
            this.sourceAssetsIdMapView = sourceAssetsIdMap;
            this.targetAssetsIdMapView = targetAssetsIdMapView;
        }

        @ProcessElement
        public void processElement(@Element Event input,
                                   OutputReceiver<Event> out,
                                   ProcessContext context) {
            Map<Long, String> sourceAssetsIdMap = context.sideInput(sourceAssetsIdMapView);
            Map<String, Long> targetAssetsIdMap = context.sideInput(targetAssetsIdMapView);
            Map<String, String> config = context.sideInput(configMap);

            Event.Builder builder = Event.newBuilder(input);
            builder.clearAssetIds();
            builder.clearCreatedTime();
            builder.clearLastUpdatedTime();
            builder.clearId();

            if (!input.hasExternalId()) {
                builder.setExternalId(StringValue.of(String.valueOf(input.getId().getValue())));
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
    private static PipelineResult runReplicateEvents(ReplicateEventsOptions options) throws IOException {
        Pipeline p = Pipeline.create(options);

       /*
        Read the job config file and parse into views.
        Config maps are published as views so they can be used by the transforms as side inputs.
         */
        PCollectionView<Map<String, String>> configMap = p
                .apply("Read config map", ReadTomlStringMap.from(options.getJobConfigFile())
                        .withMapKey("config"))
                .apply("to map view", View.asMap());

        /*
        Read the asset hierarchies from source and target. Shave off the metadata and use id + externalId
        as the basis for mapping TS to assets. Project the resulting asset collections as views so they can be
        used as side inputs to the main Event transform.
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
        Read events from source and parse them:
        - Remove system fields (id, created date, last updated date)
        - Translate the asset id links (to be done)

        Write the prepared events to target.
         */
        PCollection<Event> eventPCollection = p
                .apply("Read source events", CogniteIO.readEvents()
                        .withProjectConfigFile(options.getCdfSourceConfigFile())
                        .withHints(Hints.create()
                                .withReadShards(1000))
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)))
                .apply("Process events", ParDo.of(new PrepareEvents(configMap, sourceAssetsIdMap, targetAssetsIdMap))
                        .withSideInputs(configMap, sourceAssetsIdMap, targetAssetsIdMap))
                .apply("Write target events", CogniteIO.writeEvents()
                        .withProjectConfigFile(options.getCdfTargetConfigFile())
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
