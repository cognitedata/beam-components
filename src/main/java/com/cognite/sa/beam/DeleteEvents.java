package com.cognite.sa.beam;

import com.cognite.beam.io.CogniteIO;
import com.cognite.beam.io.config.Hints;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.config.WriterConfig;
import com.cognite.beam.io.dto.Item;
import com.cognite.beam.io.servicesV1.RequestParameters;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * This pipeline deletes events.
 *
 * This job is prepared to be deployed as a template on GCP (Dataflow) + can be executed directly on any runner.
 */
public class DeleteEvents {
    // The log to output status messages to.
    private static final Logger LOG = LoggerFactory.getLogger(DeleteEvents.class);
    private static final String appIdentifier = "DeleteEvents";

    /**
     * Custom options for this pipeline.
     */
    public interface DeleteEventsOptions extends PipelineOptions {
        /**
         * Specify the Cdf config file.
         */
        @Description("The cdf config file. The name should be in the format of gs://<bucket>/folder.")
        @Validation.Required
        ValueProvider<String> getCdfConfigFile();
        void setCdfConfigFile(ValueProvider<String> value);

    }

    /**
     * Setup the main pipeline structure and run it:
     * - Read a set of events.
     * - Convert them to items and send delete requests.
     *
     * @param options
     */
    private static PipelineResult runDeleteEvents(DeleteEventsOptions options) throws IOException {
        Pipeline p = Pipeline.create(options);

        PCollection<Item> deletedEvents = p
                .apply("Read source events", CogniteIO.readEvents()
                        .withProjectConfigFile(options.getCdfConfigFile())
                        .withHints(Hints.create()
                                .withReadShards(1000))
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier))
                        .withRequestParameters(RequestParameters.create()
                                //.withFilterParameter("type", "Valhall_AMS.EventLog")))
                .apply("Build items", MapElements.into(TypeDescriptor.of(Item.class))
                        .via(event ->
                                Item.newBuilder()
                                        .setId(event.getId().getValue())
                                        .build()))
                .apply("Delete", CogniteIO.deleteEvents()
                        .withProjectConfigFile(options.getCdfConfigFile())
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
        DeleteEventsOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DeleteEventsOptions.class);
        runDeleteEvents(options);
    }
}
