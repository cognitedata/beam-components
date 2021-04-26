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
import com.cognite.beam.io.RequestParameters;
import com.cognite.beam.io.config.*;
import com.cognite.client.config.UpsertMode;
import com.cognite.client.dto.Label;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * This pipeline reads Labels from the specified cdf instance and writes them to a target cdf.
 * <p>
 * The Labels can be linked to resources at the target based on externalId mapping from the source.
 * <p>
 * This job is prepared to be deployed as a template on GCP (Dataflow) + can be executed directly on any runner.
 */
public class ReplicateLabels {
    // The log to output status messages to.
    private static final Logger LOG = LoggerFactory.getLogger(ReplicateLabels.class);
    private static final String appIdentifier = "Replicate_Label";

    /**
     * Create target event:
     * - Remove system fields (id, created date, last updated date)
     * - Translate the asset id links
     * - Map data set ids
     */
    private static class PrepareLabels extends DoFn<Label, Label> {

        @ProcessElement
        public void processElement(@Element Label input,
                                   OutputReceiver<Label> out,
                                   ProcessContext context) {
            Label.Builder builder = input.toBuilder()
                    .clearCreatedTime();
            out.output(builder.build());
        }
    }

    /**
     * Custom options for this pipeline.
     */
    public interface ReplicateLabelsOptions extends PipelineOptions {
        @Description("The GCP secret holding the source api key. The reference should be <projectId>.<secretId>")
        @Validation.Required
        ValueProvider<String> getCdfInputSecret();

        void setCdfInputSecret(ValueProvider<String> value);

        @Description("The CDF source host name. The default value is https://api.cognitedata.com")
        @Default.String("https://api.cognitedata.com")
        ValueProvider<String> getCdfInputHost();

        void setCdfInputHost(ValueProvider<String> value);

        @Description("The GCP secret holding the target api key. The reference should be <projectId>.<secretId>")
        @Validation.Required
        ValueProvider<String> getCdfTargetSecret();

        void setCdfTargetSecret(ValueProvider<String> value);

        @Description("The CDF source host name. The default value is https://api.cognitedata.com")
        @Default.String("https://api.cognitedata.com")
        ValueProvider<String> getCdfTargetHost();

        void setCdfTargetHost(ValueProvider<String> value);

    }

    /**
     * Setup the main pipeline structure and run it:
     * - Read the config settings
     * - Read the datasets from both source and target (in order to map the labels to the proper dataset)
     * - Replicate the labels
     *
     * @param options
     * @return
     * @throws IOException
     */
    private static PipelineResult runReplicateLabels(ReplicateLabelsOptions options) throws IOException {
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
                ValueProvider.NestedValueProvider.of(options.getCdfTargetSecret(), secret -> secret.split("\\.")[0]),
                ValueProvider.NestedValueProvider.of(options.getCdfTargetSecret(), secret -> secret.split("\\.")[1]));
        ProjectConfig targetConfig = ProjectConfig.create()
                .withApiKeyFromGcpSecret(targetSecret)
                .withHost(options.getCdfTargetHost());

        Pipeline p = Pipeline.create(options);

        /*
        Read Labels from source and parse them:
        - Remove system fields ( last updated time)
        - Translate data set id link

        Write the prepared labels to target
         */
        PCollection<Label> labelPCollection = p
                .apply("Build basic query", Create.of(RequestParameters.create()))
                .apply("Read source Labels", CogniteIO.readAllLabels()
                        .withProjectConfig(sourceConfig)
                        .withHints(Hints.create())
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)))
                .apply("Prepare Labels", ParDo.of(new PrepareLabels()))
                .apply("Write target Labels", CogniteIO.writeLabels()
                        .withProjectConfig(targetConfig)
                        .withHints(Hints.create()
                                .withWriteShards(2))
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier(appIdentifier)
                                .withUpsertMode(UpsertMode.REPLACE)));

        return p.run();
    }

    /**
     * Read the pipeline options from args and run the pipeline.
     *
     * @param args
     */
    public static void main(String[] args) throws IOException {
        ReplicateLabelsOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(ReplicateLabelsOptions.class);
        runReplicateLabels(options);
    }
}