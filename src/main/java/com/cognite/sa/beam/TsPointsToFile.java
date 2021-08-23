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

package com.cognite.sa.beam;

import com.cognite.beam.io.CogniteIO;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.client.dto.TimeseriesPoint;
import com.cognite.beam.io.RequestParameters;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;

/**
 * This pipeline reads TS data points along a rolling time window from the specified cdp instance and writes
 * them to a target file.
 *
 * This job is prepared to be deployed as a template on GCP (Dataflow) + can be executed directly on any runner.
 */
public class TsPointsToFile {
    // The log to output status messages to.
    private static final Logger LOG = LoggerFactory.getLogger(TsPointsToFile.class);
    private static final String delimiter = "|";

    /* File output schema */
    private static final String fileHeader = new StringBuilder()
            .append("externalId")
            .append(delimiter).append("timestamp")
            .append(delimiter).append("timestamp_datetime")
            .append(delimiter).append("value")
            .toString();


    /**
     * Custom options for this pipeline.
     */
    public interface CdfTsPointsBqOptions extends PipelineOptions {
        @Description("The cdf config file."
                + "The name should be in the format of "
                + "gs://<bucket>/folder.")
        @Validation.Required
        ValueProvider<String> getCdfConfigFile();
        void setCdfConfigFile(ValueProvider<String> value);

        /**
         * Specify the output file location.
         */
        @Description("The output file storage location."
                + "The name should be in the format of "
                + "gs://<bucket>/folder.")
        @Default.String("./TsPoints_output")
        ValueProvider<String> getOutputFile();
        void setOutputFile(ValueProvider<String> value);
    }

    /**
     * Setup the main pipeline structure and run it.
     * @param options
     */
    private static PipelineResult runCdfTsPointsBQ(CdfTsPointsBqOptions options) throws IOException {
        Pipeline p = Pipeline.create(options);

        // Read ts points for a single TS
        PCollection<TimeseriesPoint> tsPoints = p
                .apply("Read ts points", CogniteIO.readTimeseriesPoints()
                        .withProjectConfigFile(options.getCdfConfigFile())
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("CdfTsPointsToFile"))
                        .withRequestParameters(RequestParameters.create()
                                .withItemExternalId("16HV0110/BCH/PRIM")
                                .withRootParameter("start", 1541030400000L) // Nov 1st, 2018
                                .withRootParameter("limit", 100000)));

        // Write to file
        tsPoints.apply("Map to string", MapElements.into(TypeDescriptors.strings())
                .via(tsPoint -> new StringBuilder()
                        .append(tsPoint.getExternalId())
                        .append(delimiter).append(tsPoint.getTimestamp())
                        .append(delimiter).append(Instant.ofEpochMilli(tsPoint.getTimestamp()).toString())
                        .append(delimiter).append(tsPoint.getValueNum())
                        .toString()))
                .apply("Write to file", TextIO.write().to(options.getOutputFile())
                        .withHeader(fileHeader)
                        .withSuffix(".txt")
                        .withoutSharding());

        return p.run();
    }

    /**
     * Read the pipeline options from args and run the pipeline.
     * @param args
     */
    public static void main(String[] args) throws IOException{
        CdfTsPointsBqOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(CdfTsPointsBqOptions.class);
        runCdfTsPointsBQ(options);
    }
}
