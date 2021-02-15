package com.cognite.sa.beam.bq;

import com.cognite.beam.io.CogniteIO;
import com.cognite.beam.io.config.GcpSecretConfig;
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.dto.*;
import com.cognite.beam.io.servicesV1.RequestParameters;
import com.cognite.beam.io.servicesV1.parser.SequenceParser;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Value;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CdfSequenceRowsBQ {

    private static final Logger LOG = LoggerFactory.getLogger(CdfSequenceRowsBQ.class);
    private static final String appIdentifier = "CdfSequenceRowsBQ";

    /* BQ output schema */
    private static final TableSchema SequenceRowsSchemaBQ = new TableSchema().setFields(ImmutableList.of(
            new TableFieldSchema().setName("seq_external_id").setType("STRING"),
            new TableFieldSchema().setName("row_number").setType("INT64"),
            new TableFieldSchema().setName("rows").setType("RECORD").setMode("REPEATED").setFields(ImmutableList.of(
                    new TableFieldSchema().setName("key").setType("STRING"),
                    new TableFieldSchema().setName("value").setType("STRING")
            )),
            new TableFieldSchema().setName("row_updated_time").setType("TIMESTAMP")
    ));

    /*
    Custom options for this pipeline
     */
    public interface CdfSequenceRowsBqOptions extends PipelineOptions {

        @Description("The GCP secret holding the source API key. The reference should be <projectId>.<secretId>")
        @Validation.Required
        ValueProvider<String> getCdfSecret();

        void setCdfSecret(ValueProvider<String> value);

        @Description("The CDF host name. The default value is https://api.cognitedata.com")
        @Validation.Required
        ValueProvider<String> getCdfHost();

        void setCdfHost(ValueProvider<String> value);

        @Description("The BQ table to write to. The name should be in the format <projectId>:<dataset>.<table>")
        @Validation.Required
        ValueProvider<String> getOutputMainTable();

        void setOutputMainTable(ValueProvider<String> value);

        @Description("The BQ temp storage location. Used for temp staging of writes to BQ. "
                + "The name should be in the format of gs://<bucket>/folder.")
        @Validation.Required
        ValueProvider<String> getBqTempStorage();

        void setBqTempStorage(ValueProvider<String> value);
    }

    /*
    Setup the main pipeline and run it
     */
    private static PipelineResult runCdfSequenceRowsBQ(CdfSequenceRowsBqOptions options) {

        GcpSecretConfig secretConfig = GcpSecretConfig.of(
                ValueProvider.NestedValueProvider.of(options.getCdfSecret(), secret -> secret.split("\\.")[0]),
                ValueProvider.NestedValueProvider.of(options.getCdfSecret(), secret -> secret.split("\\.")[1]));
        ProjectConfig projectConfig = ProjectConfig.create()
                .withApiKeyFromGcpSecret(secretConfig)
                .withHost(options.getCdfHost());

        Pipeline p = Pipeline.create(options);

        // Read seq headers
        PCollection<SequenceMetadata> sequenceHeaders = p.apply("Read CDF sequence headers", CogniteIO.readSequencesMetadata()
                .withProjectConfig(projectConfig)
                .withReaderConfig(ReaderConfig.create()
                        .withAppIdentifier(appIdentifier)
                ));

        PCollection<SequenceBody> sequenceBodies = sequenceHeaders
                .apply("Map to read Sequence rows requests", MapElements
                        .into(TypeDescriptor.of(RequestParameters.class))
                        .via((SequenceMetadata input) -> {
                            int limit = 10000;
                            Map<String, Object> requestParameters = new HashMap<String, Object>();
                            requestParameters.put("id", input.getId().getValue());
                            return RequestParameters.create()
                                    .withRequestParameters(requestParameters)
                                    .withRootParameter("limit", limit);

                        })
                )
                .apply("Read Sequence rows", CogniteIO.readAllSequenceRows()
                        .withProjectConfig(projectConfig)
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)
                        ));


        // Todo: SequenceBody with many rows -> Many SequenceBody objects with one row each
        PCollection<SequenceBody> sequenceBody1Row = sequenceBodies.apply("Flatten SequenceBody rows",
                ParDo.of(new DoFn<SequenceBody, SequenceBody>() {
                    @ProcessElement
                    public void processElement(@Element SequenceBody input,
                                               OutputReceiver<SequenceBody> out,
                                               ProcessContext context) {
                        for (SequenceRow row : input.getRowsList())  {
                            long rowNumber = row.getRowNumber();
                            out.output(input.toBuilder()
                                    .clearRows()
                                    .addRows(row)
                                    .build());
                        }
                    }
                }));

        sequenceBody1Row.apply("Write output to BQ", BigQueryIO.<SequenceBody>write()
                .to(options.getOutputMainTable())
                .withSchema(SequenceRowsSchemaBQ)
                .withFormatFunction((SequenceBody element) -> {
                    List<TableRow> rows = new ArrayList<>();
                    DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;
                    SequenceRow row = element.getRows(element.getRowsCount()-1);

                    for (int i = 0; i < element.getColumnsCount(); i++) {

                        String stringValue;
                        Value value = row.getValues(i);
                        switch(value.getKindCase().getNumber()) {
                            case 2:
                                stringValue = Double.toString(value.getNumberValue());
                                break;
                            case 3:
                                stringValue = value.getStringValue();
                                break;
                            case 4:
                                stringValue = Boolean.toString(value.getBoolValue());
                                break;
                            default:
                                stringValue = "";
                                break;
                        }

                        rows.add(new TableRow()
                                .set("key", element.getColumns(i).getExternalId())
                                .set("value", stringValue));
                    }

                    return new TableRow()
                            .set("seq_external_id", element.hasExternalId() ? element.getExternalId().getValue() : null)
                            .set("row_number", row.getRowNumber())
                            .set("rows", rows)
                            .set("row_updated_time", formatter.format(Instant.now()));

                })
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                .optimizedWrites()
                .withCustomGcsTempLocation(options.getBqTempStorage()));

        return p.run();
    }

    public static void main(String[] args) throws IOException {
        CdfSequenceRowsBqOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(CdfSequenceRowsBqOptions.class);
        runCdfSequenceRowsBQ(options);
    }

}
