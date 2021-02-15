package com.cognite.sa.beam.bq;

import com.cognite.beam.io.CogniteIO;
import com.cognite.beam.io.config.GcpSecretConfig;
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.dto.SequenceColumn;
import com.cognite.beam.io.dto.SequenceMetadata;
import com.cognite.beam.io.servicesV1.parser.SequenceParser;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CdfSequenceColumnsBQ {

    private static final Logger LOG = LoggerFactory.getLogger(CdfSequenceRowsBQ.class);
    private static final String appIdentifier = "CdfSequenceColumnsBQ";

    /* BQ output schema */
    private static final TableSchema SequenceColumnsSchemaBQ = new TableSchema().setFields(ImmutableList.of(
            new TableFieldSchema().setName("name").setType("STRING"),
            new TableFieldSchema().setName("external_id").setType("STRING"),
            new TableFieldSchema().setName("seq_external_id").setType("STRING"),
            new TableFieldSchema().setName("description").setType("STRING"),
            new TableFieldSchema().setName("value_type").setType("STRING"),
            new TableFieldSchema().setName("metadata").setType("RECORD").setMode("REPEATED").setFields(ImmutableList.of(
                    new TableFieldSchema().setName("key").setType("STRING"),
                    new TableFieldSchema().setName("value").setType("STRING")
            )),
            new TableFieldSchema().setName("created_time").setType("TIMESTAMP"),
            new TableFieldSchema().setName("last_updated_time").setType("TIMESTAMP"),
            new TableFieldSchema().setName("row_updated_time").setType("TIMESTAMP")
    ));

    public interface CdfSequenceColumnsBqOptions extends PipelineOptions {

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


    public static PipelineResult runCdfSequenceColumnsBQ(CdfSequenceColumnsBqOptions options) {
        GcpSecretConfig secretConfig = GcpSecretConfig.of(
                ValueProvider.NestedValueProvider.of(options.getCdfSecret(), secret -> secret.split("\\.")[0]),
                ValueProvider.NestedValueProvider.of(options.getCdfSecret(), secret -> secret.split("\\.")[1]));
        ProjectConfig projectConfig = ProjectConfig.create()
                .withApiKeyFromGcpSecret(secretConfig)
                .withHost(options.getCdfHost());

        Pipeline p = Pipeline.create(options);

        // Read seq headers
        PCollection<SequenceMetadata> sequenceHeaders = p.apply("Read CDF Sequence headers", CogniteIO.readSequencesMetadata()
                .withProjectConfig(projectConfig)
                .withReaderConfig(ReaderConfig.create()
                        .withAppIdentifier(appIdentifier)
                ));

        // KV of sequence external id and sequence column
        PCollection<KV<String, SequenceColumn>> sequenceExtIdColumns = sequenceHeaders.apply("Unpack Sequence columns",
                ParDo.of(new DoFn<SequenceMetadata, KV<String, SequenceColumn>>() {
                    @ProcessElement
                    public void processElement(@Element SequenceMetadata input,
                                               OutputReceiver<KV<String, SequenceColumn>> out,
                                               ProcessContext context) {
                        List<SequenceColumn> sequenceColumnList = input.getColumnsList();
                        for (SequenceColumn column : sequenceColumnList) {
                            out.output(KV.of(
                                    input.getExternalId().getValue(),
                                    column
                            ));
                        }
                    }
                }));

        sequenceExtIdColumns.apply("Write output to BQ", BigQueryIO.<KV<String, SequenceColumn>>write()
                .to(options.getOutputMainTable())
                .withSchema(SequenceColumnsSchemaBQ)
                .withFormatFunction((KV<String, SequenceColumn> element) -> {
                    List<TableRow> metadata = new ArrayList<>();
                    DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;

                    String seqExternalId = element.getKey();
                    SequenceColumn column = element.getValue();

                    for (Map.Entry<String, String> mElement : column.getMetadataMap().entrySet()) {
                        metadata.add(new TableRow()
                                .set("key", mElement.getKey())
                                .set("value", mElement.getValue()));
                    }

                    String valueTypeString = SequenceParser.toString(column.getValueType());

                    return new TableRow()
                            .set("name", column.hasName() ? column.getName().getValue() : null)
                            .set("external_id", column.getExternalId())
                            .set("seq_external_id", seqExternalId)
                            .set("description", column.hasDescription() ? column.getDescription().getValue() : null)
                            .set("value_type", valueTypeString)
                            .set("metadata", metadata)
                            .set("created_time", column.hasCreatedTime() ?
                                    formatter.format(Instant.ofEpochMilli(column.getCreatedTime().getValue())) : null)
                            .set("last_updated_time", column.hasLastUpdatedTime() ?
                                    formatter.format(Instant.ofEpochMilli(column.getLastUpdatedTime().getValue())) : null)
                            .set("row_updated_time", formatter.format(Instant.now()));
                })
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                .optimizedWrites()
                .withCustomGcsTempLocation(options.getBqTempStorage()));

        return p.run();

    }

    public static void main(String[] args) throws IOException {
        CdfSequenceColumnsBqOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(CdfSequenceColumnsBqOptions.class);
        runCdfSequenceColumnsBQ(options);
    }

}
