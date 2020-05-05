package com.cognite.sa.beam.bq;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class CdfRawBQTest {

    @Disabled
    @Test
    void main() throws Exception{

        String[] args = new String[4];
        args[0] = "--cdfSecret=952879457502.omv-dataflow-test";
        args[1] = "--bqTempStorage=gs://omv-dataflow-test/temp";
        args[2] = "--outputMainTable=cognite-omv:cdf_dev.cdf_raw";
        args[3] = "--fullRead=true";
        CdfRawBQ.main(args);

    }
}
