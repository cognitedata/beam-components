. $PSScriptRoot'\..\..\variables.ps1'
. $PSScriptRoot'\..\base-config.ps1'
$postfix = Get-Postfix

mvn compile exec:java -D exec.mainClass=com.cognite.sa.beam.bq.CdfTsPointsHourBQ -D exec.args="--project=$gcpProject --runner=DataFlowRunner --gcpTempLocation=$gcpBucketPrefix$postfix/temp --stagingLocation=$gcpBucketPrefix$postfix/template-stage/cdf-ts-points-hour-bq --region=europe-west1 --templateLocation=$gcpBucketPrefix$postfix/template/bq/cdf-ts-points-hour-bq --experiments=shuffle_mode=service --numWorkers=2 --maxNumWorkers=4 --experiments=enable_stackdriver_agent_metrics --workerMachineType=e2-standard-2"