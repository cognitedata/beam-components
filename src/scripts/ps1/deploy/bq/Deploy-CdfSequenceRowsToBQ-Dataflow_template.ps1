. $PSScriptRoot'\..\..\variables.ps1'
. $PSScriptRoot'\..\base-config.ps1'
$postfix = Get-Postfix

mvn compile exec:java -D exec.mainClass=com.cognite.sa.beam.bq.CdfSequenceRowsBQ -D exec.args="--project=$gcpProject --runner=DataFlowRunner --gcpTempLocation=$gcpBucketPrefix$postfix/temp --stagingLocation=$gcpBucketPrefix$postfix/template-stage/bq/cdf-sequence-rows-bq --templateLocation=$gcpBucketPrefix$postfix/template/bq/cdf-sequence-rows-bq --region=europe-west1 --numWorkers=1 --maxNumWorkers=5 --experiments=enable_stackdriver_agent_metrics --workerMachineType=e2-standard-2"
