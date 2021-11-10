. $PSScriptRoot'\..\..\variables.ps1'
. $PSScriptRoot'\..\base-config.ps1'
$postfix = Get-Postfix

mvn compile exec:java -D exec.mainClass=com.cognite.sa.beam.bq.CdfSequenceColumnsBQ -D exec.args="--project=$gcpProject --runner=DataFlowRunner --gcpTempLocation=$gcpBucketPrefix$postfix/temp --stagingLocation=$gcpBucketPrefix$postfix/template-stage/bq/cdf-sequence-columns-bq --templateLocation=$gcpBucketPrefix$postfix/template/bq/cdf-sequence-columns-bq --region=europe-west1 --numWorkers=1 --maxNumWorkers=5 --experiments=enable_stackdriver_agent_metrics --workerMachineType=e2-standard-2"
