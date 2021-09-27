. $PSScriptRoot'\..\..\variables.ps1'
. $PSScriptRoot'\..\base-config.ps1'
$postfix = Get-Postfix

mvn compile exec:java -D exec.mainClass=com.cognite.sa.beam.bq.CdfDataSetsBQ -D exec.args="--project=$gcpProject --runner=DataFlowRunner --gcpTempLocation=$gcpBucketPrefix$postfix/temp --stagingLocation=$gcpBucketPrefix$postfix/template-stage/bq/cdf-cdf-data-sets-bq --templateLocation=$gcpBucketPrefix$postfix/template/bq/cdf-data-sets-bq --region=europe-west1 --experiments=shuffle_mode=service --numWorkers=1 --maxNumWorkers=4 --experiments=enable_stackdriver_agent_metrics --workerMachineType=e2-standard-2"