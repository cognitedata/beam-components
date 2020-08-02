. $PSScriptRoot'\..\..\variables.ps1'
. $PSScriptRoot'\..\base-config.ps1'
$postfix = Get-Postfix

mvn compile exec:java -D exec.mainClass=com.cognite.sa.akerbp.bq.CdfTsHeaderBQ -D exec.args="--project=$gcpProject --runner=DataFlowRunner --gcpTempLocation=$gcpBucketPrefix$postfix/temp --stagingLocation=$gcpBucketPrefix$postfix/template-stage/bq/cdf-ts-header-bq --region=europe-west1 --templateLocation=$gcpBucketPrefix$postfix/template/bq/cdf-ts-header-bq --experiments=shuffle_mode=service --maxNumWorkers=2 --experiments=enable_stackdriver_agent_metrics --workerMachineType=e2-standard-2"