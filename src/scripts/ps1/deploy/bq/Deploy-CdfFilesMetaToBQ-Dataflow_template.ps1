. $PSScriptRoot'\..\..\variables.ps1'
. $PSScriptRoot'\..\base-config.ps1'
$postfix = Get-Postfix

mvn compile exec:java -D exec.mainClass=com.cognite.sa.beam.bq.CdfFilesBQ -D exec.args="--project=$gcpProject --runner=DataFlowRunner --gcpTempLocation=gs://akerbp-dataflow$postfix/temp --stagingLocation=$gcpBucketPrefix$postfix/template-stage/bq/cdf-files-bq --templateLocation=$gcpBucketPrefix$postfix/template/bq/cdf-files-bq --region=europe-west1 --experiments=shuffle_mode=service --numWorkers=1 --maxNumWorkers=2 --experiments=enable_stackdriver_agent_metrics"