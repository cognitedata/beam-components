. $PSScriptRoot'\..\..\variables.ps1'
. $PSScriptRoot'\..\base-config.ps1'
$postfix = Get-Postfix

mvn compile exec:java -D exec.mainClass=com.cognite.sa.beam.bq.CdfEventsBQ -D exec.args="--project=$gcpProject --runner=DataFlowRunner --gcpTempLocation=gs://$gcpBucketPrefix$postfix/temp --stagingLocation=$gcpBucketPrefix$postfix/template-stage/bq/cdf-relationships-bq --templateLocation=$gcpBucketPrefix$postfix/template/bq/cdf-relationships-bq --region=europe-west1 --experiments=shuffle_mode=service --numWorkers=2 --maxNumWorkers=2 --experiments=enable_stackdriver_agent_metrics"