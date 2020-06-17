. $PSScriptRoot'\..\..\variables.ps1'
. $PSScriptRoot'\..\base-config.ps1'
$postfix = Get-Postfix

mvn compile exec:java -D exec.mainClass=com.cognite.sa.beam.replicate.ReplicateAssets -D exec.args="--project=$gcpProject --runner=DataFlowRunner --gcpTempLocation=gs://$gcpBucketPrefix$postfix/temp --stagingLocation=gs://$gcpBucketPrefix$postfix/template-stage/replicate/replicate-assets --region=europe-west1 --templateLocation=gs://$gcpBucketPrefix$postfix/template/replicate/replicate-assets --experiments=shuffle_mode=service --maxNumWorkers=4 --experiments=enable_stackdriver_agent_metrics --workerMachineType=e2-standard-2"