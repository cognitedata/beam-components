. $PSScriptRoot'\..\..\variables.ps1'
. $PSScriptRoot'\..\base-config.ps1'
$postfix = Get-Postfix

mvn compile exec:java -D exec.mainClass=com.cognite.sa.beam.replicate.ReplicateTs -D exec.args="--project=$gcpProject --runner=DataFlowRunner --gcpTempLocation=$gcpBucketPrefix$postfix/temp --stagingLocation=$gcpBucketPrefix$postfix/template-stage/replicate/replicate-ts --region=europe-west1 --templateLocation=$gcpBucketPrefix$postfix/template/replicate/replicate-ts --experiments=shuffle_mode=service --numWorkers=2 --maxNumWorkers=3 --experiments=enable_stackdriver_agent_metrics --workerMachineType=e2-standard-2"

#--templateLocation=$gcpBucketPrefix$postfix/template/replicate/replicate-ts