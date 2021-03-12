. $PSScriptRoot'\..\..\variables.ps1'
. $PSScriptRoot'\..\base-config.ps1'
$postfix = Get-Postfix
$postfix = ''
# Default variable values are picked up from the above files.
# You can override them with local variable definitions in this file.
#
#-------------------------------------------------------------------

mvn compile exec:java -D exec.mainClass=com.cognite.sa.beam.replicate.ReplicateAssets -D exec.args="--project=$gcpProject --runner=DataFlowRunner --gcpTempLocation=$gcpBucketPrefix$postfix/temp --stagingLocation=$gcpBucketPrefix$postfix/template-stage/replicate/replicate-assets --templateLocation=$gcpBucketPrefix$postfix/template/replicate/replicate-assets --region=europe-west1 --experiments=shuffle_mode=service --numWorkers=1 --maxNumWorkers=2 --experiments=enable_stackdriver_agent_metrics --workerMachineType=e2-standard-2"