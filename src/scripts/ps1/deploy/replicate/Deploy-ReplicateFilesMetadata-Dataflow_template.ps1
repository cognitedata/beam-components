. $PSScriptRoot'\..\..\variables.ps1'
. $PSScriptRoot'\..\base-config.ps1'
$postfix = Get-Postfix
# Default variable values are picked up from the above files.
# You can override them with local variable definitions in this file.
#
#-------------------------------------------------------------------

mvn compile exec:java -D exec.mainClass=com.cognite.sa.beam.replicate.ReplicateFilesMetadata -D exec.args="--project=$gcpProject --runner=DataFlowRunner --gcpTempLocation=$gcpBucketPrefix$postfix/temp --stagingLocation=$gcpBucketPrefix$postfix/template-stage/replicate/replicate-files --templateLocation=$gcpBucketPrefix$postfix/template/replicate/replicate-files --region=europe-west1 --numWorkers=1 --maxNumWorkers=2 --experiments=enable_stackdriver_agent_metrics --workerMachineType=e2-standard-4"
