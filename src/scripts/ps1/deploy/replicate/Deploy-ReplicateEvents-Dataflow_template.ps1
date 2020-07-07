. $PSScriptRoot'\..\..\variables.ps1'
. $PSScriptRoot'\..\base-config.ps1'
$postfix = Get-Postfix
# Default variable values are picked up from the above files.
# You can override them with local variable definitions in this file.
#
#-------------------------------------------------------------------

mvn compile exec:java -D exec.mainClass=com.cognite.sa.beam.replicate.ReplicateEvents -D exec.args="--project=$gcpProject --runner=DataFlowRunner --gcpTempLocation=gs://$gcpBucketPrefix$postfix/temp --stagingLocation=gs://$gcpBucketPrefix$postfix/template-stage/replicate/replicate-events --templateLocation=gs://$gcpBucketPrefix$postfix/template/replicate/replicate-events --region=europe-west1 --experiments=shuffle_mode=service --numWorkers=1 --maxNumWorkers=4 --experiments=enable_stackdriver_agent_metrics --workerMachineType=e2-standard-2"