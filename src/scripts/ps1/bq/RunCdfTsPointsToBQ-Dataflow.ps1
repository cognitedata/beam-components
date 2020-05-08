. $PSScriptRoot'\..\variables.ps1'
. $PSScriptRoot'\bqVariables.ps1'
# Default variable values are picked up from the above file.
# You can override them with local variable definitions here.

# The CDF config file (host, instance/project, api)
#$cdfConfigFile = 'gs://beam-component/config/project-config-test.toml'

# The CDF config file (instance, key, host)
#$cdfSecret = 'projectId.secretId'

# The destination table in BQ
$outputMainTable = $outputDataSet + '.cdf_ts_point'

# BQ temp storage
#$bqTempStorage = 'gs://beam-component/temp'

# Full read. Set to _true_ for full read, _false_ for delta read
$fullRead = 'true'

mvn compile exec:java -D exec.mainClass=com.cognite.sa.beam.bq.CdfTsPointsBQ -D exec.args="--cdfSecret=$cdfSecret --cdfHost=$cdfHost --bqTempStorage=$bqTempStorage --outputMainTable=$outputMainTable --fullRead=$fullRead --project=$gcpProject --runner=DataFlowRunner --gcpTempLocation=gs://$gcpBucketPrefix-test/temp --stagingLocation=gs://$gcpBucketPrefix-test/stage/cdf-ts-points-bq --region=europe-west1 --experiments=shuffle_mode=service --numWorkers=20 --maxNumWorkers=20 --experiments=enable_stackdriver_agent_metrics"