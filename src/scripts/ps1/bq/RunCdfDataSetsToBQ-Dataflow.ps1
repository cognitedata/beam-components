. $PSScriptRoot'\..\variables.ps1'
. $PSScriptRoot'\bqVariables.ps1'
# Default variable values are picked up from the above file.
# You can override them with local variable definitions in this file.
#
#-------------------------------------------------------------------
# The CDF config file (host, api)
#
# In case you use a config file to communicate the api key (and api host)
# then set the reference here.
#
# Please note that the defaul is to use GCP Secret Manager for communicating the api key.
# If Secret Manager is unavailable (i.e. running on Flink in Azure), then use a
# config file instead.
#-------------------------------------------------------------------
#$cdfConfigFile = 'gs://beam-component/config/project-config-test.toml'

#-------------------------------------------------------------------
# The CDF api key and host.
#
# The api key is read from GCP Secret Manager. Add the Secret Manager reference
# to read from.
#
# CDF api host must be set if you use a dedicated tenant. For example:
# https://<myTenantHost>.cognitedata.com
#-------------------------------------------------------------------
#$cdfSecret = 'projectId.secretId'
#$cdfHost = 'https://api.cognitedata.com'

# The destination table in BQ
$outputMainTable = $outputDataSet + '.cdf_data_set'

# BQ temp storage
#$bqTempStorage = 'gs://beam-component/temp'

# Full read. Set to _true_ for full read, _false_ for delta read
#$fullRead = 'true'

mvn compile exec:java -D exec.mainClass=com.cognite.sa.beam.bq.CdfDataSetsBQ -D exec.args="--cdfSecret=$cdfSecret --cdfHost=$cdfHost --bqTempStorage=$bqTempStorage --outputMainTable=$outputMainTable --project=$gcpProject --runner=DataFlowRunner --gcpTempLocation=$gcpBucketPrefix-test/temp --stagingLocation=$gcpBucketPrefix-test/stage/cdf-data-sets-bq --region=europe-west1 --experiments=shuffle_mode=service --numWorkers=2 --maxNumWorkers=12 --experiments=enable_stackdriver_agent_metrics --workerMachineType=e2-standard-2"