# The CDF config file (host, instance/project, api)
$cdfConfigFile = 'gs://beam-component/config/project-config-test.toml'

# The destination table in BQ
$outputMainTable = 'cognite-sa-sandbox:stage.cdf_points_hour'

# BQ temp storage
$bqTempStorage = 'gs://beam-component/temp'

# Full read. Set to _true_ for full read, _false_ for delta read
$fullRead = 'true'

mvn compile exec:java -D exec.mainClass=com.cognite.sa.beam.bq.CdfTsPointsHourBQ -D exec.args="--cdfConfigFile=$cdfConfigFile --bqTempStorage=$bqTempStorage --outputMainTable=$outputMainTable --fullRead=$fullRead --project=cognite-sa-sandbox --runner=DataFlowRunner --gcpTempLocation=gs://beam-component/temp --stagingLocation=gs://beam-component/stage/cdf-ts-points-hour-bq --region=europe-west1 --experiments=shuffle_mode=service --numWorkers=15 --maxNumWorkers=15 --experiments=enable_stackdriver_agent_metrics"