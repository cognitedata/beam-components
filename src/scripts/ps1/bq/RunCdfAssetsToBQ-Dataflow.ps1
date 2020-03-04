# The CDF config file (host, instance/project, api)
$cdfConfigFile = 'gs://beam-component/config/project-config-test.toml'

# The destination table in BQ
$outputMainTable = 'cognite-sa-sandbox:stage.cdf_asset'

# BQ temp storage
$bqTempStorage = 'gs://beam-component/temp'

# Full read. Set to _true_ for full read, _false_ for delta read
$fullRead = 'true'

mvn compile exec:java -D exec.mainClass=com.cognite.sa.beam.bq.CdfAssetsBQ -D exec.args="--cdfConfigFile=$cdfConfigFile --bqTempStorage=$bqTempStorage --outputMainTable=$outputMainTable --fullRead=$fullRead --project=cognite-sa-sandbox --runner=DataFlowRunner --gcpTempLocation=gs://beam-componet/temp --stagingLocation=gs://beam-component/stage/cdf-assets-bq --region=europe-west1 --experiments=shuffle_mode=service --numWorkers=4 --maxNumWorkers=4 --experiments=enable_stackdriver_agent_metrics"