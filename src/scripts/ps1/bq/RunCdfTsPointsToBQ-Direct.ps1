# The CDF config file (host, instance/project, api)
$cdfConfigFile = $Env:EXAMPLE_CONFIG_FILE #ASSIGN TO LOCAL CONFIG FILE

# The destination table in BQ
$outputMainTable = 'cognite-sa-sandbox:stage.cdf_ts_points'

# BQ temp storage
$bqTempStorage = 'gs://beam-component/temp'

# Full read. Set to _true_ for full read, _false_ for delta read
$fullRead = 'true'

mvn compile exec:java -D exec.mainClass=com.cognite.sa.beam.bq.CdfTsPointsBQ -D exec.args="--cdfConfigFile=$cdfConfigFile --bqTempStorage=$bqTempStorage --outputMainTable=$outputMainTable --fullRead=$fullRead"