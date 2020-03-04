# The CDF source config file (instance, key, host)
cdfSourceConfigFile='gs://beam-component/config/config-project-config-prod.toml'

# The CDF target config file (instance, key, host)
cdfTargetConfigFile='gs://beam-component/config/config-project-config-test.toml'

# The job config file
jobConfigFile='gs://beam-component/config/config-project-config-test.toml'

mvn compile exec:java -D exec.mainClass=com.cognite.sa.beam.replicate.ReplicateTs -D exec.args="--cdfSourceConfigFile=${cdfSourceConfigFile} --cdfTargetConfigFile=${cdfTargetConfigFile} --jobConfigFile=${jobConfigFile} --project=cognite-sa-sandbox --runner=DataFlowRunner --gcpTempLocation=gs://akso-dataflow-test/temp --stagingLocation=gs://beam-component/stage/replicate-ts --region=europe-west1 --experiments=shuffle_mode=service --numWorkers=4 --maxNumWorkers=4 --workerMachineType=n2-standard-2"

# Debug
# --workerLogLevelOverrides='{\""com.cognite.beam.io\"":\""DEBUG\""}'

# In order to add profiling:
# --profilingAgentConfiguration='{\""APICurated\"" : true}'
