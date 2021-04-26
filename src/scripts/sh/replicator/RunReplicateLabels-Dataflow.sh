export gcpProject='akerbp-eureka-sa-sandbox'
# The gcp bucket prefix for dataflow artifacts
export gcpBucketPrefix='gs://akerbp-dataflow-test'

export cdfInputSecret='919896902461.akerbp-test-svc-replicator'
export cdfTargetSecret='919896902461.akerbp-dev-svc-replicator'

# The CDF host
export cdfInputHost='https://api.cognitedata.com'
export cdfTargetHost='https://api.cognitedata.com'

# Delta read override
export fullRead='true'

#---------------------------------------------------------------------
# The delta identifier
#---------------------------------------------------------------------
export deltaIdentifier='label-replicator'

mvn compile exec:java -Dexec.mainClass=com.cognite.sa.beam.replicate.ReplicateLabels -Dexec.args="--cdfInputSecret=${cdfInputSecret} --cdfInputHost=${cdfInputHost} --cdfTargetSecret=${cdfTargetSecret} --cdfTargetHost=${cdfTargetHost} --project=${gcpProject} --runner=DataFlowRunner --gcpTempLocation=${gcpBucketPrefix}/temp --stagingLocation=${gcpBucketPrefix}/stage/replicate/replicate-labels --region=europe-west1 --experiments=shuffle_mode=service --numWorkers=1 --maxNumWorkers=2 --experiments=enable_stackdriver_agent_metrics --workerMachineType=e2-standard-2"

# In order to add profiling:
# --profilingAgentConfiguration='{\""APICurated\"" : true}'
