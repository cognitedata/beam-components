#-------------------------------------------------------------------
# The CDF api key and host.
#
# The api key is read from GCP Secret Manager. Add the Secret Manager reference
# to read from.
#
# CDF api host must be set if you use a dedicated tenant. For example:
# https://<myTenantHost>.cognitedata.com
#-------------------------------------------------------------------
$cdfSecret = 'projectId.secretId'

# The CDF host
$cdfHost = 'https://api.cognitedata.com'

#-------------------------------------------------------------------
#
# The destination dataset in BQ
#-------------------------------------------------------------------
$outputDataSet = 'project:dataset'

# BQ temp storage
$bqTempStorage = 'gs://<your-gcp-bucket>/temp'

#-------------------------------------------------------------------
# Confugurations related to the BQ replicator behavior
#
# $fullRead - Set to _true_ for full read, _false_ for delta read
#-------------------------------------------------------------------
$fullRead = 'true'
