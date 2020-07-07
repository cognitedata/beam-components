#-------------------------------------------------------------------
# The CDF api key and host.
#
# The api key is read from GCP Secret Manager. Add the Secret Manager reference
# to read from.
#
# CDF api host must be set if you use a dedicated tenant. For example:
# https://<myTenantHost>.cognitedata.com
#-------------------------------------------------------------------
#$cdfInputSecret = 'projectId.secretId'
#$cdfOutputSecret = 'projectId.secretId'
$cdfInputSecret = '919896902461.akerbp-svc-read-timeseries-replicator'
#$cdfOutputSecret = '919896902461.akerbp-assets-svc-timeseries-replicator'
$cdfOutputSecret = '919896902461.akerbp-test-svc-replicator'

# The CDF host
$cdfInputHost = 'https://api.cognitedata.com'
$cdfOutputHost = 'https://api.cognitedata.com'
#$cdfOutputHost = 'https://greenfield.cognitedata.com'

# Delta read override
$fullRead = 'true'
 #>