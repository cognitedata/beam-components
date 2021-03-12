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
$cdfInputSecret = '719303257135.pgs-migration-source-project'
$cdfOutputSecret = '719303257135.pgs-migration-dest'
#$cdfInputSecret = '223824617234.svc-noc-pre-prod-replicator'
#$cdfOutputSecret = '223824617234.svc-noc-azure-dev'

# The CDF host
$cdfInputHost = 'https://api.cognitedata.com'
$cdfOutputHost = 'https://pgs.cognitedata.com'

# Delta read override
$fullRead = 'true'