# named variables
CASE_SUCCESS = '__case_success__'
CASE_OPERATION = 'operation'
CASE_TITLE = 'title'
CASE_PARAMETERS = 'parameters'
CASE_ASSERTION = 'assertion'
CASE_ASSERTION_CODE = 'ResponseMetadata.HTTPStatusCode'
CASE_SUITES = 'suites'
CASE_SUITES_ARR = 'suites_arr'
CASE_SUITES_DICT = 'suites_dict'
CASE_CLIENT_NAME = 'clientName'
CASE_RESPONSE = 'response'

ORDER = '__order__'
SUITE_STATE_START = 1
SUITE_STATE_SKIPPED = 1 << 1
EXPORTERS = 'exporters'
ERROR_INFO = 'errorInfo'
ANONYMOUS = 'anonymous'
INCLUDE_FIELDS = 'include_fields'
SUITE_LOCALS = 'suiteLocals'
SUITE_FILTERS = 'suite_filters'
SUITE_ID = 'suite_id'
INCLUDES = 'includes'
EXCLUDES = 'excludes'

HIDE = '__hide__'
HIDE_ENABLED = 'hide_enabled'
NOT_HIDE = '__not_hide__'
RESET_HOOKS = 'resetHooks'
XMIND_SUITES = 'xmind_indices'
LOAD_XMIND_SUITES = 'load_xmind_suites'
LOAD_YAML_SUITES = 'load_yaml_suites'
EQUALS_IN_SIZE = '__equals_in_size__'
CLEAR_TREE_NODE = 'clear_tree_node'

CLIENT_PROPERTIES = ['service_name', 'region_name', 'api_version', 'use_ssl', 'verify', 'endpoint_url',
                     'aws_access_key_id', 'aws_secret_access_key', 'aws_session_token', 'config']

AWS_SERVICES = ['accessanalyzer', 'account', 'acm', 'acm-pca', 'alexaforbusiness', 'amp', 'amplify', 'amplifybackend',
                'amplifyuibuilder', 'apigateway', 'apigatewaymanagementapi', 'apigatewayv2', 'appconfig',
                'appconfigdata', 'appflow', 'appintegrations', 'application-autoscaling', 'application-insights',
                'applicationcostprofiler', 'appmesh', 'apprunner', 'appstream', 'appsync', 'athena', 'auditmanager',
                'autoscaling', 'autoscaling-plans', 'backup', 'backup-gateway', 'backupstorage', 'batch',
                'billingconductor', 'braket', 'budgets', 'ce', 'chime', 'chime-sdk-identity',
                'chime-sdk-media-pipelines', 'chime-sdk-meetings', 'chime-sdk-messaging', 'cloud9', 'cloudcontrol',
                'clouddirectory', 'cloudformation', 'cloudfront', 'cloudhsm', 'cloudhsmv2', 'cloudsearch',
                'cloudsearchdomain', 'cloudtrail', 'cloudwatch', 'codeartifact', 'codebuild', 'codecommit',
                'codedeploy', 'codeguru-reviewer', 'codeguruprofiler', 'codepipeline', 'codestar',
                'codestar-connections', 'codestar-notifications', 'cognito-identity', 'cognito-idp', 'cognito-sync',
                'comprehend', 'comprehendmedical', 'compute-optimizer', 'config', 'connect', 'connect-contact-lens',
                'connectcampaigns', 'connectcases', 'connectparticipant', 'controltower', 'cur', 'customer-profiles',
                'databrew', 'dataexchange', 'datapipeline', 'datasync', 'dax', 'detective', 'devicefarm', 'devops-guru',
                'directconnect', 'discovery', 'dlm', 'dms', 'docdb', 'drs', 'ds', 'dynamodb', 'dynamodbstreams', 'ebs',
                'ec2', 'ec2-instance-connect', 'ecr', 'ecr-public', 'ecs', 'efs', 'eks', 'elastic-inference',
                'elasticache', 'elasticbeanstalk', 'elastictranscoder', 'elb', 'elbv2', 'emr', 'emr-containers',
                'emr-serverless', 'es', 'events', 'evidently', 'finspace', 'finspace-data', 'firehose', 'fis', 'fms',
                'forecast', 'forecastquery', 'frauddetector', 'fsx', 'gamelift', 'gamesparks', 'glacier',
                'globalaccelerator', 'glue', 'grafana', 'greengrass', 'greengrassv2', 'groundstation', 'guardduty',
                'health', 'healthlake', 'honeycode', 'iam', 'identitystore', 'imagebuilder', 'importexport',
                'inspector', 'inspector2', 'iot', 'iot-data', 'iot-jobs-data', 'iot1click-devices',
                'iot1click-projects', 'iotanalytics', 'iotdeviceadvisor', 'iotevents', 'iotevents-data', 'iotfleethub',
                'iotfleetwise', 'iotsecuretunneling', 'iotsitewise', 'iotthingsgraph', 'iottwinmaker', 'iotwireless',
                'ivs', 'ivschat', 'kafka', 'kafkaconnect', 'kendra', 'keyspaces', 'kinesis',
                'kinesis-video-archived-media', 'kinesis-video-media', 'kinesis-video-signaling', 'kinesisanalytics',
                'kinesisanalyticsv2', 'kinesisvideo', 'kms', 'lakeformation', 'lambda', 'lex-models', 'lex-runtime',
                'lexv2-models', 'lexv2-runtime', 'license-manager', 'license-manager-user-subscriptions', 'lightsail',
                'location', 'logs', 'lookoutequipment', 'lookoutmetrics', 'lookoutvision', 'm2', 'machinelearning',
                'macie', 'macie2', 'managedblockchain', 'marketplace-catalog', 'marketplace-entitlement',
                'marketplacecommerceanalytics', 'mediaconnect', 'mediaconvert', 'medialive', 'mediapackage',
                'mediapackage-vod', 'mediastore', 'mediastore-data', 'mediatailor', 'memorydb', 'meteringmarketplace',
                'mgh', 'mgn', 'migration-hub-refactor-spaces', 'migrationhub-config', 'migrationhuborchestrator',
                'migrationhubstrategy', 'mobile', 'mq', 'mturk', 'mwaa', 'neptune', 'network-firewall',
                'networkmanager', 'nimble', 'opensearch', 'opsworks', 'opsworkscm', 'organizations', 'outposts',
                'panorama', 'personalize', 'personalize-events', 'personalize-runtime', 'pi', 'pinpoint',
                'pinpoint-email', 'pinpoint-sms-voice', 'pinpoint-sms-voice-v2', 'polly', 'pricing', 'privatenetworks',
                'proton', 'qldb', 'qldb-session', 'quicksight', 'ram', 'rbin', 'rds', 'rds-data', 'redshift',
                'redshift-data', 'redshift-serverless', 'rekognition', 'resiliencehub', 'resource-explorer-2',
                'resource-groups', 'resourcegroupstaggingapi', 'robomaker', 'rolesanywhere', 'route53',
                'route53-recovery-cluster', 'route53-recovery-control-config', 'route53-recovery-readiness',
                'route53domains', 'route53resolver', 'rum', 's3', 's3control', 's3outposts', 'sagemaker',
                'sagemaker-a2i-runtime', 'sagemaker-edge', 'sagemaker-featurestore-runtime', 'sagemaker-runtime',
                'savingsplans', 'scheduler', 'schemas', 'sdb', 'secretsmanager', 'securityhub', 'serverlessrepo',
                'service-quotas', 'servicecatalog', 'servicecatalog-appregistry', 'servicediscovery', 'ses', 'sesv2',
                'shield', 'signer', 'sms', 'sms-voice', 'snow-device-management', 'snowball', 'sns', 'sqs', 'ssm',
                'ssm-contacts', 'ssm-incidents', 'sso', 'sso-admin', 'sso-oidc', 'stepfunctions', 'storagegateway',
                'sts', 'support', 'support-app', 'swf', 'synthetics', 'textract', 'timestream-query',
                'timestream-write', 'transcribe', 'transfer', 'translate', 'voice-id', 'waf', 'waf-regional', 'wafv2',
                'wellarchitected', 'wisdom', 'workdocs', 'worklink', 'workmail', 'workmailmessageflow', 'workspaces',
                'workspaces-web', 'xray']
