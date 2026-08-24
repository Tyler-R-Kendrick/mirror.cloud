// Package catalog holds hand-built canonical models for v1 emulate-tier
// services so the spine boots before specs-sync. S1 replaces these with
// generated models from vendored specs.
package catalog

import "github.com/tyler-r-kendrick/mirror.cloud/internal/model"

func op(name, method, uri string, code int, ro bool) model.Operation {
	if code == 0 {
		code = 200
	}
	return model.Operation{
		Name:        name,
		HTTP:        model.HTTPBinding{Method: method, URI: uri, Code: code},
		Target:      name,
		QueryAction: name,
		Confidence:  model.ConfDeclared,
		Readonly:    ro,
	}
}

func ops(list ...model.Operation) []model.Operation { return list }

func svc(id, prefix string, proto model.Protocol, target, ver, xmlns string, o []model.Operation) model.Service {
	return model.Service{
		ID:             id,
		Protocol:       proto,
		EndpointPrefix: prefix,
		TargetPrefix:   target,
		QueryVersion:   ver,
		XMLNamespace:   xmlns,
		Operations:     o,
		Shapes:         map[string]model.Shape{},
	}
}

// Bundle returns the v1 emulate-tier + GCS model.
func Bundle() *model.Bundle {
	s3ops := []model.Operation{}
	for _, n := range []string{
		"CreateBucket", "DeleteBucket", "HeadBucket", "ListBuckets", "GetBucketLocation",
		"GetBucketVersioning", "PutBucketVersioning", "GetBucketTagging", "PutBucketTagging",
		"GetBucketNotificationConfiguration", "PutBucketNotificationConfiguration",
		"GetBucketAcl", "PutBucketAcl", "GetObjectAcl", "PutObjectAcl",
		"GetBucketPolicy", "PutBucketPolicy", "DeleteBucketPolicy",
		"GetBucketCors", "PutBucketCors", "DeleteBucketCors",
		"GetBucketWebsite", "PutBucketWebsite", "DeleteBucketWebsite",
		"GetBucketLogging", "PutBucketLogging",
		"GetBucketLifecycleConfiguration", "PutBucketLifecycleConfiguration", "DeleteBucketLifecycle",
		"GetBucketReplication", "PutBucketReplication",
		"GetBucketEncryption", "PutBucketEncryption", "DeleteBucketEncryption",
		"GetBucketObjectLockConfiguration", "PutBucketObjectLockConfiguration",
		"GetBucketRequestPayment", "PutBucketRequestPayment",
		"GetBucketAccelerateConfiguration", "PutBucketAccelerateConfiguration",
		"PutObject", "GetObject", "HeadObject", "DeleteObject", "DeleteObjects", "CopyObject",
		"ListObjects", "ListObjectsV2", "ListObjectVersions",
		"CreateMultipartUpload", "UploadPart", "UploadPartCopy", "CompleteMultipartUpload",
		"AbortMultipartUpload", "ListParts", "ListMultipartUploads",
		"GetObjectTagging", "PutObjectTagging",
		"PutPublicAccessBlock", "GetPublicAccessBlock", "DeletePublicAccessBlock",
		"PutBucketOwnershipControls", "GetBucketOwnershipControls", "DeleteBucketOwnershipControls",
		"GetBucketPolicyStatus", "GetObjectAttributes",
		"DeleteBucketTagging", "DeleteObjectTagging",
		"PutObjectLegalHold", "GetObjectLegalHold", "PutObjectRetention", "GetObjectRetention",
		"RestoreObject",
		"PutBucketAnalyticsConfiguration", "GetBucketAnalyticsConfiguration", "DeleteBucketAnalyticsConfiguration", "ListBucketAnalyticsConfigurations",
		"PutBucketInventoryConfiguration", "GetBucketInventoryConfiguration", "DeleteBucketInventoryConfiguration", "ListBucketInventoryConfigurations",
		"PutBucketMetricsConfiguration", "GetBucketMetricsConfiguration", "DeleteBucketMetricsConfiguration", "ListBucketMetricsConfigurations",
		"PutBucketIntelligentTieringConfiguration", "GetBucketIntelligentTieringConfiguration", "DeleteBucketIntelligentTieringConfiguration", "ListBucketIntelligentTieringConfigurations",
		"CreateBucketMetadataConfiguration", "CreateBucketMetadataTableConfiguration", "CreateSession",
		"DeleteBucketMetadataConfiguration", "DeleteBucketMetadataTableConfiguration", "DeleteBucketReplication",
		"DeleteObjectAnnotation", "GetBucketAbac", "GetBucketMetadataConfiguration",
		"GetBucketMetadataTableConfiguration", "GetObjectAnnotation", "GetObjectLockConfiguration",
		"GetObjectTorrent", "ListDirectoryBuckets", "ListObjectAnnotations",
		"PutBucketAbac", "PutObjectAnnotation", "PutObjectLockConfiguration",
		"RenameObject", "SelectObjectContent", "UpdateBucketMetadataAnnotationTableConfiguration",
		"UpdateBucketMetadataInventoryTableConfiguration", "UpdateBucketMetadataJournalTableConfiguration",
		"UpdateObjectEncryption", "WriteGetObjectResponse",
	} {
		s3ops = append(s3ops, op(n, "POST", "/", 200, stringsHas(n, "Get", "Head", "List")))
	}
	ddb := []string{"CreateTable", "DeleteTable", "DescribeTable", "ListTables", "UpdateTable",
		"PutItem", "GetItem", "DeleteItem", "UpdateItem", "BatchGetItem", "BatchWriteItem",
		"Query", "Scan", "TransactGetItems", "TransactWriteItems",
		"TagResource", "UntagResource", "ListTagsOfResource",
		"DescribeTimeToLive", "UpdateTimeToLive",
		"DescribeContinuousBackups", "UpdateContinuousBackups",
		"DescribeEndpoints", "DescribeLimits",
		"PutResourcePolicy", "GetResourcePolicy", "DeleteResourcePolicy",
		"CreateBackup", "ListBackups", "DescribeBackup", "DeleteBackup", "RestoreTableFromBackup",
		"EnableKinesisStreamingDestination", "DisableKinesisStreamingDestination", "DescribeKinesisStreamingDestination",
		"BatchExecuteStatement", "CreateGlobalTable", "DescribeContributorInsights", "DescribeExport",
		"DescribeGlobalTable", "DescribeGlobalTableSettings", "DescribeImport", "DescribeTableReplicaAutoScaling",
		"ExecuteStatement", "ExecuteTransaction", "ExportTableToPointInTime", "ImportTable",
		"ListContributorInsights", "ListExports", "ListGlobalTables", "ListImports",
		"RestoreTableToPointInTime", "SearchVectors", "UpdateContributorInsights", "UpdateGlobalTable",
		"UpdateGlobalTableSettings", "UpdateKinesisStreamingDestination", "UpdateTableReplicaAutoScaling",
		"ListStreams", "DescribeStream", "GetShardIterator", "GetRecords"}
	sqs := []string{"CreateQueue", "DeleteQueue", "GetQueueUrl", "ListQueues", "GetQueueAttributes",
		"SetQueueAttributes", "SendMessage", "SendMessageBatch", "ReceiveMessage",
		"DeleteMessage", "DeleteMessageBatch", "ChangeMessageVisibility",
		"ChangeMessageVisibilityBatch", "PurgeQueue", "TagQueue", "UntagQueue", "ListQueueTags",
		"AddPermission", "CancelMessageMoveTask", "ListDeadLetterSourceQueues", "ListMessageMoveTasks",
		"RemovePermission", "StartMessageMoveTask"}
	sns := []string{"CreateTopic", "DeleteTopic", "ListTopics", "GetTopicAttributes", "SetTopicAttributes",
		"Subscribe", "ConfirmSubscription", "Unsubscribe", "ListSubscriptions",
		"ListSubscriptionsByTopic", "Publish", "PublishBatch", "TagResource", "UntagResource",
		"AddPermission", "CheckIfPhoneNumberIsOptedOut", "CreatePlatformApplication", "CreatePlatformEndpoint",
		"CreateSMSSandboxPhoneNumber", "DeleteEndpoint", "DeletePlatformApplication", "DeleteSMSSandboxPhoneNumber",
		"GetDataProtectionPolicy", "GetEndpointAttributes", "GetPlatformApplicationAttributes", "GetSMSAttributes",
		"GetSMSSandboxAccountStatus", "GetSubscriptionAttributes", "ListEndpointsByPlatformApplication",
		"ListOriginationNumbers", "ListPhoneNumbersOptedOut", "ListPlatformApplications", "ListSMSSandboxPhoneNumbers",
		"ListTagsForResource", "OptInPhoneNumber", "PutDataProtectionPolicy", "RemovePermission",
		"SetEndpointAttributes", "SetPlatformApplicationAttributes", "SetSMSAttributes", "SetSubscriptionAttributes",
		"VerifySMSSandboxPhoneNumber"}
	sts := []string{"GetCallerIdentity", "AssumeRole", "GetSessionToken", "GetFederationToken",
		"AssumeRoleWithSAML", "AssumeRoleWithWebIdentity", "AssumeRoot", "DecodeAuthorizationMessage",
		"GetAccessKeyInfo", "GetDelegatedAccessToken", "GetWebIdentityToken"}
	iam := []string{
		"CreateRole", "GetRole", "UpdateRole", "DeleteRole", "ListRoles", "UpdateAssumeRolePolicy",
		"PutRolePolicy", "GetRolePolicy", "DeleteRolePolicy", "ListRolePolicies",
		"AttachRolePolicy", "DetachRolePolicy", "ListAttachedRolePolicies",
		"CreatePolicy", "GetPolicy", "DeletePolicy", "ListPolicies",
		"CreatePolicyVersion", "GetPolicyVersion", "DeletePolicyVersion", "ListPolicyVersions", "SetDefaultPolicyVersion",
		"CreateUser", "GetUser", "UpdateUser", "DeleteUser", "ListUsers",
		"PutUserPolicy", "GetUserPolicy", "DeleteUserPolicy", "ListUserPolicies",
		"AttachUserPolicy", "DetachUserPolicy", "ListAttachedUserPolicies",
		"CreateAccessKey", "ListAccessKeys", "UpdateAccessKey", "DeleteAccessKey",
		"CreateLoginProfile", "GetLoginProfile", "UpdateLoginProfile", "DeleteLoginProfile",
		"CreateGroup", "GetGroup", "UpdateGroup", "DeleteGroup", "ListGroups",
		"AddUserToGroup", "RemoveUserFromGroup", "ListGroupsForUser",
		"PutGroupPolicy", "GetGroupPolicy", "DeleteGroupPolicy", "ListGroupPolicies",
		"AttachGroupPolicy", "DetachGroupPolicy", "ListAttachedGroupPolicies",
		"CreateInstanceProfile", "GetInstanceProfile", "DeleteInstanceProfile", "ListInstanceProfiles",
		"AddRoleToInstanceProfile", "RemoveRoleFromInstanceProfile", "ListInstanceProfilesForRole",
		"TagRole", "UntagRole", "ListRoleTags", "TagUser", "UntagUser", "ListUserTags",
		"CreateAccountAlias", "ListAccountAliases", "DeleteAccountAlias",
		"GetAccountSummary", "GetAccountPasswordPolicy", "UpdateAccountPasswordPolicy", "DeleteAccountPasswordPolicy",
		"CreateOpenIDConnectProvider", "GetOpenIDConnectProvider", "DeleteOpenIDConnectProvider", "ListOpenIDConnectProviders", "UpdateOpenIDConnectProviderThumbprint",
		"CreateSAMLProvider", "GetSAMLProvider", "DeleteSAMLProvider", "ListSAMLProviders", "UpdateSAMLProvider",
		"SimulatePrincipalPolicy", "SimulateCustomPolicy",
		"AcceptDelegationRequest", "AcquireRole", "AddClientIDToOpenIDConnectProvider", "AssociateDelegationRequest",
		"ChangePassword", "CreateDelegationRequest", "CreateServiceLinkedRole", "CreateServiceSpecificCredential",
		"CreateVirtualMFADevice", "DeactivateMFADevice", "DeleteRolePermissionsBoundary", "DeleteSSHPublicKey",
		"DeleteServerCertificate", "DeleteServiceLinkedRole", "DeleteServiceSpecificCredential",
		"DeleteSigningCertificate", "DeleteUserPermissionsBoundary", "DeleteVirtualMFADevice",
		"DisableOrganizationsRootCredentialsManagement", "DisableOrganizationsRootSessions",
		"DisableOutboundWebIdentityFederation", "EnableMFADevice", "EnableOrganizationsRootCredentialsManagement",
		"EnableOrganizationsRootSessions", "EnableOutboundWebIdentityFederation",
		"GenerateCredentialReport", "GenerateOrganizationsAccessReport", "GenerateServiceLastAccessedDetails",
		"GetAccessKeyLastUsed", "GetAccountAuthorizationDetails", "GetAccountProperties",
		"GetContextKeysForCustomPolicy", "GetContextKeysForPrincipalPolicy", "GetCredentialReport",
		"GetDelegationRequest", "GetHumanReadableSummary", "GetMFADevice", "GetOrganizationsAccessReport",
		"GetOutboundWebIdentityFederationInfo", "GetRoleTemplateVersion", "GetSSHPublicKey",
		"GetServerCertificate", "GetServiceLastAccessedDetails", "GetServiceLastAccessedDetailsWithEntities",
		"GetServiceLinkedRoleDeletionStatus", "ListDelegationRequests", "ListEntitiesForPolicy",
		"ListInstanceProfileTags", "ListMFADeviceTags", "ListMFADevices", "ListOpenIDConnectProviderTags",
		"ListOrganizationsFeatures", "ListPoliciesGrantingServiceAccess", "ListPolicyTags", "ListSAMLProviderTags",
		"ListSSHPublicKeys", "ListServerCertificateTags", "ListServerCertificates",
		"ListServiceSpecificCredentials", "ListSigningCertificates", "ListVirtualMFADevices",
		"PutAccountProperties", "PutRolePermissionsBoundary", "PutUserPermissionsBoundary",
		"RejectDelegationRequest", "RemoveClientIDFromOpenIDConnectProvider", "ResetServiceSpecificCredential",
		"ResyncMFADevice", "SendDelegationToken", "SetSecurityTokenServicePreferences",
		"TagInstanceProfile", "TagMFADevice", "TagOpenIDConnectProvider", "TagPolicy", "TagSAMLProvider",
		"TagServerCertificate", "UntagInstanceProfile", "UntagMFADevice", "UntagOpenIDConnectProvider",
		"UntagPolicy", "UntagSAMLProvider", "UntagServerCertificate",
		"UpdateDelegationRequest", "UpdateRoleDescription", "UpdateSSHPublicKey", "UpdateServerCertificate",
		"UpdateServiceSpecificCredential", "UpdateSigningCertificate",
		"UploadSSHPublicKey", "UploadServerCertificate", "UploadSigningCertificate",
	}
	ssm := []string{
		"PutParameter", "GetParameter", "GetParameters", "GetParametersByPath",
		"DeleteParameter", "DeleteParameters", "DescribeParameters", "LabelParameterVersion",
		"UnlabelParameterVersion", "GetParameterHistory",
		"AddTagsToResource", "RemoveTagsFromResource", "ListTagsForResource",
		"CreateDocument", "GetDocument", "DeleteDocument", "UpdateDocument", "DescribeDocument",
		"ListDocuments", "ListDocumentVersions", "UpdateDocumentDefaultVersion",
		"CreateAssociation", "DescribeAssociation", "UpdateAssociation", "DeleteAssociation",
		"ListAssociations",
		"SendCommand", "ListCommands", "ListCommandInvocations", "GetCommandInvocation", "CancelCommand",
		"CreatePatchBaseline", "GetPatchBaseline", "UpdatePatchBaseline", "DeletePatchBaseline",
		"DescribePatchBaselines", "RegisterDefaultPatchBaseline", "GetDefaultPatchBaseline",
		"CreateMaintenanceWindow", "GetMaintenanceWindow", "UpdateMaintenanceWindow", "DeleteMaintenanceWindow",
		"DescribeMaintenanceWindows",
		"RegisterTargetWithMaintenanceWindow", "DeregisterTargetFromMaintenanceWindow", "DescribeMaintenanceWindowTargets",
		"RegisterTaskWithMaintenanceWindow", "DeregisterTaskFromMaintenanceWindow", "DescribeMaintenanceWindowTasks",
		"StartAutomationExecution", "GetAutomationExecution", "StopAutomationExecution", "DescribeAutomationExecutions",
		"CreateOpsItem", "GetOpsItem", "UpdateOpsItem", "DeleteOpsItem", "DescribeOpsItems",
		"CreateResourceDataSync", "ListResourceDataSync", "DeleteResourceDataSync",
		"GetServiceSetting", "UpdateServiceSetting", "ResetServiceSetting",
		"AssociateOpsItemRelatedItem", "CancelMaintenanceWindowExecution",
		"CreateActivation", "CreateAssociationBatch", "CreateCloudConnector", "CreateOpsMetadata",
		"DeleteActivation", "DeleteCloudConnector", "DeleteInventory", "DeleteOpsMetadata", "DeleteResourcePolicy",
		"DeregisterManagedInstance", "DeregisterPatchBaselineForPatchGroup",
		"DescribeActivations", "DescribeAssociationExecutionTargets", "DescribeAssociationExecutions",
		"DescribeAutomationStepExecutions", "DescribeAvailablePatches", "DescribeDocumentPermission",
		"DescribeEffectiveInstanceAssociations", "DescribeEffectivePatchesForPatchBaseline",
		"DescribeInstanceAssociationsStatus", "DescribeInstanceInformation",
		"DescribeInstancePatchStates", "DescribeInstancePatchStatesForPatchGroup",
		"DescribeInstancePatches", "DescribeInstanceProperties", "DescribeInventoryDeletions",
		"DescribeMaintenanceWindowExecutionTaskInvocations", "DescribeMaintenanceWindowExecutionTasks",
		"DescribeMaintenanceWindowExecutions", "DescribeMaintenanceWindowSchedule",
		"DescribeMaintenanceWindowsForTarget", "DescribePatchGroupState", "DescribePatchGroups",
		"DescribePatchProperties", "DescribeSessions",
		"DisassociateOpsItemRelatedItem",
		"GetAccessToken", "GetCalendarState", "GetCloudConnector", "GetConnectionStatus",
		"GetDeployablePatchSnapshotForInstance", "GetExecutionPreview", "GetInventory", "GetInventorySchema",
		"GetMaintenanceWindowExecution", "GetMaintenanceWindowExecutionTask",
		"GetMaintenanceWindowExecutionTaskInvocation", "GetMaintenanceWindowTask",
		"GetOpsMetadata", "GetOpsSummary", "GetPatchBaselineForPatchGroup", "GetResourcePolicies",
		"ListAssociationVersions", "ListCloudConnectors", "ListComplianceItems", "ListComplianceSummaries",
		"ListDocumentMetadataHistory", "ListInventoryEntries", "ListNodes", "ListNodesSummary",
		"ListOpsItemEvents", "ListOpsItemRelatedItems", "ListOpsMetadata", "ListResourceComplianceSummaries",
		"ModifyDocumentPermission",
		"PutComplianceItems", "PutInventory", "PutResourcePolicy",
		"RegisterPatchBaselineForPatchGroup", "ResumeSession", "SendAutomationSignal",
		"StartAccessRequest", "StartAssociationsOnce", "StartChangeRequestExecution",
		"StartExecutionPreview", "StartSession",
		"TerminateSession",
		"UpdateAssociationStatus", "UpdateCloudConnector", "UpdateDocumentMetadata",
		"UpdateMaintenanceWindowTarget", "UpdateMaintenanceWindowTask", "UpdateManagedInstanceRole",
		"UpdateOpsMetadata", "UpdateResourceDataSync", "ValidateCloudConnector",
	}
	sm := []string{"CreateSecret", "GetSecretValue", "PutSecretValue", "UpdateSecret", "DeleteSecret",
		"RestoreSecret", "ListSecrets", "DescribeSecret", "ListSecretVersionIds",
		"GetRandomPassword", "TagResource", "UntagResource",
		"BatchGetSecretValue", "CancelRotateSecret", "DeleteResourcePolicy", "GetResourcePolicy",
		"PutResourcePolicy", "RemoveRegionsFromReplication", "ReplicateSecretToRegions", "RotateSecret",
		"StopReplicationToReplica", "UpdateSecretVersionStage", "ValidateResourcePolicy"}
	gcs := []string{"storage.buckets.insert", "storage.buckets.get", "storage.buckets.list",
		"storage.buckets.delete", "storage.buckets.patch",
		"storage.objects.insert", "storage.objects.get", "storage.objects.list",
		"storage.objects.delete", "storage.objects.copy", "storage.objects.rewrite",
		"storage.objects.compose", "storage.objects.patch",
		"storage.anywhereCaches.disable", "storage.anywhereCaches.get", "storage.anywhereCaches.insert",
		"storage.anywhereCaches.list", "storage.anywhereCaches.pause", "storage.anywhereCaches.resume",
		"storage.anywhereCaches.update",
		"storage.bucketAccessControls.delete", "storage.bucketAccessControls.get", "storage.bucketAccessControls.insert",
		"storage.bucketAccessControls.list", "storage.bucketAccessControls.patch", "storage.bucketAccessControls.update",
		"storage.buckets.getIamPolicy", "storage.buckets.getStorageLayout", "storage.buckets.lockRetentionPolicy",
		"storage.buckets.operations.advanceRelocateBucket", "storage.buckets.operations.cancel",
		"storage.buckets.operations.get", "storage.buckets.operations.list",
		"storage.buckets.relocate", "storage.buckets.restore", "storage.buckets.setIamPolicy",
		"storage.buckets.testIamPermissions", "storage.buckets.update",
		"storage.channels.stop",
		"storage.defaultObjectAccessControls.delete", "storage.defaultObjectAccessControls.get",
		"storage.defaultObjectAccessControls.insert", "storage.defaultObjectAccessControls.list",
		"storage.defaultObjectAccessControls.patch", "storage.defaultObjectAccessControls.update",
		"storage.folders.delete", "storage.folders.deleteRecursive", "storage.folders.get",
		"storage.folders.insert", "storage.folders.list", "storage.folders.rename",
		"storage.managedFolders.delete", "storage.managedFolders.get", "storage.managedFolders.getIamPolicy",
		"storage.managedFolders.insert", "storage.managedFolders.list", "storage.managedFolders.setIamPolicy",
		"storage.managedFolders.testIamPermissions", "storage.managedFolders.update",
		"storage.notifications.delete", "storage.notifications.get", "storage.notifications.insert",
		"storage.notifications.list",
		"storage.objectAccessControls.delete", "storage.objectAccessControls.get", "storage.objectAccessControls.insert",
		"storage.objectAccessControls.list", "storage.objectAccessControls.patch", "storage.objectAccessControls.update",
		"storage.objects.bulkRestore", "storage.objects.getIamPolicy", "storage.objects.move",
		"storage.objects.restore", "storage.objects.setIamPolicy", "storage.objects.testIamPermissions",
		"storage.objects.update",
		"storage.projects.hmacKeys.create", "storage.projects.hmacKeys.delete", "storage.projects.hmacKeys.get",
		"storage.projects.hmacKeys.list", "storage.projects.hmacKeys.update",
		"storage.projects.serviceAccount.get",
		"storage.rapidCaches.disable", "storage.rapidCaches.get", "storage.rapidCaches.insert",
		"storage.rapidCaches.list", "storage.rapidCaches.update"}
	kms := []string{"CreateKey", "DescribeKey", "ListKeys", "Encrypt", "Decrypt", "GenerateDataKey",
		"CancelKeyDeletion", "ConnectCustomKeyStore", "CreateAlias", "CreateCustomKeyStore", "CreateGrant",
		"DeleteAlias", "DeleteCustomKeyStore", "DeleteImportedKeyMaterial", "DeriveSharedSecret",
		"DescribeCustomKeyStores", "DisableKey", "DisableKeyRotation", "DisconnectCustomKeyStore",
		"EnableKey", "EnableKeyRotation", "GenerateDataKeyPair", "GenerateDataKeyPairWithoutPlaintext",
		"GenerateDataKeyWithoutPlaintext", "GenerateMac", "GenerateRandom", "GetKeyLastUsage", "GetKeyPolicy",
		"GetKeyRotationStatus", "GetParametersForImport", "GetPublicKey", "ImportKeyMaterial", "ListAliases",
		"ListGrants", "ListKeyPolicies", "ListKeyRotations", "ListResourceTags", "ListRetirableGrants",
		"PutKeyPolicy", "ReEncrypt", "ReplicateKey", "RetireGrant", "RevokeGrant", "RotateKeyOnDemand",
		"ScheduleKeyDeletion", "Sign", "TagResource", "UntagResource", "UpdateAlias", "UpdateCustomKeyStore",
		"UpdateKeyDescription", "UpdatePrimaryRegion", "Verify", "VerifyMac"}
	cwlogs := []string{"CreateLogGroup", "DeleteLogGroup", "DescribeLogGroups",
		"CreateLogStream", "DescribeLogStreams", "DeleteLogStream",
		"PutLogEvents", "GetLogEvents", "FilterLogEvents",
		"PutRetentionPolicy", "DeleteRetentionPolicy",
		"PutSubscriptionFilter", "DescribeSubscriptionFilters", "DeleteSubscriptionFilter",
		"PutMetricFilter", "DescribeMetricFilters", "DeleteMetricFilter",
		"PutResourcePolicy", "DescribeResourcePolicies", "DeleteResourcePolicy",
		"TagLogGroup", "UntagLogGroup", "ListTagsLogGroup",
		"PutDestination", "DescribeDestinations", "DeleteDestination",
		"PutQueryDefinition", "DescribeQueryDefinitions", "DeleteQueryDefinition",
		"StartQuery", "GetQueryResults", "StopQuery",
		"AssociateKmsKey", "DisassociateKmsKey",
		"AssociateSourceToS3TableIntegration", "CancelExportTask", "CancelImportTask",
		"CreateDelivery", "CreateExportTask", "CreateImportTask", "CreateLogAnomalyDetector",
		"CreateLookupTable", "CreateScheduledQuery",
		"DeleteAccountPolicy", "DeleteDataProtectionPolicy", "DeleteDelivery", "DeleteDeliveryDestination",
		"DeleteDeliveryDestinationPolicy", "DeleteDeliverySource", "DeleteIndexPolicy", "DeleteIntegration",
		"DeleteLogAnomalyDetector", "DeleteLookupTable", "DeleteScheduledQuery", "DeleteSyslogConfiguration",
		"DeleteTransformer",
		"DescribeAccountPolicies", "DescribeConfigurationTemplates", "DescribeDeliveries",
		"DescribeDeliveryDestinations", "DescribeDeliverySources", "DescribeExportTasks", "DescribeFieldIndexes",
		"DescribeImportTaskBatches", "DescribeImportTasks", "DescribeIndexPolicies", "DescribeLookupTables",
		"DescribeQueries",
		"DisassociateSourceFromS3TableIntegration",
		"GetDataProtectionPolicy", "GetDelivery", "GetDeliveryDestination", "GetDeliveryDestinationPolicy",
		"GetDeliverySource", "GetIntegration", "GetLogAnomalyDetector", "GetLogFields", "GetLogGroupFields",
		"GetLogObject", "GetLogRecord", "GetLookupTable", "GetScheduledQuery", "GetScheduledQueryHistory",
		"GetStorageTierPolicy", "GetTransformer",
		"ListAggregateLogGroupSummaries", "ListAnomalies", "ListIntegrations", "ListLogAnomalyDetectors",
		"ListLogGroups", "ListLogGroupsForQuery", "ListScheduledQueries", "ListSourcesForS3TableIntegration",
		"ListSyslogConfigurations", "ListTagsForResource",
		"PutAccountPolicy", "PutBearerTokenAuthentication", "PutDataProtectionPolicy", "PutDeliveryDestination",
		"PutDeliveryDestinationPolicy", "PutDeliverySource", "PutDestinationPolicy", "PutIndexPolicy",
		"PutIntegration", "PutLogGroupDeletionProtection", "PutStorageTierPolicy", "PutSyslogConfiguration",
		"PutTransformer",
		"StartLiveTail", "TagResource", "TestMetricFilter", "TestTransformer", "UntagResource",
		"UpdateAnomaly", "UpdateDeliveryConfiguration", "UpdateLogAnomalyDetector", "UpdateLookupTable",
		"UpdateScheduledQuery"}
	ev := []string{"PutEvents", "PutRule", "PutTargets", "ListRules", "ListTargetsByRule", "DeleteRule", "RemoveTargets",
		"ActivateEventSource", "CancelReplay", "CreateApiDestination", "CreateArchive", "CreateConnection",
		"CreateEndpoint", "CreateEventBus", "CreatePartnerEventSource", "DeactivateEventSource",
		"DeauthorizeConnection", "DeleteApiDestination", "DeleteArchive", "DeleteConnection", "DeleteEndpoint",
		"DeleteEventBus", "DeletePartnerEventSource", "DescribeApiDestination", "DescribeArchive",
		"DescribeConnection", "DescribeEndpoint", "DescribeEventBus", "DescribeEventSource",
		"DescribePartnerEventSource", "DescribeReplay", "DescribeRule", "DisableRule", "EnableRule",
		"ListApiDestinations", "ListArchives", "ListConnections", "ListEndpoints", "ListEventBuses",
		"ListEventSources", "ListPartnerEventSourceAccounts", "ListPartnerEventSources", "ListReplays",
		"ListRuleNamesByTarget", "ListTagsForResource", "PutPartnerEvents", "PutPermission", "RemovePermission",
		"StartReplay", "TagResource", "TestEventPattern", "UntagResource", "UpdateApiDestination",
		"UpdateArchive", "UpdateConnection", "UpdateEndpoint", "UpdateEventBus"}
	lam := []string{
		"CreateFunction", "GetFunction", "ListFunctions", "DeleteFunction", "Invoke",
		"UpdateFunctionCode", "UpdateFunctionConfiguration", "GetFunctionConfiguration",
		"PublishVersion", "ListVersionsByFunction",
		"CreateAlias", "GetAlias", "UpdateAlias", "DeleteAlias", "ListAliases",
		"AddPermission", "RemovePermission", "GetPolicy",
		"TagResource", "UntagResource", "ListTags",
		"PutFunctionConcurrency", "GetFunctionConcurrency", "DeleteFunctionConcurrency",
		"CreateEventSourceMapping", "GetEventSourceMapping", "ListEventSourceMappings",
		"UpdateEventSourceMapping", "DeleteEventSourceMapping",
		"AddLayerVersionPermission", "CheckpointDurableExecution", "CreateCapacityProvider",
		"CreateCodeSigningConfig", "CreateFunctionUrlConfig", "DeleteCapacityProvider",
		"DeleteCodeSigningConfig", "DeleteFunctionCodeSigningConfig", "DeleteFunctionEventInvokeConfig",
		"DeleteFunctionUrlConfig", "DeleteLayerVersion", "DeleteProvisionedConcurrencyConfig",
		"DeleteResourcePolicy", "GetAccountSettings", "GetCapacityProvider", "GetCodeSigningConfig",
		"GetDurableExecution", "GetDurableExecutionHistory", "GetDurableExecutionState",
		"GetFunctionCodeSigningConfig", "GetFunctionEventInvokeConfig", "GetFunctionRecursionConfig",
		"GetFunctionScalingConfig", "GetFunctionUrlConfig", "GetLayerVersion", "GetLayerVersionByArn",
		"GetLayerVersionPolicy", "GetProvisionedConcurrencyConfig", "GetResourcePolicy",
		"GetRuntimeManagementConfig", "InvokeAsync", "InvokeWithResponseStream",
		"ListCapacityProviders", "ListCodeSigningConfigs", "ListDurableExecutionsByFunction",
		"ListFunctionEventInvokeConfigs", "ListFunctionUrlConfigs", "ListFunctionVersionsByCapacityProvider",
		"ListFunctionsByCodeSigningConfig", "ListLayerVersions", "ListLayers",
		"ListProvisionedConcurrencyConfigs", "PublishLayerVersion", "PutFunctionCodeSigningConfig",
		"PutFunctionEventInvokeConfig", "PutFunctionRecursionConfig", "PutFunctionScalingConfig",
		"PutProvisionedConcurrencyConfig", "PutResourcePolicy", "PutRuntimeManagementConfig",
		"RemoveLayerVersionPermission", "SendDurableExecutionCallbackFailure",
		"SendDurableExecutionCallbackHeartbeat", "SendDurableExecutionCallbackSuccess",
		"StopDurableExecution", "UpdateCapacityProvider", "UpdateCodeSigningConfig",
		"UpdateFunctionEventInvokeConfig", "UpdateFunctionUrlConfig",
	}
	cfn := []string{"CreateStack", "UpdateStack", "DeleteStack", "DescribeStacks", "ListStacks",
		"GetTemplate", "ListStackResources", "DescribeStackEvents", "ValidateTemplate",
		"DescribeStackResource", "GetTemplateSummary", "ListExports",
		"CreateChangeSet", "DescribeChangeSet", "ExecuteChangeSet", "DeleteChangeSet", "ListChangeSets",
		"UpdateTerminationProtection", "SignalResource",
		"ActivateOrganizationsAccess", "ActivateType", "BatchDescribeTypeConfigurations",
		"CancelUpdateStack", "ContinueUpdateRollback", "CreateGeneratedTemplate", "CreateStackInstances",
		"CreateStackRefactor", "CreateStackSet", "DeactivateOrganizationsAccess", "DeactivateType",
		"DeleteGeneratedTemplate", "DeleteStackInstances", "DeleteStackSet", "DeregisterType",
		"DescribeAccountLimits", "DescribeChangeSetHooks", "DescribeEvents", "DescribeGeneratedTemplate",
		"DescribeOrganizationsAccess", "DescribePublisher", "DescribeResourceScan",
		"DescribeStackDriftDetectionStatus", "DescribeStackInstance", "DescribeStackRefactor",
		"DescribeStackResourceDrifts", "DescribeStackResources", "DescribeStackSet", "DescribeStackSetOperation",
		"DescribeType", "DescribeTypeRegistration", "DetectStackDrift", "DetectStackResourceDrift",
		"DetectStackSetDrift", "EstimateTemplateCost", "ExecuteStackRefactor", "GetGeneratedTemplate",
		"GetHookResult", "GetStackPolicy", "ImportStacksToStackSet", "ListGeneratedTemplates", "ListHookResults",
		"ListImports", "ListResourceScanRelatedResources", "ListResourceScanResources", "ListResourceScans",
		"ListStackInstanceResourceDrifts", "ListStackInstances", "ListStackRefactorActions", "ListStackRefactors",
		"ListStackSetAutoDeploymentTargets", "ListStackSetOperationResults", "ListStackSetOperations",
		"ListStackSets", "ListTypeRegistrations", "ListTypeVersions", "ListTypes", "PublishType",
		"RecordHandlerProgress", "RegisterPublisher", "RegisterType", "RollbackStack", "SetStackPolicy",
		"SetTypeConfiguration", "SetTypeDefaultVersion", "StartResourceScan", "StopStackSetOperation",
		"TestType", "UpdateGeneratedTemplate", "UpdateStackInstances", "UpdateStackSet"}
	kin := []string{"CreateStream", "DeleteStream", "ListStreams", "DescribeStream", "DescribeStreamSummary",
		"PutRecord", "PutRecords", "GetShardIterator", "GetRecords", "ListShards",
		"AddTagsToStream", "DecreaseStreamRetentionPeriod", "DeleteResourcePolicy", "DeregisterStreamConsumer",
		"DescribeAccountSettings", "DescribeLimits", "DescribeStreamConsumer", "DisableEnhancedMonitoring",
		"EnableEnhancedMonitoring", "GetResourcePolicy", "IncreaseStreamRetentionPeriod", "ListStreamConsumers",
		"ListTagsForStream", "MergeShards", "PutResourcePolicy", "RegisterStreamConsumer",
		"RemoveTagsFromStream", "SplitShard", "StartStreamEncryption", "StopStreamEncryption",
		"SubscribeToShard", "UpdateAccountSettings", "UpdateMaxRecordSize", "UpdateShardCount",
		"UpdateStreamMode", "UpdateStreamWarmThroughput"}
	apigw := []string{"CreateRestApi", "GetRestApi", "GetRestApis", "DeleteRestApi", "UpdateRestApi",
		"CreateResource", "GetResource", "GetResources", "DeleteResource",
		"PutMethod", "GetMethod", "DeleteMethod",
		"PutIntegration", "GetIntegration", "DeleteIntegration",
		"PutMethodResponse", "GetMethodResponse", "DeleteMethodResponse",
		"PutIntegrationResponse", "GetIntegrationResponse", "DeleteIntegrationResponse",
		"CreateDeployment", "GetDeployment", "GetDeployments", "DeleteDeployment",
		"CreateStage", "GetStage", "UpdateStage", "DeleteStage",
		"CreateAuthorizer", "GetAuthorizer", "GetAuthorizers", "DeleteAuthorizer",
		"CreateApiKey", "GetApiKey", "GetApiKeys", "DeleteApiKey",
		"CreateUsagePlan", "GetUsagePlan", "GetUsagePlans", "DeleteUsagePlan",
		"ExecuteApi",
		"CreateBasePathMapping", "CreateDocumentationPart", "CreateDocumentationVersion", "CreateDomainName",
		"CreateDomainNameAccessAssociation", "CreateModel", "CreateRequestValidator", "CreateUsagePlanKey", "CreateVpcLink",
		"DeleteBasePathMapping", "DeleteClientCertificate", "DeleteDocumentationPart", "DeleteDocumentationVersion",
		"DeleteDomainName", "DeleteDomainNameAccessAssociation", "DeleteGatewayResponse", "DeleteModel",
		"DeleteRequestValidator", "DeleteUsagePlanKey", "DeleteVpcLink",
		"FlushStageAuthorizersCache", "FlushStageCache", "GenerateClientCertificate",
		"GetAccount", "GetBasePathMapping", "GetBasePathMappings", "GetClientCertificate", "GetClientCertificates",
		"GetDocumentationPart", "GetDocumentationParts", "GetDocumentationVersion", "GetDocumentationVersions",
		"GetDomainName", "GetDomainNameAccessAssociations", "GetDomainNames", "GetExport", "GetGatewayResponse",
		"GetGatewayResponses", "GetModel", "GetModelTemplate", "GetModels", "GetRequestValidator",
		"GetRequestValidators", "GetSdk", "GetSdkType", "GetSdkTypes", "GetStages", "GetTags", "GetUsage",
		"GetUsagePlanKey", "GetUsagePlanKeys", "GetVpcLink", "GetVpcLinks",
		"ImportApiKeys", "ImportDocumentationParts", "ImportRestApi",
		"PutGatewayResponse", "PutRestApi", "RejectDomainNameAccessAssociation",
		"TagResource", "TestInvokeAuthorizer", "TestInvokeMethod", "UntagResource",
		"UpdateAccount", "UpdateApiKey", "UpdateAuthorizer", "UpdateBasePathMapping", "UpdateClientCertificate",
		"UpdateDeployment", "UpdateDocumentationPart", "UpdateDocumentationVersion", "UpdateDomainName",
		"UpdateGatewayResponse", "UpdateIntegration", "UpdateIntegrationResponse", "UpdateMethod",
		"UpdateMethodResponse", "UpdateModel", "UpdateRequestValidator", "UpdateResource",
		"UpdateUsage", "UpdateUsagePlan", "UpdateVpcLink"}
	cw := []string{"PutMetricData", "ListMetrics", "GetMetricStatistics", "PutMetricAlarm", "DescribeAlarms", "DeleteAlarms",
		"AssociateDatasetKmsKey", "DeleteAlarmMuteRule", "DeleteAnomalyDetector", "DeleteDashboards",
		"DeleteInsightRules", "DeleteMetricStream", "DescribeAlarmContributors", "DescribeAlarmHistory",
		"DescribeAlarmsForMetric", "DescribeAnomalyDetectors", "DescribeInsightRules", "DisableAlarmActions",
		"DisableInsightRules", "DisassociateDatasetKmsKey", "EnableAlarmActions", "EnableInsightRules",
		"GetAlarmMuteRule", "GetDashboard", "GetDataset", "GetInsightRuleReport",
		"GetMetricData", "GetMetricStream", "GetMetricWidgetImage", "GetOTelEnrichment",
		"ListAlarmMuteRules", "ListDashboards", "ListManagedInsightRules", "ListMetricStreams",
		"PutAlarmMuteRule", "PutAnomalyDetector", "PutCompositeAlarm", "PutDashboard",
		"PutInsightRule", "PutLogAlarm", "PutManagedInsightRules", "PutMetricStream",
		"SetAlarmState", "StartMetricStreams", "StartOTelEnrichment", "StopMetricStreams",
		"StopOTelEnrichment"}
	r53 := []string{"CreateHostedZone", "GetHostedZone", "ListHostedZones", "DeleteHostedZone",
		"ChangeResourceRecordSets", "ListResourceRecordSets",
		"ActivateKeySigningKey", "AssociateVPCWithHostedZone", "ChangeCidrCollection", "ChangeTagsForResource",
		"CreateCidrCollection", "CreateHealthCheck", "CreateKeySigningKey", "CreateQueryLoggingConfig",
		"CreateReusableDelegationSet", "CreateTrafficPolicy", "CreateTrafficPolicyInstance",
		"CreateTrafficPolicyVersion", "CreateVPCAssociationAuthorization",
		"DeactivateKeySigningKey", "DeleteCidrCollection", "DeleteHealthCheck", "DeleteKeySigningKey",
		"DeleteQueryLoggingConfig", "DeleteReusableDelegationSet", "DeleteTrafficPolicy",
		"DeleteTrafficPolicyInstance", "DeleteVPCAssociationAuthorization",
		"DisableHostedZoneDNSSEC", "DisassociateVPCFromHostedZone", "EnableHostedZoneDNSSEC",
		"GetAccountLimit", "GetChange", "GetCheckerIpRanges", "GetDNSSEC", "GetGeoLocation",
		"GetHealthCheck", "GetHealthCheckCount", "GetHealthCheckLastFailureReason", "GetHealthCheckStatus",
		"GetHostedZoneCount", "GetHostedZoneLimit", "GetQueryLoggingConfig", "GetReusableDelegationSet",
		"GetReusableDelegationSetLimit", "GetTrafficPolicy", "GetTrafficPolicyInstance",
		"GetTrafficPolicyInstanceCount",
		"ListCidrBlocks", "ListCidrCollections", "ListCidrLocations", "ListGeoLocations", "ListHealthChecks",
		"ListHostedZonesByName", "ListHostedZonesByVPC", "ListQueryLoggingConfigs", "ListReusableDelegationSets",
		"ListTagsForResource", "ListTagsForResources", "ListTrafficPolicies", "ListTrafficPolicyInstances",
		"ListTrafficPolicyInstancesByHostedZone", "ListTrafficPolicyInstancesByPolicy", "ListTrafficPolicyVersions",
		"ListVPCAssociationAuthorizations", "TestDNSAnswer",
		"UpdateHealthCheck", "UpdateHostedZoneComment", "UpdateHostedZoneFeatures",
		"UpdateTrafficPolicyComment", "UpdateTrafficPolicyInstance"}
	acm := []string{"RequestCertificate", "DescribeCertificate", "ListCertificates", "DeleteCertificate",
		"GetCertificate", "AddTagsToCertificate", "ListTagsForCertificate", "RemoveTagsFromCertificate",
		"CreateAcmeDomainValidation", "CreateAcmeEndpoint", "CreateAcmeExternalAccountBinding", "DeleteAcmeDomainValidation",
		"DeleteAcmeEndpoint", "DeleteAcmeExternalAccountBinding", "DescribeAcmeAccount", "DescribeAcmeDomainValidation",
		"DescribeAcmeEndpoint", "DescribeAcmeExternalAccountBinding", "ExportCertificate", "GetAccountConfiguration",
		"GetAcmeExternalAccountBindingCredentials", "ImportCertificate", "ListAcmeAccounts", "ListAcmeDomainValidations",
		"ListAcmeEndpoints", "ListAcmeExternalAccountBindings", "ListCertificateDomainValidations", "ListTagsForResource",
		"PutAccountConfiguration", "RenewCertificate", "ResendValidationEmail", "RevokeAcmeAccount",
		"RevokeAcmeExternalAccountBinding", "RevokeCertificate", "SearchCertificates", "TagResource",
		"UntagResource", "UpdateAcmeDomainValidation", "UpdateAcmeEndpoint", "UpdateCertificateOptions"}
	rds := []string{
		"CreateDBInstance", "DescribeDBInstances", "ModifyDBInstance", "DeleteDBInstance", "RebootDBInstance",
		"StartDBInstance", "StopDBInstance", "CreateDBInstanceReadReplica", "PromoteReadReplica",
		"RestoreDBInstanceFromDBSnapshot",
		"CreateDBCluster", "DescribeDBClusters", "ModifyDBCluster", "DeleteDBCluster", "FailoverDBCluster",
		"RestoreDBClusterFromSnapshot",
		"CreateDBSnapshot", "DescribeDBSnapshots", "DeleteDBSnapshot", "CopyDBSnapshot",
		"CreateDBClusterSnapshot", "DescribeDBClusterSnapshots", "DeleteDBClusterSnapshot",
		"CreateDBSubnetGroup", "DescribeDBSubnetGroups", "DeleteDBSubnetGroup",
		"CreateDBParameterGroup", "DescribeDBParameterGroups", "DeleteDBParameterGroup",
		"ModifyDBParameterGroup", "ResetDBParameterGroup", "DescribeDBParameters",
		"CreateDBClusterParameterGroup", "DescribeDBClusterParameterGroups", "DeleteDBClusterParameterGroup",
		"CreateOptionGroup", "DescribeOptionGroups", "DeleteOptionGroup",
		"AddRoleToDBInstance", "RemoveRoleFromDBInstance",
		"CreateEventSubscription", "DescribeEventSubscriptions", "DeleteEventSubscription",
		"AddTagsToResource", "RemoveTagsFromResource", "ListTagsForResource",
		"AddRoleToDBCluster", "AddSourceIdentifierToSubscription", "ApplyPendingMaintenanceAction",
		"AuthorizeDBSecurityGroupIngress", "BacktrackDBCluster", "CancelExportTask",
		"CopyDBClusterParameterGroup", "CopyDBClusterSnapshot", "CopyDBParameterGroup", "CopyOptionGroup",
		"CreateBlueGreenDeployment", "CreateCustomDBEngineVersion", "CreateDBClusterEndpoint",
		"CreateDBProxy", "CreateDBProxyEndpoint", "CreateDBSecurityGroup", "CreateDBShardGroup",
		"CreateGlobalCluster", "CreateIntegration", "CreateTenantDatabase",
		"DeleteBlueGreenDeployment", "DeleteCustomDBEngineVersion", "DeleteDBClusterAutomatedBackup",
		"DeleteDBClusterEndpoint", "DeleteDBInstanceAutomatedBackup", "DeleteDBProxy", "DeleteDBProxyEndpoint",
		"DeleteDBSecurityGroup", "DeleteDBShardGroup", "DeleteGlobalCluster", "DeleteIntegration",
		"DeleteTenantDatabase", "DeregisterDBProxyTargets",
		"DescribeAccountAttributes", "DescribeBlueGreenDeployments", "DescribeCertificates",
		"DescribeDBClusterAutomatedBackups", "DescribeDBClusterBacktracks", "DescribeDBClusterEndpoints",
		"DescribeDBClusterParameters", "DescribeDBClusterSnapshotAttributes", "DescribeDBEngineVersions",
		"DescribeDBInstanceAutomatedBackups", "DescribeDBLogFiles", "DescribeDBMajorEngineVersions",
		"DescribeDBProxies", "DescribeDBProxyEndpoints", "DescribeDBProxyTargetGroups", "DescribeDBProxyTargets",
		"DescribeDBRecommendations", "DescribeDBSecurityGroups", "DescribeDBShardGroups",
		"DescribeDBSnapshotAttributes", "DescribeDBSnapshotTenantDatabases",
		"DescribeEngineDefaultClusterParameters", "DescribeEngineDefaultParameters",
		"DescribeEventCategories", "DescribeEvents", "DescribeExportTasks", "DescribeGlobalClusters",
		"DescribeIntegrations", "DescribeOptionGroupOptions", "DescribeOrderableDBInstanceOptions",
		"DescribePendingMaintenanceActions", "DescribeReservedDBInstances", "DescribeReservedDBInstancesOfferings",
		"DescribeServerlessV2PlatformVersions", "DescribeSourceRegions", "DescribeTenantDatabases",
		"DescribeValidDBInstanceModifications",
		"DisableHttpEndpoint", "DownloadDBLogFilePortion", "EnableHttpEndpoint", "FailoverGlobalCluster",
		"ModifyActivityStream", "ModifyCertificates", "ModifyCurrentDBClusterCapacity",
		"ModifyCustomDBEngineVersion", "ModifyDBClusterEndpoint", "ModifyDBClusterParameterGroup",
		"ModifyDBClusterSnapshotAttribute", "ModifyDBProxy", "ModifyDBProxyEndpoint", "ModifyDBProxyTargetGroup",
		"ModifyDBRecommendation", "ModifyDBShardGroup", "ModifyDBSnapshot", "ModifyDBSnapshotAttribute",
		"ModifyDBSubnetGroup", "ModifyEventSubscription", "ModifyGlobalCluster", "ModifyIntegration",
		"ModifyOptionGroup", "ModifyTenantDatabase",
		"PromoteReadReplicaDBCluster", "PurchaseReservedDBInstancesOffering",
		"RebootDBCluster", "RebootDBShardGroup", "RegisterDBProxyTargets",
		"RemoveFromGlobalCluster", "RemoveRoleFromDBCluster", "RemoveSourceIdentifierFromSubscription",
		"ResetDBClusterParameterGroup",
		"RestoreDBClusterFromS3", "RestoreDBClusterToPointInTime", "RestoreDBInstanceFromS3", "RestoreDBInstanceToPointInTime",
		"RevokeDBSecurityGroupIngress",
		"StartActivityStream", "StartDBCluster", "StartDBInstanceAutomatedBackupsReplication", "StartExportTask",
		"StopActivityStream", "StopDBCluster", "StopDBInstanceAutomatedBackupsReplication",
		"SwitchoverBlueGreenDeployment", "SwitchoverGlobalCluster", "SwitchoverReadReplica",
	}
	ecs := []string{
		"CreateCluster", "DescribeClusters", "ListClusters", "DeleteCluster",
		"UpdateCluster", "UpdateClusterSettings", "PutClusterCapacityProviders",
		"RegisterTaskDefinition", "DescribeTaskDefinition", "ListTaskDefinitions", "DeregisterTaskDefinition",
		"CreateService", "DescribeServices", "ListServices", "UpdateService", "DeleteService",
		"RunTask", "StartTask", "StopTask", "DescribeTasks", "ListTasks",
		"TagResource", "UntagResource", "ListTagsForResource",
		"PutAccountSetting", "PutAccountSettingDefault", "ListAccountSettings", "DeleteAccountSetting",
		"CreateTaskSet", "DescribeTaskSets", "UpdateTaskSet", "DeleteTaskSet",
		"PutAttributes", "ListAttributes", "DeleteAttributes",
		"RegisterContainerInstance", "DescribeContainerInstances", "ListContainerInstances", "DeregisterContainerInstance",
		"ContinueServiceDeployment", "CreateCapacityProvider", "CreateDaemon", "CreateExpressGatewayService",
		"DeleteCapacityProvider", "DeleteDaemon", "DeleteDaemonTaskDefinition", "DeleteExpressGatewayService",
		"DeleteTaskDefinitions", "DescribeCapacityProviders", "DescribeDaemon", "DescribeDaemonDeployments",
		"DescribeDaemonRevisions", "DescribeDaemonTaskDefinition", "DescribeExpressGatewayService", "DescribeServiceDeployments",
		"DescribeServiceRevisions", "DiscoverPollEndpoint", "ExecuteCommand", "GetTaskProtection",
		"ListDaemonDeployments", "ListDaemonTaskDefinitions", "ListDaemons", "ListServiceDeployments",
		"ListServicesByNamespace", "ListTaskDefinitionFamilies", "RegisterDaemonTaskDefinition", "StopServiceDeployment",
		"SubmitAttachmentStateChanges", "SubmitContainerStateChange", "SubmitTaskStateChange", "UpdateCapacityProvider",
		"UpdateContainerAgent", "UpdateContainerInstancesState", "UpdateDaemon", "UpdateExpressGatewayService",
		"UpdateServicePrimaryTaskSet", "UpdateTaskProtection",
	}
	elb := []string{
		"CreateLoadBalancer", "DescribeLoadBalancers", "DeleteLoadBalancer",
		"ModifyLoadBalancerAttributes", "DescribeLoadBalancerAttributes",
		"CreateTargetGroup", "DescribeTargetGroups", "DeleteTargetGroup", "ModifyTargetGroup",
		"CreateListener", "DescribeListeners", "DeleteListener",
		"RegisterTargets", "DeregisterTargets", "DescribeTargetHealth",
		"AddTags", "RemoveTags", "DescribeTags",
		"AddListenerCertificates", "AddTrustStoreRevocations", "CreateRule", "CreateTrustStore",
		"DeleteRule", "DeleteSharedTrustStoreAssociation", "DeleteTrustStore", "DescribeAccountLimits",
		"DescribeCapacityReservation", "DescribeListenerAttributes", "DescribeListenerCertificates", "DescribeRules",
		"DescribeSSLPolicies", "DescribeTargetGroupAttributes", "DescribeTrustStoreAssociations", "DescribeTrustStoreRevocations",
		"DescribeTrustStores", "GetResourcePolicy", "GetTrustStoreCaCertificatesBundle", "GetTrustStoreRevocationContent",
		"ModifyCapacityReservation", "ModifyIpPools", "ModifyListener", "ModifyListenerAttributes",
		"ModifyRule", "ModifyTargetGroupAttributes", "ModifyTrustStore", "RemoveListenerCertificates",
		"RemoveTrustStoreRevocations", "SetIpAddressType", "SetRulePriorities", "SetSecurityGroups",
		"SetSubnets",
	}
	ecache := []string{
		"CreateCacheCluster", "DescribeCacheClusters", "DeleteCacheCluster", "ModifyCacheCluster",
		"CreateReplicationGroup", "DescribeReplicationGroups", "DeleteReplicationGroup",
		"CreateCacheSubnetGroup", "DescribeCacheSubnetGroups", "DeleteCacheSubnetGroup",
		"CreateSnapshot", "DescribeSnapshots", "DeleteSnapshot",
		"AddTagsToResource", "ListTagsForResource", "RemoveTagsFromResource",
		"AuthorizeCacheSecurityGroupIngress", "BatchApplyUpdateAction", "BatchStopUpdateAction", "CompleteMigration",
		"CopyServerlessCacheSnapshot", "CopySnapshot", "CreateCacheParameterGroup", "CreateCacheSecurityGroup",
		"CreateGlobalReplicationGroup", "CreateServerlessCache", "CreateServerlessCacheSnapshot", "CreateUser",
		"CreateUserGroup", "DecreaseNodeGroupsInGlobalReplicationGroup", "DecreaseReplicaCount",
		"DeleteCacheParameterGroup", "DeleteCacheSecurityGroup", "DeleteGlobalReplicationGroup",
		"DeleteServerlessCache", "DeleteServerlessCacheSnapshot", "DeleteUser", "DeleteUserGroup",
		"DescribeCacheEngineVersions", "DescribeCacheParameterGroups", "DescribeCacheParameters",
		"DescribeCacheSecurityGroups", "DescribeEngineDefaultParameters", "DescribeEvents",
		"DescribeGlobalReplicationGroups", "DescribeReservedCacheNodes", "DescribeReservedCacheNodesOfferings",
		"DescribeServerlessCacheSnapshots", "DescribeServerlessCaches", "DescribeServiceUpdates",
		"DescribeUpdateActions", "DescribeUserGroups", "DescribeUsers", "DisassociateGlobalReplicationGroup",
		"ExportServerlessCacheSnapshot", "FailoverGlobalReplicationGroup",
		"IncreaseNodeGroupsInGlobalReplicationGroup", "IncreaseReplicaCount", "ListAllowedNodeTypeModifications",
		"ModifyCacheParameterGroup", "ModifyCacheSubnetGroup", "ModifyGlobalReplicationGroup",
		"ModifyReplicationGroup", "ModifyReplicationGroupShardConfiguration", "ModifyServerlessCache",
		"ModifyUser", "ModifyUserGroup", "PurchaseReservedCacheNodesOffering",
		"RebalanceSlotsInGlobalReplicationGroup", "RebootCacheCluster", "ResetCacheParameterGroup",
		"RevokeCacheSecurityGroupIngress", "StartMigration", "TestFailover", "TestMigration",
	}
	asg := []string{
		"CreateAutoScalingGroup", "DescribeAutoScalingGroups", "UpdateAutoScalingGroup", "DeleteAutoScalingGroup",
		"CreateLaunchConfiguration", "DescribeLaunchConfigurations", "DeleteLaunchConfiguration",
		"SetDesiredCapacity", "TerminateInstanceInAutoScalingGroup",
		"CreateOrUpdateTags", "DescribeTags", "DeleteTags",
		"AttachInstances", "AttachLoadBalancerTargetGroups", "AttachLoadBalancers", "AttachTrafficSources",
		"BatchDeleteScheduledAction", "BatchPutScheduledUpdateGroupAction", "CancelInstanceRefresh",
		"CompleteLifecycleAction", "DeleteLifecycleHook", "DeleteNotificationConfiguration", "DeletePolicy",
		"DeleteScheduledAction", "DeleteWarmPool", "DescribeAccountLimits", "DescribeAdjustmentTypes",
		"DescribeAutoScalingInstances", "DescribeAutoScalingNotificationTypes", "DescribeInstanceRefreshes",
		"DescribeLifecycleHookTypes", "DescribeLifecycleHooks", "DescribeLoadBalancerTargetGroups",
		"DescribeLoadBalancers", "DescribeMetricCollectionTypes", "DescribeNotificationConfigurations",
		"DescribePolicies", "DescribeScalingActivities", "DescribeScalingProcessTypes", "DescribeScheduledActions",
		"DescribeTerminationPolicyTypes", "DescribeTrafficSources", "DescribeWarmPool",
		"DetachInstances", "DetachLoadBalancerTargetGroups", "DetachLoadBalancers", "DetachTrafficSources",
		"DisableMetricsCollection", "EnableMetricsCollection", "EnterStandby", "ExecutePolicy", "ExitStandby",
		"GetPredictiveScalingForecast", "LaunchInstances", "PutLifecycleHook", "PutNotificationConfiguration",
		"PutScalingPolicy", "PutScheduledUpdateGroupAction", "PutWarmPool", "RecordLifecycleActionHeartbeat",
		"ResumeProcesses", "RollbackInstanceRefresh", "SetInstanceHealth", "SetInstanceProtection",
		"StartInstanceRefresh", "SuspendProcesses",
	}
	ecr := []string{
		"CreateRepository", "DescribeRepositories", "DeleteRepository",
		"PutImage", "BatchGetImage", "ListImages", "BatchDeleteImage",
		"GetAuthorizationToken", "SetRepositoryPolicy", "GetRepositoryPolicy", "DeleteRepositoryPolicy",
		"TagResource", "ListTagsForResource", "UntagResource",
		"BatchCheckLayerAvailability", "BatchGetRepositoryScanningConfiguration", "CompleteLayerUpload", "CreatePullThroughCacheRule",
		"CreateRepositoryCreationTemplate", "DeleteLifecyclePolicy", "DeletePullThroughCacheRule", "DeleteRegistryPolicy",
		"DeleteRepositoryCreationTemplate", "DeleteSigningConfiguration", "DeregisterPullTimeUpdateExclusion", "DescribeImageReplicationStatus",
		"DescribeImageScanFindings", "DescribeImageSigningStatus", "DescribeImages", "DescribePullThroughCacheRules",
		"DescribeRegistry", "DescribeRepositoryCreationTemplates", "GetAccountSetting", "GetDownloadUrlForLayer",
		"GetLifecyclePolicy", "GetLifecyclePolicyPreview", "GetRegistryPolicy", "GetRegistryScanningConfiguration",
		"GetSigningConfiguration", "InitiateLayerUpload", "ListImageReferrers", "ListPullTimeUpdateExclusions",
		"PutAccountSetting", "PutImageScanningConfiguration", "PutImageTagMutability", "PutLifecyclePolicy",
		"PutRegistryPolicy", "PutRegistryScanningConfiguration", "PutReplicationConfiguration", "PutSigningConfiguration",
		"RegisterPullTimeUpdateExclusion", "StartImageScan", "StartLifecyclePolicyPreview", "UpdateImageStorageClass",
		"UpdatePullThroughCacheRule", "UpdateRepositoryCreationTemplate", "UploadLayerPart", "ValidatePullThroughCacheRule",
	}
	tag := []string{"GetResources", "TagResources", "UntagResources", "GetTagKeys", "GetTagValues",
		"GetComplianceSummary", "StartReportCreation", "DescribeReportCreation", "ListRequiredTags"}
	aas := []string{
		"RegisterScalableTarget", "DeregisterScalableTarget", "DescribeScalableTargets",
		"PutScalingPolicy", "DeleteScalingPolicy", "DescribeScalingPolicies",
		"PutScheduledAction", "DeleteScheduledAction", "DescribeScheduledActions",
		"DescribeScalingActivities", "GetPredictiveScalingForecast",
		"TagResource", "UntagResource", "ListTagsForResource",
	}
	rs := []string{
		"CreateCluster", "DescribeClusters", "ModifyCluster", "DeleteCluster", "RebootCluster",
		"PauseCluster", "ResumeCluster", "ResizeCluster", "RestoreFromClusterSnapshot",
		"CreateClusterSnapshot", "DescribeClusterSnapshots", "DeleteClusterSnapshot", "CopyClusterSnapshot",
		"CreateClusterSubnetGroup", "DescribeClusterSubnetGroups", "DeleteClusterSubnetGroup", "ModifyClusterSubnetGroup",
		"CreateClusterParameterGroup", "DescribeClusterParameterGroups", "DeleteClusterParameterGroup",
		"ModifyClusterParameterGroup", "ResetClusterParameterGroup", "DescribeClusterParameters",
		"EnableSnapshotCopy", "DisableSnapshotCopy",
		"CreateSnapshotCopyGrant", "DescribeSnapshotCopyGrants", "DeleteSnapshotCopyGrant",
		"CreateEventSubscription", "DescribeEventSubscriptions", "DeleteEventSubscription",
		"GetClusterCredentials", "ModifyClusterIamRoles",
		"CreateTags", "DescribeTags", "DeleteTags",
		"AcceptReservedNodeExchange", "AddPartner", "AssociateDataShareConsumer",
		"AuthorizeClusterSecurityGroupIngress", "AuthorizeDataShare", "AuthorizeEndpointAccess", "AuthorizeSnapshotAccess",
		"BatchDeleteClusterSnapshots", "BatchModifyClusterSnapshots", "CancelResize",
		"CreateAuthenticationProfile", "CreateClusterSecurityGroup", "CreateCustomDomainAssociation",
		"CreateEndpointAccess", "CreateHsmClientCertificate", "CreateHsmConfiguration", "CreateIntegration",
		"CreateQev2IdcApplication", "CreateRedshiftIdcApplication", "CreateScheduledAction",
		"CreateSnapshotSchedule", "CreateUsageLimit",
		"DeauthorizeDataShare", "DeleteAuthenticationProfile", "DeleteClusterSecurityGroup",
		"DeleteCustomDomainAssociation", "DeleteEndpointAccess", "DeleteHsmClientCertificate",
		"DeleteHsmConfiguration", "DeleteIntegration", "DeletePartner", "DeleteQev2IdcApplication",
		"DeleteRedshiftIdcApplication", "DeleteResourcePolicy", "DeleteScheduledAction",
		"DeleteSnapshotSchedule", "DeleteUsageLimit", "DeregisterNamespace",
		"DescribeAccountAttributes", "DescribeAuthenticationProfiles", "DescribeClusterDbRevisions",
		"DescribeClusterSecurityGroups", "DescribeClusterTracks", "DescribeClusterVersions",
		"DescribeCustomDomainAssociations", "DescribeDataShares", "DescribeDataSharesForConsumer",
		"DescribeDataSharesForProducer", "DescribeDefaultClusterParameters", "DescribeEndpointAccess",
		"DescribeEndpointAuthorization", "DescribeEventCategories", "DescribeEvents",
		"DescribeHsmClientCertificates", "DescribeHsmConfigurations", "DescribeInboundIntegrations",
		"DescribeIntegrations", "DescribeLoggingStatus", "DescribeNodeConfigurationOptions",
		"DescribeOrderableClusterOptions", "DescribePartners", "DescribeQev2IdcApplications",
		"DescribeRedshiftIdcApplications", "DescribeReservedNodeExchangeStatus", "DescribeReservedNodeOfferings",
		"DescribeReservedNodes", "DescribeResize", "DescribeScheduledActions", "DescribeSnapshotSchedules",
		"DescribeStorage", "DescribeTableRestoreStatus", "DescribeUsageLimits",
		"DisableLogging", "DisassociateDataShareConsumer", "EnableLogging", "FailoverPrimaryCompute",
		"GetClusterCredentialsWithIAM", "GetIdentityCenterAuthToken",
		"GetReservedNodeExchangeConfigurationOptions", "GetReservedNodeExchangeOfferings", "GetResourcePolicy",
		"ListRecommendations",
		"ModifyAquaConfiguration", "ModifyAuthenticationProfile", "ModifyClusterDbRevision",
		"ModifyClusterMaintenance", "ModifyClusterSnapshot", "ModifyClusterSnapshotSchedule",
		"ModifyCustomDomainAssociation", "ModifyEndpointAccess", "ModifyEventSubscription", "ModifyIntegration",
		"ModifyLakehouseConfiguration", "ModifyQev2IdcApplication", "ModifyRedshiftIdcApplication",
		"ModifyScheduledAction", "ModifySnapshotCopyRetentionPeriod", "ModifySnapshotSchedule", "ModifyUsageLimit",
		"PurchaseReservedNodeOffering", "PutResourcePolicy", "RegisterNamespace", "RejectDataShare",
		"RestoreTableFromClusterSnapshot", "RevokeClusterSecurityGroupIngress", "RevokeEndpointAccess",
		"RevokeSnapshotAccess", "RotateEncryptionKey", "UpdatePartnerStatus",
	}
	eks := []string{
		"CreateCluster", "DescribeCluster", "ListClusters", "DeleteCluster", "UpdateClusterConfig",
		"CreateNodegroup", "DescribeNodegroup", "ListNodegroups", "DeleteNodegroup",
		"CreateFargateProfile", "DescribeFargateProfile", "ListFargateProfiles", "DeleteFargateProfile",
		"ListTagsForResource", "TagResource", "UntagResource",
		"ActivateCertificateAuthority", "AssociateAccessPolicy", "AssociateEncryptionConfig", "AssociateIdentityProviderConfig",
		"CancelUpdate", "CreateAccessEntry", "CreateAddon", "CreateCapability",
		"CreateCertificateAuthority", "CreateEksAnywhereSubscription", "CreatePodIdentityAssociation", "DeleteAccessEntry",
		"DeleteAddon", "DeleteCapability", "DeleteCertificateAuthority", "DeleteEksAnywhereSubscription",
		"DeletePodIdentityAssociation", "DeregisterCluster", "DescribeAccessEntry", "DescribeAddon",
		"DescribeAddonConfiguration", "DescribeAddonVersions", "DescribeCapability", "DescribeCertificateAuthority",
		"DescribeClusterVersions", "DescribeEksAnywhereSubscription", "DescribeIdentityProviderConfig", "DescribeInsight",
		"DescribeInsightsRefresh", "DescribePodIdentityAssociation", "DescribeUpdate", "DisassociateAccessPolicy",
		"DisassociateIdentityProviderConfig", "ListAccessEntries", "ListAccessPolicies", "ListAddons",
		"ListAssociatedAccessPolicies", "ListCapabilities", "ListCertificateAuthorities", "ListEksAnywhereSubscriptions",
		"ListIdentityProviderConfigs", "ListInsights", "ListPodIdentityAssociations", "ListUpdates",
		"RegisterCluster", "StartInsightsRefresh", "UpdateAccessEntry", "UpdateAddon",
		"UpdateCapability", "UpdateClusterVersion", "UpdateEksAnywhereSubscription", "UpdateNodegroupConfig",
		"UpdateNodegroupVersion", "UpdatePodIdentityAssociation",
	}

	ec2 := []string{
		"CreateVpc", "DescribeVpcs", "DeleteVpc",
		"CreateSubnet", "DescribeSubnets", "DeleteSubnet",
		"CreateSecurityGroup", "DescribeSecurityGroups", "DeleteSecurityGroup",
		"RunInstances", "DescribeInstances", "TerminateInstances",
	}
	ses := []string{
		"VerifyEmailIdentity", "DeleteIdentity", "ListIdentities", "GetIdentityVerificationAttributes",
		"SendEmail", "SendRawEmail", "GetSendQuota", "GetSendStatistics",
	}
	cognito := []string{
		"CreateUserPool", "DescribeUserPool", "ListUserPools", "DeleteUserPool",
		"AdminCreateUser", "AdminGetUser", "ListUsers", "AdminDeleteUser",
		"AdminSetUserPassword", "CreateUserPoolClient", "DescribeUserPoolClient", "ListUserPoolClients", "DeleteUserPoolClient",
		"SignUp", "ConfirmSignUp", "InitiateAuth", "AdminInitiateAuth", "GetUser", "GlobalSignOut",
	}
	states := []string{
		"CreateStateMachine", "UpdateStateMachine", "DeleteStateMachine", "DescribeStateMachine", "ListStateMachines",
		"PublishStateMachineVersion", "ListStateMachineVersions", "DeleteStateMachineVersion",
		"CreateStateMachineAlias", "DescribeStateMachineAlias", "ListStateMachineAliases", "UpdateStateMachineAlias", "DeleteStateMachineAlias",
		"StartExecution", "StartSyncExecution", "StopExecution", "DescribeExecution", "ListExecutions", "GetExecutionHistory",
		"CreateActivity", "DeleteActivity", "DescribeActivity", "ListActivities", "GetActivityTask",
		"SendTaskSuccess", "SendTaskFailure", "SendTaskHeartbeat",
		"TestState", "ValidateStateMachineDefinition",
		"TagResource", "UntagResource", "ListTagsForResource",
	}
	firehose := []string{
		"CreateDeliveryStream", "DeleteDeliveryStream", "DescribeDeliveryStream", "ListDeliveryStreams",
		"PutRecord", "PutRecordBatch", "UpdateDestination",
		"ListTagsForDeliveryStream", "TagDeliveryStream", "UntagDeliveryStream",
		"StartDeliveryStreamEncryption", "StopDeliveryStreamEncryption",
	}
	cloudfront := []string{
		"CreateDistribution", "GetDistribution", "ListDistributions", "DeleteDistribution",
		"GetDistributionConfig", "UpdateDistribution",
		"CreateInvalidation", "GetInvalidation", "ListInvalidations",
	}
	scheduler := []string{
		"CreateSchedule", "GetSchedule", "ListSchedules", "UpdateSchedule", "DeleteSchedule",
		"CreateScheduleGroup", "GetScheduleGroup", "ListScheduleGroups", "DeleteScheduleGroup",
	}
	es := []string{
		"CreateDomain", "DescribeDomain", "DescribeDomains", "ListDomainNames", "DeleteDomain",
		"UpdateDomainConfig", "DescribeDomainConfig",
		"AddTags", "ListTags", "RemoveTags",
		"IndexDocument", "GetDocument", "DeleteDocument", "Search",
	}
	glue := []string{
		"CreateDatabase", "GetDatabase", "GetDatabases", "UpdateDatabase", "DeleteDatabase",
		"CreateTable", "GetTable", "GetTables", "UpdateTable", "DeleteTable",
		"CreateJob", "GetJob", "GetJobs", "DeleteJob", "StartJobRun", "GetJobRun", "GetJobRuns",
		"CreateCrawler", "GetCrawler", "GetCrawlers", "DeleteCrawler",
	}
	athena := []string{
		"StartQueryExecution", "GetQueryExecution", "GetQueryResults", "StopQueryExecution", "ListQueryExecutions",
		"CreateWorkGroup", "GetWorkGroup", "ListWorkGroups", "DeleteWorkGroup",
	}
	cloudtrail := []string{
		"CreateTrail", "DescribeTrails", "GetTrail", "DeleteTrail", "UpdateTrail",
		"StartLogging", "StopLogging", "GetTrailStatus",
		"PutEventSelectors", "GetEventSelectors", "LookupEvents",
	}
	organizations := []string{
		"CreateOrganization", "DescribeOrganization", "DeleteOrganization",
		"CreateAccount", "DescribeAccount", "ListAccounts", "MoveAccount", "ListParents", "ListChildren",
		"CreateOrganizationalUnit", "DescribeOrganizationalUnit", "ListOrganizationalUnitsForParent", "DeleteOrganizationalUnit",
		"ListRoots", "EnablePolicyType", "DisablePolicyType",
		"CreatePolicy", "DescribePolicy", "ListPolicies", "DeletePolicy",
		"AttachPolicy", "DetachPolicy", "ListPoliciesForTarget", "ListTargetsForPolicy",
	}
	transfer := []string{
		"CreateServer", "DescribeServer", "ListServers", "DeleteServer", "StartServer", "StopServer",
		"CreateUser", "DescribeUser", "ListUsers", "DeleteUser",
		"ImportSshPublicKey", "DeleteSshPublicKey",
	}
	wafv2 := []string{
		"CreateWebACL", "GetWebACL", "ListWebACLs", "DeleteWebACL", "UpdateWebACL",
		"CreateIPSet", "GetIPSet", "ListIPSets", "DeleteIPSet",
		"CreateRuleGroup", "GetRuleGroup", "ListRuleGroups", "DeleteRuleGroup",
	}
	appconfig := []string{
		"CreateApplication", "GetApplication", "ListApplications", "DeleteApplication",
		"CreateEnvironment", "GetEnvironment", "ListEnvironments", "DeleteEnvironment",
		"CreateConfigurationProfile", "GetConfigurationProfile", "ListConfigurationProfiles", "DeleteConfigurationProfile",
		"CreateHostedConfigurationVersion", "GetHostedConfigurationVersion", "ListHostedConfigurationVersions",
		"StartDeployment", "GetDeployment", "GetLatestConfiguration",
	}
	codebuild := []string{
		"CreateProject", "BatchGetProjects", "ListProjects", "UpdateProject", "DeleteProject",
		"StartBuild", "BatchGetBuilds", "ListBuilds", "StopBuild",
	}
	batch := []string{
		"CreateComputeEnvironment", "DescribeComputeEnvironments", "DeleteComputeEnvironment",
		"CreateJobQueue", "DescribeJobQueues", "DeleteJobQueue",
		"RegisterJobDefinition", "DescribeJobDefinitions", "DeregisterJobDefinition",
		"SubmitJob", "DescribeJobs", "ListJobs", "TerminateJob",
	}
	emr := []string{
		"RunJobFlow", "DescribeCluster", "ListClusters", "TerminateJobFlows",
		"AddJobFlowSteps", "ListSteps", "DescribeStep", "SetTerminationProtection",
	}
	kafka := []string{
		"CreateCluster", "DescribeCluster", "ListClusters", "DeleteCluster",
		"GetBootstrapBrokers", "ListNodes", "UpdateBrokerCount",
	}
	backup := []string{
		"CreateBackupVault", "DescribeBackupVault", "ListBackupVaults", "DeleteBackupVault",
		"CreateBackupPlan", "GetBackupPlan", "ListBackupPlans", "DeleteBackupPlan",
		"StartBackupJob", "DescribeBackupJob", "ListBackupJobs",
	}
	cognitoIdent := []string{
		"CreateIdentityPool", "DescribeIdentityPool", "ListIdentityPools", "DeleteIdentityPool", "UpdateIdentityPool",
		"GetId", "GetCredentialsForIdentity", "GetOpenIdToken",
		"SetIdentityPoolRoles", "GetIdentityPoolRoles",
	}
	sesv2 := []string{
		"CreateEmailIdentity", "GetEmailIdentity", "ListEmailIdentities", "DeleteEmailIdentity",
		"SendEmail", "SendBulkEmail", "GetAccount",
	}
	cfgops := []string{
		"PutConfigurationRecorder", "DescribeConfigurationRecorders", "DeleteConfigurationRecorder",
		"PutDeliveryChannel", "DescribeDeliveryChannels", "DeleteDeliveryChannel",
		"StartConfigurationRecorder", "StopConfigurationRecorder",
		"PutConfigRule", "DescribeConfigRules", "DeleteConfigRule",
		"PutEvaluations", "GetComplianceDetailsByConfigRule", "DescribeComplianceByConfigRule",
		"GetResourceConfigHistory", "ListDiscoveredResources",
	}
	xray := []string{
		"PutTraceSegments", "BatchGetTraces", "GetTraceSummaries", "GetServiceGraph",
		"CreateGroup", "GetGroup", "GetGroups", "UpdateGroup", "DeleteGroup",
		"PutTelemetryRecords", "GetSamplingRules",
	}
	guardduty := []string{
		"CreateDetector", "GetDetector", "ListDetectors", "UpdateDetector", "DeleteDetector",
		"CreateIPSet", "GetIPSet", "ListIPSets", "DeleteIPSet",
		"CreateFilter", "GetFilter", "ListFilters", "DeleteFilter",
		"CreateSampleFindings", "GetFindings", "ListFindings",
		"CreateMembers", "GetMembers", "ListMembers", "DeleteMembers",
	}
	mqops := []string{
		"CreateBroker", "DescribeBroker", "ListBrokers", "DeleteBroker", "RebootBroker",
		"CreateUser", "DescribeUser", "ListUsers", "DeleteUser",
		"CreateConfiguration", "DescribeConfiguration", "ListConfigurations", "DeleteConfiguration",
		"DescribeBrokerEngineTypes",
	}
	docdb := []string{
		"CreateDBCluster", "DescribeDBClusters", "ModifyDBCluster", "DeleteDBCluster", "FailoverDBCluster",
		"CreateDBInstance", "DescribeDBInstances", "DeleteDBInstance",
		"CreateDBSubnetGroup", "DescribeDBSubnetGroups", "DeleteDBSubnetGroup",
		"CreateDBClusterSnapshot", "DescribeDBClusterSnapshots", "DeleteDBClusterSnapshot",
	}
	neptune := []string{
		"CreateDBCluster", "DescribeDBClusters", "ModifyDBCluster", "DeleteDBCluster", "FailoverDBCluster",
		"CreateDBInstance", "DescribeDBInstances", "DeleteDBInstance",
		"CreateDBSubnetGroup", "DescribeDBSubnetGroups", "DeleteDBSubnetGroup",
		"CreateDBClusterSnapshot", "DescribeDBClusterSnapshots", "DeleteDBClusterSnapshot",
	}
	iotops := []string{
		"CreateThing", "DescribeThing", "ListThings", "UpdateThing", "DeleteThing",
		"CreateThingType", "DescribeThingType", "ListThingTypes", "DeleteThingType",
		"CreatePolicy", "GetPolicy", "ListPolicies", "DeletePolicy",
		"CreateTopicRule", "GetTopicRule", "ListTopicRules", "DeleteTopicRule",
		"CreateJob", "DescribeJob", "ListJobs", "DeleteJob",
		"CreateKeysAndCertificate", "DescribeCertificate", "ListCertificates", "DeleteCertificate",
		"AttachThingPrincipal", "ListThingPrincipals", "DetachThingPrincipal",
	}
	pipes := []string{
		"CreatePipe", "DescribePipe", "ListPipes", "UpdatePipe", "DeletePipe",
		"StartPipe", "StopPipe",
		"TagResource", "UntagResource", "ListTagsForResource",
	}
	codepipeline := []string{
		"CreatePipeline", "GetPipeline", "GetPipelineState", "ListPipelines", "UpdatePipeline", "DeletePipeline",
		"StartPipelineExecution", "GetPipelineExecution", "ListPipelineExecutions", "StopPipelineExecution",
	}
	appsync := []string{
		"CreateGraphqlApi", "GetGraphqlApi", "ListGraphqlApis", "UpdateGraphqlApi", "DeleteGraphqlApi",
		"CreateApiKey", "ListApiKeys", "DeleteApiKey",
		"StartSchemaCreation", "GetSchemaCreationStatus",
		"CreateDataSource", "GetDataSource", "ListDataSources", "DeleteDataSource",
		"GraphQL",
	}
	apigwv2 := []string{
		"CreateApi", "GetApi", "GetApis", "UpdateApi", "DeleteApi",
		"CreateRoute", "GetRoute", "GetRoutes", "DeleteRoute",
		"CreateIntegration", "GetIntegration", "GetIntegrations", "DeleteIntegration",
		"CreateStage", "GetStage", "GetStages", "DeleteStage",
	}
	codecommit := []string{
		"CreateRepository", "GetRepository", "ListRepositories", "UpdateRepository", "DeleteRepository",
		"CreateBranch", "GetBranch", "ListBranches", "DeleteBranch",
		"PutFile", "GetFile", "DeleteFile",
	}
	codedeploy := []string{
		"CreateApplication", "GetApplication", "ListApplications", "DeleteApplication",
		"CreateDeploymentGroup", "GetDeploymentGroup", "ListDeploymentGroups", "DeleteDeploymentGroup",
		"CreateDeployment", "GetDeployment", "ListDeployments", "StopDeployment",
	}
	amplify := []string{
		"CreateApp", "GetApp", "ListApps", "UpdateApp", "DeleteApp",
		"CreateBranch", "GetBranch", "ListBranches", "DeleteBranch",
		"StartJob", "GetJob", "ListJobs",
	}
	inspector := []string{
		"CreateAssessmentTarget", "DescribeAssessmentTargets", "DeleteAssessmentTarget",
		"CreateAssessmentTemplate", "DescribeAssessmentTemplates", "DeleteAssessmentTemplate",
		"StartAssessmentRun", "DescribeAssessmentRuns", "ListAssessmentRuns", "StopAssessmentRun",
	}
	securityhub := []string{
		"EnableSecurityHub", "DisableSecurityHub", "DescribeHub",
		"BatchImportFindings", "GetFindings", "BatchUpdateFindings",
		"CreateInsight", "GetInsights", "DeleteInsight",
	}
	timestream := []string{
		"CreateDatabase", "DescribeDatabase", "ListDatabases", "UpdateDatabase", "DeleteDatabase",
		"CreateTable", "DescribeTable", "ListTables", "DeleteTable",
		"WriteRecords", "Query", "DescribeEndpoints",
	}
	qldb := []string{
		"CreateLedger", "DescribeLedger", "ListLedgers", "UpdateLedger", "DeleteLedger",
		"GetDigest", "GetBlock", "SendCommand",
	}
	dms := []string{
		"CreateReplicationInstance", "DescribeReplicationInstances", "DeleteReplicationInstance",
		"CreateEndpoint", "DescribeEndpoints", "DeleteEndpoint",
		"CreateReplicationTask", "DescribeReplicationTasks", "StartReplicationTask", "StopReplicationTask", "DeleteReplicationTask",
	}
	mediaconvert := []string{
		"CreateQueue", "GetQueue", "ListQueues", "DeleteQueue",
		"CreateJobTemplate", "GetJobTemplate", "ListJobTemplates", "DeleteJobTemplate",
		"CreateJob", "GetJob", "ListJobs", "CancelJob",
	}
	elasticsearch := []string{
		"CreateElasticsearchDomain", "DescribeElasticsearchDomain", "DescribeElasticsearchDomains",
		"ListDomainNames", "UpdateElasticsearchDomainConfig", "DeleteElasticsearchDomain",
	}
	elasticbeanstalk := []string{
		"CreateApplication", "DescribeApplications", "UpdateApplication", "DeleteApplication",
		"CreateApplicationVersion", "DescribeApplicationVersions", "DeleteApplicationVersion",
		"CreateEnvironment", "DescribeEnvironments", "UpdateEnvironment", "TerminateEnvironment",
	}
	swfops := []string{
		"RegisterDomain", "DescribeDomain", "ListDomains", "DeprecateDomain",
		"RegisterWorkflowType", "DescribeWorkflowType", "ListWorkflowTypes",
		"RegisterActivityType", "DescribeActivityType", "ListActivityTypes",
		"StartWorkflowExecution", "DescribeWorkflowExecution", "TerminateWorkflowExecution", "ListOpenWorkflowExecutions",
		"PollForActivityTask",
	}
	efsops := []string{
		"CreateFileSystem", "DescribeFileSystems", "DeleteFileSystem",
		"CreateMountTarget", "DescribeMountTargets", "DeleteMountTarget",
		"CreateAccessPoint", "DescribeAccessPoints", "DeleteAccessPoint",
	}
	glacier := []string{
		"CreateVault", "DescribeVault", "ListVaults", "DeleteVault",
		"UploadArchive", "DeleteArchive",
		"InitiateJob", "DescribeJob", "ListJobs",
	}
	servicediscovery := []string{
		"CreateHttpNamespace", "CreatePrivateDnsNamespace", "GetNamespace", "ListNamespaces", "DeleteNamespace",
		"CreateService", "GetService", "ListServices", "DeleteService",
		"RegisterInstance", "GetInstance", "ListInstances", "DeregisterInstance",
	}
	ramops := []string{
		"CreateResourceShare", "GetResourceShares", "UpdateResourceShare", "DeleteResourceShare",
		"AssociateResourceShare", "DisassociateResourceShare", "GetResourceShareAssociations",
		"ListResources",
	}
	sagemaker := []string{
		"CreateNotebookInstance", "DescribeNotebookInstance", "ListNotebookInstances", "DeleteNotebookInstance",
		"CreateModel", "DescribeModel", "ListModels", "DeleteModel",
		"CreateEndpointConfig", "DescribeEndpointConfig", "DeleteEndpointConfig",
		"CreateEndpoint", "DescribeEndpoint", "ListEndpoints", "DeleteEndpoint",
	}
	workspaces := []string{
		"CreateWorkspaces", "DescribeWorkspaces", "StopWorkspaces", "StartWorkspaces",
		"RebootWorkspaces", "TerminateWorkspaces", "ModifyWorkspaceState",
		"DescribeWorkspaceBundles", "DescribeWorkspaceDirectories",
	}
	transcribe := []string{
		"StartTranscriptionJob", "GetTranscriptionJob", "ListTranscriptionJobs", "DeleteTranscriptionJob",
		"CreateVocabulary", "GetVocabulary", "ListVocabularies", "DeleteVocabulary",
	}
	rekognition := []string{
		"CreateCollection", "DescribeCollection", "ListCollections", "DeleteCollection",
		"IndexFaces", "SearchFacesByImage",
		"DetectLabels", "DetectFaces", "DetectText", "DetectModerationLabels",
	}
	comprehend := []string{
		"DetectSentiment", "DetectEntities", "DetectKeyPhrases", "DetectDominantLanguage", "BatchDetectSentiment",
		"CreateEndpoint", "DescribeEndpoint", "ListEndpoints", "DeleteEndpoint",
		"StartDocumentClassificationJob", "DescribeDocumentClassificationJob", "ListDocumentClassificationJobs",
	}
	mediastore := []string{
		"CreateContainer", "DescribeContainer", "ListContainers", "DeleteContainer",
		"PutContainerPolicy", "GetContainerPolicy",
	}
	kinesisanalytics := []string{
		"CreateApplication", "DescribeApplication", "ListApplications", "UpdateApplication", "DeleteApplication",
		"StartApplication", "StopApplication",
		"AddApplicationInput", "AddApplicationOutput",
	}
	translate := []string{
		"TranslateText",
		"CreateTerminology", "GetTerminology", "ListTerminologies", "DeleteTerminology",
		"StartTextTranslationJob", "DescribeTextTranslationJob", "ListTextTranslationJobs", "StopTextTranslationJob",
	}
	textract := []string{
		"DetectDocumentText", "AnalyzeDocument",
		"StartDocumentTextDetection", "GetDocumentTextDetection",
		"StartDocumentAnalysis", "GetDocumentAnalysis",
	}
	polly := []string{
		"SynthesizeSpeech", "DescribeVoices",
		"StartSpeechSynthesisTask", "GetSpeechSynthesisTask", "ListSpeechSynthesisTasks",
	}
	fsxops := []string{
		"CreateFileSystem", "DescribeFileSystems", "UpdateFileSystem", "DeleteFileSystem",
		"CreateBackup", "DescribeBackups", "DeleteBackup",
	}
	s3control := []string{
		"CreateAccessPoint", "GetAccessPoint", "ListAccessPoints", "DeleteAccessPoint",
		"PutPublicAccessBlock", "GetPublicAccessBlock", "DeletePublicAccessBlock",
	}
	route53resolver := []string{
		"CreateResolverEndpoint", "GetResolverEndpoint", "ListResolverEndpoints", "DeleteResolverEndpoint",
		"CreateResolverRule", "GetResolverRule", "ListResolverRules", "DeleteResolverRule",
	}
	servicecatalog := []string{
		"CreateProduct", "DescribeProduct", "DeleteProduct",
		"CreatePortfolio", "DescribePortfolio", "ListPortfolios", "DeletePortfolio",
		"AssociateProductWithPortfolio", "DisassociateProductFromPortfolio",
	}
	shield := []string{
		"CreateProtection", "DescribeProtection", "ListProtections", "DeleteProtection",
		"CreateSubscription", "GetSubscriptionState",
	}
	wafclassic := []string{
		"CreateWebACL", "GetWebACL", "ListWebACLs", "DeleteWebACL",
		"CreateIPSet", "GetIPSet", "ListIPSets", "DeleteIPSet",
	}
	storagegateway := []string{
		"ActivateGateway", "DescribeGatewayInformation", "ListGateways", "DeleteGateway",
		"CreateNFSFileShare", "DescribeNFSFileShares", "DeleteFileShare",
	}
	lakeformation := []string{
		"PutDataLakeSettings", "GetDataLakeSettings",
		"GrantPermissions", "ListPermissions", "RevokePermissions",
		"RegisterResource", "ListResources", "DeregisterResource",
	}
	connect := []string{
		"CreateInstance", "DescribeInstance", "ListInstances", "DeleteInstance",
		"CreateUser", "DescribeUser", "ListUsers", "DeleteUser",
	}
	pinpoint := []string{
		"CreateApp", "GetApp", "GetApps", "DeleteApp",
		"CreateCampaign", "GetCampaign", "DeleteCampaign",
		"SendMessages",
	}
	daxops := []string{
		"CreateCluster", "DescribeClusters", "DeleteCluster",
		"CreateParameterGroup", "DescribeParameterGroups", "DeleteParameterGroup",
	}
	memorydb := []string{
		"CreateCluster", "DescribeClusters", "DeleteCluster",
		"CreateUser", "DescribeUsers", "DeleteUser",
	}
	keyspaces := []string{
		"CreateKeyspace", "GetKeyspace", "ListKeyspaces", "DeleteKeyspace",
		"CreateTable", "GetTable", "ListTables", "DeleteTable",
	}
	mwaa := []string{
		"CreateEnvironment", "GetEnvironment", "ListEnvironments", "UpdateEnvironment", "DeleteEnvironment",
		"CreateCliToken",
	}
	ssoadmin := []string{
		"CreatePermissionSet", "DescribePermissionSet", "ListPermissionSets", "DeletePermissionSet",
		"CreateAccountAssignment", "ListAccountAssignments", "DeleteAccountAssignment",
	}
	acmpca := []string{
		"CreateCertificateAuthority", "DescribeCertificateAuthority", "ListCertificateAuthorities", "DeleteCertificateAuthority",
		"IssueCertificate", "GetCertificate",
	}
	lightsail := []string{
		"CreateInstances", "GetInstance", "GetInstances", "DeleteInstance",
		"AllocateStaticIp", "GetStaticIp", "ReleaseStaticIp",
	}
	location := []string{
		"CreatePlaceIndex", "DescribePlaceIndex", "ListPlaceIndexes", "DeletePlaceIndex",
		"SearchPlaceIndexForText",
		"CreateGeofenceCollection", "DescribeGeofenceCollection", "DeleteGeofenceCollection",
	}
	kendra := []string{
		"CreateIndex", "DescribeIndex", "ListIndices", "DeleteIndex",
		"Query",
		"CreateDataSource", "DescribeDataSource", "DeleteDataSource",
	}
	quicksight := []string{
		"CreateDataSet", "DescribeDataSet", "ListDataSets", "DeleteDataSet",
		"CreateDashboard", "DescribeDashboard", "ListDashboards", "DeleteDashboard",
	}
	identitystore := []string{
		"CreateUser", "DescribeUser", "ListUsers", "DeleteUser",
		"CreateGroup", "DescribeGroup", "ListGroups", "DeleteGroup",
	}
	workmail := []string{
		"CreateOrganization", "DescribeOrganization", "ListOrganizations", "DeleteOrganization",
		"CreateUser", "DescribeUser", "ListUsers", "DeleteUser",
	}
	directconnect := []string{
		"CreateConnection", "DescribeConnections", "DeleteConnection",
		"CreateLag", "DescribeLags", "DeleteLag",
	}
	dsops := []string{
		"CreateDirectory", "DescribeDirectories", "DeleteDirectory",
		"CreateMicrosoftAD", "CreateAlias", "DescribeTrusts",
	}
	gamelift := []string{
		"CreateFleet", "DescribeFleetAttributes", "ListFleets", "DeleteFleet",
		"CreateGameSession", "DescribeGameSessions", "CreatePlayerSession",
	}
	forecast := []string{
		"CreateDataset", "DescribeDataset", "ListDatasets", "DeleteDataset",
		"CreatePredictor", "DescribePredictor", "DeletePredictor",
	}
	personalize := []string{
		"CreateDatasetGroup", "DescribeDatasetGroup", "ListDatasetGroups", "DeleteDatasetGroup",
		"CreateSolution", "DescribeSolution", "DeleteSolution",
	}
	lexmodels := []string{
		"PutBot", "GetBot", "GetBots", "DeleteBot",
		"PutIntent", "GetIntent", "DeleteIntent",
	}
	medialive := []string{
		"CreateChannel", "DescribeChannel", "ListChannels", "DeleteChannel",
		"CreateInput", "DescribeInput", "DeleteInput",
	}
	mediapackage := []string{
		"CreateChannel", "DescribeChannel", "ListChannels", "DeleteChannel",
		"CreateOriginEndpoint", "DescribeOriginEndpoint", "DeleteOriginEndpoint",
	}
	mediaconnect := []string{
		"CreateFlow", "DescribeFlow", "ListFlows", "DeleteFlow",
		"StartFlow", "StopFlow",
	}
	elastictranscoder := []string{
		"CreatePipeline", "ReadPipeline", "ListPipelines", "DeletePipeline",
		"CreateJob", "ReadJob", "ListJobsByPipeline",
	}
	cloudhsmv2 := []string{
		"CreateCluster", "DescribeClusters", "DeleteCluster",
		"CreateHsm", "DeleteHsm", "DescribeBackups",
	}
	macie2 := []string{
		"EnableMacie", "GetMacieSession", "DisableMacie",
		"CreateClassificationJob", "DescribeClassificationJob", "ListClassificationJobs",
	}
	accessanalyzer := []string{
		"CreateAnalyzer", "GetAnalyzer", "ListAnalyzers", "DeleteAnalyzer",
		"ListFindings", "CreateArchiveRule", "DeleteArchiveRule",
	}
	comprehendmedical := []string{
		"DetectEntitiesV2", "DetectPHI", "InferICD10CM",
		"StartEntitiesDetectionV2Job", "DescribeEntitiesDetectionV2Job", "ListEntitiesDetectionV2Jobs",
	}
	frauddetector := []string{
		"PutDetector", "GetDetectors", "DeleteDetector",
		"PutEventType", "GetEventTypes", "DeleteEventType",
	}
	appmesh := []string{
		"CreateMesh", "DescribeMesh", "ListMeshes", "DeleteMesh",
		"CreateVirtualNode", "DescribeVirtualNode", "DeleteVirtualNode",
	}
	healthlake := []string{
		"CreateFHIRDatastore", "DescribeFHIRDatastore", "ListFHIRDatastores", "DeleteFHIRDatastore",
		"StartFHIRImportJob", "DescribeFHIRImportJob",
	}
	lookoutmetrics := []string{
		"CreateAnomalyDetector", "DescribeAnomalyDetector", "ListAnomalyDetectors", "DeleteAnomalyDetector",
		"CreateAlert", "DescribeAlert", "ListAlerts",
	}
	bedrock := []string{
		"CreateGuardrail", "GetGuardrail", "ListGuardrails", "DeleteGuardrail",
		"ListFoundationModels", "CreateModelCustomizationJob",
	}
	fisops := []string{
		"CreateExperimentTemplate", "GetExperimentTemplate", "ListExperimentTemplates", "DeleteExperimentTemplate",
		"StartExperiment", "GetExperiment",
	}
	ceops := []string{
		"CreateAnomalyMonitor", "GetAnomalyMonitors", "DeleteAnomalyMonitor",
		"CreateCostCategoryDefinition", "DescribeCostCategoryDefinition", "DeleteCostCategoryDefinition",
		"GetCostAndUsage",
	}
	resourcegroups := []string{
		"CreateGroup", "GetGroup", "ListGroups", "DeleteGroup",
		"GroupResources", "ListGroupResources", "UngroupResources",
	}
	verifiedpermissions := []string{
		"CreatePolicyStore", "GetPolicyStore", "ListPolicyStores", "DeletePolicyStore",
		"CreatePolicy", "GetPolicy", "DeletePolicy",
	}
	supportops := []string{
		"CreateCase", "DescribeCases", "ResolveCase",
		"DescribeServices", "DescribeSeverityLevels", "AddCommunicationToCase",
	}
	codeartifact := []string{
		"CreateDomain", "DescribeDomain", "ListDomains", "DeleteDomain",
		"CreateRepository", "DescribeRepository", "DeleteRepository",
	}
	cloudcontrol := []string{
		"CreateResource", "GetResource", "ListResources", "DeleteResource", "UpdateResource", "GetResourceRequestStatus",
	}
	serverlessrepo := []string{
		"CreateApplication", "GetApplication", "ListApplications", "DeleteApplication", "CreateApplicationVersion", "ListApplicationVersions",
	}
	account := []string{
		"PutAlternateContact", "GetAlternateContact", "DeleteAlternateContact",
		"PutContactInformation", "GetContactInformation", "ListRegions",
	}
	iotwireless := []string{
		"CreateDestination", "GetDestination", "ListDestinations", "DeleteDestination",
		"CreateWirelessDevice", "GetWirelessDevice",
	}
	s3tables := []string{
		"CreateTableBucket", "GetTableBucket", "ListTableBuckets", "DeleteTableBucket",
		"CreateNamespace", "ListNamespaces",
		"CreateTable", "GetTable", "ListTables", "DeleteTable", "RenameTable", "GetTableMetadataLocation", "UpdateTableMetadataLocation",
	}
	synthetics := []string{"CreateCanary", "GetCanary", "ListCanaries", "DeleteCanary", "StartCanary", "StopCanary"}
	apprunner := []string{"CreateService", "DescribeService", "ListServices", "DeleteService", "PauseService", "ResumeService"}
	proton := []string{"CreateEnvironment", "GetEnvironment", "ListEnvironments", "DeleteEnvironment", "CreateService", "GetService"}
	resiliencehub := []string{"CreateApp", "DescribeApp", "ListApps", "DeleteApp", "CreateResiliencyPolicy", "ListResiliencyPolicies"}
	resourceexplorer2 := []string{"CreateIndex", "GetIndex", "ListIndexes", "DeleteIndex", "CreateView", "GetView"}
	rumops := []string{"CreateAppMonitor", "GetAppMonitor", "ListAppMonitors", "DeleteAppMonitor", "PutRumEvents", "GetAppMonitorData"}
	schemasops := []string{"CreateRegistry", "DescribeRegistry", "ListRegistries", "DeleteRegistry", "CreateSchema", "DescribeSchema"}
	dsqlops := []string{"CreateCluster", "GetCluster", "ListClusters", "DeleteCluster", "UpdateCluster", "GetVpcEndpointServiceName"}
	codeconnections := []string{"CreateConnection", "GetConnection", "ListConnections", "DeleteConnection", "CreateHost", "GetHost"}
	iotdata := []string{"UpdateThingShadow", "GetThingShadow", "DeleteThingShadow", "ListNamedShadowsForThing", "Publish", "GetRetainedMessage"}
	managedblockchain := []string{"CreateNetwork", "GetNetwork", "ListNetworks", "DeleteNetwork", "CreateMember", "GetMember"}
	kinesisanalyticsv2 := []string{"CreateApplication", "DescribeApplication", "ListApplications", "DeleteApplication", "StartApplication", "StopApplication"}
	mk := func(names []string) []model.Operation {
		o := make([]model.Operation, 0, len(names))
		for _, n := range names {
			o = append(o, op(n, "POST", "/", 200, stringsHas(n, "Get", "Describe", "List", "Head")))
		}
		return o
	}

	return &model.Bundle{
		SchemaVersion: "1",
		Provider:      model.ProviderAWS,
		Services: []model.Service{
			svc("aws.s3", "s3", model.ProtoRESTXML, "", "", "http://s3.amazonaws.com/doc/2006-03-01/", s3ops),
			svc("aws.dynamodb", "dynamodb", model.ProtoAWSJSON10, "DynamoDB_20120810", "", "", mk(ddb)),
			svc("aws.sqs", "sqs", model.ProtoAWSJSON10, "AmazonSQS", "2012-11-05", "", mk(sqs)),
			svc("aws.sns", "sns", model.ProtoAWSQuery, "", "2010-03-31", "http://sns.amazonaws.com/doc/2010-03-31/", mk(sns)),
			svc("aws.sts", "sts", model.ProtoAWSQuery, "", "2011-06-15", "https://sts.amazonaws.com/doc/2011-06-15/", mk(sts)),
			svc("aws.iam", "iam", model.ProtoAWSQuery, "", "2010-05-08", "https://iam.amazonaws.com/doc/2010-05-08/", mk(iam)),
			svc("aws.ssm", "ssm", model.ProtoAWSJSON11, "AmazonSSM", "", "", mk(ssm)),
			svc("aws.secretsmanager", "secretsmanager", model.ProtoAWSJSON11, "secretsmanager", "", "", mk(sm)),
			svc("gcp.storage", "storage", model.ProtoGCPRESTSON, "", "", "", mk(gcs)),
			svc("aws.kms", "kms", model.ProtoAWSJSON11, "TrentService", "", "", mk(kms)),
			svc("aws.logs", "logs", model.ProtoAWSJSON11, "Logs_20140328", "", "", mk(cwlogs)),
			svc("aws.events", "events", model.ProtoAWSJSON11, "AWSEvents", "", "", mk(ev)),
			svc("aws.lambda", "lambda", model.ProtoRESTJSON1, "", "", "", mk(lam)),
			svc("aws.cloudformation", "cloudformation", model.ProtoAWSQuery, "", "2010-05-15", "http://cloudformation.amazonaws.com/doc/2010-05-15/", mk(cfn)),
			svc("aws.kinesis", "kinesis", model.ProtoAWSJSON11, "Kinesis_20131202", "", "", mk(kin)),
			svc("aws.apigateway", "apigateway", model.ProtoRESTJSON1, "", "", "", mk(apigw)),
			svc("aws.monitoring", "monitoring", model.ProtoAWSJSON10, "GraniteServiceVersion20100801", "2010-08-01", "", mk(cw)),
			svc("aws.route53", "route53", model.ProtoRESTXML, "", "2013-04-01", "https://route53.amazonaws.com/doc/2013-04-01/", mk(r53)),
			svc("aws.acm", "acm", model.ProtoAWSJSON11, "CertificateManager", "", "", mk(acm)),
			svc("aws.rds", "rds", model.ProtoAWSQuery, "", "2014-10-31", "http://rds.amazonaws.com/doc/2014-10-31/", mk(rds)),
			svc("aws.ecs", "ecs", model.ProtoAWSJSON11, "AmazonEC2ContainerServiceV20141113", "", "", mk(ecs)),
			svc("aws.elasticloadbalancing", "elasticloadbalancing", model.ProtoAWSQuery, "", "2015-12-01", "https://elasticloadbalancing.amazonaws.com/doc/2015-12-01/", mk(elb)),
			svc("aws.elasticache", "elasticache", model.ProtoAWSQuery, "", "2015-02-02", "http://elasticache.amazonaws.com/doc/2015-02-02/", mk(ecache)),
			svc("aws.autoscaling", "autoscaling", model.ProtoAWSQuery, "", "2011-01-01", "http://autoscaling.amazonaws.com/doc/2011-01-01/", mk(asg)),
			svc("aws.api.ecr", "api.ecr", model.ProtoAWSJSON11, "AmazonEC2ContainerRegistry_V20150921", "", "", mk(ecr)),
			svc("aws.tagging", "tagging", model.ProtoAWSJSON11, "ResourceGroupsTaggingAPI_20170126", "", "", mk(tag)),
			svc("aws.application-autoscaling", "application-autoscaling", model.ProtoAWSJSON11, "AnyScaleFrontendService", "", "", mk(aas)),
			svc("aws.redshift", "redshift", model.ProtoAWSQuery, "", "2012-12-01", "http://redshift.amazonaws.com/doc/2012-12-01/", mk(rs)),
			svc("aws.eks", "eks", model.ProtoRESTJSON1, "", "", "", mk(eks)),
			svc("aws.ec2", "ec2", model.ProtoEC2Query, "", "2016-11-15", "http://ec2.amazonaws.com/doc/2016-11-15/", mk(ec2)),
			svc("aws.ses", "ses", model.ProtoAWSQuery, "", "2010-12-01", "http://ses.amazonaws.com/doc/2010-12-01/", mk(ses)),
			svc("aws.cognito-idp", "cognito-idp", model.ProtoAWSJSON11, "AWSCognitoIdentityProviderService", "", "", mk(cognito)),
			svc("aws.states", "states", model.ProtoAWSJSON10, "AWSStepFunctions", "", "", mk(states)),
			svc("aws.firehose", "firehose", model.ProtoAWSJSON11, "Firehose_20150804", "", "", mk(firehose)),
			svc("aws.cloudfront", "cloudfront", model.ProtoRESTXML, "", "2020-05-31", "http://cloudfront.amazonaws.com/doc/2020-05-31/", mk(cloudfront)),
			svc("aws.scheduler", "scheduler", model.ProtoAWSJSON10, "Scheduler", "", "", mk(scheduler)),
			svc("aws.es", "es", model.ProtoRESTJSON1, "AmazonOpenSearchService", "", "", mk(es)),
			svc("aws.glue", "glue", model.ProtoAWSJSON11, "AWSGlue", "", "", mk(glue)),
			svc("aws.athena", "athena", model.ProtoAWSJSON11, "AmazonAthena", "", "", mk(athena)),
			svc("aws.cloudtrail", "cloudtrail", model.ProtoAWSJSON11, "CloudTrail_20131101", "", "", mk(cloudtrail)),
			svc("aws.organizations", "organizations", model.ProtoAWSJSON11, "AWSOrganizationsV20161128", "", "", mk(organizations)),
			svc("aws.transfer", "transfer", model.ProtoAWSJSON11, "TransferService", "", "", mk(transfer)),
			svc("aws.wafv2", "wafv2", model.ProtoAWSJSON11, "AWSWAF_20190729", "", "", mk(wafv2)),
			svc("aws.appconfig", "appconfig", model.ProtoAWSJSON11, "AmazonAppConfig", "", "", mk(appconfig)),
			svc("aws.codebuild", "codebuild", model.ProtoAWSJSON11, "CodeBuild_20161006", "", "", mk(codebuild)),
			svc("aws.batch", "batch", model.ProtoAWSJSON11, "AWSBatch", "", "", mk(batch)),
			svc("aws.elasticmapreduce", "elasticmapreduce", model.ProtoAWSJSON11, "ElasticMapReduce", "", "", mk(emr)),
			svc("aws.kafka", "kafka", model.ProtoAWSJSON11, "Kafka", "", "", mk(kafka)),
			svc("aws.backup", "backup", model.ProtoAWSJSON11, "AWSBackup", "", "", mk(backup)),
			svc("aws.cognito-identity", "cognito-identity", model.ProtoAWSJSON11, "AWSCognitoIdentityService", "", "", mk(cognitoIdent)),
			svc("aws.sesv2", "sesv2", model.ProtoAWSJSON11, "AmazonSESv2", "", "", mk(sesv2)),
			svc("aws.config", "config", model.ProtoAWSJSON11, "StarlingDoveService", "", "", mk(cfgops)),
			svc("aws.xray", "xray", model.ProtoAWSJSON11, "AWSXRay", "", "", mk(xray)),
			svc("aws.guardduty", "guardduty", model.ProtoAWSJSON11, "AWSGuardDuty", "", "", mk(guardduty)),
			svc("aws.mq", "mq", model.ProtoAWSJSON11, "AmazonMQ", "", "", mk(mqops)),
			svc("aws.docdb", "docdb", model.ProtoAWSQuery, "", "2014-10-31", "http://rds.amazonaws.com/doc/2014-10-31/", mk(docdb)),
			svc("aws.neptune", "neptune", model.ProtoAWSQuery, "", "2014-10-31", "http://rds.amazonaws.com/doc/2014-10-31/", mk(neptune)),
			svc("aws.iot", "iot", model.ProtoAWSJSON11, "AWSIot", "", "", mk(iotops)),
			svc("aws.pipes", "pipes", model.ProtoAWSJSON11, "AWSPipes", "", "", mk(pipes)),
			svc("aws.codepipeline", "codepipeline", model.ProtoAWSJSON11, "CodePipeline_20150709", "", "", mk(codepipeline)),
			svc("aws.appsync", "appsync", model.ProtoAWSJSON11, "AWSAppSync", "", "", mk(appsync)),
			svc("aws.apigatewayv2", "apigatewayv2", model.ProtoAWSJSON11, "ApiGatewayV2", "", "", mk(apigwv2)),
			svc("aws.codecommit", "codecommit", model.ProtoAWSJSON11, "CodeCommit_20150413", "", "", mk(codecommit)),
			svc("aws.codedeploy", "codedeploy", model.ProtoAWSJSON11, "CodeDeploy_20141006", "", "", mk(codedeploy)),
			svc("aws.amplify", "amplify", model.ProtoAWSJSON11, "AWSAmplify", "", "", mk(amplify)),
			svc("aws.inspector", "inspector", model.ProtoAWSJSON11, "InspectorService", "", "", mk(inspector)),
			svc("aws.securityhub", "securityhub", model.ProtoAWSJSON11, "SecurityHub", "", "", mk(securityhub)),
			svc("aws.timestream", "timestream", model.ProtoAWSJSON11, "Timestream_20181101", "", "", mk(timestream)),
			svc("aws.qldb", "qldb", model.ProtoAWSJSON11, "AmazonQLDB", "", "", mk(qldb)),
			svc("aws.dms", "dms", model.ProtoAWSJSON11, "AmazonDMS20160101", "", "", mk(dms)),
			svc("aws.mediaconvert", "mediaconvert", model.ProtoAWSJSON11, "MediaConvert", "", "", mk(mediaconvert)),
			svc("aws.elasticsearch", "elasticsearch", model.ProtoAWSJSON11, "EsHttpService", "", "", mk(elasticsearch)),
			svc("aws.elasticbeanstalk", "elasticbeanstalk", model.ProtoAWSQuery, "", "2010-12-01", "https://elasticbeanstalk.amazonaws.com/docs/2010-12-01/", mk(elasticbeanstalk)),
			svc("aws.swf", "swf", model.ProtoAWSJSON11, "SimpleWorkflowService", "", "", mk(swfops)),
			svc("aws.elasticfilesystem", "elasticfilesystem", model.ProtoAWSJSON11, "elasticfilesystem", "", "", mk(efsops)),
			svc("aws.glacier", "glacier", model.ProtoAWSJSON11, "Glacier", "", "", mk(glacier)),
			svc("aws.servicediscovery", "servicediscovery", model.ProtoAWSJSON11, "Route53AutoNaming_v20170314", "", "", mk(servicediscovery)),
			svc("aws.ram", "ram", model.ProtoAWSJSON11, "AWSResourceAccessManager", "", "", mk(ramops)),
			svc("aws.sagemaker", "sagemaker", model.ProtoAWSJSON11, "SageMaker", "", "", mk(sagemaker)),
			svc("aws.workspaces", "workspaces", model.ProtoAWSJSON11, "WorkspacesService", "", "", mk(workspaces)),
			svc("aws.transcribe", "transcribe", model.ProtoAWSJSON11, "Transcribe", "", "", mk(transcribe)),
			svc("aws.rekognition", "rekognition", model.ProtoAWSJSON11, "RekognitionService", "", "", mk(rekognition)),
			svc("aws.comprehend", "comprehend", model.ProtoAWSJSON11, "Comprehend_20171127", "", "", mk(comprehend)),
			svc("aws.mediastore", "mediastore", model.ProtoAWSJSON11, "MediaStore_20170901", "", "", mk(mediastore)),
			svc("aws.kinesisanalytics", "kinesisanalytics", model.ProtoAWSJSON11, "KinesisAnalytics_20150814", "", "", mk(kinesisanalytics)),
			svc("aws.translate", "translate", model.ProtoAWSJSON11, "AWSShineFrontendService_20170701", "", "", mk(translate)),
			svc("aws.textract", "textract", model.ProtoAWSJSON11, "Textract", "", "", mk(textract)),
			svc("aws.polly", "polly", model.ProtoAWSJSON11, "Polly", "", "", mk(polly)),
			svc("aws.fsx", "fsx", model.ProtoAWSJSON11, "AWSSimbaAPIService_v20180301", "", "", mk(fsxops)),
			svc("aws.s3control", "s3-control", model.ProtoAWSJSON11, "AWSS3ControlService", "", "", mk(s3control)),
			svc("aws.route53resolver", "route53resolver", model.ProtoAWSJSON11, "Route53Resolver", "", "", mk(route53resolver)),
			svc("aws.servicecatalog", "servicecatalog", model.ProtoAWSJSON11, "AWS242ServiceCatalogService", "", "", mk(servicecatalog)),
			svc("aws.shield", "shield", model.ProtoAWSJSON11, "AWSShield_20160616", "", "", mk(shield)),
			svc("aws.waf", "waf", model.ProtoAWSJSON11, "AWSWAF_20150824", "", "", mk(wafclassic)),
			svc("aws.storagegateway", "storagegateway", model.ProtoAWSJSON11, "StorageGateway_20130630", "", "", mk(storagegateway)),
			svc("aws.lakeformation", "lakeformation", model.ProtoAWSJSON11, "AWSLakeFormation", "", "", mk(lakeformation)),
			svc("aws.connect", "connect", model.ProtoAWSJSON11, "AmazonConnect", "", "", mk(connect)),
			svc("aws.pinpoint", "pinpoint", model.ProtoAWSJSON11, "AmazonPinpoint", "", "", mk(pinpoint)),
			svc("aws.dax", "dax", model.ProtoAWSJSON11, "AmazonDAXV3", "", "", mk(daxops)),
			svc("aws.memorydb", "memorydb", model.ProtoAWSJSON11, "AmazonMemoryDB", "", "", mk(memorydb)),
			svc("aws.keyspaces", "keyspaces", model.ProtoAWSJSON11, "Cassandra", "", "", mk(keyspaces)),
			svc("aws.mwaa", "airflow", model.ProtoAWSJSON11, "AmazonMWAA", "", "", mk(mwaa)),
			svc("aws.sso-admin", "sso", model.ProtoAWSJSON11, "SWBExternalService", "", "", mk(ssoadmin)),
			svc("aws.acm-pca", "acm-pca", model.ProtoAWSJSON11, "ACMPrivateCA", "", "", mk(acmpca)),
			svc("aws.lightsail", "lightsail", model.ProtoAWSJSON11, "Lightsail_20161128", "", "", mk(lightsail)),
			svc("aws.location", "geo", model.ProtoAWSJSON11, "LocationService", "", "", mk(location)),
			svc("aws.kendra", "kendra", model.ProtoAWSJSON11, "AWSKendraFrontendService", "", "", mk(kendra)),
			svc("aws.quicksight", "quicksight", model.ProtoAWSJSON11, "AmazonQuickSight", "", "", mk(quicksight)),
			svc("aws.identitystore", "identitystore", model.ProtoAWSJSON11, "AWSIdentityStore", "", "", mk(identitystore)),
			svc("aws.workmail", "workmail", model.ProtoAWSJSON11, "WorkMailService", "", "", mk(workmail)),
			svc("aws.directconnect", "directconnect", model.ProtoAWSJSON11, "OvertureService", "", "", mk(directconnect)),
			svc("aws.ds", "ds", model.ProtoAWSJSON11, "DirectoryService_20150416", "", "", mk(dsops)),
			svc("aws.gamelift", "gamelift", model.ProtoAWSJSON11, "GameLift", "", "", mk(gamelift)),
			svc("aws.forecast", "forecast", model.ProtoAWSJSON11, "AmazonForecast", "", "", mk(forecast)),
			svc("aws.personalize", "personalize", model.ProtoAWSJSON11, "AmazonPersonalize", "", "", mk(personalize)),
			svc("aws.lex-models", "lex", model.ProtoAWSJSON11, "AWSMeadService", "", "", mk(lexmodels)),
			svc("aws.medialive", "medialive", model.ProtoAWSJSON11, "MediaLive", "", "", mk(medialive)),
			svc("aws.mediapackage", "mediapackage", model.ProtoAWSJSON11, "MediaPackage", "", "", mk(mediapackage)),
			svc("aws.mediaconnect", "mediaconnect", model.ProtoAWSJSON11, "MediaConnect", "", "", mk(mediaconnect)),
			svc("aws.elastictranscoder", "elastictranscoder", model.ProtoAWSJSON11, "ElasticTranscoder", "", "", mk(elastictranscoder)),
			svc("aws.cloudhsmv2", "cloudhsmv2", model.ProtoAWSJSON11, "BaldrApiService", "", "", mk(cloudhsmv2)),
			svc("aws.macie2", "macie2", model.ProtoAWSJSON11, "Macie2", "", "", mk(macie2)),
			svc("aws.access-analyzer", "access-analyzer", model.ProtoAWSJSON11, "AccessAnalyzer", "", "", mk(accessanalyzer)),
			svc("aws.comprehendmedical", "comprehendmedical", model.ProtoAWSJSON11, "ComprehendMedical_20181030", "", "", mk(comprehendmedical)),
			svc("aws.frauddetector", "frauddetector", model.ProtoAWSJSON11, "AWSHawksNestServiceFacade", "", "", mk(frauddetector)),
			svc("aws.appmesh", "appmesh", model.ProtoAWSJSON11, "AppMesh", "", "", mk(appmesh)),
			svc("aws.healthlake", "healthlake", model.ProtoAWSJSON11, "HealthLake", "", "", mk(healthlake)),
			svc("aws.lookoutmetrics", "lookoutmetrics", model.ProtoAWSJSON11, "LookoutMetrics", "", "", mk(lookoutmetrics)),
			svc("aws.bedrock", "bedrock", model.ProtoAWSJSON11, "AmazonBedrockControlPlaneService", "", "", mk(bedrock)),
			svc("aws.fis", "fis", model.ProtoAWSJSON11, "AWSFIS", "", "", mk(fisops)),
			svc("aws.ce", "ce", model.ProtoAWSJSON11, "AWSInsightsIndexService", "", "", mk(ceops)),
			svc("aws.resource-groups", "resource-groups", model.ProtoAWSJSON11, "ResourceGroups", "", "", mk(resourcegroups)),
			svc("aws.verifiedpermissions", "verifiedpermissions", model.ProtoAWSJSON11, "VerifiedPermissions", "", "", mk(verifiedpermissions)),
			svc("aws.support", "support", model.ProtoAWSJSON11, "AWSSupport_20130415", "", "", mk(supportops)),
			svc("aws.codeartifact", "codeartifact", model.ProtoAWSJSON11, "CodeArtifactControlPlaneService", "", "", mk(codeartifact)),
			svc("aws.cloudcontrol", "cloudcontrolapi", model.ProtoAWSJSON11, "CloudApiService", "", "", mk(cloudcontrol)),
			svc("aws.serverlessrepo", "serverlessrepo", model.ProtoAWSJSON11, "ServerlessApplicationRepository", "", "", mk(serverlessrepo)),
			svc("aws.account", "account", model.ProtoAWSJSON11, "Account", "", "", mk(account)),
			svc("aws.iotwireless", "api.iotwireless", model.ProtoAWSJSON11, "IoTWireless", "", "", mk(iotwireless)),
			svc("aws.s3tables", "s3tables", model.ProtoAWSJSON11, "S3Tables", "", "", mk(s3tables)),
			svc("aws.synthetics", "synthetics", model.ProtoAWSJSON11, "Synthetics", "", "", mk(synthetics)),
			svc("aws.apprunner", "apprunner", model.ProtoAWSJSON11, "AppRunner", "", "", mk(apprunner)),
			svc("aws.proton", "proton", model.ProtoAWSJSON11, "AwsProton20200720", "", "", mk(proton)),
			svc("aws.resiliencehub", "resiliencehub", model.ProtoAWSJSON11, "AWSResilienceHub", "", "", mk(resiliencehub)),
			svc("aws.resource-explorer-2", "resource-explorer-2", model.ProtoAWSJSON11, "ResourceExplorerService", "", "", mk(resourceexplorer2)),
			svc("aws.rum", "rum", model.ProtoAWSJSON11, "RUMFrontendService", "", "", mk(rumops)),
			svc("aws.schemas", "schemas", model.ProtoAWSJSON11, "schemas", "", "", mk(schemasops)),
			svc("aws.dsql", "dsql", model.ProtoAWSJSON11, "DSQL", "", "", mk(dsqlops)),
			svc("aws.codeconnections", "codeconnections", model.ProtoAWSJSON11, "CodeConnections_20231201", "", "", mk(codeconnections)),
			svc("aws.iot-data", "data.iot", model.ProtoAWSJSON11, "IotDataPlane", "", "", mk(iotdata)),
			svc("aws.managedblockchain", "managedblockchain", model.ProtoAWSJSON11, "AmazonManagedBlockchain", "", "", mk(managedblockchain)),
			svc("aws.kinesisanalyticsv2", "kinesisanalyticsv2", model.ProtoAWSJSON11, "KinesisAnalytics_20180523", "", "", mk(kinesisanalyticsv2)),
		},
	}
}

func stringsHas(n string, prefixes ...string) bool {
	for _, p := range prefixes {
		if len(n) >= len(p) && n[:len(p)] == p {
			return true
		}
	}
	return false
}
