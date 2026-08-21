package iam

import (
	"context"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// extraOps remaining IAM ops served as control-plane KV.
// leftoverOps are remaining Smithy operations served as control-plane KV.
// ponytail: no MFA hardware, credential report jobs, or org-root federation; upgrade is per-op AWS shapes.
func ExtraOps() []string { return extraOps() }

func extraOps() []string {
	return []string{
		"AcceptDelegationRequest",
		"AcquireRole",
		"AddClientIDToOpenIDConnectProvider",
		"AssociateDelegationRequest",
		"ChangePassword",
		"CreateDelegationRequest",
		"CreateServiceLinkedRole",
		"CreateServiceSpecificCredential",
		"CreateVirtualMFADevice",
		"DeactivateMFADevice",
		"DeleteRolePermissionsBoundary",
		"DeleteSSHPublicKey",
		"DeleteServerCertificate",
		"DeleteServiceLinkedRole",
		"DeleteServiceSpecificCredential",
		"DeleteSigningCertificate",
		"DeleteUserPermissionsBoundary",
		"DeleteVirtualMFADevice",
		"DisableOrganizationsRootCredentialsManagement",
		"DisableOrganizationsRootSessions",
		"DisableOutboundWebIdentityFederation",
		"EnableMFADevice",
		"EnableOrganizationsRootCredentialsManagement",
		"EnableOrganizationsRootSessions",
		"EnableOutboundWebIdentityFederation",
		"GenerateCredentialReport",
		"GenerateOrganizationsAccessReport",
		"GenerateServiceLastAccessedDetails",
		"GetAccessKeyLastUsed",
		"GetAccountAuthorizationDetails",
		"GetAccountProperties",
		"GetContextKeysForCustomPolicy",
		"GetContextKeysForPrincipalPolicy",
		"GetCredentialReport",
		"GetDelegationRequest",
		"GetHumanReadableSummary",
		"GetMFADevice",
		"GetOrganizationsAccessReport",
		"GetOutboundWebIdentityFederationInfo",
		"GetRoleTemplateVersion",
		"GetSSHPublicKey",
		"GetServerCertificate",
		"GetServiceLastAccessedDetails",
		"GetServiceLastAccessedDetailsWithEntities",
		"GetServiceLinkedRoleDeletionStatus",
		"ListDelegationRequests",
		"ListEntitiesForPolicy",
		"ListInstanceProfileTags",
		"ListMFADeviceTags",
		"ListMFADevices",
		"ListOpenIDConnectProviderTags",
		"ListOrganizationsFeatures",
		"ListPoliciesGrantingServiceAccess",
		"ListPolicyTags",
		"ListSAMLProviderTags",
		"ListSSHPublicKeys",
		"ListServerCertificateTags",
		"ListServerCertificates",
		"ListServiceSpecificCredentials",
		"ListSigningCertificates",
		"ListVirtualMFADevices",
		"PutAccountProperties",
		"PutRolePermissionsBoundary",
		"PutUserPermissionsBoundary",
		"RejectDelegationRequest",
		"RemoveClientIDFromOpenIDConnectProvider",
		"ResetServiceSpecificCredential",
		"ResyncMFADevice",
		"SendDelegationToken",
		"SetSecurityTokenServicePreferences",
		"TagInstanceProfile",
		"TagMFADevice",
		"TagOpenIDConnectProvider",
		"TagPolicy",
		"TagSAMLProvider",
		"TagServerCertificate",
		"UntagInstanceProfile",
		"UntagMFADevice",
		"UntagOpenIDConnectProvider",
		"UntagPolicy",
		"UntagSAMLProvider",
		"UntagServerCertificate",
		"UpdateDelegationRequest",
		"UpdateRoleDescription",
		"UpdateSSHPublicKey",
		"UpdateServerCertificate",
		"UpdateServiceSpecificCredential",
		"UpdateSigningCertificate",
		"UploadSSHPublicKey",
		"UploadServerCertificate",
		"UploadSigningCertificate",
	}
}

func (p *Pack) extra(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	op := req.Operation
	kind, idKey, listKey, wrap := extraShape(op)
	id := first(req.Input, idKey, "UserName", "RoleName", "SerialNumber", "ServerCertificateName",
		"OpenIDConnectProviderArn", "SAMLProviderArn", "PolicyArn", "SSHPublicKeyId", "CertificateId")
	switch {
	case isExtraWrite(op):
		if id == "" {
			id = p.deps.Rand.Hex(8)
		}
		rec := map[string]any{}
		for k, v := range req.Input {
			rec[k] = v
		}
		if idKey != "" {
			if _, ok := rec[idKey]; !ok {
				rec[idKey] = id
			}
		}
		if op == "GenerateCredentialReport" || op == "GenerateOrganizationsAccessReport" || op == "GenerateServiceLastAccessedDetails" {
			rec["State"] = "COMPLETE"
			rec["JobId"] = id
		}
		p.put(ctx, req, kind+":"+id, rec)
		out := map[string]any{}
		if wrap != "" {
			out[wrap] = rec
		} else {
			out = rec
		}
		if idKey != "" {
			out[idKey] = id
		}
		return &spi.Response{Output: out}, nil
	case strings.HasPrefix(op, "Delete") || strings.HasPrefix(op, "Untag") || strings.HasPrefix(op, "Remove") ||
		strings.HasPrefix(op, "Deactivate") || strings.HasPrefix(op, "Disable") || strings.HasPrefix(op, "Reject"):
		if id != "" {
			_ = p.col(req).Delete(ctx, kind+":"+id)
		}
		return &spi.Response{Output: map[string]any{}}, nil
	case strings.HasPrefix(op, "List"):
		if id != "" {
			if rec, ok := p.get(ctx, req, kind+":"+id); ok {
				return &spi.Response{Output: map[string]any{listKey: []any{rec}}}, nil
			}
		}
		return p.listKind(ctx, req, kind+":", listKey)
	default:
		if rec, ok := p.get(ctx, req, kind+":"+id); ok {
			if wrap != "" {
				return &spi.Response{Output: map[string]any{wrap: rec}}, nil
			}
			return &spi.Response{Output: rec}, nil
		}
		out := map[string]any{}
		if wrap != "" {
			out[wrap] = map[string]any{}
		}
		if op == "GetCredentialReport" {
			out["Content"] = ""
			out["State"] = "COMPLETE"
		}
		if op == "GetAccessKeyLastUsed" {
			out["UserName"] = first(req.Input, "UserName")
			out["AccessKeyLastUsed"] = map[string]any{"ServiceName": "N/A"}
		}
		return &spi.Response{Output: out}, nil
	}
}

func isExtraWrite(op string) bool {
	for _, p := range []string{"Create", "Put", "Update", "Tag", "Upload", "Add", "Change", "Enable", "Generate",
		"Set", "Send", "Accept", "Acquire", "Associate", "Resync", "Reset"} {
		if strings.HasPrefix(op, p) && !strings.HasPrefix(op, "Untag") {
			return true
		}
	}
	return false
}

func extraShape(op string) (kind, idKey, listKey, wrap string) {
	switch {
	case strings.Contains(op, "Delegation"):
		return "ldele", "DelegationRequestId", "DelegationRequests", "DelegationRequest"
	case strings.Contains(op, "MFA"):
		return "lmfa", "SerialNumber", "MFADevices", "MFADevice"
	case strings.Contains(op, "SSH"):
		return "lssh", "SSHPublicKeyId", "SSHPublicKeys", "SSHPublicKey"
	case strings.Contains(op, "ServerCertificate"):
		return "lscert", "ServerCertificateName", "ServerCertificateMetadataList", "ServerCertificate"
	case strings.Contains(op, "SigningCertificate"):
		return "lsign", "CertificateId", "Certificates", "Certificate"
	case strings.Contains(op, "ServiceSpecific"):
		return "lssc", "ServiceSpecificCredentialId", "ServiceSpecificCredentials", "ServiceSpecificCredential"
	case strings.Contains(op, "ServiceLinked"):
		return "lslr", "RoleName", "Roles", "Role"
	case strings.Contains(op, "OpenIDConnect"):
		return "loidc", "OpenIDConnectProviderArn", "OpenIDConnectProviderList", ""
	case strings.Contains(op, "SAML"):
		return "lsaml", "SAMLProviderArn", "SAMLProviderList", ""
	case strings.Contains(op, "InstanceProfile"):
		return "lip", "InstanceProfileName", "InstanceProfiles", "InstanceProfile"
	case strings.Contains(op, "Organizations"):
		return "lorg", "JobId", "Jobs", ""
	case strings.Contains(op, "CredentialReport") || strings.Contains(op, "LastAccessed"):
		return "lrep", "JobId", "Jobs", ""
	case strings.Contains(op, "PermissionsBoundary"):
		return "lbound", "UserName", "Users", ""
	default:
		return "lmisc", "UserName", "Users", ""
	}
}
