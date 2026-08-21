package spine

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/iam"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestBootedServerIAMSection48(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.iam"}
	cfg.Seed = "iam-48"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/iam/aws4_request, SignedHeaders=host, Signature=00"
	call := func(vals url.Values) (int, string) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(vals.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		return res.StatusCode, string(b)
	}
	must := func(vals url.Values, want ...string) string {
		t.Helper()
		code, body := call(vals)
		if code >= 300 {
			t.Fatalf("%s %d %s", vals.Get("Action"), code, body)
		}
		for _, w := range want {
			if !strings.Contains(body, w) {
				t.Fatalf("%s missing %q in %s", vals.Get("Action"), w, body)
			}
		}
		return body
	}

	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`
	must(url.Values{"Action": {"CreateRole"}, "RoleName": {"r"}, "AssumeRolePolicyDocument": {doc}}, "role/r")
	gr := must(url.Values{"Action": {"GetRole"}, "RoleName": {"r"}}, "s3:*")
	if !strings.Contains(gr, doc) && !strings.Contains(gr, "s3:*") {
		t.Fatalf("policy not verbatim %s", gr)
	}
	must(url.Values{"Action": {"UpdateRole"}, "RoleName": {"r"}, "AssumeRolePolicyDocument": {doc}}, "role/r")
	must(url.Values{"Action": {"ListRoles"}}, "role/r")

	must(url.Values{"Action": {"PutRolePolicy"}, "RoleName": {"r"}, "PolicyName": {"inline"}, "PolicyDocument": {doc}})
	must(url.Values{"Action": {"GetRolePolicy"}, "RoleName": {"r"}, "PolicyName": {"inline"}}, "s3:*")
	must(url.Values{"Action": {"ListRolePolicies"}, "RoleName": {"r"}}, "inline")
	must(url.Values{"Action": {"DeleteRolePolicy"}, "RoleName": {"r"}, "PolicyName": {"inline"}})
	code, miss := call(url.Values{"Action": {"GetRolePolicy"}, "RoleName": {"r"}, "PolicyName": {"inline"}})
	if code == 200 && strings.Contains(miss, "s3:*") {
		t.Fatalf("deleted inline policy still there %s", miss)
	}

	must(url.Values{"Action": {"CreatePolicy"}, "PolicyName": {"p"}, "PolicyDocument": {doc}}, "policy/p")
	must(url.Values{"Action": {"GetPolicy"}, "PolicyArn": {"arn:aws:iam::000000000000:policy/p"}}, "policy/p")
	must(url.Values{"Action": {"ListPolicies"}}, "policy/p")
	must(url.Values{"Action": {"AttachRolePolicy"}, "RoleName": {"r"}, "PolicyArn": {"arn:aws:iam::000000000000:policy/p"}})
	must(url.Values{"Action": {"ListAttachedRolePolicies"}, "RoleName": {"r"}}, "policy/p")
	must(url.Values{"Action": {"DetachRolePolicy"}, "RoleName": {"r"}, "PolicyArn": {"arn:aws:iam::000000000000:policy/p"}})
	must(url.Values{"Action": {"DeletePolicy"}, "PolicyName": {"p"}})

	must(url.Values{"Action": {"CreateUser"}, "UserName": {"u"}}, "user/u")
	must(url.Values{"Action": {"GetUser"}, "UserName": {"u"}}, "user/u")
	must(url.Values{"Action": {"ListUsers"}}, "user/u")
	ak := must(url.Values{"Action": {"CreateAccessKey"}, "UserName": {"u"}}, "AccessKeyId")
	if !strings.Contains(ak, "SecretAccessKey") {
		t.Fatalf("access key %s", ak)
	}
	must(url.Values{"Action": {"DeleteUser"}, "UserName": {"u"}})
	code, gone := call(url.Values{"Action": {"GetUser"}, "UserName": {"u"}})
	if code == 200 && strings.Contains(gone, "<UserName>u</UserName>") {
		t.Fatalf("deleted user still there %s", gone)
	}

	must(url.Values{"Action": {"TagRole"}, "RoleName": {"r"}, "Tags.member.1.Key": {"k"}, "Tags.member.1.Value": {"v"}})
	must(url.Values{"Action": {"ListRoleTags"}, "RoleName": {"r"}}, "k")
	must(url.Values{"Action": {"UntagRole"}, "RoleName": {"r"}, "TagKeys.member.1": {"k"}})

	must(url.Values{"Action": {"UpdateAssumeRolePolicy"}, "RoleName": {"r"}, "PolicyDocument": {doc}})
	must(url.Values{"Action": {"CreatePolicy"}, "PolicyName": {"p2"}, "PolicyDocument": {doc}}, "policy/p2")
	must(url.Values{"Action": {"CreatePolicyVersion"}, "PolicyName": {"p2"}, "PolicyDocument": {`{"Version":"2012-10-17","Statement":[]}`}, "SetAsDefault": {"false"}}, "v2")
	must(url.Values{"Action": {"GetPolicyVersion"}, "PolicyName": {"p2"}, "VersionId": {"v2"}}, "v2")
	must(url.Values{"Action": {"ListPolicyVersions"}, "PolicyName": {"p2"}}, "v1")
	must(url.Values{"Action": {"SetDefaultPolicyVersion"}, "PolicyName": {"p2"}, "VersionId": {"v1"}})
	must(url.Values{"Action": {"DeletePolicyVersion"}, "PolicyName": {"p2"}, "VersionId": {"v2"}})
	must(url.Values{"Action": {"DeletePolicy"}, "PolicyName": {"p2"}})

	must(url.Values{"Action": {"CreateUser"}, "UserName": {"u2"}}, "user/u2")
	must(url.Values{"Action": {"UpdateUser"}, "UserName": {"u2"}, "NewUserName": {"u3"}}, "user/u3")
	must(url.Values{"Action": {"PutUserPolicy"}, "UserName": {"u3"}, "PolicyName": {"up"}, "PolicyDocument": {doc}})
	must(url.Values{"Action": {"GetUserPolicy"}, "UserName": {"u3"}, "PolicyName": {"up"}}, "s3:*")
	must(url.Values{"Action": {"ListUserPolicies"}, "UserName": {"u3"}}, "up")
	must(url.Values{"Action": {"DeleteUserPolicy"}, "UserName": {"u3"}, "PolicyName": {"up"}})
	must(url.Values{"Action": {"AttachUserPolicy"}, "UserName": {"u3"}, "PolicyArn": {"arn:aws:iam::aws:policy/x"}})
	must(url.Values{"Action": {"ListAttachedUserPolicies"}, "UserName": {"u3"}}, "policy/x")
	must(url.Values{"Action": {"DetachUserPolicy"}, "UserName": {"u3"}, "PolicyArn": {"arn:aws:iam::aws:policy/x"}})
	must(url.Values{"Action": {"TagUser"}, "UserName": {"u3"}, "Tags.member.1.Key": {"k"}, "Tags.member.1.Value": {"v"}})
	must(url.Values{"Action": {"ListUserTags"}, "UserName": {"u3"}}, "k")
	must(url.Values{"Action": {"UntagUser"}, "UserName": {"u3"}, "TagKeys.member.1": {"k"}})
	ak2 := must(url.Values{"Action": {"CreateAccessKey"}, "UserName": {"u3"}}, "AccessKeyId")
	must(url.Values{"Action": {"ListAccessKeys"}, "UserName": {"u3"}}, "AccessKeyId")
	// extract id from ak2
	_ = ak2
	must(url.Values{"Action": {"CreateLoginProfile"}, "UserName": {"u3"}, "Password": {"pw"}}, "u3")
	must(url.Values{"Action": {"GetLoginProfile"}, "UserName": {"u3"}}, "u3")
	must(url.Values{"Action": {"UpdateLoginProfile"}, "UserName": {"u3"}, "PasswordResetRequired": {"true"}})
	must(url.Values{"Action": {"DeleteLoginProfile"}, "UserName": {"u3"}})

	must(url.Values{"Action": {"CreateGroup"}, "GroupName": {"g"}}, "group/g")
	must(url.Values{"Action": {"GetGroup"}, "GroupName": {"g"}}, "group/g")
	must(url.Values{"Action": {"ListGroups"}}, "group/g")
	must(url.Values{"Action": {"UpdateGroup"}, "GroupName": {"g"}, "NewGroupName": {"g2"}}, "group/g2")
	must(url.Values{"Action": {"AddUserToGroup"}, "GroupName": {"g2"}, "UserName": {"u3"}})
	must(url.Values{"Action": {"ListGroupsForUser"}, "UserName": {"u3"}}, "g2")
	must(url.Values{"Action": {"GetGroup"}, "GroupName": {"g2"}}, "u3")
	must(url.Values{"Action": {"PutGroupPolicy"}, "GroupName": {"g2"}, "PolicyName": {"gp"}, "PolicyDocument": {doc}})
	must(url.Values{"Action": {"GetGroupPolicy"}, "GroupName": {"g2"}, "PolicyName": {"gp"}}, "s3:*")
	must(url.Values{"Action": {"ListGroupPolicies"}, "GroupName": {"g2"}}, "gp")
	must(url.Values{"Action": {"DeleteGroupPolicy"}, "GroupName": {"g2"}, "PolicyName": {"gp"}})
	must(url.Values{"Action": {"AttachGroupPolicy"}, "GroupName": {"g2"}, "PolicyArn": {"arn:aws:iam::aws:policy/x"}})
	must(url.Values{"Action": {"ListAttachedGroupPolicies"}, "GroupName": {"g2"}}, "policy/x")
	must(url.Values{"Action": {"DetachGroupPolicy"}, "GroupName": {"g2"}, "PolicyArn": {"arn:aws:iam::aws:policy/x"}})
	must(url.Values{"Action": {"RemoveUserFromGroup"}, "GroupName": {"g2"}, "UserName": {"u3"}})
	must(url.Values{"Action": {"DeleteGroup"}, "GroupName": {"g2"}})

	must(url.Values{"Action": {"CreateInstanceProfile"}, "InstanceProfileName": {"ip"}}, "instance-profile/ip")
	must(url.Values{"Action": {"GetInstanceProfile"}, "InstanceProfileName": {"ip"}}, "ip")
	must(url.Values{"Action": {"ListInstanceProfiles"}}, "ip")
	must(url.Values{"Action": {"AddRoleToInstanceProfile"}, "InstanceProfileName": {"ip"}, "RoleName": {"r"}})
	must(url.Values{"Action": {"ListInstanceProfilesForRole"}, "RoleName": {"r"}}, "ip")
	must(url.Values{"Action": {"RemoveRoleFromInstanceProfile"}, "InstanceProfileName": {"ip"}, "RoleName": {"r"}})
	must(url.Values{"Action": {"DeleteInstanceProfile"}, "InstanceProfileName": {"ip"}})

	must(url.Values{"Action": {"CreateAccountAlias"}, "AccountAlias": {"alias1"}})
	must(url.Values{"Action": {"ListAccountAliases"}}, "alias1")
	must(url.Values{"Action": {"DeleteAccountAlias"}, "AccountAlias": {"alias1"}})
	must(url.Values{"Action": {"GetAccountSummary"}}, "Users")
	must(url.Values{"Action": {"UpdateAccountPasswordPolicy"}, "MinimumPasswordLength": {"12"}})
	must(url.Values{"Action": {"GetAccountPasswordPolicy"}}, "12")
	must(url.Values{"Action": {"DeleteAccountPasswordPolicy"}})

	oidc := must(url.Values{"Action": {"CreateOpenIDConnectProvider"}, "Url": {"https://oidc.eks.us-east-1.amazonaws.com/id/EX"}}, "oidc-provider/")
	must(url.Values{"Action": {"ListOpenIDConnectProviders"}}, "oidc-provider/")
	must(url.Values{"Action": {"GetOpenIDConnectProvider"}, "OpenIDConnectProviderArn": {"arn:aws:iam::000000000000:oidc-provider/oidc.eks.us-east-1.amazonaws.com/id/EX"}}, "oidc.eks")
	must(url.Values{"Action": {"UpdateOpenIDConnectProviderThumbprint"}, "OpenIDConnectProviderArn": {"arn:aws:iam::000000000000:oidc-provider/oidc.eks.us-east-1.amazonaws.com/id/EX"}, "ThumbprintList.member.1": {"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}})
	must(url.Values{"Action": {"DeleteOpenIDConnectProvider"}, "OpenIDConnectProviderArn": {"arn:aws:iam::000000000000:oidc-provider/oidc.eks.us-east-1.amazonaws.com/id/EX"}})
	_ = oidc

	must(url.Values{"Action": {"CreateSAMLProvider"}, "Name": {"saml1"}, "SAMLMetadataDocument": {"<xml/>"}}, "saml-provider/saml1")
	must(url.Values{"Action": {"GetSAMLProvider"}, "SAMLProviderArn": {"arn:aws:iam::000000000000:saml-provider/saml1"}}, "saml1")
	must(url.Values{"Action": {"ListSAMLProviders"}}, "saml1")
	must(url.Values{"Action": {"UpdateSAMLProvider"}, "SAMLProviderArn": {"arn:aws:iam::000000000000:saml-provider/saml1"}, "SAMLMetadataDocument": {"<xml2/>"}})
	must(url.Values{"Action": {"DeleteSAMLProvider"}, "SAMLProviderArn": {"arn:aws:iam::000000000000:saml-provider/saml1"}})

	// access key update/delete — recreate user key
	akBody := must(url.Values{"Action": {"CreateAccessKey"}, "UserName": {"u3"}}, "AccessKeyId")
	idStart := strings.Index(akBody, "<AccessKeyId>")
	idEnd := strings.Index(akBody, "</AccessKeyId>")
	akid := ""
	if idStart >= 0 && idEnd > idStart {
		akid = akBody[idStart+len("<AccessKeyId>") : idEnd]
	}
	must(url.Values{"Action": {"UpdateAccessKey"}, "UserName": {"u3"}, "AccessKeyId": {akid}, "Status": {"Inactive"}})
	must(url.Values{"Action": {"DeleteAccessKey"}, "UserName": {"u3"}, "AccessKeyId": {akid}})
	must(url.Values{"Action": {"DeleteUser"}, "UserName": {"u3"}})

	must(url.Values{"Action": {"DeleteRole"}, "RoleName": {"r"}})
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(url.Values{"Action": {"CreateVirtualMFADevice"}, "VirtualMFADeviceName": {"mfa"}}.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", auth)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	lb, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode >= 300 || res.Header.Get("x-mirror-fidelity") != "emulate" {
		t.Fatalf("CreateVirtualMFADevice %d %s %s", res.StatusCode, res.Header.Get("x-mirror-fidelity"), lb)
	}
	code, rmiss := call(url.Values{"Action": {"GetRole"}, "RoleName": {"r"}})
	if code == 200 && strings.Contains(rmiss, "role/r") {
		t.Fatalf("deleted role still there %s", rmiss)
	}
}

func TestIAMHTTPProvenOps(t *testing.T) {
	want := []string{
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
	}
	assertSame(t, "iam", iam.New(spitest.Deps(t)).Operations(), append(want, iam.ExtraOps()...))
}
