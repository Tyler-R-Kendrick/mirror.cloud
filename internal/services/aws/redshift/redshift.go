// Package redshift emulates cluster control-plane records and a Firehose COPY row store (not a SQL engine).
package redshift

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.redshift", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Redshift-lite.
type Pack struct{ deps spi.Deps }

// CopyInput is the narrow data-plane contract used by Firehose's Redshift COPY destination.
type CopyInput struct {
	Cluster, Database, Table, Username, Password, Columns, Options string
	Data                                                           [][]byte
}

type tableData struct {
	Columns []string
	Rows    []map[string]any
}

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.redshift" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	core := []string{
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
	}
	return append(core, extraOps()...)
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateCluster":
		id := first(req.Input, "ClusterIdentifier")
		rec := map[string]any{
			"ClusterIdentifier": id, "ClusterStatus": "available",
			"NodeType": first(req.Input, "NodeType"), "MasterUsername": first(req.Input, "MasterUsername"), "DBName": first(req.Input, "DBName"),
			"Endpoint": map[string]any{"Address": id + "." + req.Identity.Region + ".redshift.amazonaws.com", "Port": 5439},
		}
		if rec["DBName"] == "" {
			rec["DBName"] = "dev"
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "rscluster").Put(ctx, id, b)
		if password := first(req.Input, "MasterUserPassword"); password != "" {
			credential, _ := json.Marshal(map[string]any{"Username": rec["MasterUsername"], "PasswordHash": fmt.Sprintf("%x", sha256.Sum256([]byte(password)))})
			_ = p.col(req, "rscredential").Put(ctx, id, credential)
		}
		return &spi.Response{Output: map[string]any{"Cluster": rec}}, nil
	case "DescribeClusters":
		return listOrGet(ctx, p.col(req, "rscluster"), first(req.Input, "ClusterIdentifier"), "Clusters")
	case "ModifyCluster", "RebootCluster", "PauseCluster", "ResumeCluster":
		id := first(req.Input, "ClusterIdentifier")
		b, ok, _ := p.col(req, "rscluster").Get(ctx, id)
		rec := map[string]any{"ClusterIdentifier": id}
		if ok {
			_ = json.Unmarshal(b, &rec)
		}
		if req.Operation == "PauseCluster" {
			rec["ClusterStatus"] = "paused"
		} else {
			rec["ClusterStatus"] = "available"
		}
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "rscluster").Put(ctx, id, nb)
		return &spi.Response{Output: map[string]any{"Cluster": rec}}, nil
	case "DeleteCluster":
		id := first(req.Input, "ClusterIdentifier")
		_ = p.col(req, "rscluster").Delete(ctx, id)
		_ = p.col(req, "rscredential").Delete(ctx, id)
		tables, _, _ := p.col(req, "rstable").List(ctx, id+"|", "", 0)
		for _, table := range tables {
			_ = p.col(req, "rstable").Delete(ctx, table.Key)
		}
		return &spi.Response{Output: map[string]any{"Cluster": map[string]any{"ClusterIdentifier": id, "ClusterStatus": "deleting"}}}, nil
	case "CreateClusterSnapshot":
		id := first(req.Input, "SnapshotIdentifier")
		rec := map[string]any{"SnapshotIdentifier": id, "ClusterIdentifier": first(req.Input, "ClusterIdentifier"), "Status": "available"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "rssnap").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"Snapshot": rec}}, nil
	case "DescribeClusterSnapshots":
		return listOrGet(ctx, p.col(req, "rssnap"), first(req.Input, "SnapshotIdentifier"), "Snapshots")
	case "DeleteClusterSnapshot":
		_ = p.col(req, "rssnap").Delete(ctx, first(req.Input, "SnapshotIdentifier"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateClusterSubnetGroup":
		id := first(req.Input, "ClusterSubnetGroupName")
		rec := map[string]any{"ClusterSubnetGroupName": id}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "rssubnet").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"ClusterSubnetGroup": rec}}, nil
	case "DescribeClusterSubnetGroups":
		return listOrGet(ctx, p.col(req, "rssubnet"), first(req.Input, "ClusterSubnetGroupName"), "ClusterSubnetGroups")
	case "DeleteClusterSubnetGroup":
		_ = p.col(req, "rssubnet").Delete(ctx, first(req.Input, "ClusterSubnetGroupName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateClusterParameterGroup":
		id := first(req.Input, "ParameterGroupName")
		rec := map[string]any{"ParameterGroupName": id, "ParameterGroupFamily": first(req.Input, "ParameterGroupFamily")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "rspg").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"ClusterParameterGroup": rec}}, nil
	case "DescribeClusterParameterGroups":
		return listOrGet(ctx, p.col(req, "rspg"), first(req.Input, "ParameterGroupName"), "ParameterGroups")
	case "DeleteClusterParameterGroup":
		_ = p.col(req, "rspg").Delete(ctx, first(req.Input, "ParameterGroupName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateTags":
		b, _ := json.Marshal(req.Input["Tags"])
		_ = p.col(req, "rstags").Put(ctx, first(req.Input, "ResourceName"), b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeTags":
		b, ok, _ := p.col(req, "rstags").Get(ctx, first(req.Input, "ResourceName"))
		var tags any = []any{}
		if ok {
			_ = json.Unmarshal(b, &tags)
		}
		return &spi.Response{Output: map[string]any{"TaggedResources": []any{map[string]any{"Tags": tags}}}}, nil
	case "DeleteTags":
		_ = p.col(req, "rstags").Delete(ctx, first(req.Input, "ResourceName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ResizeCluster":
		id := first(req.Input, "ClusterIdentifier")
		b, ok, _ := p.col(req, "rscluster").Get(ctx, id)
		rec := map[string]any{"ClusterIdentifier": id, "ClusterStatus": "resizing"}
		if ok {
			_ = json.Unmarshal(b, &rec)
			rec["ClusterStatus"] = "resizing"
		}
		if n := first(req.Input, "NodeType"); n != "" {
			rec["NodeType"] = n
		}
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "rscluster").Put(ctx, id, nb)
		return &spi.Response{Output: map[string]any{"Cluster": rec}}, nil
	case "RestoreFromClusterSnapshot":
		id := first(req.Input, "ClusterIdentifier")
		rec := map[string]any{
			"ClusterIdentifier": id, "ClusterStatus": "available",
			"Endpoint": map[string]any{"Address": id + "." + req.Identity.Region + ".redshift.amazonaws.com", "Port": 5439},
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "rscluster").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"Cluster": rec}}, nil
	case "CopyClusterSnapshot":
		src, dst := first(req.Input, "SourceSnapshotIdentifier"), first(req.Input, "TargetSnapshotIdentifier")
		b, _, _ := p.col(req, "rssnap").Get(ctx, src)
		rec := map[string]any{"SnapshotIdentifier": dst, "Status": "available"}
		_ = json.Unmarshal(b, &rec)
		rec["SnapshotIdentifier"] = dst
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "rssnap").Put(ctx, dst, nb)
		return &spi.Response{Output: map[string]any{"Snapshot": rec}}, nil
	case "ModifyClusterSubnetGroup":
		id := first(req.Input, "ClusterSubnetGroupName")
		rec := map[string]any{"ClusterSubnetGroupName": id}
		if b, ok, _ := p.col(req, "rssubnet").Get(ctx, id); ok {
			_ = json.Unmarshal(b, &rec)
		}
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "rssubnet").Put(ctx, id, nb)
		return &spi.Response{Output: map[string]any{"ClusterSubnetGroup": rec}}, nil
	case "ModifyClusterParameterGroup", "ResetClusterParameterGroup":
		name := first(req.Input, "ParameterGroupName")
		b, _ := json.Marshal(req.Input)
		_ = p.col(req, "rspg-params").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeClusterParameters":
		name := first(req.Input, "ParameterGroupName")
		b, ok, _ := p.col(req, "rspg-params").Get(ctx, name)
		params := []any{}
		if ok {
			var rec map[string]any
			_ = json.Unmarshal(b, &rec)
			params = append(params, rec)
		}
		return &spi.Response{Output: map[string]any{"Parameters": params}}, nil
	case "EnableSnapshotCopy":
		id := first(req.Input, "ClusterIdentifier")
		b, ok, _ := p.col(req, "rscluster").Get(ctx, id)
		rec := map[string]any{"ClusterIdentifier": id}
		if ok {
			_ = json.Unmarshal(b, &rec)
		}
		rec["ClusterSnapshotCopyStatus"] = map[string]any{"DestinationRegion": first(req.Input, "DestinationRegion")}
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "rscluster").Put(ctx, id, nb)
		return &spi.Response{Output: map[string]any{"Cluster": rec}}, nil
	case "DisableSnapshotCopy":
		id := first(req.Input, "ClusterIdentifier")
		b, ok, _ := p.col(req, "rscluster").Get(ctx, id)
		rec := map[string]any{"ClusterIdentifier": id}
		if ok {
			_ = json.Unmarshal(b, &rec)
		}
		delete(rec, "ClusterSnapshotCopyStatus")
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "rscluster").Put(ctx, id, nb)
		return &spi.Response{Output: map[string]any{"Cluster": rec}}, nil
	case "CreateSnapshotCopyGrant":
		n := first(req.Input, "SnapshotCopyGrantName")
		rec := map[string]any{"SnapshotCopyGrantName": n, "KmsKeyId": first(req.Input, "KmsKeyId")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "rscopygrant").Put(ctx, n, b)
		return &spi.Response{Output: map[string]any{"SnapshotCopyGrant": rec}}, nil
	case "DescribeSnapshotCopyGrants":
		return listOrGet(ctx, p.col(req, "rscopygrant"), first(req.Input, "SnapshotCopyGrantName"), "SnapshotCopyGrants")
	case "DeleteSnapshotCopyGrant":
		_ = p.col(req, "rscopygrant").Delete(ctx, first(req.Input, "SnapshotCopyGrantName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateEventSubscription":
		n := first(req.Input, "SubscriptionName")
		rec := map[string]any{"CustSubscriptionId": n, "SnsTopicArn": first(req.Input, "SnsTopicArn"), "Status": "active"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "rsev").Put(ctx, n, b)
		return &spi.Response{Output: map[string]any{"EventSubscription": rec}}, nil
	case "DescribeEventSubscriptions":
		return listOrGet(ctx, p.col(req, "rsev"), first(req.Input, "SubscriptionName"), "EventSubscriptionsList")
	case "DeleteEventSubscription":
		_ = p.col(req, "rsev").Delete(ctx, first(req.Input, "SubscriptionName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetClusterCredentials":
		user := first(req.Input, "DbUser")
		return &spi.Response{Output: map[string]any{
			"DbUser": "IAM:" + user, "DbPassword": p.deps.Rand.Derive("rs:" + user).Hex(16),
			"Expiration": "2099-01-01T00:00:00Z",
		}}, nil
	case "ModifyClusterIamRoles":
		id := first(req.Input, "ClusterIdentifier")
		b, ok, _ := p.col(req, "rscluster").Get(ctx, id)
		rec := map[string]any{"ClusterIdentifier": id}
		if ok {
			_ = json.Unmarshal(b, &rec)
		}
		rec["IamRoles"] = req.Input["AddIamRoles"]
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "rscluster").Put(ctx, id, nb)
		return &spi.Response{Output: map[string]any{"Cluster": rec}}, nil
	default:
		return p.extra(ctx, req)
	}
}

// CreateTable declares the existing table required by a Firehose COPY command.
func (p *Pack) CreateTable(ctx context.Context, identity spi.Identity, cluster, database, table string, columns []string) error {
	if cluster == "" || database == "" || table == "" || len(columns) == 0 {
		return errors.New("cluster, database, table, and columns are required")
	}
	req := &spi.Request{Identity: identity}
	if _, ok, _ := p.col(req, "rscluster").Get(ctx, cluster); !ok {
		return errors.New("Redshift cluster not found")
	}
	for _, column := range columns {
		if strings.TrimSpace(column) == "" {
			return errors.New("Redshift table column is empty")
		}
	}
	body, _ := json.Marshal(tableData{Columns: columns})
	return p.col(req, "rstable").Put(ctx, redshiftTableKey(cluster, database, table), body)
}

// Copy loads records into a declared table using the common Firehose COPY formats.
func (p *Pack) Copy(ctx context.Context, identity spi.Identity, input CopyInput) error {
	req := &spi.Request{Identity: identity}
	clusterBody, ok, _ := p.col(req, "rscluster").Get(ctx, input.Cluster)
	if !ok {
		return errors.New("Redshift cluster not found")
	}
	var cluster map[string]any
	_ = json.Unmarshal(clusterBody, &cluster)
	if first(cluster, "ClusterStatus") != "available" || first(cluster, "DBName") != input.Database {
		return errors.New("Redshift cluster is unavailable or database does not exist")
	}
	credentialBody, ok, _ := p.col(req, "rscredential").Get(ctx, input.Cluster)
	if !ok {
		return errors.New("Redshift credentials are unavailable")
	}
	var credential map[string]any
	_ = json.Unmarshal(credentialBody, &credential)
	if first(credential, "Username") != input.Username || first(credential, "PasswordHash") != fmt.Sprintf("%x", sha256.Sum256([]byte(input.Password))) {
		return errors.New("Redshift credentials are invalid")
	}
	key := redshiftTableKey(input.Cluster, input.Database, input.Table)
	return p.col(req, "rstable").Txn(ctx, func(tx spi.Tx) error {
		body, ok, err := tx.Get(key)
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("Redshift table not found")
		}
		var table tableData
		if json.Unmarshal(body, &table) != nil || len(table.Columns) == 0 {
			return errors.New("Redshift table is invalid")
		}
		columns := table.Columns
		if input.Columns != "" {
			columns = splitColumns(input.Columns)
			if !columnsExist(table.Columns, columns) {
				return errors.New("Redshift COPY column does not exist")
			}
		}
		rows, err := parseCopyRows(input.Data, columns, input.Options)
		if err != nil {
			return err
		}
		// ponytail: whole-table JSON rewrite; move to a SQL engine when the Redshift Data API lands.
		table.Rows = append(table.Rows, rows...)
		stored, _ := json.Marshal(table)
		return tx.Put(key, stored)
	})
}

// TableRows returns a copy of rows loaded by COPY for local data-plane consumers.
func (p *Pack) TableRows(ctx context.Context, identity spi.Identity, cluster, database, table string) ([]map[string]any, error) {
	req := &spi.Request{Identity: identity}
	body, ok, err := p.col(req, "rstable").Get(ctx, redshiftTableKey(cluster, database, table))
	if err != nil || !ok {
		return nil, errors.New("Redshift table not found")
	}
	var data tableData
	if json.Unmarshal(body, &data) != nil {
		return nil, errors.New("Redshift table is invalid")
	}
	return data.Rows, nil
}

func redshiftTableKey(cluster, database, table string) string {
	return cluster + "|" + database + "|" + table
}

func splitColumns(value string) []string {
	columns := strings.Split(value, ",")
	for index := range columns {
		columns[index] = strings.TrimSpace(columns[index])
	}
	return columns
}

func columnsExist(table, requested []string) bool {
	available := map[string]bool{}
	for _, column := range table {
		available[column] = true
	}
	for _, column := range requested {
		if column == "" || !available[column] {
			return false
		}
	}
	return true
}

func parseCopyRows(data [][]byte, columns []string, options string) ([]map[string]any, error) {
	jsonFormat := strings.Contains(strings.ToUpper(options), "JSON")
	delimiter := "|"
	if index := strings.Index(strings.ToLower(options), "delimiter"); index >= 0 {
		remainder := strings.TrimSpace(options[index+len("delimiter"):])
		if len(remainder) < 3 || (remainder[0] != '\'' && remainder[0] != '"') {
			return nil, errors.New("Redshift COPY delimiter is invalid")
		}
		end := strings.IndexByte(remainder[1:], remainder[0])
		if end < 0 {
			return nil, errors.New("Redshift COPY delimiter is invalid")
		}
		delimiter = remainder[1 : end+1]
		if delimiter == `\t` {
			delimiter = "\t"
		}
		if len(delimiter) != 1 {
			return nil, errors.New("Redshift COPY delimiter must be one byte")
		}
	}
	var rows []map[string]any
	for _, line := range bytes.Split(bytes.Join(data, nil), []byte{'\n'}) {
		line = bytes.TrimSuffix(line, []byte{'\r'})
		if len(line) == 0 {
			continue
		}
		if jsonFormat {
			row := map[string]any{}
			if json.Unmarshal(line, &row) != nil {
				return nil, errors.New("Redshift COPY JSON row is invalid")
			}
			rows = append(rows, row)
			continue
		}
		fields := strings.Split(string(line), delimiter)
		if len(fields) != len(columns) {
			return nil, errors.New("Redshift COPY row has the wrong column count")
		}
		row := make(map[string]any, len(columns))
		for index, column := range columns {
			row[column] = fields[index]
		}
		rows = append(rows, row)
	}
	if len(rows) == 0 {
		return nil, errors.New("Redshift COPY contains no rows")
	}
	return rows, nil
}

func listOrGet(ctx context.Context, c spi.Collection, id, listKey string) (*spi.Response, error) {
	if id != "" {
		b, ok, _ := c.Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "ClusterNotFound", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{listKey: []any{rec}}}, nil
	}
	kvs, _, _ := c.List(ctx, "", "", 0)
	var items []any
	for _, kv := range kvs {
		var rec map[string]any
		_ = json.Unmarshal(kv.Value, &rec)
		items = append(items, rec)
	}
	return &spi.Response{Output: map[string]any{listKey: items}}, nil
}

func first(in map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := in[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}
