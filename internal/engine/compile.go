package engine

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/common/types/ref"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bir"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/prim"
)

// compileAll turns the bundle's expression sources into runnable programs.
//
// bir.Validate already compiled every expression to prove it is well-formed
// and references only bindings in scope. This compiles them again against the
// same declarations plus real function implementations, because validation
// deliberately keeps the runtime out of the loader.
func (e *Engine) compileAll() error {
	if e.ir.Compiled == nil {
		return fmt.Errorf("engine: %s was not loaded through bir.Load", e.ir.ServiceID)
	}
	names := e.bindingNames()
	env, err := runtimeEnv(names)
	if err != nil {
		return err
	}
	paths := make([]string, 0, len(e.ir.Compiled.Programs))
	for p := range e.ir.Compiled.Programs {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		src := e.ir.Compiled.Programs[p].Source
		ast, iss := env.Compile(src)
		if iss != nil && iss.Err() != nil {
			return fmt.Errorf("engine: %s: %s: %w", e.ir.ServiceID, p, iss.Err())
		}
		prg, err := env.Program(ast)
		if err != nil {
			return fmt.Errorf("engine: %s: %s: %w", e.ir.ServiceID, p, err)
		}
		e.programs[p] = prg
	}
	return nil
}

// bindingNames is the union of every name any scope in this bundle can bind.
// The loader already rejected out-of-scope references per operation, so a
// single permissive runtime environment cannot loosen that guarantee.
func (e *Engine) bindingNames() []string {
	seen := map[string]bool{
		"id": true, "rec": true, "event": true, "fx": true,
		"hit": true, "item": true, "arn": true, "items": true,
	}
	for name := range e.ir.Resources {
		seen[name] = true
	}
	for _, op := range e.ir.Operations {
		for b := range op.Reads {
			seen[b] = true
			seen[b+"_found"] = true
		}
		for b := range op.Let {
			seen[b] = true
		}
		if op.Select != nil && op.Select.Binding != "" {
			seen[op.Select.Binding] = true
		}
		if op.List != nil {
			for b := range op.List.Reads {
				seen[b] = true
				seen[b+"_found"] = true
			}
			for b := range op.List.Let {
				seen[b] = true
			}
		}
	}
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func runtimeEnv(names []string) (*cel.Env, error) {
	opts := []cel.EnvOption{
		cel.Variable("input", cel.MapType(cel.StringType, cel.DynType)),
		cel.Variable("identity", cel.MapType(cel.StringType, cel.DynType)),
		cel.Variable("now", cel.TimestampType),
		cel.Variable("now_ms", cel.IntType),
		cel.Variable("endpoint", cel.StringType),
		cel.Variable("headers", cel.MapType(cel.StringType, cel.StringType)),
		cel.Variable("has_http", cel.BoolType),
	}
	for _, n := range names {
		opts = append(opts, cel.Variable(n, cel.DynType))
	}
	opts = append(opts, runtimeFuncs()...)
	return cel.NewEnv(opts...)
}

// runtimeFuncs implements the helpers behavior data may call. They are pure by
// construction: string and value manipulation only, no store, clock or
// randomness, so an expression cannot become an effect by accident.
func runtimeFuncs() []cel.EnvOption {
	str := cel.StringType
	num := cel.IntType
	dyn := cel.DynType

	return []cel.EnvOption{
		cel.Function("md5hex", cel.Overload("md5hex_1", []*cel.Type{str}, str,
			cel.UnaryBinding(func(v ref.Val) ref.Val {
				sum := md5.Sum([]byte(fmt.Sprint(v.Value())))
				return types.String(hex.EncodeToString(sum[:]))
			}))),

		cel.Function("sha256hex", cel.Overload("sha256hex_1", []*cel.Type{str}, str,
			cel.UnaryBinding(func(v ref.Val) ref.Val {
				sum := sha256.Sum256([]byte(fmt.Sprint(v.Value())))
				return types.String(hex.EncodeToString(sum[:]))
			}))),

		// b64urlhex decodes hex and base64url-encodes without padding, so a
		// digest renders the way PKCE's S256 challenge expects it.
		cel.Function("b64urlhex", cel.Overload("b64urlhex_1", []*cel.Type{str}, str,
			cel.UnaryBinding(func(v ref.Val) ref.Val {
				raw, err := hex.DecodeString(fmt.Sprint(v.Value()))
				if err != nil {
					return types.String("")
				}
				return types.String(base64.RawURLEncoding.EncodeToString(raw))
			}))),

		// b64 / b64decode are std encoding for opaque wire tokens (SQS move
		// handles are JSON documents in base64, not hex digests).
		cel.Function("b64", cel.Overload("b64_1", []*cel.Type{str}, str,
			cel.UnaryBinding(func(v ref.Val) ref.Val {
				return types.String(base64.StdEncoding.EncodeToString([]byte(fmt.Sprint(v.Value()))))
			}))),
		cel.Function("b64decode", cel.Overload("b64decode_1", []*cel.Type{str}, str,
			cel.UnaryBinding(func(v ref.Val) ref.Val {
				raw, err := base64.StdEncoding.DecodeString(fmt.Sprint(v.Value()))
				if err != nil {
					return types.String("")
				}
				return types.String(string(raw))
			}))),

		cel.Function("validReceiptHandle", cel.Overload("validReceiptHandle_1", []*cel.Type{str}, cel.BoolType,
			cel.UnaryBinding(func(v ref.Val) ref.Val {
				h := fmt.Sprint(v.Value())
				if len(h) != 64 {
					return types.Bool(false)
				}
				_, err := hex.DecodeString(h)
				return types.Bool(err == nil)
			}))),
		cel.Function("validMessageContents", cel.Overload("validMessageContents_1", []*cel.Type{str}, cel.BoolType,
			cel.UnaryBinding(func(v ref.Val) ref.Val {
				for _, r := range fmt.Sprint(v.Value()) {
					if r != '\t' && r != '\n' && r != '\r' && (r < 0x20 || r > 0xD7FF && r < 0xE000 || r > 0xFFFD && r < 0x10000) {
						return types.Bool(false)
					}
				}
				return types.Bool(true)
			}))),
		cel.Function("validMessageGroupID", cel.Overload("validMessageGroupID_1", []*cel.Type{str}, cel.BoolType,
			cel.UnaryBinding(func(v ref.Val) ref.Val {
				s := fmt.Sprint(v.Value())
				if len(s) == 0 || len(s) > 128 {
					return types.Bool(false)
				}
				const punctuation = `!"#$%&'()*+,-./:;<=>?@[\]^_` + "`" + `{|}~`
				for _, r := range s {
					if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && !strings.ContainsRune(punctuation, r) {
						return types.Bool(false)
					}
				}
				return types.Bool(true)
			}))),
		cel.Function("validQueueName", cel.Overload("validQueueName_1", []*cel.Type{str}, cel.BoolType,
			cel.UnaryBinding(func(v ref.Val) ref.Val {
				return types.Bool(validQueueName(fmt.Sprint(v.Value())))
			}))),
		cel.Function("sqsIAMPrincipals", cel.Overload("sqsIAMPrincipals_2", []*cel.Type{dyn, str}, dyn,
			cel.BinaryBinding(func(accounts, region ref.Val) ref.Val {
				return types.DefaultTypeAdapter.NativeToValue(sqsIAMPrincipals(fromCEL(accounts), fmt.Sprint(region.Value())))
			}))),
		cel.Function("sqsSQSActions", cel.Overload("sqsSQSActions_1", []*cel.Type{dyn}, dyn,
			cel.UnaryBinding(func(actions ref.Val) ref.Val {
				return types.DefaultTypeAdapter.NativeToValue(sqsSQSActions(fromCEL(actions)))
			}))),
		cel.Function("sqsQueueAttrConflict", cel.Overload("sqsQueueAttrConflict_2", []*cel.Type{dyn, dyn}, str,
			cel.BinaryBinding(func(effective, requested ref.Val) ref.Val {
				return types.String(sqsQueueAttrConflict(fromCEL(effective), fromCEL(requested)))
			}))),
		cel.Function("sqsMergeQueueAttrs", cel.Overload("sqsMergeQueueAttrs_2", []*cel.Type{dyn, dyn}, dyn,
			cel.BinaryBinding(func(existing, updates ref.Val) ref.Val {
				return types.DefaultTypeAdapter.NativeToValue(sqsMergeQueueAttrs(fromCEL(existing), fromCEL(updates)))
			}))),

		// trim is strings.TrimSpace.
		cel.Function("trim", cel.Overload("trim_1", []*cel.Type{str}, str,
			cel.UnaryBinding(func(v ref.Val) ref.Val {
				return types.String(strings.TrimSpace(fmt.Sprint(v.Value())))
			}))),

		// coalesce returns the first argument that is neither null nor empty,
		// which is how behavior data states a documented default.
		cel.Function("coalesce", cel.Overload("coalesce_2", []*cel.Type{dyn, dyn}, dyn,
			cel.BinaryBinding(func(a, b ref.Val) ref.Val {
				if !blank(a) {
					return a
				}
				return b
			}))),

		cel.Function("clamp", cel.Overload("clamp_3", []*cel.Type{num, num, num}, num,
			cel.FunctionBinding(func(args ...ref.Val) ref.Val {
				v, lo, hi := asInt(args[0]), asInt(args[1]), asInt(args[2])
				if v < lo {
					v = lo
				}
				if v > hi {
					v = hi
				}
				return types.Int(v)
			}))),

		cel.Function("seconds", cel.Overload("seconds_1", []*cel.Type{dyn}, cel.DurationType,
			cel.UnaryBinding(func(v ref.Val) ref.Val {
				return types.Duration{Duration: time.Duration(asInt(v)) * time.Second}
			}))),

		cel.Function("parseJSON", cel.Overload("parseJSON_1", []*cel.Type{str}, dyn,
			cel.UnaryBinding(func(v ref.Val) ref.Val {
				var out any
				if err := json.Unmarshal([]byte(fmt.Sprint(v.Value())), &out); err != nil {
					return types.NullValue
				}
				return types.DefaultTypeAdapter.NativeToValue(out)
			}))),

		// The inverse of parseJSON, for the services that keep a structured
		// document inside a string-valued attribute -- an SQS queue's access
		// policy, an SNS topic's, an IAM role's trust document. Reading one
		// without being able to write it back means a bundle can inspect such
		// a document but never amend it, which is what AddPermission does.
		//
		// Marshalling sorts map keys, so the same document always serializes
		// to the same bytes and a stored policy does not churn between
		// otherwise identical writes.
		cel.Function("toJSON", cel.Overload("toJSON_1", []*cel.Type{dyn}, str,
			cel.UnaryBinding(func(v ref.Val) ref.Val {
				raw, err := json.Marshal(fromCEL(v))
				if err != nil {
					return types.String("")
				}
				return types.String(string(raw))
			}))),

		// CEL's core has no way to build a map from another map minus some
		// keys: comprehensions over a map yield a list. Every `Untag*`
		// operation in every provider needs exactly that, so it is a function
		// rather than a per-service primitive.
		cel.Function("without", cel.Overload("without_2", []*cel.Type{dyn, dyn}, dyn,
			cel.BinaryBinding(func(m, keys ref.Val) ref.Val {
				src, _ := fromCEL(m).(map[string]any)
				drop := map[string]bool{}
				if list, ok := fromCEL(keys).([]any); ok {
					for _, k := range list {
						drop[fmt.Sprint(k)] = true
					}
				}
				out := map[string]any{}
				for k, v := range src {
					if !drop[k] {
						out[k] = v
					}
				}
				return types.DefaultTypeAdapter.NativeToValue(out)
			}))),

		// merge layers maps left to right, which is how a service states
		// "defaults, then what was stored, then what was set".
		cel.Function("merge", cel.Overload("merge_2", []*cel.Type{dyn, dyn}, dyn,
			cel.BinaryBinding(func(a, b ref.Val) ref.Val {
				out := map[string]any{}
				for _, layer := range []ref.Val{a, b} {
					if m, ok := fromCEL(layer).(map[string]any); ok {
						for k, v := range m {
							out[k] = v
						}
					}
				}
				return types.DefaultTypeAdapter.NativeToValue(out)
			}))),

		// indices answers with 0..n-1 for a list, which is the one thing CEL's
		// comprehensions cannot give a bundle: `map` binds the element and
		// never its position.
		//
		// AWS batch responses are correlated by position rather than by a
		// caller-supplied id -- Comprehend's BatchDetectSentiment answers a
		// ResultList whose rows carry an Index into the TextList that was
		// sent, and its ErrorList carries the same Index for the rows that
		// failed. Without this a bundle can answer the right number of rows
		// and cannot say which input each one is about.
		//
		// It takes the list rather than a count so it cannot be asked for a
		// range larger than something already in memory.
		cel.Function("indices", cel.Overload("indices_1", []*cel.Type{dyn}, dyn,
			cel.UnaryBinding(func(v ref.Val) ref.Val {
				list, _ := fromCEL(v).([]any)
				out := make([]any, len(list))
				for i := range list {
					out[i] = int64(i)
				}
				return types.DefaultTypeAdapter.NativeToValue(out)
			}))),

		// series answers 0..n-1 for a count, where indices takes a list.
		//
		// RunInstances is why: the caller sends MinCount and the service
		// creates that many instances, so the count is the request's to choose
		// and there is no list to take it from. That is exactly the reason
		// indices refuses a count, so the bound it got from the list is
		// replaced by an explicit one -- seriesMax, far above any real launch.
		//
		// Above the bound this refuses rather than truncating. The hand-written
		// pack looped MinCount times with no bound at all, so one request
		// naming a million instances built a million records; answering a
		// truncated list instead would be a wrong answer where an error is an
		// honest one. A count that is absent, unparseable or below one answers
		// an empty range, and the bundle decides what that means.
		cel.Function("series", cel.Overload("series_1", []*cel.Type{dyn}, dyn,
			cel.UnaryBinding(func(v ref.Val) ref.Val {
				n := asInt(v)
				if n > seriesMax {
					return types.NewErr(
						"engine: series(%d) exceeds the %d the engine will build in one call", n, seriesMax)
				}
				out := make([]any, 0, max(n, 0))
				for i := int64(0); i < n; i++ {
					out = append(out, i)
				}
				return types.DefaultTypeAdapter.NativeToValue(out)
			}))),

		// rollup builds a map from a list of records, each element contributing
		// element[keyMember] -> element[valueMember]. CEL comprehensions turn a
		// list into another list, never into a map keyed by an element's member,
		// which is the shape of every "ids to their X" answer -- Secrets
		// Manager's VersionIdsToStages is the first one transcribed. Later
		// duplicates overwrite earlier ones, matching the map assignment the
		// packs wrote.
		cel.Function("rollup", cel.Overload("rollup_3", []*cel.Type{dyn, str, str}, dyn,
			cel.FunctionBinding(func(args ...ref.Val) ref.Val {
				list, _ := fromCEL(args[0]).([]any)
				key, val := fmt.Sprint(args[1].Value()), fmt.Sprint(args[2].Value())
				out := map[string]any{}
				for _, e := range list {
					if m, ok := e.(map[string]any); ok {
						out[fmt.Sprint(m[key])] = m[val]
					}
				}
				return types.DefaultTypeAdapter.NativeToValue(out)
			}))),

		// sum folds a list of numbers into their total, the accumulation CEL's
		// macros cannot express. Ints stay ints.
		cel.Function("sum", cel.Overload("sum_1", []*cel.Type{dyn}, dyn,
			cel.UnaryBinding(func(v ref.Val) ref.Val {
				list, _ := fromCEL(v).([]any)
				var total int64
				for _, e := range list {
					n, _ := toFloat(e)
					total += int64(n)
				}
				return types.DefaultTypeAdapter.NativeToValue(total)
			}))),

		// slice is list[start:end] with the bounds clamped into range, so a
		// cursor past the end is an empty page rather than an error.
		cel.Function("slice", cel.Overload("slice_3", []*cel.Type{dyn, num, num}, dyn,
			cel.FunctionBinding(func(args ...ref.Val) ref.Val {
				list, _ := fromCEL(args[0]).([]any)
				start, end := asInt(args[1]), asInt(args[2])
				n := int64(len(list))
				start = min(max(start, 0), n)
				end = min(max(end, start), n)
				out := make([]any, 0, end-start)
				return types.DefaultTypeAdapter.NativeToValue(append(out, list[start:end]...))
			}))),

		cel.Function("lastSegment", cel.Overload("lastSegment_2", []*cel.Type{str, str}, str,
			cel.BinaryBinding(func(s, sep ref.Val) ref.Val {
				parts := strings.Split(fmt.Sprint(s.Value()), fmt.Sprint(sep.Value()))
				return types.String(parts[len(parts)-1])
			}))),

		// lower case-folds ASCII. See the declaration in internal/bir/celenv.go
		// for why the Unicode form is not offered.
		cel.Function("lower", cel.Overload("lower_1", []*cel.Type{str}, str,
			cel.UnaryBinding(func(s ref.Val) ref.Val {
				return types.String(asciiLower(fmt.Sprint(s.Value())))
			}))),

		// upper is the same fold the other way. See the declaration in
		// internal/bir/celenv.go.
		cel.Function("upper", cel.Overload("upper_1", []*cel.Type{str}, str,
			cel.UnaryBinding(func(s ref.Val) ref.Val {
				return types.String(asciiUpper(fmt.Sprint(s.Value())))
			}))),

		// arn builds "arn:<partition>:<rest joined by :>" from parts. String
		// assembly, not provider logic: the engine stays free of service names.
		cel.Function("arn", cel.Overload("arn_2", []*cel.Type{str, dyn}, str,
			cel.BinaryBinding(func(partition, parts ref.Val) ref.Val {
				out := []string{"arn", fmt.Sprint(partition.Value())}
				if list, ok := fromCEL(parts).([]any); ok {
					for _, p := range list {
						out = append(out, fmt.Sprint(p))
					}
				}
				return types.String(strings.Join(out, ":"))
			}))),

		cel.Function("join", cel.Overload("join_2", []*cel.Type{dyn, str}, str,
			cel.BinaryBinding(func(list, sep ref.Val) ref.Val {
				items, _ := fromCEL(list).([]any)
				parts := make([]string, 0, len(items))
				for _, p := range items {
					parts = append(parts, fmt.Sprint(p))
				}
				return types.String(strings.Join(parts, fmt.Sprint(sep.Value())))
			}))),

		// split is strings.Split: CEL's core has no tokenization, and a Bearer
		// header's token, a blob token's underscore-separated fields and a
		// pathname's extension are all read by splitting.
		cel.Function("split", cel.Overload("split_2", []*cel.Type{str, str}, dyn,
			cel.BinaryBinding(func(s, sep ref.Val) ref.Val {
				parts := strings.Split(fmt.Sprint(s.Value()), fmt.Sprint(sep.Value()))
				out := make([]any, len(parts))
				for i, p := range parts {
					out[i] = p
				}
				return types.DefaultTypeAdapter.NativeToValue(out)
			}))),

		// iso8601ms renders a timestamp the way JavaScript's toISOString does:
		// always with milliseconds. CEL's string(timestamp) drops a zero
		// fraction, and Vercel Blob's uploadedAt carries it either way.
		cel.Function("iso8601ms", cel.Overload("iso8601ms_1", []*cel.Type{cel.TimestampType}, str,
			cel.UnaryBinding(func(v ref.Val) ref.Val {
				t, ok := v.(types.Timestamp)
				if !ok {
					return types.String(fmt.Sprint(v.Value()))
				}
				return types.String(t.Time.UTC().Format("2006-01-02T15:04:05.000Z07:00"))
			}))),

		cel.Function("tagmatch", cel.Overload("tagmatch_3", []*cel.Type{str, dyn, str}, cel.BoolType,
			cel.FunctionBinding(func(args ...ref.Val) ref.Val {
				expr := fmt.Sprint(args[0].Value())
				tags, _ := fromCEL(args[1]).(map[string]any)
				container := fmt.Sprint(args[2].Value())
				ev, err := bir.ParseTagExpr(expr)
				if err != nil {
					return types.NewErr("engine: invalid tag condition %q: %v", expr, err)
				}
				return types.Bool(ev(tags, container))
			}))),

		cel.Function("tagkeys", cel.Overload("tagkeys_1", []*cel.Type{str}, dyn,
			cel.UnaryBinding(func(v ref.Val) ref.Val {
				out := make([]any, 0)
				for _, k := range bir.TagExprKeys(fmt.Sprint(v.Value())) {
					out = append(out, k)
				}
				return types.DefaultTypeAdapter.NativeToValue(out)
			}))),

		cel.Function("hier", cel.Overload("hier_3", []*cel.Type{dyn, str, str}, dyn,
			cel.FunctionBinding(func(args ...ref.Val) ref.Val {
				raw, _ := fromCEL(args[0]).([]any)
				names := make([]string, 0, len(raw))
				for _, n := range raw {
					names = append(names, fmt.Sprint(n))
				}
				entries := hierList(names, fmt.Sprint(args[1].Value()), fmt.Sprint(args[2].Value()))
				out := make([]any, 0, len(entries))
				for _, e := range entries {
					out = append(out, e)
				}
				return types.DefaultTypeAdapter.NativeToValue(out)
			}))),

		// zeros materializes n NUL bytes, bounded like series for the same
		// reason: an unbounded repeat lets one request allocate without limit.
		cel.Function("zeros", cel.Overload("zeros_1", []*cel.Type{num}, str,
			cel.UnaryBinding(func(v ref.Val) ref.Val {
				n := asInt(v)
				if n > zerosMax {
					return types.NewErr(
						"engine: zeros(%d) exceeds the %d the engine will build in one call", n, zerosMax)
				}
				return types.String(strings.Repeat("\x00", int(max(n, 0))))
			}))),

		// substr slices bytes, not code points: page arithmetic is defined on
		// byte offsets and a body that is not UTF-8 must not shift them.
		cel.Function("substr", cel.Overload("substr_3", []*cel.Type{str, num, num}, str,
			cel.FunctionBinding(func(args ...ref.Val) ref.Val {
				s := fmt.Sprint(args[0].Value())
				start := max(int64(0), min(asInt(args[1]), int64(len(s))))
				end := max(start, min(asInt(args[2]), int64(len(s))))
				return types.String(s[start:end])
			}))),

		cel.Function("blen", cel.Overload("blen_1", []*cel.Type{str}, num,
			cel.UnaryBinding(func(v ref.Val) ref.Val {
				return types.Int(len(fmt.Sprint(v.Value())))
			}))),

		// queueFromArn is lastSegment with the ARN separator, named for the
		// thing bundles actually write.
		cel.Function("queueFromArn", cel.Overload("queueFromArn_1", []*cel.Type{str}, str,
			cel.UnaryBinding(func(v ref.Val) ref.Val {
				parts := strings.Split(fmt.Sprint(v.Value()), ":")
				return types.String(parts[len(parts)-1])
			}))),

		// md5MessageAttributes is the AWS SQS attribute digest (length-prefixed
		// name/type/transport/value, names sorted).
		cel.Function("md5MessageAttributes", cel.Overload("md5MessageAttributes_1", []*cel.Type{dyn}, str,
			cel.UnaryBinding(func(attrs ref.Val) ref.Val {
				src, _ := fromCEL(attrs).(map[string]any)
				if len(src) == 0 {
					return types.String("")
				}
				names := make([]string, 0, len(src))
				for name := range src {
					names = append(names, name)
				}
				sort.Strings(names)
				d := md5.New()
				writeField := func(value []byte) {
					var length [4]byte
					binary.BigEndian.PutUint32(length[:], uint32(len(value)))
					_, _ = d.Write(length[:])
					_, _ = d.Write(value)
				}
				for _, name := range names {
					attribute, _ := src[name].(map[string]any)
					if attribute == nil {
						continue
					}
					dataType := fmt.Sprint(attribute["DataType"])
					writeField([]byte(name))
					writeField([]byte(dataType))
					transport := byte(1)
					if strings.HasPrefix(dataType, "Binary") {
						transport = 2
					}
					_, _ = d.Write([]byte{transport})
					value := fmt.Sprint(attribute["StringValue"])
					if transport == 2 {
						switch raw := attribute["BinaryValue"].(type) {
						case []byte:
							value = string(raw)
						default:
							decoded, err := base64.StdEncoding.DecodeString(fmt.Sprint(raw))
							if err == nil {
								value = string(decoded)
							}
						}
					}
					writeField([]byte(value))
				}
				return types.String(hex.EncodeToString(d.Sum(nil)))
			}))),

		// messageSize is body bytes plus attribute name/type/value bytes — the
		// SQS MaximumMessageSize accounting the pack uses.
		cel.Function("messageSize", cel.Overload("messageSize_2", []*cel.Type{str, dyn}, cel.IntType,
			cel.BinaryBinding(func(body, attrs ref.Val) ref.Val {
				size := len([]byte(fmt.Sprint(body.Value())))
				src, _ := fromCEL(attrs).(map[string]any)
				for name, raw := range src {
					attribute, _ := raw.(map[string]any)
					if attribute == nil {
						continue
					}
					str := func(v any) string {
						if v == nil {
							return ""
						}
						return fmt.Sprint(v)
					}
					size += len(name) + len(str(attribute["DataType"])) +
						len(str(attribute["StringValue"])) + len(str(attribute["BinaryValue"]))
				}
				return types.Int(size)
			}))),

		cel.Function("knownQueueAttribute", cel.Overload("knownQueueAttribute_1", []*cel.Type{str}, cel.BoolType,
			cel.UnaryBinding(func(v ref.Val) ref.Val {
				switch fmt.Sprint(v.Value()) {
				case "ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible", "ApproximateNumberOfMessagesDelayed",
					"QueueArn", "VisibilityTimeout", "DelaySeconds", "MaximumMessageSize", "MessageRetentionPeriod",
					"ReceiveMessageWaitTimeSeconds", "SqsManagedSseEnabled", "CreatedTimestamp", "LastModifiedTimestamp",
					"FifoQueue", "ContentBasedDeduplication", "DeduplicationScope", "FifoThroughputLimit",
					"RedrivePolicy", "Policy", "KmsMasterKeyId", "KmsDataKeyReusePeriodSeconds":
					return types.Bool(true)
				default:
					return types.Bool(false)
				}
			}))),
		cel.Function("validRedrivePolicy", cel.Overload("validRedrivePolicy_1", []*cel.Type{str}, cel.BoolType,
			cel.UnaryBinding(func(v ref.Val) ref.Val {
				raw := fmt.Sprint(v.Value())
				var policy map[string]any
				if json.Unmarshal([]byte(raw), &policy) != nil {
					return types.Bool(false)
				}
				arn := fmt.Sprint(policy["deadLetterTargetArn"])
				parts := strings.Split(arn, ":")
				if len(parts) != 6 || parts[0] != "arn" || parts[2] != "sqs" || parts[3] == "" || parts[4] == "" || parts[5] == "" {
					return types.Bool(false)
				}
				count := 0
				switch value := policy["maxReceiveCount"].(type) {
				case string:
					n, err := strconv.Atoi(value)
					if err != nil {
						return types.Bool(false)
					}
					count = n
				case float64:
					count = int(value)
				case int:
					count = value
				default:
					return types.Bool(false)
				}
				return types.Bool(count >= 1 && count <= 1000)
			}))),
		cel.Function("sseConflict", cel.Overload("sseConflict_1", []*cel.Type{dyn}, cel.BoolType,
			cel.UnaryBinding(func(v ref.Val) ref.Val {
				attrs, _ := fromCEL(v).(map[string]any)
				kms := fmt.Sprint(attrs["KmsMasterKeyId"])
				sse := fmt.Sprint(attrs["SqsManagedSseEnabled"])
				return types.Bool(kms != "" && kms != "<nil>" && sse == "true")
			}))),

		cel.Function("partition", cel.Overload("partition_1", []*cel.Type{str}, str,
			cel.UnaryBinding(func(v ref.Val) ref.Val {
				region := fmt.Sprint(v.Value())
				switch {
				case strings.HasPrefix(region, "cn-"):
					return types.String("aws-cn")
				case strings.HasPrefix(region, "us-gov-"):
					return types.String("aws-us-gov")
				case strings.HasPrefix(region, "us-iso-b-"):
					return types.String("aws-iso-b")
				case strings.HasPrefix(region, "us-iso-"):
					return types.String("aws-iso")
				default:
					return types.String("aws")
				}
			}))),
		cel.Function("sqsTags", cel.Overload("sqsTags_1", []*cel.Type{dyn}, dyn,
			cel.UnaryBinding(func(v ref.Val) ref.Val {
				input, _ := fromCEL(v).(map[string]any)
				if input == nil {
					return types.DefaultTypeAdapter.NativeToValue(map[string]any{})
				}
				for _, key := range []string{"Tags", "tags"} {
					if tags, ok := input[key].(map[string]any); ok && len(tags) > 0 {
						return types.DefaultTypeAdapter.NativeToValue(tags)
					}
				}
				keys, values := map[string]string{}, map[string]string{}
				for key, value := range input {
					if strings.HasPrefix(key, "Tag.") || strings.HasPrefix(key, "Tags.member.") {
						base := strings.TrimSuffix(strings.TrimSuffix(key, ".Key"), ".Value")
						switch {
						case strings.HasSuffix(key, ".Key"):
							keys[base] = fmt.Sprint(value)
						case strings.HasSuffix(key, ".Value"):
							values[base] = fmt.Sprint(value)
						}
					}
				}
				out := make(map[string]any, len(keys))
				for base, name := range keys {
					out[name] = values[base]
				}
				return types.DefaultTypeAdapter.NativeToValue(out)
			}))),
		cel.Function("sqsTagKeys", cel.Overload("sqsTagKeys_1", []*cel.Type{dyn}, dyn,
			cel.UnaryBinding(func(v ref.Val) ref.Val {
				input, _ := fromCEL(v).(map[string]any)
				if input == nil {
					return types.DefaultTypeAdapter.NativeToValue([]any{})
				}
				if raw, ok := input["TagKeys"].([]any); ok {
					return types.DefaultTypeAdapter.NativeToValue(raw)
				}
				var names []string
				for key, value := range input {
					if strings.HasPrefix(key, "TagKey.") || strings.HasPrefix(key, "TagKeys.member.") {
						names = append(names, fmt.Sprint(value))
					}
				}
				sort.Strings(names)
				out := make([]any, len(names))
				for i, n := range names {
					out[i] = n
				}
				return types.DefaultTypeAdapter.NativeToValue(out)
			}))),
		cel.Function("validMessageAttributes", cel.Overload("validMessageAttributes_1", []*cel.Type{dyn}, cel.BoolType,
			cel.UnaryBinding(func(v ref.Val) ref.Val {
				return types.Bool(validMessageAttributes(fromCEL(v)))
			}))),

		// filterAttrs narrows a map to a requested subset. An empty request or
		// one naming "All" (or its "." shorthand) returns everything, which is
		// the convention every provider that has such a parameter follows.
		cel.Function("filterAttrs", cel.Overload("filterAttrs_2", []*cel.Type{dyn, dyn}, dyn,
			cel.BinaryBinding(func(attrs, want ref.Val) ref.Val {
				src, _ := fromCEL(attrs).(map[string]any)
				if src == nil {
					src = map[string]any{}
				}
				names, ok := fromCEL(want).([]any)
				if !ok || len(names) == 0 {
					// Empty filter means "return none" for SQS MessageAttributeNames.
					return types.DefaultTypeAdapter.NativeToValue(map[string]any{})
				}
				for _, n := range names {
					name := fmt.Sprint(n)
					if name == "All" || name == "*" || name == ".*" || name == "." {
						return types.DefaultTypeAdapter.NativeToValue(src)
					}
				}
				out := map[string]any{}
				for _, n := range names {
					name := fmt.Sprint(n)
					if strings.HasSuffix(name, ".*") {
						prefix := strings.TrimSuffix(name, ".*")
						for key, value := range src {
							if strings.HasPrefix(key, prefix) {
								out[key] = value
							}
						}
						continue
					}
					if v, ok := src[name]; ok {
						out[name] = v
					}
				}
				return types.DefaultTypeAdapter.NativeToValue(out)
			}))),

		// prim dispatches to a named pure primitive from the registry. An
		// unregistered name fails loudly rather than returning a
		// plausible-looking value; a bundle declares the names it calls
		// under `primitives:`, and the loader refuses unknown names and
		// versions before any request runs.
		cel.Function("prim", cel.Overload("prim_2", []*cel.Type{str, dyn}, dyn,
			cel.BinaryBinding(func(name, args ref.Val) ref.Val {
				n := fmt.Sprint(name.Value())
				f, ok := prim.Lookup(n)
				if !ok {
					return types.NewErr("engine: primitive %q is not registered (registered: %s)", n, strings.Join(prim.Names(), ", "))
				}
				list, _ := fromCEL(args).([]any)
				out, err := f.Call(list)
				if err != nil {
					return types.NewErr("engine: primitive %q: %v", n, err)
				}
				return types.DefaultTypeAdapter.NativeToValue(out)
			}))),
	}
}

// seriesMax bounds what one call may build. It is generous next to any real
// launch -- EC2's own RunInstances rejects counts far below it -- and finite,
// which is the property that matters: the count comes from the request.
const seriesMax = 1024

// zerosMax bounds one zeros() call. A page blob create asks for its full
// size up front; the bound is far above what the pinned Azurite tests stage
// and far below the 8 TiB the real service allows.
const zerosMax = 1 << 24

func blank(v ref.Val) bool {
	if v == nil || v == types.NullValue {
		return true
	}
	switch t := v.Value().(type) {
	case nil:
		return true
	case string:
		return t == ""
	}
	return false
}

func asInt(v ref.Val) int64 {
	switch t := v.Value().(type) {
	case int64:
		return t
	case int:
		return int64(t)
	case float64:
		return int64(t)
	case string:
		var n int64
		_, _ = fmt.Sscanf(t, "%d", &n)
		return n
	}
	return 0
}

// asciiLower folds A-Z and leaves every other byte alone, so a value that is
// already a key stays the same length and the same bytes outside that range.
func asciiUpper(s string) string {
	out := []byte(s)
	for i, c := range out {
		if c >= 'a' && c <= 'z' {
			out[i] = c - ('a' - 'A')
		}
	}
	return string(out)
}

func asciiLower(s string) string {
	out := []byte(s)
	for i, c := range out {
		if c >= 'A' && c <= 'Z' {
			out[i] = c + ('a' - 'A')
		}
	}
	return string(out)
}
