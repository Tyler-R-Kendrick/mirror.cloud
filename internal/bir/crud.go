package bir

import "fmt"

// Why three keys stand for ten lines.
//
// Fingerprinting every operation in the tree by structure -- which keys it
// has, which effects, what its output is -- found 2,469 operations falling
// into 222 shapes, and the top of the list is not interesting: read one record
// and fault when it is missing; write one record; and in both cases answer
// with the record's own members by name. That is the same handful of lines
// several hundred times over, and it is what `list:` was already added for
// when it turned out to be the same eight lines in a hundred hand-written
// packs.
//
// The shorthand is expanded here, at load, into exactly the long form a bundle
// would otherwise spell out. Nothing downstream is taught about it: the
// validator checks the expansion as it checks any operation, the engine runs
// it, the equivalence gate replays it, and a bundle can be rewritten from long
// form to short with the recorded trace proving nothing changed.
//
// It stays narrow on purpose. A get must name its not-found error, nothing
// else may; only a delete may say what a missing record means, and a delete
// answers nothing; none may sit beside any long-form key. The long form is not
// deprecated -- it is what everything that is not exactly this shape uses.

// expandCrud rewrites every get, create, put and delete shorthand in place.
func expandCrud(s *Service) error {
	var problems Errors
	for name, op := range s.Operations {
		var verb string
		var spec *CrudSpec
		for _, sp := range [...]struct {
			verb string
			spec *CrudSpec
		}{{"get", op.Get}, {"create", op.Create}, {"put", op.Put}, {"delete", op.Delete}} {
			if sp.spec == nil {
				continue
			}
			if spec != nil {
				problems = append(problems, fmt.Errorf(
					"operations.%s: %s and %s are each a whole operation; use one", name, verb, sp.verb))
				spec = nil
				break
			}
			verb, spec = sp.verb, sp.spec
		}
		if spec == nil {
			continue
		}
		if op.Reads != nil || op.Let != nil || op.Require != nil || op.Select != nil || op.Wait != nil ||
			op.Effects != nil || op.List != nil || op.Batch != nil || op.Output != nil {
			problems = append(problems, fmt.Errorf(
				"operations.%s: %s is the whole operation and cannot sit beside reads, let, require, select, wait, effects, list, batch or output; spell the operation out in full instead", name, verb))
			continue
		}
		res, ok := s.Resources[spec.Resource]
		if !ok {
			problems = append(problems, fmt.Errorf("operations.%s.%s: unknown resource %q", name, verb, spec.Resource))
			continue
		}
		if (verb == "get") != (spec.Error != "") {
			if verb == "get" {
				problems = append(problems, fmt.Errorf("operations.%s.get: names no error to answer when the record is absent", name))
			} else {
				problems = append(problems, fmt.Errorf("operations.%s.%s: error is meaningless here; a %s has nothing to find", name, verb, verb))
			}
			continue
		}
		if verb != "delete" && spec.Missing != "" {
			problems = append(problems, fmt.Errorf("operations.%s.%s: missing is a delete's policy for an absent record and means nothing here", name, verb))
			continue
		}
		if verb == "delete" && spec.Wrap != "" {
			problems = append(problems, fmt.Errorf("operations.%s.delete: a delete answers nothing, so there is nothing to wrap", name))
			continue
		}
		if verb == "delete" {
			op.Effects = []Effect{{Delete: &DeleteEffect{Resource: spec.Resource, Key: spec.Key, Missing: spec.Missing}}}
			op.Get, op.Create, op.Put, op.Delete = nil, nil, nil, nil
			s.Operations[name] = op
			continue
		}

		out := map[string]string{}
		if spec.Wrap != "" {
			out[spec.Wrap] = "rec"
		} else {
			for member := range res.Record {
				out[member] = "rec." + member
			}
		}
		w := &WriteEffect{Resource: spec.Resource, Key: spec.Key}
		switch verb {
		case "get":
			op.Reads = map[string]Read{"rec": {Resource: spec.Resource, Key: spec.Key}}
			op.Require = []Require{{Cond: "rec_found", Error: spec.Error}}
		case "create":
			op.Effects = []Effect{{Create: w}}
		case "put":
			op.Effects = []Effect{{Put: w}}
		}
		op.Output = out
		op.Get, op.Create, op.Put, op.Delete = nil, nil, nil, nil
		s.Operations[name] = op
	}
	if len(problems) > 0 {
		return problems
	}
	return nil
}
