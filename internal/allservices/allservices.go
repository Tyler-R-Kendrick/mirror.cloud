// Package allservices imports every registered service so a binary that links
// it serves the full surface.
//
// Two kinds of service land in the registry and this is the one place that
// knows both: the hand-written packs under internal/services, one file each
// (svc_<provider>_<service>.go, so that a sparse checkout without a service
// leaves out its file and nothing else; scripts/sparse.py), and the
// data-defined services in internal/bundled, which registers one engine per
// Behavior IR bundle under behavior/. Extraction moves a service from the
// first kind to the second, and the only edit is deleting its file here.
//
// Keeping the list in a package rather than in main means the binary and the
// tests that assert over the registered set cannot disagree about which
// services exist.
package allservices

import (
	// Data-defined services. This import is not a list: it registers every
	// bundle under behavior/, so adding a service adds no Go anywhere.
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"

	// CI/CD AfterInvoke executors (gated by MIRROR_CICD_EXECUTE=1).
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/register"
)
