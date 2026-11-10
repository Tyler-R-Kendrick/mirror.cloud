package specboot

// This file is the tree saying it holds every service. A build or a checkout
// of only some of them leaves it out, and then a wire name whose service is
// absent is skipped instead of being a panic. See specboot.go.
func init() { complete = true }
