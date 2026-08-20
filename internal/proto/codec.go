// Package proto defines wire codecs. Generated dispatch tables call into
// these; the codecs themselves are the thin-adapter exception to the
// no-hand-written-protocol rule.
package proto

import (
	"net/http"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// Codec decodes an HTTP request into a decoded input map and encodes a
// Response or Fault back onto the wire, per one protocol.
type Codec interface {
	Protocol() model.Protocol
	// Route identifies the operation from the raw request.
	Route(svc *model.Service, r *http.Request) (op *model.Operation, err error)
	Decode(svc *model.Service, op *model.Operation, r *http.Request) (*spi.Request, error)
	Encode(svc *model.Service, op *model.Operation, w http.ResponseWriter, resp *spi.Response) error
	EncodeFault(svc *model.Service, op *model.Operation, w http.ResponseWriter, f *spi.Fault, requestID string) error
}
