// Package kafka is the MSK topic plane: the in-process message bus aws.firehose
// reads from. The cluster API is served by behavior/aws/kafka; this package
// holds only what no SPI operation reaches -- publishing a topic record and
// reading records back -- over the collections that bundle owns.
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// Topics publishes and reads MSK topic records.
type Topics struct{ deps spi.Deps }

// Message is a locally published MSK topic record. ClusterARN is stored so the
// bundle's DeleteCluster can remove a cluster's messages by predicate.
type Message struct {
	ClusterARN string `json:",omitempty"`
	Data       []byte
	Timestamp  time.Time
}

// New constructs the topic plane.
func New(d spi.Deps) *Topics { return &Topics{deps: d} }

func (t *Topics) col(identity spi.Identity, n string) spi.Collection {
	return t.deps.Store.Scope(identity.Account, identity.Region).Collection(n)
}

func (t *Topics) cluster(ctx context.Context, identity spi.Identity, clusterARN string) error {
	if _, ok, _ := t.col(identity, "msk").Get(ctx, clusterARN); !ok {
		return &spi.Fault{Code: "NotFoundException", HTTPStatus: 400, Fault: "client"}
	}
	return nil
}

// Publish stores a topic message and notifies in-process consumers.
func (t *Topics) Publish(ctx context.Context, identity spi.Identity, clusterARN, topic string, data []byte) error {
	if err := t.cluster(ctx, identity, clusterARN); err != nil {
		return err
	}
	message := Message{ClusterARN: clusterARN, Data: append([]byte(nil), data...), Timestamp: t.deps.Clock.Now()}
	encoded, _ := json.Marshal(message)
	key := clusterARN + "|" + topic + "|" + fmt.Sprintf("%020d-%s", message.Timestamp.UnixNano(), t.deps.Rand.Hex(8))
	if err := t.col(identity, "mskrecords").Put(ctx, key, encoded); err != nil {
		return err
	}
	if t.deps.Bus != nil {
		event, _ := json.Marshal(map[string]any{"Account": identity.Account, "Region": identity.Region, "ClusterARN": clusterARN, "Topic": topic, "Message": message})
		return t.deps.Bus.Publish(ctx, "kafka", event)
	}
	return nil
}

// Messages returns topic messages at or after the requested timestamp.
func (t *Topics) Messages(ctx context.Context, identity spi.Identity, clusterARN, topic string, from time.Time) ([]Message, error) {
	if err := t.cluster(ctx, identity, clusterARN); err != nil {
		return nil, err
	}
	items, _, err := t.col(identity, "mskrecords").List(ctx, clusterARN+"|"+topic+"|", "", 0)
	if err != nil {
		return nil, err
	}
	messages := make([]Message, 0, len(items))
	for _, item := range items {
		var message Message
		if json.Unmarshal(item.Value, &message) == nil && !message.Timestamp.Before(from) {
			messages = append(messages, message)
		}
	}
	return messages, nil
}
