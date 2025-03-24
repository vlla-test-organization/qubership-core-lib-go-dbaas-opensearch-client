package testdata

type AcknowledgedResponse struct {
	Acknowledged       bool   `json:"acknowledged,omitempty"`
	ShardsAcknowledged bool   `json:"shards_acknowledged,omitempty"`
	Index              string `json:"index,omitempty"`
}
