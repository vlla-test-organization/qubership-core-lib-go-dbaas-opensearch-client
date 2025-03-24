package testdata

type GetResponse struct {
	Index  string                 `json:"_index,omitempty"`
	Found  bool                   `json:"found,omitempty"`
	Source map[string]interface{} `json:"_source,omitempty"`
}
