package testdata

type ReindexResponse struct {
	Took    int `json:"took,omitempty"`
	Total   int `json:"total,omitempty"`
	Updated int `json:"updated,omitempty"`
	Created int `json:"created,omitempty"`
	Deleted int `json:"deleted,omitempty"`
}

type GetResponse struct {
	Index  string                 `json:"_index,omitempty"`
	Found  bool                   `json:"found,omitempty"`
	Source map[string]interface{} `json:"_source,omitempty"`
}

type OperationResponse struct {
	Index      string `json:"_index,omitempty"`
	DocumentId string `json:"_id,omitempty"`
	Version    int    `json:"_version,omitempty"`
	Result     string `json:"result,omitempty"`
}

type MgetResponse struct {
	Docs []GetResponse `json:"docs,omitempty"`
}
