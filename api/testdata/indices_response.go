package testdata

type GetTemplateResponse struct {
	IndexTemplates []TemplateResponse `json:"index_templates,omitempty"`
}

type TemplateResponse struct {
	Name          string        `json:"name,omitempty"`
	IndexTemplate IndexTemplate `json:"index_template,omitempty"`
}

type IndexTemplate struct {
	IndexPatterns []string               `json:"index_patterns,omitempty"`
	Template      map[string]interface{} `json:"template,omitempty"`
}

type ResolveResponse struct {
	Aliases     []interface{} `json:"aliases,omitempty"`
	Indices     []interface{} `json:"indices,omitempty"`
	DataStreams []interface{} `json:"data_streams,omitempty"`
}

type RolloverResponse struct {
	Acknowledged       bool   `json:"acknowledged,omitempty"`
	ShardsAcknowledged bool   `json:"shards_acknowledged,omitempty"`
	OldIndex           string `json:"old_index,omitempty"`
	NewIndex           string `json:"new_index,omitempty"`
	RolledOver         bool   `json:"rolled_over,omitempty"`
	DryRun             bool   `json:"dry_run,omitempty"`
}
