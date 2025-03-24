package testdata

type SearchResponse struct {
	ScrollId string `json:"_scroll_id,omitempty"`
	Hits     Hits   `json:"hits,omitempty"`
}

type Hits struct {
	Total map[string]interface{} `json:"total,omitempty"`
	Hits  []InternalHits         `json:"hits,omitempty"`
}

type InternalHits struct {
	Index  string                 `json:"_index,omitempty"`
	Id     string                 `json:"_id,omitempty"`
	Fields map[string]interface{} `json:"fields,omitempty"`
}

type MSearchResponse struct {
	Responses []Response `json:"responses,omitempty"`
}

type Response struct {
	Hits   Hits `json:"hits,omitempty"`
	Status int  `json:"status,omitempty"`
}
