package model

// OpensearchConnProperties is used for storing connection properties for database
type OpensearchConnProperties struct {
	Url            string
	Username       string
	Password       string
	ResourcePrefix string
}
