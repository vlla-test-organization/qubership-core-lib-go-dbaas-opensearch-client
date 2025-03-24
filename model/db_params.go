package model

import (
	"context"

	"github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3/model/rest"
)

// DbParams allows store parameters for database creation
type DbParams struct {
	// func which creates classifier out of context
	Classifier func(ctx context.Context) map[string]interface{}

	// Parameters for database customization
	BaseDbParams rest.BaseDbParams

	//dleimiter for database name
	Delimiter *string
}

type DelimiterStruct struct {
	Delimiter string
}
