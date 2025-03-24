package osdbaas

import (
	"context"

	dbaasbase "github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3"
	"github.com/netcracker/qubership-core-lib-go-dbaas-opensearch-client/v5/model"
	"github.com/stretchr/testify/assert"
)

func (suite *DatabaseTestSuite) TestOsClient_Normalize_error() {
	dbaasPool := dbaasbase.NewDbaaSPool()

	osClient, _ := NewClient(dbaasPool).ServiceDatabase(model.DbParams{}).GetOpensearchClient()

	_, err := osClient.Normalize(context.Background(), "any_name")
	assert.NotNil(suite.T(), err)
}
