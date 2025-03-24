package osdbaas

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"

	"github.com/netcracker/qubership-core-lib-go/v3/configloader"
	dbaasbase "github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3"
	"github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3/model"
	. "github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3/testutils"
	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

const (
	dbaasAgentUrlEnvName     = "dbaas.agent"
	keycloakUrlEnvName       = "identity.provider.url"
	namespaceEnvName         = "microservice.namespace"
	vaultAddressEnvName      = "vault.addr"
	testServiceName          = "service_test"
	propMicroserviceName     = "microservice.name"
	createDatabaseV3         = "/api/v3/dbaas/test_namespace/databases"
	getDatabaseV3            = "/api/v3/dbaas/test_namespace/databases/get-by-classifier/opensearch"
	username                 = "service_test"
	password                 = "qwerty127"

	testPrefix = "test"
)

type DatabaseTestSuite struct {
	suite.Suite
	database Database
}

func (suite *DatabaseTestSuite) SetupSuite() {
	StartMockServer()
	os.Setenv(dbaasAgentUrlEnvName, GetMockServerUrl())
	os.Setenv(keycloakUrlEnvName, GetMockServerUrl())
	os.Setenv(namespaceEnvName, "test_namespace")
	os.Setenv(propMicroserviceName, testServiceName)
	os.Setenv(vaultAddressEnvName, GetMockServerUrl())
	os.Setenv(dbaasOpensearchSSlProperty, sslModeEnable)

	yamlParams := configloader.YamlPropertySourceParams{ConfigFilePath: "testdata/application.yaml"}
	configloader.Init(configloader.BasePropertySources(yamlParams)...)
}

func (suite *DatabaseTestSuite) TearDownSuite() {
	os.Unsetenv(dbaasAgentUrlEnvName)
	os.Unsetenv(keycloakUrlEnvName)
	os.Unsetenv(namespaceEnvName)
	os.Unsetenv(propMicroserviceName)
	os.Unsetenv(vaultAddressEnvName)
	os.Unsetenv(dbaasOpensearchSSlProperty)
	StopMockServer()
}

func (suite *DatabaseTestSuite) BeforeTest(suiteName, testName string) {
	suite.T().Cleanup(ClearHandlers)
	dbaasPool := dbaasbase.NewDbaaSPool()
	client := NewClient(dbaasPool)
	suite.database = client.ServiceDatabase()
}

func (suite *DatabaseTestSuite) TestServiceDbaasOsClient_FindDbaaSOpensearchConnection() {
	AddHandler(Contains(getDatabaseV3), defaultDbaasResponseHandler)

	ctx := context.Background()
	actualResponse, err := suite.database.FindConnectionProperties(ctx)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), password, actualResponse.Password)
	assert.Equal(suite.T(), username, actualResponse.Username)
	assert.Equal(suite.T(), testPrefix, actualResponse.ResourcePrefix)
}

func (suite *DatabaseTestSuite) TestServiceDbaasOsClient_FindDbaaSOpensearchConnection_ConnectionNotFound() {
	yamlParams := configloader.YamlPropertySourceParams{ConfigFilePath: "testdata/application.yaml"}
	configloader.Init(configloader.BasePropertySources(yamlParams)...)
	AddHandler(Contains(getDatabaseV3), func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusNotFound)
	})
	ctx := context.Background()

	if _, err := suite.database.FindConnectionProperties(ctx); assert.Error(suite.T(), err) {
		assert.IsType(suite.T(), model.DbaaSCreateDbError{}, err)
		assert.Equal(suite.T(), 404, err.(model.DbaaSCreateDbError).HttpCode)
	}
}

func (suite *DatabaseTestSuite) TestServiceDbaasOsClient_GetDbaaSOpensearchConnection() {
	AddHandler(Contains(createDatabaseV3), defaultDbaasResponseHandler)
	ctx := context.Background()

	actualResponse, err := suite.database.GetConnectionProperties(ctx)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), password, actualResponse.Password)
	assert.Equal(suite.T(), username, actualResponse.Username)
}

func (suite *DatabaseTestSuite) TestServiceDbaasOsClient_GetDbaaSOpensearchTLSConnection() {
	AddHandler(Contains(createDatabaseV3), defaultDbaasResponseHandler)
	ctx := context.Background()

	actualResponse, err := suite.database.GetConnectionProperties(ctx)
	assert.Nil(suite.T(), err)
	assert.Contains(suite.T(), actualResponse.Url, "https")
}

func (suite *DatabaseTestSuite) TestServiceDbaasOsClient_GetDbaaSOpensearchConnection_ConnectionNotFound() {
	AddHandler(Contains(createDatabaseV3), func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusNotFound)
	})
	ctx := context.Background()

	if _, err := suite.database.GetConnectionProperties(ctx); assert.Error(suite.T(), err) {
		assert.IsType(suite.T(), model.DbaaSCreateDbError{}, err)
		assert.Equal(suite.T(), 404, err.(model.DbaaSCreateDbError).HttpCode)
	}
}

func (suite *DatabaseTestSuite) TestServiceDbaasOsClient_GetOsClient_WithoutOptions() {
	actualOsClient, err := suite.database.GetOpensearchClient()
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), actualOsClient)
}

func (suite *DatabaseTestSuite) TestServiceDbaasOsClient_GetOsClient_WithOptions() {
	options := &opensearch.Config{
		DisableRetry: true,
	}
	actualOsClient, err := suite.database.GetOpensearchClient(options)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), actualOsClient)
}

func TestSuite(t *testing.T) {
	suite.Run(t, new(DatabaseTestSuite))
}

func defaultDbaasResponseHandler(writer http.ResponseWriter, request *http.Request) {
	writer.WriteHeader(http.StatusOK)
	connectionProperties := map[string]interface{}{
		"password":       "qwerty127",
		"url":            "http://localhost:9200",
		"username":       "service_test",
		"resourcePrefix": testPrefix,
	}
	dbResponse := model.LogicalDb{
		Id:                   "123",
		ConnectionProperties: connectionProperties,
	}
	jsonResponse, e := json.Marshal(dbResponse)
	fmt.Println(e)
	writer.Write(jsonResponse)
}