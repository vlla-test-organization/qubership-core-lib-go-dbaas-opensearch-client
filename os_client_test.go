package osdbaas

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/netcracker/qubership-core-lib-go/v3/serviceloader"
	"github.com/netcracker/qubership-core-lib-go/v3/security"
	"github.com/netcracker/qubership-core-lib-go/v3/context-propagation/baseproviders/tenant"
	dbaasbase "github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3"
	"github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3/cache"
	basemodel "github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3/model"
	"github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3/model/rest"
	. "github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3/testutils"
	"github.com/netcracker/qubership-core-lib-go-dbaas-opensearch-client/v5/api/testdata"
	. "github.com/netcracker/qubership-core-lib-go-dbaas-opensearch-client/v5/model"
	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	opensearchPort  = "9200"
	opensearchImage = "opensearchproject/opensearch:2.11.1"

	adminPassword = "admin"
	wrongPassword = "admin123"

	statusCreated = 201
)

func init() {
	serviceloader.Register(1, &security.DummyToken{})
	serviceloader.Register(1, &security.TenantContextObject{})
}

func (suite *DatabaseTestSuite) TestOsClient_GetConnection_ConnectionError() {
	ctx := context.Background()

	AddHandler(Contains(createDatabaseV3), func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
		jsonString := osDbaasResponseHandler("localhost:65000", adminPassword)
		writer.Write(jsonString)
	})

	params := DbParams{Classifier: ServiceClassifier, BaseDbParams: rest.BaseDbParams{}}
	osClient := &osClientImpl{
		osConfig:        &opensearch.Config{},
		params:          params,
		opensearchCache: &cache.DbaaSCache{LogicalDbCache: make(map[cache.Key]interface{})},
		dbaasClient:     dbaasbase.NewDbaasClient(),
	}

	impl := &OpensearchClientImpl{
		osClientImpl: osClient,
	}

	transport, err := impl.GetClient(ctx)
	assert.NotNil(suite.T(), err)
	assert.Nil(suite.T(), transport)
}

func (suite *DatabaseTestSuite) TestOsClient_UpdatePrefix_ServiceClassifier() {
	classifier := ServiceClassifier(context.Background())

	prefix := "a-{tenantId}-test"
	updatedPrefix := updateNamePrefix(classifier, prefix)
	assert.Equal(suite.T(), "a-{tenantId}-test", updatedPrefix)
}

func (suite *DatabaseTestSuite) TestOsClient_UpdatePrefix_TenantClassifier() {
	ctx := context.WithValue(context.Background(), "Tenant-Context", tenant.NewTenantContextObject("123"))
	classifier := TenantClassifier(ctx)

	prefix := "a-{tenantId}-test"
	updatedPrefix := updateNamePrefix(classifier, prefix)
	assert.Equal(suite.T(), "a-123-test", updatedPrefix)
}

func (suite *DatabaseTestSuite) TestOsClient_GetConnection_NewClient() {
	suite.T().Skip()
	ctx := context.Background()
	commonIdentifier := "os-test-container-network"
	network := prepareNetwork(suite.T(), ctx, commonIdentifier)
	port, osContainer := prepareTestContainer(suite.T(), ctx, commonIdentifier)
	defer func() {
		err := osContainer.Terminate(ctx)
		if err != nil {
			suite.T().Fatal(err)
		}
		err = network.Remove(ctx)
		if err != nil {
			suite.T().Fatal(err)
		}
	}()
	addr, err := osContainer.PortEndpoint(ctx, port, "")
	if err != nil {
		suite.T().Fatal(err)
	}

	AddHandler(Contains(createDatabaseV3), func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
		jsonString := osDbaasResponseHandler(addr, adminPassword)
		writer.Write(jsonString)
	})

	params := DbParams{Classifier: ServiceClassifier, BaseDbParams: rest.BaseDbParams{}}
	config := opensearch.Config{}
	config.Transport = &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}
	osClient := osClientImpl{
		osConfig:        &config,
		params:          params,
		opensearchCache: &cache.DbaaSCache{LogicalDbCache: make(map[cache.Key]interface{})},
		dbaasClient:     dbaasbase.NewDbaasClient(),
	}
	time.Sleep(3 * time.Second)
	impl := &OpensearchClientImpl{
		osClientImpl: &osClient,
	}

	// check that connection allows storing and getting info from db
	suite.checkConnectionIsWorking(impl, ctx)
}

func (suite *DatabaseTestSuite) TestOsClient_GetConnection_UpdatePassword() {
	suite.T().Skip()
	ctx := context.Background()
	commonIdentifier := "os-test-container-network"
	network := prepareNetwork(suite.T(), ctx, commonIdentifier)
	port, osContainer := prepareTestContainer(suite.T(), ctx, commonIdentifier)
	defer func() {
		err := osContainer.Terminate(ctx)
		if err != nil {
			suite.T().Fatal(err)
		}
		err = network.Remove(ctx)
		if err != nil {
			suite.T().Fatal(err)
		}
	}()
	addr, err := osContainer.PortEndpoint(ctx, port, "")
	if err != nil {
		suite.T().Fatal(err)
	}

	// create database with wrong password
	AddHandler(matches(createDatabaseV3), func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
		jsonString := osDbaasResponseHandler(addr, wrongPassword)
		writer.Write(jsonString)
	})
	// update right password
	AddHandler(matches(getDatabaseV3), func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
		jsonString := osDbaasResponseHandler(addr, adminPassword)
		writer.Write(jsonString)
	})
	AddHandler(matches(getDatabaseV3), func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusNotFound)
	})
	config := &opensearch.Config{}
	config.Transport = &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}
	params := DbParams{Classifier: ServiceClassifier, BaseDbParams: rest.BaseDbParams{}}
	osClient := &osClientImpl{
		osConfig:        config,
		params:          params,
		opensearchCache: &cache.DbaaSCache{LogicalDbCache: make(map[cache.Key]interface{})},
		dbaasClient:     dbaasbase.NewDbaasClient(),
	}
	time.Sleep(3 * time.Second)
	impl := &OpensearchClientImpl{
		osClientImpl: osClient,
	}

	// check that connection allows storing and getting info from db
	suite.checkConnectionIsWorking(impl, ctx)
}

func matches(submatch string) func(string) bool {
	return func(path string) bool {
		return strings.EqualFold(path, submatch)
	}
}

func osDbaasResponseHandler(address string, password string) []byte {
	url := fmt.Sprintf("https://%s", address)
	connectionProperties := map[string]interface{}{
		"username":       "admin",
		"password":       password,
		"url":            url,
		"resourcePrefix": testPrefix,
	}
	dbResponse := basemodel.LogicalDb{
		Id:                   "123",
		ConnectionProperties: connectionProperties,
	}
	jsonResponse, _ := json.Marshal(dbResponse)
	return jsonResponse
}

func prepareTestContainer(t *testing.T, ctx context.Context, identifier string) (nat.Port, testcontainers.Container) {
	os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")

	env := map[string]string{
		"discovery.type":               "single-node",
		"DISABLE_INSTALL_DEMO_CONFIG":  "true",
		"network.host":                 "0.0.0.0",
		"TESTCONTAINERS_RYUK_DISABLED": "true",
	}
	absPathAdmin, err := filepath.Abs("./testdata/admin.pem")
	absPathAdminKey, err := filepath.Abs("./testdata/admin-key.pem")
	absPathAdminRoot, err := filepath.Abs("./testdata/admin-root-ca.pem")
	absPathTransport, err := filepath.Abs("./testdata/transport-crt.pem")
	absPathTransportKey, err := filepath.Abs("./testdata/transport-key.pem")
	absPathTransportRoot, err := filepath.Abs("./testdata/transport-root-ca.pem")
	absPathCustomOs, err := filepath.Abs("./testdata/custom-opensearch.yml")
	port, _ := nat.NewPort("tcp", opensearchPort)
	req := testcontainers.ContainerRequest{
		Image:        opensearchImage,
		ExposedPorts: []string{port.Port()},
		Env:          env,
		Mounts: testcontainers.Mounts(
			testcontainers.BindMount(absPathAdmin, "/usr/share/opensearch/config/admin.pem"),
			testcontainers.BindMount(absPathAdminKey, "/usr/share/opensearch/config/admin-key.pem"),
			testcontainers.BindMount(absPathAdminRoot, "/usr/share/opensearch/config/admin-root-ca.pem"),
			testcontainers.BindMount(absPathTransport, "/usr/share/opensearch/config/transport-crt.pem"),
			testcontainers.BindMount(absPathTransportKey, "/usr/share/opensearch/config/transport-key.pem"),
			testcontainers.BindMount(absPathTransportRoot, "/usr/share/opensearch/config/transport-root-ca.pem"),
			testcontainers.BindMount(absPathCustomOs, "/usr/share/opensearch/config/opensearch.yml"),
		),
		Networks:    []string{identifier},
		NetworkMode: "bridge",
		WaitingFor:  wait.ForListeningPort(port).WithStartupTimeout(120 * time.Second),
	}
	osContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Error(err)
	}

	os.Unsetenv("TESTCONTAINERS_RYUK_DISABLED")

	return port, osContainer
}

func prepareNetwork(t *testing.T, ctx context.Context, commonIdentifier string) testcontainers.Network {
	network, networkErr := testcontainers.GenericNetwork(ctx, testcontainers.GenericNetworkRequest{
		NetworkRequest: testcontainers.NetworkRequest{
			CheckDuplicate: false,
			Name:           commonIdentifier,
			Driver:         "bridge",
			SkipReaper:     true,
		},
	})
	if networkErr != nil {
		t.Error(networkErr)
	}
	return network
}

func (suite *DatabaseTestSuite) checkConnectionIsWorking(client OpensearchClient, ctx context.Context) {
	prefix, err := client.GetPrefix(ctx)
	assert.Nil(suite.T(), err)
	indexName := "database_test"
	docId := "1"
	document := suite.createSampleDocument()
	req := opensearchapi.DocumentCreateReq{
		Index:      prefix + "_" + indexName,
		DocumentID: docId,
		Body:       document,
	}
	transport, err := client.GetClient(ctx)
	assert.Nil(suite.T(), err)
	createResponse, err := transport.Document.Create(ctx, req)
	rawResp := createResponse.Inspect().Response
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), createResponse)
	assert.Equal(suite.T(), statusCreated, rawResp.StatusCode)
	time.Sleep(time.Second)

	getReq := opensearchapi.DocumentGetReq{
		Index:      prefix + "_" + indexName,
		DocumentID: docId,
	}
	getResponse, err := transport.Document.Get(ctx, getReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), getResponse)
	getData, err := ioutil.ReadAll(getResponse.Inspect().Response.Body)
	assert.Nil(suite.T(), err)
	var getParsedBody testdata.GetResponse
	err = json.Unmarshal(getData, &getParsedBody)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), statusOk, getResponse.Inspect().Response.StatusCode)
	assert.True(suite.T(), getParsedBody.Found)

	deleteReq := opensearchapi.DocumentDeleteReq{
		Index:      prefix + "_" + indexName,
		DocumentID: docId,
	}
	deleteResponse, err := transport.Document.Delete(ctx, deleteReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), deleteResponse)
	assert.Equal(suite.T(), statusOk, deleteResponse.Inspect().Response.StatusCode)
}

func (suite *DatabaseTestSuite) createSampleDocument() *strings.Reader {
	return strings.NewReader(`{
        "title": "Moneyball",
        "director": "Bennett Miller",
        "year": 2011
    }`)
}
