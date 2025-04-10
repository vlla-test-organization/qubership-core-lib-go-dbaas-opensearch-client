package osdbaas

import (
	"context"
	"os"
	"testing"

	"github.com/netcracker/qubership-core-lib-go/v3/configloader"
	"github.com/netcracker/qubership-core-lib-go/v3/serviceloader"
	"github.com/netcracker/qubership-core-lib-go/v3/security"
	"github.com/netcracker/qubership-core-lib-go/v3/context-propagation/baseproviders/tenant"
	"github.com/netcracker/qubership-core-lib-go/v3/context-propagation/ctxmanager"
	dbaasbase "github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3"
	"github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3/model/rest"
	"github.com/netcracker/qubership-core-lib-go-dbaas-opensearch-client/v5/model"
	"github.com/stretchr/testify/assert"
)

func init() {
	serviceloader.Register(1, &security.DummyToken{})
	serviceloader.Register(1, &security.TenantContextObject{})
	ctxmanager.Register([]ctxmanager.ContextProvider{tenant.TenantProvider{}})
}

func setup() {
	os.Setenv(propMicroserviceName, "test_service")
	os.Setenv(namespaceEnvName, "test_space")
	configloader.InitWithSourcesArray([]*configloader.PropertySource{configloader.EnvPropertySource()})
}

func tearDown() {
	os.Unsetenv(propMicroserviceName)
	os.Unsetenv(namespaceEnvName)
}

func TestNewServiceDbaasClient_WithoutParams(t *testing.T) {
	setup()
	defer tearDown()
	dbaasPool := dbaasbase.NewDbaaSPool()
	commonClient := NewClient(dbaasPool)
	serviceDB := commonClient.ServiceDatabase()
	assert.NotNil(t, serviceDB)
	db := serviceDB.(*opensearchDatabase)
	ctx := context.Background()
	assert.Equal(t, ServiceClassifier(ctx), db.params.Classifier(ctx))
}

func TestNewServiceDbaasClient_WithParams(t *testing.T) {
	setup()
	defer tearDown()
	dbaasPool := dbaasbase.NewDbaaSPool()
	commonClient := NewClient(dbaasPool)
	params := model.DbParams{
		Classifier:   stubClassifier,
		BaseDbParams: rest.BaseDbParams{},
	}
	serviceDB := commonClient.ServiceDatabase(params)
	assert.NotNil(t, serviceDB)
	db := serviceDB.(*opensearchDatabase)
	ctx := context.Background()
	assert.Equal(t, stubClassifier(ctx), db.params.Classifier(ctx))
}

func TestNewTenantDbaasClient_WithoutParams(t *testing.T) {
	setup()
	defer tearDown()
	dbaasPool := dbaasbase.NewDbaaSPool()
	commonClient := NewClient(dbaasPool)
	tenantDb := commonClient.TenantDatabase()
	assert.NotNil(t, tenantDb)
	db := tenantDb.(*opensearchDatabase)
	ctx := createTenantContext()
	assert.Equal(t, TenantClassifier(ctx), db.params.Classifier(ctx))
}

func TestNewTenantDbaasClient_WithParams(t *testing.T) {
	setup()
	defer tearDown()
	dbaasPool := dbaasbase.NewDbaaSPool()
	commonClient := NewClient(dbaasPool)
	params := model.DbParams{
		Classifier:   stubClassifier,
		BaseDbParams: rest.BaseDbParams{},
	}
	tenantDb := commonClient.TenantDatabase(params)
	assert.NotNil(t, tenantDb)
	db := tenantDb.(*opensearchDatabase)
	ctx := context.Background()
	assert.Equal(t, stubClassifier(ctx), db.params.Classifier(ctx))
}

func TestCreateServiceClassifier(t *testing.T) {
	setup()
	defer tearDown()
	expected := map[string]interface{}{
		"microserviceName": "test_service",
		"scope":            "service",
		"namespace":        "test_space",
	}
	actual := ServiceClassifier(context.Background())
	assert.Equal(t, expected, actual)
}

func TestCreateTenantClassifier(t *testing.T) {
	setup()
	defer tearDown()
	ctx := createTenantContext()
	expected := map[string]interface{}{
		"microserviceName": "test_service",
		"tenantId":         "id",
		"namespace":        "test_space",
		"scope":            "tenant",
	}
	actual := TenantClassifier(ctx)
	assert.Equal(t, expected, actual)
}

func TestCreateTenantClassifier_WithoutTenantId(t *testing.T) {
	setup()
	defer tearDown()
	ctx := context.Background()

	assert.Panics(t, func() {
		TenantClassifier(ctx)
	})
}

func stubClassifier(ctx context.Context) map[string]interface{} {
	return map[string]interface{}{
		"scope":            "service",
		"microserviceName": "service_test",
	}
}

func createTenantContext() context.Context {
	incomingHeaders := map[string]interface{}{tenant.TenantHeader: "id"}
	return ctxmanager.InitContext(context.Background(), incomingHeaders)
}
