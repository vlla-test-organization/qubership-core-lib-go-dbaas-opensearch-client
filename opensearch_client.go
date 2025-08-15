package osdbaas

import (
	"context"

	dbaasbase "github.com/vlla-test-organization/qubership-core-lib-go-dbaas-base-client/v3"
	"github.com/vlla-test-organization/qubership-core-lib-go-dbaas-base-client/v3/cache"
	"github.com/vlla-test-organization/qubership-core-lib-go-dbaas-opensearch-client/v5/model"
	"github.com/vlla-test-organization/qubership-core-lib-go/v3/logging"
)

var logger logging.Logger

func init() {
	logger = logging.GetLogger("osdbaas")
}

type DbaaSOpensearchClient struct {
	osClientCache cache.DbaaSCache
	pool          *dbaasbase.DbaaSPool
}

func NewClient(pool *dbaasbase.DbaaSPool) *DbaaSOpensearchClient {
	localCache := cache.DbaaSCache{
		LogicalDbCache: make(map[cache.Key]interface{}),
	}
	return &DbaaSOpensearchClient{
		osClientCache: localCache,
		pool:          pool,
	}
}

func (d *DbaaSOpensearchClient) ServiceDatabase(params ...model.DbParams) Database {
	return &opensearchDatabase{
		params:    d.buildServiceDbParams(params),
		dbaasPool: d.pool,
		osCache:   &d.osClientCache,
	}
}

func (d *DbaaSOpensearchClient) buildServiceDbParams(params []model.DbParams) model.DbParams {
	localParams := model.DbParams{}
	if params != nil {
		localParams = params[0]
	}
	if localParams.Classifier == nil {
		localParams.Classifier = ServiceClassifier
	}
	setDelimiter(&localParams)
	return localParams
}

func (d *DbaaSOpensearchClient) TenantDatabase(params ...model.DbParams) Database {
	return &opensearchDatabase{
		params:    d.buildTenantDbParams(params),
		dbaasPool: d.pool,
		osCache:   &d.osClientCache,
	}
}

func (d *DbaaSOpensearchClient) buildTenantDbParams(params []model.DbParams) model.DbParams {
	localParams := model.DbParams{}
	if params != nil {
		localParams = params[0]
	}
	if localParams.Classifier == nil {
		localParams.Classifier = TenantClassifier
	}
	setDelimiter(&localParams)
	return localParams
}

func ServiceClassifier(ctx context.Context) map[string]interface{} {
	return dbaasbase.BaseServiceClassifier(ctx)
}

func TenantClassifier(ctx context.Context) map[string]interface{} {
	return dbaasbase.BaseTenantClassifier(ctx)
}

func setDelimiter(localParams *model.DbParams) {
	if localParams.Delimiter == nil {
		defaultDelimiter := "_"
		localParams.Delimiter = &defaultDelimiter
	}
}
