package osdbaas

import (
	"context"
	"strings"

	"github.com/netcracker/qubership-core-lib-go/v3/configloader"
	dbaasbase "github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3"
	"github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3/cache"
	"github.com/netcracker/qubership-core-lib-go-dbaas-opensearch-client/v5/model"
	"github.com/opensearch-project/opensearch-go/v4"
)

type Database interface {
	GetOpensearchClient(config ...*opensearch.Config) (OpensearchClient, error)
	GetConnectionProperties(ctx context.Context) (*model.OpensearchConnProperties, error)
	FindConnectionProperties(ctx context.Context) (*model.OpensearchConnProperties, error)
}

type opensearchDatabase struct {
	dbaasPool *dbaasbase.DbaaSPool
	params    model.DbParams
	osCache   *cache.DbaaSCache
}

func (o opensearchDatabase) GetOpensearchClient(config ...*opensearch.Config) (OpensearchClient, error) {
	var osConfig *opensearch.Config
	if config != nil {
		osConfig = config[0]
	} else {
		osConfig = &opensearch.Config{}
	}
	return &OpensearchClientImpl{
		osClientImpl: &osClientImpl{
			dbaasClient:     o.dbaasPool.Client,
			opensearchCache: o.osCache,
			params:          o.params,
			osConfig:        osConfig,
		},
	}, nil
}

func (o opensearchDatabase) GetConnectionProperties(ctx context.Context) (*model.OpensearchConnProperties, error) {
	classifier := o.params.Classifier(ctx)

	osLogicalDb, err := o.dbaasPool.GetOrCreateDb(ctx, DB_TYPE, classifier, o.params.BaseDbParams)
	if err != nil {
		logger.Error("Error acquiring connection properties from DBaaS: %v", err)
		return nil, err
	}
	osConnectionProperties := toOsConnProperties(osLogicalDb.ConnectionProperties)
	return &osConnectionProperties, nil
}

func (o opensearchDatabase) FindConnectionProperties(ctx context.Context) (*model.OpensearchConnProperties, error) {
	classifier := o.params.Classifier(ctx)
	responseBody, err := o.dbaasPool.GetConnection(ctx, DB_TYPE, classifier, o.params.BaseDbParams)
	if err != nil {
		logger.ErrorC(ctx, "Error finding connection properties from DBaaS: %v", err)
		return nil, err
	}
	logger.Info("Found connection to pg db with classifier %+v", classifier)
	osConnProperties := toOsConnProperties(responseBody)
	return &osConnProperties, err
}

func toOsConnProperties(connProperties map[string]interface{}) model.OpensearchConnProperties {
	url := buildTlsUrl(connProperties)
	return model.OpensearchConnProperties{
		Url:            url,
		Username:       connProperties["username"].(string),
		Password:       connProperties["password"].(string),
		ResourcePrefix: connProperties["resourcePrefix"].(string),
	}
}

func buildTlsUrl(connProperties map[string]interface{}) string {
	url := connProperties["url"].(string)
	sslMode := configloader.GetOrDefaultString(dbaasOpensearchSSlProperty, sslModeAuto)
	isTlsSupported := false
	if tls, ok := connProperties["tls"].(bool); ok && tls {
		logger.Debug("TLS is enabled in opensearch adapter")
		isTlsSupported = true
	}
	if isNeededToSecure(sslMode, isTlsSupported) && !strings.Contains(url, "https") {
		logger.Warn("TLS is requested for opensearch, but URL is not secured. Will update protocol to HTTPS")
		url = strings.Replace(url, "http", "https", -1)
	}
	if sslMode == sslModeDisable && strings.Contains(url, "https") {
		logger.Warn("TLS for opensearch is disabled in client, but URL is secured. Will update protocol to HTTP")
		url = strings.Replace(url, "https", "http", -1)
	}
	return url
}

func isNeededToSecure(sslMode string, isTlsSupported bool) bool {
	return sslMode == sslModeEnable || (sslMode == sslModeAuto && isTlsSupported)
}
