package osdbaas

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	dbaasbase "github.com/vlla-test-organization/qubership-core-lib-go-dbaas-base-client/v3"
	"github.com/vlla-test-organization/qubership-core-lib-go-dbaas-base-client/v3/cache"
	"github.com/vlla-test-organization/qubership-core-lib-go-dbaas-base-client/v3/model/rest"
	"github.com/vlla-test-organization/qubership-core-lib-go-dbaas-opensearch-client/v5/model"
	"github.com/vlla-test-organization/qubership-core-lib-go/v3/utils"
)

const (
	DB_TYPE            = "opensearch"
	statusUnauthorized = 401
	statusForbidden    = 403
	statusOk           = 200
	statusNotFound     = 404
)

// osClientImpl stores cache of databases and params for databases creation
type osClientImpl struct {
	dbaasClient     dbaasbase.DbaaSClient
	opensearchCache *cache.DbaaSCache
	params          model.DbParams
	osConfig        *opensearch.Config
}

type osCache struct {
	client               *opensearchapi.Client
	connectionProperties map[string]interface{}
}

func (o *osClientImpl) createNewOsClient(ctx context.Context, classifier map[string]interface{}) func() (interface{}, error) {
	return func() (interface{}, error) {
		logger.Debug("Create opensearch database with classifier %+v", classifier)
		o.params.BaseDbParams.Settings = o.updateSettings(o.params.BaseDbParams.Settings)
		o.params.BaseDbParams.NamePrefix = updateNamePrefix(classifier, o.params.BaseDbParams.NamePrefix)
		logicalDb, err := o.dbaasClient.GetOrCreateDb(ctx, DB_TYPE, classifier, o.params.BaseDbParams)
		if err != nil {
			return nil, err
		}
		connectionProperties := logicalDb.ConnectionProperties
		url := connectionProperties["url"].(string)

		o.osConfig.Addresses = []string{url}
		o.osConfig.Username = connectionProperties["username"].(string)
		o.osConfig.Password = connectionProperties["password"].(string)
		sslTransportClient, err := buldSSLTransport(connectionProperties)
		if err != nil {
			return nil, err
		}
		if sslTransportClient != nil {
			logger.Debugf("Setting secured transport client")
			o.osConfig.Transport = sslTransportClient
		}
		client, err := opensearchapi.NewClient(
			opensearchapi.Config{
				Client: *o.osConfig,
			})
		if err != nil {
			return nil, err
		}
		logger.Debug("Build go-os client for database with classifier %+v and type %s", classifier, DB_TYPE)
		osClient := &osCache{
			client:               client,
			connectionProperties: connectionProperties,
		}
		return osClient, nil
	}
}

func buldSSLTransport(connectionProperties map[string]interface{}) (*http.Transport, error) {
	if tls, ok := connectionProperties["tls"].(bool); ok && tls {
		httpTransport, ok := http.DefaultTransport.(*http.Transport)
		if !ok {
			return nil, errors.New("an error occurred during building http.Transport")
		}
		logger.Infof("Connection to opensearch will be secured")
		transportClone := httpTransport.Clone()
		transportClone.TLSClientConfig = utils.GetTlsConfig()
		return transportClone, nil
	}
	return nil, nil
}

func (o *osClientImpl) updateSettings(settings map[string]interface{}) map[string]interface{} {
	if settings == nil {
		settings = make(map[string]interface{})
	}
	settings["createOnly"] = []string{"user"}
	settings["resourcePrefix"] = true
	return settings
}

func (o *osClientImpl) getNewPassword(ctx context.Context, classifier map[string]interface{}, params rest.BaseDbParams) (string, error) {
	newConnection, err := o.dbaasClient.GetConnection(ctx, DB_TYPE, classifier, params)
	if err != nil {
		logger.ErrorC(ctx, "Can't update connection with dbaas")
		return "", err
	}
	if newPassword, ok := newConnection["password"]; ok {
		return newPassword.(string), nil
	}
	return "", errors.New("connection string doesn't contain password field")
}

func (o *osClientImpl) isPasswordValid(client *osCache) (bool, error) {
	prefix := client.connectionProperties["resourcePrefix"].(string)
	indexName := prefix + "_os_index"
	existsReq := opensearchapi.DocumentExistsReq{
		Index:      indexName,
		DocumentID: "0",
	}
	resp, err := client.client.Document.Exists(
		context.Background(),
		existsReq,
	)
	if resp != nil {
		if resp.StatusCode == statusOk || resp.StatusCode == statusNotFound {
			return true, nil
		}
		if resp.StatusCode == statusForbidden || resp.StatusCode == statusUnauthorized {
			return false, nil
		}
	}
	logger.Errorf("Error during ping request %+v", err.Error())
	return false, err
}

func updateNamePrefix(classifier map[string]interface{}, prefix string) string {
	if strings.Contains(prefix, "{tenantId}") && classifier["scope"] == "tenant" {
		return strings.Replace(prefix, "{tenantId}", fmt.Sprintf("%v", classifier["tenantId"]), 1)
	} else {
		return prefix
	}

}
