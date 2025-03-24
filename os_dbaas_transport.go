package osdbaas

import (
	"context"
	"strings"

	"github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3/cache"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
)

type OpensearchClient interface {
	GetClient(ctx context.Context) (*opensearchapi.Client, error)
	GetPrefix(ctx context.Context) (string, error)
	Normalize(ctx context.Context, name string) (string, error)
}

type OpensearchClientImpl struct {
	osClientImpl *osClientImpl
}

func (this *OpensearchClientImpl) GetClient(ctx context.Context) (*opensearchapi.Client, error) {
	clientImpl := this.osClientImpl
	classifier := this.osClientImpl.params.Classifier(ctx)
	params := this.osClientImpl.params.BaseDbParams
	key := cache.NewKey(DB_TYPE, classifier)
	conn, err := clientImpl.opensearchCache.Cache(key, clientImpl.createNewOsClient(ctx, classifier))
	if err != nil {
		logger.ErrorC(ctx, "Can't get transport client")
		return nil, err
	}
	osClient := conn.(*osCache)
	if valid, err := clientImpl.isPasswordValid(osClient); !valid && err == nil {
		logger.Info("authentication error, try to get new password")
		password, err := clientImpl.getNewPassword(ctx, classifier, params)
		if err != nil {
			return nil, err
		}
		clientImpl.osConfig.Password = password
		osClient.client, err = opensearchapi.NewClient(
			opensearchapi.Config{
				Client: *clientImpl.osConfig,
			})
		if err != nil {
			return nil, err
		}
		logger.Info("db password updated successfully")
	} else if err != nil {
		return nil, err
	}

	return osClient.client, nil
}

func (this *OpensearchClientImpl) GetPrefix(ctx context.Context) (string, error) {
	clientImpl := this.osClientImpl
	classifier := clientImpl.params.Classifier(ctx)
	key := cache.NewKey(DB_TYPE, classifier)
	conn, err := clientImpl.opensearchCache.Cache(key, clientImpl.createNewOsClient(ctx, classifier))
	if err != nil {
		logger.ErrorC(ctx, "Can't get resource prefix")
		return "", err
	}
	osClient := conn.(*osCache)
	return osClient.connectionProperties["resourcePrefix"].(string), nil
}

func (this *OpensearchClientImpl) Normalize(ctx context.Context, name string) (string, error) {
	prefix, err := this.GetPrefix(ctx)
	if err != nil {
		return "", err
	}
	if strings.HasPrefix(name, prefix+*this.osClientImpl.params.Delimiter) {
		return name, nil
	}
	return prefix + *this.osClientImpl.params.Delimiter + name, nil
}
