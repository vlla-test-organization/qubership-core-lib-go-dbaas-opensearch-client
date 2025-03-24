package osapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/netcracker/qubership-core-lib-go/v3/configloader"
	dbaasbase "github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3"
	. "github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3/model"
	. "github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3/testutils"
	osdbaas "github.com/netcracker/qubership-core-lib-go-dbaas-opensearch-client/v5"
	"github.com/netcracker/qubership-core-lib-go-dbaas-opensearch-client/v5/api/testdata"
	"github.com/netcracker/qubership-core-lib-go-dbaas-opensearch-client/v5/model"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	opensearchPort  = "9200"
	opensearchImage = "opensearchproject/opensearch:2.11.1"
)

const (
	dbaasAgentUrlEnvName     = "dbaas.agent"
	keycloakUrlEnvName       = "identity.provider.url"
	namespaceEnvName         = "microservice.namespace"
	vaultAddressEnvName      = "vault.addr"
	testServiceName          = "service_test"
	propMicroserviceName     = "microservice.name"
	createDatabaseV3         = "/api/v3/dbaas/test_namespace/databases"
	testToken                = "test-token"
	testTokenExpiresIn       = 300
	testPrefix               = "test"
	testDelimiter            = "_"
	index                    = "_index"
)

const (
	statusCreated  = 201
	statusOk       = 200
	statusNotFound = 404
)

type OsAPITestSuite struct {
	suite.Suite
	ctx         context.Context
	osContainer testcontainers.Container
	database    osdbaas.Database
	addr        string
	network     testcontainers.Network
}

func (suite *OsAPITestSuite) SetupSuite() {
	StartMockServer()

	os.Setenv(dbaasAgentUrlEnvName, GetMockServerUrl())
	os.Setenv(keycloakUrlEnvName, GetMockServerUrl())
	os.Setenv(namespaceEnvName, "test_namespace")
	os.Setenv(propMicroserviceName, testServiceName)
	os.Setenv(vaultAddressEnvName, GetMockServerUrl())

	yamlParams := configloader.YamlPropertySourceParams{ConfigFilePath: "testdata/application.yaml"}
	configloader.Init(configloader.BasePropertySources(yamlParams)...)

	suite.ctx = context.Background()
	commonIdentifier := "os-test-container-network"
	var port nat.Port
	port, suite.osContainer = prepareTestContainer(suite.T(), suite.ctx, commonIdentifier)

	dbaasPool := dbaasbase.NewDbaaSPool()
	client := osdbaas.NewClient(dbaasPool)
	params := model.DbParams{}
	params.BaseDbParams.NamePrefix = "ga"
	suite.database = client.ServiceDatabase(params)

	var err error
	suite.addr, err = suite.osContainer.PortEndpoint(suite.ctx, port, "")
	if err != nil {
		suite.T().Fatal(err)
	}
}

func (suite *OsAPITestSuite) TearDownSuite() {
	defer func() {
		err := suite.osContainer.Terminate(suite.ctx)
		if err != nil {
			suite.T().Fatal(err)
		}
	}()
	os.Unsetenv(dbaasAgentUrlEnvName)
	os.Unsetenv(keycloakUrlEnvName)
	os.Unsetenv(namespaceEnvName)
	os.Unsetenv(propMicroserviceName)
	os.Unsetenv(vaultAddressEnvName)
	StopMockServer()
}

func (suite *OsAPITestSuite) BeforeTest(suiteName, testName string) {
	AddHandler(Contains(createDatabaseV3), func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
		jsonString := osDbaasResponseHandler(suite.addr)
		writer.Write(jsonString)
	})
}

func (suite *OsAPITestSuite) Test_AddDocumentAndGetIt() {
	osClient, _ := suite.database.GetOpensearchClient()

	indexName := "get_doc_index"
	normalizeName, err2 := osClient.Normalize(suite.ctx, indexName)
	assert.Nil(suite.T(), err2)
	docId := "1"
	suite.upsertDocument(normalizeName, docId, osClient)

	getReq := opensearchapi.DocumentGetReq{
		Index:      normalizeName,
		DocumentID: docId,
	}
	ctx := suite.ctx
	client, _ := osClient.GetClient(ctx)
	getResponse, err := client.Document.Get(ctx, getReq)
	assert.Nil(suite.T(), err)
	suite.checkGetResponse(getResponse.Inspect(), true)

	getSourceReq := opensearchapi.DocumentSourceReq{Index: normalizeName, DocumentID: docId}

	getSourceResponse, err := client.Document.Source(ctx, getSourceReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), getSourceResponse.Inspect().Response.Body)
	assert.Equal(suite.T(), statusOk, getSourceResponse.Inspect().Response.StatusCode)
}

func (suite *OsAPITestSuite) Test_BulkOperations() {
	osClient, _ := suite.database.GetOpensearchClient()

	normalizeName, err2 := osClient.Normalize(suite.ctx, "movies")
	print(normalizeName)
	assert.Nil(suite.T(), err2)
	s := `{"create":{"_id":"1","_index":"` + normalizeName + `"}}
{"title":"Prisoners","year":2013}
{"create":{"_id":"2","_index":"` + normalizeName + `"}}
{"title":"Prisoners2","year":2016}
`
	bulkBody := strings.NewReader(s)
	ctx := suite.ctx
	client, _ := osClient.GetClient(ctx)
	req := opensearchapi.BulkReq{Body: bulkBody}
	bulkResponse, err := client.Bulk(ctx, req)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), bulkResponse.Inspect().Response.Body)
	assert.Equal(suite.T(), 200, bulkResponse.Inspect().Response.StatusCode)

	getReq := opensearchapi.DocumentGetReq{
		Index:      "test_movies",
		DocumentID: "2",
	}
	getResponse, err := client.Document.Get(ctx, getReq)
	suite.checkGetResponse(getResponse.Inspect(), true)
}

func (suite *OsAPITestSuite) Test_CreateIndex() {
	osClient, _ := suite.database.GetOpensearchClient()

	indexName := "create_index"
	createBody := strings.NewReader(`
		{
		  "aliases": {
			"alias_test_1": {},
			"alias_test_2": {}
		  }
		}
	`)

	client, _ := osClient.GetClient(suite.ctx)
	req := opensearchapi.IndicesCreateReq{Body: createBody, Index: indexName}
	creationResponse, err := client.Indices.Create(suite.ctx, req)
	assert.Nil(suite.T(), err)
	t := &testdata.AcknowledgedResponse{}
	inspect := creationResponse.Inspect()
	parsedBody := (*suite.buildResponseBody(&inspect, t)).(*testdata.AcknowledgedResponse)
	assert.True(suite.T(), parsedBody.Acknowledged)

	// check such index exists
	existsReq := opensearchapi.IndicesExistsReq{Indices: []string{indexName}}
	existsResponse, err := client.Indices.Exists(suite.ctx, existsReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), existsResponse)
	assert.Equal(suite.T(), statusOk, existsResponse.StatusCode)

	// check created aliases exist
	existsAliasReq := opensearchapi.AliasExistsReq{Indices: []string{indexName}, Alias: []string{"alias_test_1", "alias_test_2"}}
	existsAliasResponse, err := client.Indices.Alias.Exists(suite.ctx, existsAliasReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), existsAliasResponse)
	assert.Equal(suite.T(), statusOk, existsAliasResponse.StatusCode)
}

func (suite *OsAPITestSuite) Test_CloneIndex() {
	osClient, _ := suite.database.GetOpensearchClient()
	assert.NotNil(suite.T(), osClient)

	indexName := "index_for_cloning"
	normalizeName, err2 := osClient.Normalize(suite.ctx, indexName)
	assert.Nil(suite.T(), err2)
	docId := "1"
	suite.upsertDocument(normalizeName, docId, osClient)

	client, _ := osClient.GetClient(suite.ctx)
	// block write operations on initial index
	blockReq := opensearchapi.IndicesBlockReq{Indices: []string{normalizeName}, Block: "write"}
	blockResponse, err := client.Indices.Block(suite.ctx, blockReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), blockResponse)

	targetIndex := "target_index"
	normalizeTargetName, err2 := osClient.Normalize(suite.ctx, targetIndex)
	assert.Nil(suite.T(), err2)
	cloneBody := strings.NewReader(`{
	  "settings": {
		"index": {
		  "number_of_shards": 1,
		  "number_of_replicas": 1
		}
	  },
	  "aliases": {
		"sample-alias1": {}
	  }
	}`)
	cloneReq := opensearchapi.IndicesCloneReq{Index: normalizeName, Target: normalizeTargetName, Body: cloneBody}
	cloneResponse, err := client.Indices.Clone(suite.ctx, cloneReq) // not migration case
	assert.Nil(suite.T(), err)
	t := &testdata.AcknowledgedResponse{}
	inspect := cloneResponse.Inspect()
	parsedBody := (*suite.buildResponseBody(&inspect, t)).(*testdata.AcknowledgedResponse)
	assert.True(suite.T(), parsedBody.Acknowledged)
	assert.Equal(suite.T(), testPrefix+testDelimiter+targetIndex, parsedBody.Index)

	// check such index exists and contains all information
	getReq := opensearchapi.DocumentGetReq{
		Index:      normalizeTargetName,
		DocumentID: "1",
	}
	getResponse, err := client.Document.Get(suite.ctx, getReq)
	inspect = getResponse.Inspect()
	assert.Nil(suite.T(), err)
	suite.checkGetResponse(inspect, true)

	// check created aliases exist
	existsAliasReq := opensearchapi.AliasExistsReq{Indices: []string{normalizeTargetName}, Alias: []string{"sample-alias1"}}
	existsAliasResponse, err := client.Indices.Alias.Exists(suite.ctx, existsAliasReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), existsAliasResponse)
	assert.Equal(suite.T(), statusOk, existsAliasResponse.StatusCode)
}

func (suite *OsAPITestSuite) Test_RolloverIndex() {
	osClient, _ := suite.database.GetOpensearchClient()
	assert.NotNil(suite.T(), osClient)

	indexName := "rollover_index"
	normalizeName, err2 := osClient.Normalize(suite.ctx, indexName)
	assert.Nil(suite.T(), err2)
	createBody := strings.NewReader(`
		{
		  "aliases": {
			"rollover_alias": {}
		  }
		}
	`)
	client, _ := osClient.GetClient(suite.ctx)
	req := opensearchapi.IndicesCreateReq{Body: createBody, Index: normalizeName}
	creationResponse, err := client.Indices.Create(suite.ctx, req)
	assert.Nil(suite.T(), err)
	acknowledgedType := &testdata.AcknowledgedResponse{}
	inspect := creationResponse.Inspect()
	parsedBody := (*suite.buildResponseBody(&inspect, acknowledgedType)).(*testdata.AcknowledgedResponse)
	assert.True(suite.T(), parsedBody.Acknowledged)

	targetIndex := "rollover_target_index"
	normalizeTargetName, err2 := osClient.Normalize(suite.ctx, targetIndex)
	assert.Nil(suite.T(), err2)
	rolloverBody := strings.NewReader(`{
	  "settings": {
		"index": {
		  "number_of_replicas": 1
		}
	  },
	  "aliases": {
		"rollover_another_alias": {}
	  }
	}`)
	rolloverReq := opensearchapi.IndicesRolloverReq{Alias: "rollover_alias", Index: normalizeTargetName, Body: rolloverBody}
	rolloverResponse, err := client.Indices.Rollover(suite.ctx, rolloverReq)
	assert.Nil(suite.T(), err)
	t := &testdata.RolloverResponse{}
	inspect = rolloverResponse.Inspect()
	parsedRolloverBody := (*suite.buildResponseBody(&inspect, t)).(*testdata.RolloverResponse)
	assert.True(suite.T(), parsedRolloverBody.Acknowledged)
	assert.True(suite.T(), parsedRolloverBody.RolledOver)
	assert.Equal(suite.T(), testPrefix+testDelimiter+targetIndex, parsedRolloverBody.NewIndex)

	// check created aliases exist
	existsAliasReq := opensearchapi.AliasExistsReq{Indices: []string{normalizeTargetName}, Alias: []string{"rollover_another_alias", "rollover_alias"}}
	existsAliasResponse, err := client.Indices.Alias.Exists(suite.ctx, existsAliasReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), existsAliasResponse)
	assert.Equal(suite.T(), statusOk, existsAliasResponse.StatusCode)

	notExistsAliasReq := opensearchapi.AliasExistsReq{Indices: []string{normalizeName}, Alias: []string{"rollover_alias"}}
	notExistsAliasResponse, err := client.Indices.Alias.Exists(suite.ctx, notExistsAliasReq)
	assert.NotNil(suite.T(), notExistsAliasResponse)
	assert.Equal(suite.T(), statusNotFound, notExistsAliasResponse.StatusCode)
}

func (suite *OsAPITestSuite) Test_ShrinkIndex() {
	osClient, _ := suite.database.GetOpensearchClient()
	assert.NotNil(suite.T(), osClient)

	indexName := "index_for_shrinking"
	createBody := strings.NewReader(`
		{
		  "settings": {
			"index.number_of_shards": 4,
			"index.blocks.write" : true
		  },
		  "aliases": {
			"shrink_alias": {}
		  }
		}
	`)
	client, _ := osClient.GetClient(suite.ctx)
	req := opensearchapi.IndicesCreateReq{Body: createBody, Index: indexName}
	creationResponse, err := client.Indices.Create(suite.ctx, req)
	inspect := creationResponse.Inspect()
	assert.Nil(suite.T(), err)
	t := &testdata.AcknowledgedResponse{}
	parsedBody := (*suite.buildResponseBody(&inspect, t)).(*testdata.AcknowledgedResponse)
	assert.True(suite.T(), parsedBody.Acknowledged)

	targetIndex := "target_shrink_index"
	shrinkBody := strings.NewReader(`{
	  "settings": {
		"index": {
		  "number_of_shards": 1
		}
	  },
	  "aliases": {
		"shrink_sample-alias": {}
	  }
	}`)
	shrinkReq := opensearchapi.IndicesShrinkReq{Index: indexName, Target: targetIndex, Body: shrinkBody}
	shrinkResponse, err := client.Indices.Shrink(suite.ctx, shrinkReq)
	inspect = shrinkResponse.Inspect()
	parsedBody = (*suite.buildResponseBody(&inspect, t)).(*testdata.AcknowledgedResponse)
	assert.True(suite.T(), parsedBody.Acknowledged)

	// check created aliases exist
	existsAliasReq := opensearchapi.AliasExistsReq{Indices: []string{targetIndex}, Alias: []string{"shrink_sample-alias"}}
	existsAliasResponse, err := client.Indices.Alias.Exists(suite.ctx, existsAliasReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), existsAliasResponse)
	assert.Equal(suite.T(), statusOk, existsAliasResponse.StatusCode)
}

func (suite *OsAPITestSuite) Test_SplitIndex() {
	osClient, _ := suite.database.GetOpensearchClient()
	assert.NotNil(suite.T(), osClient)

	indexName := "index_for_spliting"
	createBody := strings.NewReader(`
		{
		  "settings": {
			"index.number_of_shards": 1,
			"index.blocks.write" : true
		  }
		}
	`)
	req := opensearchapi.IndicesCreateReq{Body: createBody, Index: indexName}
	client, _ := osClient.GetClient(suite.ctx)
	creationResponse, err := client.Indices.Create(suite.ctx, req)
	inspect := creationResponse.Inspect()
	assert.Nil(suite.T(), err)
	t := &testdata.AcknowledgedResponse{}
	parsedBody := (*suite.buildResponseBody(&inspect, t)).(*testdata.AcknowledgedResponse)
	assert.True(suite.T(), parsedBody.Acknowledged)

	targetIndex := "target_split_index"
	splitBody := strings.NewReader(`{
	  "settings": {
		"index": {
		  "number_of_shards": 4
		}
	  },
	  "aliases": {
		"split_sample-alias": {}
	  }
	}`)
	splitReq := opensearchapi.IndicesSplitReq{Index: indexName, Target: targetIndex, Body: splitBody}
	splitResponse, err := client.Indices.Split(suite.ctx, splitReq)
	inspect = splitResponse.Inspect()
	parsedBody = (*suite.buildResponseBody(&inspect, t)).(*testdata.AcknowledgedResponse)
	assert.True(suite.T(), parsedBody.Acknowledged)

	// check created aliases exist
	existsAliasReq := opensearchapi.AliasExistsReq{Indices: []string{targetIndex}, Alias: []string{"split_sample-alias"}}
	existsAliasResponse, err := client.Indices.Alias.Exists(suite.ctx, existsAliasReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), existsAliasResponse)
	assert.Equal(suite.T(), statusOk, existsAliasResponse.StatusCode)
}

func (suite *OsAPITestSuite) Test_PutComposableIndexTemplate() {
	osClient, _ := suite.database.GetOpensearchClient()
	assert.NotNil(suite.T(), osClient)

	templateName := "put_composable_idx_template"
	normalizeTemplateName, err2 := osClient.Normalize(suite.ctx, templateName)
	assert.Nil(suite.T(), err2)
	putTemplateBody := strings.NewReader(`
		{
		  "index_patterns" : ["test_put_composable_tem*"],
		  "template": {
			"settings" : {
				"number_of_shards" : 1
			},
			"aliases" : {
				"composable_template_alias" : {}
			},
			"mappings" : {
			  "properties": {
				"title":  { "type": "text"}
			  }
			}
		  }
		}
`)
	putTemplateReq := opensearchapi.IndexTemplateCreateReq{IndexTemplate: normalizeTemplateName, Body: putTemplateBody}
	client, _ := osClient.GetClient(suite.ctx)
	putTemplateResponse, err := client.IndexTemplate.Create(suite.ctx, putTemplateReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), putTemplateResponse)
	inspect := putTemplateResponse.Inspect()
	t := &testdata.AcknowledgedResponse{}
	parsedBody := (*suite.buildResponseBody(&inspect, t)).(*testdata.AcknowledgedResponse)
	assert.True(suite.T(), parsedBody.Acknowledged)

	indexName := "put_composable_template_index"
	normalizeName, err2 := osClient.Normalize(suite.ctx, indexName)
	assert.Nil(suite.T(), err2)
	suite.createTestIndex(normalizeName, osClient)

	// check created aliases exist
	existsAliasReq := opensearchapi.AliasExistsReq{Indices: []string{normalizeName}, Alias: []string{"composable_template_alias"}}
	existsAliasResponse, err := client.Indices.Alias.Exists(suite.ctx, existsAliasReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), existsAliasResponse)
	assert.Equal(suite.T(), statusOk, existsAliasResponse.StatusCode)

	// check mapping
	getMappingReq := opensearchapi.MappingGetReq{Indices: []string{normalizeName}}
	getMappingResponse, err := client.Indices.Mapping.Get(suite.ctx, &getMappingReq)
	inspect = getMappingResponse.Inspect()
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), getMappingResponse)
	assert.Equal(suite.T(), statusOk, inspect.Response.StatusCode)

	suite.checkGetMappingResponse(inspect.Response.Body, normalizeName)
}

func (suite *OsAPITestSuite) Test_SimulateIndexTemplate() {
	osClient, _ := suite.database.GetOpensearchClient()
	assert.NotNil(suite.T(), osClient)

	templateName := "simulate_idx_template"
	simulateTemplateBody := strings.NewReader(`
		{
		  "index_patterns" : ["simulate_tem*"],
		  "template": {
			"settings" : {
				"number_of_shards" : 1
			},
			"aliases" : {
				"simulate_template_alias" : {}
			},
			"mappings" : {
			  "properties": {
				"title":  { "type": "text"}
			  }
			}
		  }
		}
`)
	client, _ := osClient.GetClient(suite.ctx)
	simulateTemplateReq := opensearchapi.IndexTemplateSimulateReq{IndexTemplate: templateName, Body: simulateTemplateBody}
	simulateTemplateResponse, err := client.IndexTemplate.Simulate(suite.ctx, simulateTemplateReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), simulateTemplateResponse.Inspect().Response.Body)
	assert.Equal(suite.T(), statusOk, simulateTemplateResponse.Inspect().Response.StatusCode)
}

func (suite *OsAPITestSuite) Test_Reindex() {
	osClient, _ := suite.database.GetOpensearchClient()
	assert.NotNil(suite.T(), osClient)

	indexName := "index_for_reindex"
	docId := "111"
	suite.upsertDocument(indexName, docId, osClient)
	time.Sleep(3 * time.Second)

	targetIndex := "target_reindex_index"
	reindexBody := strings.NewReader(`{
	  "source": {
		"index": "index_for_reindex"
	  },
	  "dest": {
		"index": "target_reindex_index"
	  }
	}`)
	client, _ := osClient.GetClient(suite.ctx)
	reindexReq := opensearchapi.ReindexReq{Body: reindexBody}
	reindexResponse, err := client.Reindex(suite.ctx, reindexReq)
	inspect := reindexResponse.Inspect()
	assert.Nil(suite.T(), err)
	t := &testdata.ReindexResponse{}
	parsedBody := (*suite.buildResponseBody(&inspect, t)).(*testdata.ReindexResponse)
	assert.Equal(suite.T(), 1, parsedBody.Total)
	time.Sleep(3 * time.Second)

	// check such index exists and contains all information
	getReq := opensearchapi.DocumentGetReq{
		Index:      targetIndex,
		DocumentID: docId,
	}
	getResponse, err := client.Document.Get(suite.ctx, getReq)
	inspect = getResponse.Inspect()
	assert.Nil(suite.T(), err)
	suite.checkGetResponse(inspect, true)
}

func (suite *OsAPITestSuite) Test_CAT() {
	osClient, _ := suite.database.GetOpensearchClient()
	assert.NotNil(suite.T(), osClient)

	indexName := "index_for_cat"
	createBody := strings.NewReader(`
		{
		  "aliases": {
			"cat_alias": {}
		  }
		}
	`)
	createReq := opensearchapi.IndicesCreateReq{Body: createBody, Index: indexName}
	client, _ := osClient.GetClient(suite.ctx)
	creationResponse, err := client.Indices.Create(suite.ctx, createReq)
	inspect := creationResponse.Inspect()
	assert.Nil(suite.T(), err)
	t := &testdata.AcknowledgedResponse{}
	parsedBody := (*suite.buildResponseBody(&inspect, t)).(*testdata.AcknowledgedResponse)
	assert.True(suite.T(), parsedBody.Acknowledged)

	docId := "111"
	suite.upsertDocument(indexName, docId, osClient)

	// catAliases
	catAliasesReq := opensearchapi.CatAliasesReq{Aliases: []string{"cat_alias"}}
	catAliasesResponse, err := client.Cat.Aliases(suite.ctx, &catAliasesReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), catAliasesResponse)
	assert.Equal(suite.T(), statusOk, catAliasesResponse.Inspect().Response.StatusCode)

	// cat count
	catCountReq := opensearchapi.CatCountReq{Indices: []string{indexName}}
	catCountResponse, err := client.Cat.Count(suite.ctx, &catCountReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), catCountResponse)
	assert.Equal(suite.T(), statusOk, catCountResponse.Inspect().Response.StatusCode)
}

func (suite *OsAPITestSuite) Test_Search() {
	osClient, _ := suite.database.GetOpensearchClient()
	assert.NotNil(suite.T(), osClient)

	indexName := "index_for_search"
	normalizeName, err2 := osClient.Normalize(suite.ctx, indexName)
	assert.Nil(suite.T(), err2)
	docId := "111"
	suite.upsertDocument(normalizeName, docId, osClient)
	time.Sleep(3 * time.Second)

	searchBody := strings.NewReader(`
	{
	  "query": {
		"match_all": {}
	  }
	}`)
	searchReq := opensearchapi.SearchReq{Indices: []string{normalizeName}, Body: searchBody, Params: opensearchapi.SearchParams{Scroll: time.Minute}}
	client, _ := osClient.GetClient(suite.ctx)
	searchResponse, err := client.Search(suite.ctx, &searchReq)
	inspect := searchResponse.Inspect()
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), searchResponse)
	t := &testdata.SearchResponse{}
	parsedBody := (*suite.buildResponseBody(&inspect, t)).(*testdata.SearchResponse)
	scrollId := parsedBody.ScrollId
	hits := parsedBody.Hits.Hits
	assert.Equal(suite.T(), 1, len(hits))
	assert.Equal(suite.T(), testPrefix+testDelimiter+indexName, hits[0].Index)
	assert.Equal(suite.T(), docId, hits[0].Id)

	clearReq := opensearchapi.ScrollGetReq{ScrollID: scrollId}
	clearResponse, err := client.Scroll.Get(suite.ctx, clearReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), clearResponse)
	assert.Equal(suite.T(), statusOk, clearResponse.Inspect().Response.StatusCode)
}

func (suite *OsAPITestSuite) Test_DocumentCRUD() {
	osClient, _ := suite.database.GetOpensearchClient()
	assert.NotNil(suite.T(), osClient)

	indexName := "index_for_crud"
	docId := "111"
	suite.upsertDocument(indexName, docId, osClient)

	// check if document exists
	existsReq := opensearchapi.DocumentExistsReq{Index: indexName, DocumentID: "111"}
	client, _ := osClient.GetClient(suite.ctx)
	existsResponse, err := client.Document.Exists(suite.ctx, existsReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), existsResponse)
	assert.Equal(suite.T(), statusOk, existsResponse.StatusCode)

	// update document
	updateBody := strings.NewReader(`{
    "doc": {
			"first_name" : "Bruce",
			"last_name" : "Wayne"
		}
}`)
	updateReq := opensearchapi.UpdateReq{Index: indexName, DocumentID: docId, Body: updateBody}
	updateResponse, err := client.Update(suite.ctx, updateReq)
	inspect := updateResponse.Inspect()
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), updateResponse)
	assert.Equal(suite.T(), statusOk, inspect.Response.StatusCode)
	t := &testdata.OperationResponse{}
	updateResponseBody := (*suite.buildResponseBody(&inspect, t)).(*testdata.OperationResponse)
	assert.Equal(suite.T(), "updated", updateResponseBody.Result)
	assert.Equal(suite.T(), 2, updateResponseBody.Version)

	// check documents amount
	countReq := opensearchapi.IndicesCountReq{Indices: []string{indexName}}
	countResponse, err := client.Indices.Count(suite.ctx, &countReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), countResponse)
	assert.Equal(suite.T(), statusOk, countResponse.Inspect().Response.StatusCode)

	// delete document
	deleteReq := opensearchapi.DocumentDeleteReq{Index: indexName, DocumentID: docId}
	deleteResponse, err := client.Document.Delete(suite.ctx, deleteReq)
	inspect = deleteResponse.Inspect()
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), deleteResponse)
	deleteResponseBody := (*suite.buildResponseBody(&inspect, t)).(*testdata.OperationResponse)
	assert.Equal(suite.T(), "deleted", deleteResponseBody.Result)
	assert.Equal(suite.T(), 3, deleteResponseBody.Version)

	// check document does not exist anymore
	getReq := opensearchapi.DocumentGetReq{
		Index:      indexName,
		DocumentID: docId,
	}
	getResponse, _ := client.Document.Get(suite.ctx, getReq)
	inspect = getResponse.Inspect()
	assert.Equal(suite.T(), statusNotFound, inspect.Response.StatusCode)
	assert.False(suite.T(), getResponse.Found)
}

func (suite *OsAPITestSuite) Test_IndexCRUD() {
	osClient, _ := suite.database.GetOpensearchClient()
	assert.NotNil(suite.T(), osClient)

	indexName := "crud_index"
	normalizeName, err2 := osClient.Normalize(suite.ctx, indexName)
	assert.Nil(suite.T(), err2)
	createBody := strings.NewReader(`
		{
		  "aliases": {
			"alias_crud": {}
		  }
		}
	`)
	client, _ := osClient.GetClient(suite.ctx)
	req := opensearchapi.IndicesCreateReq{Body: createBody, Index: normalizeName}
	creationResponse, err := client.Indices.Create(suite.ctx, req)
	assert.Nil(suite.T(), err)
	assert.True(suite.T(), creationResponse.Acknowledged)

	getReq := opensearchapi.IndicesGetReq{Indices: []string{normalizeName}}
	getResponse, err := client.Indices.Get(suite.ctx, getReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), getResponse)
	inspect := getResponse.Inspect()
	getBody := suite.buildMapResponse(&inspect)
	_, isExists := getBody[testPrefix+testDelimiter+indexName]
	assert.True(suite.T(), isExists)

	closeReq := opensearchapi.IndicesCloseReq{Index: normalizeName}
	closeResponse, err := client.Indices.Close(suite.ctx, closeReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), closeResponse)
	assert.Equal(suite.T(), statusOk, closeResponse.Inspect().Response.StatusCode)

	openReq := opensearchapi.IndicesOpenReq{Index: normalizeName}
	openResponse, err := client.Indices.Open(suite.ctx, openReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), openResponse)
	assert.Equal(suite.T(), statusOk, openResponse.Inspect().Response.StatusCode)

	deleteReq := opensearchapi.IndicesDeleteReq{Indices: []string{normalizeName}}
	deleteResponse, err := client.Indices.Delete(suite.ctx, deleteReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), deleteResponse)
	assert.Equal(suite.T(), statusOk, deleteResponse.Inspect().Response.StatusCode)

	getReq = opensearchapi.IndicesGetReq{Indices: []string{normalizeName}}
	getResponse, _ = client.Indices.Get(suite.ctx, getReq)
	assert.NotNil(suite.T(), getResponse)
	assert.Equal(suite.T(), statusNotFound, getResponse.Inspect().Response.StatusCode)
}

func (suite *OsAPITestSuite) Test_AliasesCRUD() {
	osClient, _ := suite.database.GetOpensearchClient()
	assert.NotNil(suite.T(), osClient)

	indexName := "crud_aliases"
	suite.createTestIndex(indexName, osClient)

	client, _ := osClient.GetClient(suite.ctx)
	aliasName := "crud_aliases_alias"
	putAliasReq := opensearchapi.AliasPutReq{Indices: []string{indexName}, Alias: aliasName}
	putAliasesResponse, err := client.Indices.Alias.Put(suite.ctx, putAliasReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), putAliasesResponse)

	// check created aliases exist
	existsAliasReq := opensearchapi.AliasExistsReq{Indices: []string{indexName}, Alias: []string{aliasName}}
	existsAliasResponse, err := client.Indices.Alias.Exists(suite.ctx, existsAliasReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), existsAliasResponse)
	assert.Equal(suite.T(), statusOk, existsAliasResponse.StatusCode)

	// delete alias
	deleteReq := opensearchapi.AliasDeleteReq{Indices: []string{indexName}, Alias: []string{aliasName}}
	deleteResponse, err := client.Indices.Alias.Delete(suite.ctx, deleteReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), deleteResponse)
	assert.Equal(suite.T(), statusOk, deleteResponse.Inspect().Response.StatusCode)

	// check alias does not exist anymore
	existsAliasReq = opensearchapi.AliasExistsReq{Indices: []string{indexName}, Alias: []string{aliasName}}
	existsAliasResponse, _ = client.Indices.Alias.Exists(suite.ctx, existsAliasReq)
	assert.NotNil(suite.T(), existsAliasResponse)
	assert.Equal(suite.T(), statusNotFound, existsAliasResponse.StatusCode)
}

func (suite *OsAPITestSuite) Test_Mappings() {
	osClient, _ := suite.database.GetOpensearchClient()
	assert.NotNil(suite.T(), osClient)

	indexName := "index_for_mapping"
	normalizeName, err2 := osClient.Normalize(suite.ctx, indexName)
	assert.Nil(suite.T(), err2)
	suite.createTestIndex(normalizeName, osClient)

	mappingBody := strings.NewReader(`
	{
	 	"properties": {
			"title":  { "type": "text"}
		}
	}`)
	client, _ := osClient.GetClient(suite.ctx)
	mappingReq := opensearchapi.MappingPutReq{Indices: []string{normalizeName}, Body: mappingBody}
	mappingResponse, err := client.Indices.Mapping.Put(suite.ctx, mappingReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), mappingResponse)
	assert.Equal(suite.T(), statusOk, mappingResponse.Inspect().Response.StatusCode)

	getMappingReq := opensearchapi.MappingGetReq{Indices: []string{normalizeName}}
	getMappingResponse, err := client.Indices.Mapping.Get(suite.ctx, &getMappingReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), getMappingResponse)
	assert.Equal(suite.T(), statusOk, getMappingResponse.Inspect().Response.StatusCode)
	suite.checkGetMappingResponse(getMappingResponse.Inspect().Response.Body, normalizeName)

	getFieldMappingReq := opensearchapi.MappingFieldReq{Indices: []string{normalizeName}, Fields: []string{"title"}}
	getFieldMappingResponse, err := client.Indices.Mapping.Field(suite.ctx, &getFieldMappingReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), getFieldMappingResponse)
	assert.Equal(suite.T(), statusOk, getFieldMappingResponse.Inspect().Response.StatusCode)
	suite.checkFieldMappingResponse(getFieldMappingResponse.Inspect().Response.Body, normalizeName)
}

func (suite *OsAPITestSuite) Test_Settings() {
	osClient, _ := suite.database.GetOpensearchClient()
	assert.NotNil(suite.T(), osClient)

	indexName := "index_for_settings"
	normalizeName, err2 := osClient.Normalize(suite.ctx, indexName)
	assert.Nil(suite.T(), err2)
	suite.createTestIndex(normalizeName, osClient)

	putSettingsBody := strings.NewReader(`
	{
	  "index" : {
		"number_of_replicas" : 2
	  }
	}`)
	client, _ := osClient.GetClient(suite.ctx)
	putSettingsReq := opensearchapi.SettingsPutReq{Indices: []string{normalizeName}, Body: putSettingsBody}
	putSettingsResponse, err := client.Indices.Settings.Put(suite.ctx, putSettingsReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), putSettingsResponse)
	assert.Equal(suite.T(), statusOk, putSettingsResponse.Inspect().Response.StatusCode)

	getSettingsReq := opensearchapi.SettingsGetReq{Indices: []string{normalizeName}}
	getSettingsResponse, err := client.Indices.Settings.Get(suite.ctx, &getSettingsReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), getSettingsResponse)
	assert.Equal(suite.T(), statusOk, getSettingsResponse.Inspect().Response.StatusCode)

	response := suite.readBodyIntoMap(getSettingsResponse.Inspect().Response.Body)
	rawIndexSettings, isExists := response[testPrefix+testDelimiter+indexName]
	assert.True(suite.T(), isExists)
	indexSettings := rawIndexSettings.(map[string]interface{})
	rawIndex, ok := indexSettings["settings"]
	assert.True(suite.T(), ok)
	index := rawIndex.(map[string]interface{})
	rawSettings, ok := index["index"]
	assert.True(suite.T(), ok)
	settings := rawSettings.(map[string]interface{})
	val, ok := settings["number_of_replicas"]
	assert.True(suite.T(), ok)
	assert.Equal(suite.T(), "2", val)
}

func (suite *OsAPITestSuite) Test_IndexTemplate() {
	osClient, _ := suite.database.GetOpensearchClient()
	assert.NotNil(suite.T(), osClient)

	templateName := "composable_template"
	normalizeTemplateName, err2 := osClient.Normalize(suite.ctx, templateName)
	assert.Nil(suite.T(), err2)
	putTemplateBody := strings.NewReader(`
		{
		  "index_patterns" : ["put_composable_tem*"],
		  "template": {
			"mappings" : {
			  "properties": {
				"title":  { "type": "text"}
			  }
			}
		  }
		}
`)
	client, _ := osClient.GetClient(suite.ctx)
	putTemplateReq := opensearchapi.IndexTemplateCreateReq{IndexTemplate: normalizeTemplateName, Body: putTemplateBody}
	putTemplateResponse, err := client.IndexTemplate.Create(suite.ctx, putTemplateReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), putTemplateResponse)
	assert.True(suite.T(), putTemplateResponse.Acknowledged)

	existsReq := opensearchapi.IndexTemplateExistsReq{IndexTemplate: normalizeTemplateName}
	existsResponse, err := client.IndexTemplate.Exists(suite.ctx, existsReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), existsResponse)
	assert.Equal(suite.T(), statusOk, existsResponse.StatusCode)

	getReq := opensearchapi.IndexTemplateGetReq{IndexTemplates: []string{normalizeTemplateName}}
	getResponse, err := client.IndexTemplate.Get(suite.ctx, &getReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), getResponse)
	assert.Equal(suite.T(), statusOk, getResponse.Inspect().Response.StatusCode)
	t := &testdata.GetTemplateResponse{}
	inspect := getResponse.Inspect()
	parsedGetBody := (*suite.buildResponseBody(&inspect, t)).(*testdata.GetTemplateResponse)
	listOfTemplates := parsedGetBody.IndexTemplates
	assert.Equal(suite.T(), 1, len(listOfTemplates))
	assert.Equal(suite.T(), testPrefix+testDelimiter+templateName, listOfTemplates[0].Name)

	deleteReq := opensearchapi.IndexTemplateDeleteReq{IndexTemplate: normalizeTemplateName}
	deleteResponse, err := client.IndexTemplate.Delete(suite.ctx, deleteReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), deleteResponse)
	assert.Equal(suite.T(), statusOk, deleteResponse.Inspect().Response.StatusCode)

	existsReq = opensearchapi.IndexTemplateExistsReq{IndexTemplate: normalizeTemplateName}
	existsResponse, _ = client.IndexTemplate.Exists(suite.ctx, existsReq)
	assert.NotNil(suite.T(), existsResponse)
	assert.Equal(suite.T(), statusNotFound, existsResponse.StatusCode)
}

func (suite *OsAPITestSuite) Test_IndexRefresh() {
	osClient, _ := suite.database.GetOpensearchClient()
	assert.NotNil(suite.T(), osClient)

	indexName := "index_refresh"
	suite.createTestIndex(indexName, osClient)

	client, _ := osClient.GetClient(suite.ctx)
	refreshReq := opensearchapi.IndicesRefreshReq{Indices: []string{indexName}}
	refreshResponse, err := client.Indices.Refresh(suite.ctx, &refreshReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), refreshResponse)
	assert.Equal(suite.T(), statusOk, refreshResponse.Inspect().Response.StatusCode)
}

func (suite *OsAPITestSuite) Test_IndexClearCache() {
	osClient, _ := suite.database.GetOpensearchClient()
	assert.NotNil(suite.T(), osClient)

	indexName := "index_clear_cache"
	suite.createTestIndex(indexName, osClient)

	client, _ := osClient.GetClient(suite.ctx)
	clearCacheReq := opensearchapi.IndicesClearCacheReq{Indices: []string{indexName}}
	clearCacheResponse, err := client.Indices.ClearCache(suite.ctx, &clearCacheReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), clearCacheResponse)
	assert.Equal(suite.T(), statusOk, clearCacheResponse.Inspect().Response.StatusCode)
}

func (suite *OsAPITestSuite) Test_IndexFlush() {
	osClient, _ := suite.database.GetOpensearchClient()
	assert.NotNil(suite.T(), osClient)

	indexName := "index_flush"
	suite.createTestIndex(indexName, osClient)

	client, _ := osClient.GetClient(suite.ctx)
	flushReq := opensearchapi.IndicesFlushReq{Indices: []string{indexName}}
	flushResponse, err := client.Indices.Flush(suite.ctx, &flushReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), flushResponse)
	assert.Equal(suite.T(), statusOk, flushResponse.Inspect().Response.StatusCode)
}

func (suite *OsAPITestSuite) Test_IndexForcemerge() {
	osClient, _ := suite.database.GetOpensearchClient()
	assert.NotNil(suite.T(), osClient)

	indexName := "index_forcemerge"
	suite.createTestIndex(indexName, osClient)

	client, _ := osClient.GetClient(suite.ctx)
	forcemergeReq := opensearchapi.IndicesForcemergeReq{Indices: []string{indexName}}
	forcemergeResponse, err := client.Indices.Forcemerge(suite.ctx, &forcemergeReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), forcemergeResponse)
	assert.Equal(suite.T(), statusOk, forcemergeResponse.Inspect().Response.StatusCode)
}

func (suite *OsAPITestSuite) Test_IndexRecovery() {
	osClient, _ := suite.database.GetOpensearchClient()
	client, _ := osClient.GetClient(suite.ctx)
	assert.NotNil(suite.T(), osClient)

	indexName := "index_recovery"
	normalizeName, err2 := osClient.Normalize(suite.ctx, indexName)
	assert.Nil(suite.T(), err2)
	suite.createTestIndex(normalizeName, osClient)

	recoveryReq := opensearchapi.IndicesRecoveryReq{Indices: []string{normalizeName}}
	recoveryResponse, err := client.Indices.Recovery(suite.ctx, &recoveryReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), recoveryResponse)
	assert.Equal(suite.T(), statusOk, recoveryResponse.Inspect().Response.StatusCode)
	inspect := recoveryResponse.Inspect()
	parsedResponse := suite.buildMapResponse(&inspect)
	_, isIndexRecoveryInfoExists := parsedResponse[testPrefix+testDelimiter+indexName]
	assert.True(suite.T(), isIndexRecoveryInfoExists)
}

func (suite *OsAPITestSuite) Test_IndexResolve() {
	osClient, _ := suite.database.GetOpensearchClient()
	client, _ := osClient.GetClient(suite.ctx)
	assert.NotNil(suite.T(), osClient)

	indexName := "resolve_index"
	createBody := strings.NewReader(`
		{
		  "aliases": {
			"resolve_alias": {}
		  }
		}
	`)
	req := opensearchapi.IndicesCreateReq{Body: createBody, Index: indexName}
	creationResponse, err := client.Indices.Create(suite.ctx, req)
	assert.Nil(suite.T(), err)
	assert.True(suite.T(), creationResponse.Acknowledged)

	resolveReq := opensearchapi.IndicesResolveReq{Indices: []string{"resolve*"}, Params: opensearchapi.IndicesResolveParams{ExpandWildcards: "all"}}
	resolveResponse, err := client.Indices.Resolve(suite.ctx, resolveReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), resolveResponse)
	assert.Equal(suite.T(), statusOk, resolveResponse.Inspect().Response.StatusCode)
	t := &testdata.ResolveResponse{}
	inspect := resolveResponse.Inspect()
	parsedResponse := (*suite.buildResponseBody(&inspect, t)).(*testdata.ResolveResponse)
	assert.Equal(suite.T(), 1, len(parsedResponse.Aliases))
	assert.Equal(suite.T(), 1, len(parsedResponse.Indices))
}

func (suite *OsAPITestSuite) Test_IndexSegments() {
	osClient, _ := suite.database.GetOpensearchClient()
	assert.NotNil(suite.T(), osClient)

	indexName := "index_segments"
	normalizeName, err2 := osClient.Normalize(suite.ctx, indexName)
	assert.Nil(suite.T(), err2)
	suite.createTestIndex(normalizeName, osClient)

	client, _ := osClient.GetClient(suite.ctx)
	segmentsReq := opensearchapi.IndicesSegmentsReq{Indices: []string{normalizeName}}
	segmentsResponse, err := client.Indices.Segments(suite.ctx, &segmentsReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), segmentsResponse)
	assert.Equal(suite.T(), statusOk, segmentsResponse.Inspect().Response.StatusCode)
	inspect := segmentsResponse.Inspect()
	parsedResponse := suite.buildMapResponse(&inspect)
	indices := parsedResponse["indices"].(map[string]interface{})
	_, isActualIndexInfoExists := indices[testPrefix+testDelimiter+indexName]
	assert.True(suite.T(), isActualIndexInfoExists)
}

func (suite *OsAPITestSuite) Test_IndexShardStores() {
	osClient, _ := suite.database.GetOpensearchClient()
	assert.NotNil(suite.T(), osClient)

	indexName := "index_shard_stores"
	normalizeName, err2 := osClient.Normalize(suite.ctx, indexName)
	assert.Nil(suite.T(), err2)
	suite.createTestIndex(normalizeName, osClient)

	client, _ := osClient.GetClient(suite.ctx)
	shardReq := opensearchapi.IndicesShardStoresReq{Indices: []string{normalizeName}}
	shardResponse, err := client.Indices.ShardStores(suite.ctx, &shardReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), shardResponse)
	assert.Equal(suite.T(), statusOk, shardResponse.Inspect().Response.StatusCode)
	inspect := shardResponse.Inspect()
	parsedResponse := suite.buildMapResponse(&inspect)
	indices := parsedResponse["indices"].(map[string]interface{})
	_, isActualIndexInfoExists := indices[testPrefix+testDelimiter+indexName]
	assert.True(suite.T(), isActualIndexInfoExists)
}

func (suite *OsAPITestSuite) Test_IndexStats() {
	osClient, _ := suite.database.GetOpensearchClient()
	assert.NotNil(suite.T(), osClient)

	indexName := "index_stats"
	suite.createTestIndex(indexName, osClient)

	client, _ := osClient.GetClient(suite.ctx)
	shardReq := opensearchapi.IndicesStatsReq{Indices: []string{indexName}}
	shardResponse, err := client.Indices.Stats(suite.ctx, &shardReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), shardResponse)
	assert.Equal(suite.T(), statusOk, shardResponse.Inspect().Response.StatusCode)
}

func (suite *OsAPITestSuite) Test_IndexValidateQuery() {
	osClient, _ := suite.database.GetOpensearchClient()
	assert.NotNil(suite.T(), osClient)

	indexName := "index_validate_query"
	suite.createTestIndex(indexName, osClient)

	bodyReq := strings.NewReader(`
	{
	  "query": {
		"match_all": {}
	  }
	}`)
	client, _ := osClient.GetClient(suite.ctx)
	validateReq := opensearchapi.IndicesValidateQueryReq{Indices: []string{indexName}, Body: bodyReq}
	validateResponse, err := client.Indices.ValidateQuery(suite.ctx, validateReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), validateResponse)
	assert.Equal(suite.T(), statusOk, validateResponse.Inspect().Response.StatusCode)
	assert.True(suite.T(), validateResponse.Valid)
}

func (suite *OsAPITestSuite) Test_Mget() {
	osClient, _ := suite.database.GetOpensearchClient()
	assert.NotNil(suite.T(), osClient)

	client, _ := osClient.GetClient(suite.ctx)
	firstDocument := suite.createSampleDocument()
	firstIndexName := "mget_index_1"
	docId := "1"
	req := opensearchapi.IndexReq{
		Index:      firstIndexName,
		DocumentID: docId,
		Body:       firstDocument,
	}
	insertResponse, _ := client.Index(suite.ctx, req)
	assert.Equal(suite.T(), statusCreated, insertResponse.Inspect().Response.StatusCode)

	secondDocument := suite.createSampleDocument()
	secondIndexName := "mget_index_2"
	secondReq := opensearchapi.IndexReq{
		Index:      secondIndexName,
		DocumentID: docId,
		Body:       secondDocument,
	}
	insertResponse, _ = client.Index(suite.ctx, secondReq)
	assert.Equal(suite.T(), statusCreated, insertResponse.Inspect().Response.StatusCode)

	mgetBody := strings.NewReader(`
	{
	  "docs": [
		{
		  "_index": "mget_index_1",
		  "_id": "1"
		},
		{
		  "_index": "mget_index_2",
		  "_id": "1"
		}
	  ]
	}`)
	mgetReq := opensearchapi.MGetReq{Body: mgetBody}
	mgetResponse, err := client.MGet(suite.ctx, mgetReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), mgetResponse)
	assert.Equal(suite.T(), statusOk, mgetResponse.Inspect().Response.StatusCode)
	assert.Equal(suite.T(), 2, len(mgetResponse.Docs))
}

func (suite *OsAPITestSuite) Test_Msearch() {
	osClient, _ := suite.database.GetOpensearchClient()
	assert.NotNil(suite.T(), osClient)
	client, _ := osClient.GetClient(suite.ctx)

	firstDocument := suite.createSampleDocument()
	firstIndexName := "msearch_index_1"
	firstNormalizeName, err2 := osClient.Normalize(suite.ctx, firstIndexName)
	assert.Nil(suite.T(), err2)
	docId := "1"
	req := opensearchapi.IndexReq{
		Index:      firstNormalizeName,
		DocumentID: docId,
		Body:       firstDocument,
	}
	insertResponse, _ := client.Index(suite.ctx, req)
	assert.Equal(suite.T(), statusCreated, insertResponse.Inspect().Response.StatusCode)

	secondDocument := suite.createSampleDocument()
	secondIndexName := "msearch_index_2"
	secondNormalizeName, err2 := osClient.Normalize(suite.ctx, secondIndexName)
	assert.Nil(suite.T(), err2)
	secondReq := opensearchapi.IndexReq{
		Index:      secondNormalizeName,
		DocumentID: docId,
		Body:       secondDocument,
	}
	insertResponse, _ = client.Index(suite.ctx, secondReq)
	assert.Equal(suite.T(), statusCreated, insertResponse.Inspect().Response.StatusCode)
	time.Sleep(3 * time.Second)
	msearchBody := strings.NewReader(`
		{"index" : "` + firstNormalizeName + `"}
		{"query" : {"match_all" : {}}}
		{"index" : "` + secondNormalizeName + `"}
		{"query" : {"match_all" : {}}}
`)
	msearchReq := opensearchapi.MSearchReq{Body: msearchBody}
	msearchResponse, err := client.MSearch(suite.ctx, msearchReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), msearchResponse)
	assert.Equal(suite.T(), statusOk, msearchResponse.Inspect().Response.StatusCode)
	t := &testdata.MSearchResponse{}
	inspect := msearchResponse.Inspect()
	parsedBody := (*suite.buildResponseBody(&inspect, t)).(*testdata.MSearchResponse)
	responses := parsedBody.Responses
	assert.Equal(suite.T(), 2, len(responses))
	firstResponse := responses[0]
	hits := firstResponse.Hits.Hits
	assert.Equal(suite.T(), 1, len(hits))
	assert.Equal(suite.T(), testPrefix+testDelimiter+firstIndexName, hits[0].Index)
}

func (suite *OsAPITestSuite) Test_Termvectors() {
	osClient, _ := suite.database.GetOpensearchClient()
	assert.NotNil(suite.T(), osClient)
	client, _ := osClient.GetClient(suite.ctx)

	firstIndexName := "termvecotrs_index_1"
	firstNormalizeName, err2 := osClient.Normalize(suite.ctx, firstIndexName)
	assert.Nil(suite.T(), err2)
	docId := "1"
	suite.upsertDocument(firstNormalizeName, docId, osClient)

	secondIndexName := "termvecotrs_index_2"
	secondNormalizeName, err2 := osClient.Normalize(suite.ctx, secondIndexName)
	assert.Nil(suite.T(), err2)
	suite.upsertDocument(secondNormalizeName, docId, osClient)

	termVecBody := strings.NewReader(`{
	  "fields" : ["text"],
	  "offsets" : true,
	  "payloads" : true,
	  "positions" : true,
	  "term_statistics" : true,
	  "field_statistics" : true
	}`)
	termVecReq := opensearchapi.TermvectorsReq{Index: firstNormalizeName, DocumentID: docId, Body: termVecBody}
	termVecResponse, err := client.Termvectors(suite.ctx, termVecReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), termVecResponse)
	assert.Equal(suite.T(), statusOk, termVecResponse.Inspect().Response.StatusCode)
	inspect := termVecResponse.Inspect()
	parsedBody := suite.buildMapResponse(&inspect)

	value, isIndexInfoExists := parsedBody[index]
	assert.True(suite.T(), isIndexInfoExists)
	assert.Equal(suite.T(), testPrefix+testDelimiter+firstIndexName, value)

	mtermVectorsBody := strings.NewReader(`
	{
	   "docs": [
		  {
			 "_index": "termvecotrs_index_1",
			 "_id": "1",
			 "term_statistics": true
		  },
		  {
			 "_index": "termvecotrs_index_2",
			 "_id": "1"
		  }
	   ]
	}
	`)
	mtermVectorsReq := opensearchapi.MTermvectorsReq{Body: mtermVectorsBody}
	mtermVecResponse, err := client.MTermvectors(suite.ctx, mtermVectorsReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), mtermVecResponse)
	assert.Equal(suite.T(), statusOk, mtermVecResponse.Inspect().Response.StatusCode)
}

func (suite *OsAPITestSuite) Test_Ping() {
	osClient, _ := suite.database.GetOpensearchClient()
	assert.NotNil(suite.T(), osClient)
	client, _ := osClient.GetClient(suite.ctx)

	pingReq := opensearchapi.PingReq{}
	pingResponse, err := client.Ping(suite.ctx, &pingReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), pingResponse)
	assert.Equal(suite.T(), statusOk, pingResponse.StatusCode)
}

func (suite *OsAPITestSuite) Test_RankEval() {
	osClient, _ := suite.database.GetOpensearchClient()
	assert.NotNil(suite.T(), osClient)
	client, _ := osClient.GetClient(suite.ctx)

	firstIndexName := "rankeval_index_1"
	docId := "1"
	suite.upsertDocument(firstIndexName, docId, osClient)

	secondIndexName := "rankeval_index_2"
	suite.upsertDocument(secondIndexName, docId, osClient)

	thirdIndexName := "rankeval_index_3"
	suite.upsertDocument(thirdIndexName, docId, osClient)

	rankEvalBody := strings.NewReader(`
	{
	  "requests": [
		{
		  "id": "query_1",
		  "request": {
			  "query": { "match": { "title": "Moneyball" } }
		  },
		  "ratings": [
			{ "_index": "rankeval_index_1", "_id": "1", "rating": 0 },
			{ "_index": "rankeval_index_2", "_id": "1", "rating": 3 }
		  ]
		},
		{
		  "id": "query_2",
		  "request": {
			"query": { "match": { "director": "Bennett Miller" } }
		  },
		  "ratings": [
			{ "_index": "rankeval_index_3", "_id": "1", "rating": 1 }
		  ]
		}
	  ],
	"metric": {
		"precision": {
		  "ignore_unlabeled": false
		}
	  }
	}
`)
	rankEvalReq := opensearchapi.RankEvalReq{Body: rankEvalBody}
	rankEvalResponse, err := client.RankEval(suite.ctx, rankEvalReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), rankEvalResponse)
	assert.Equal(suite.T(), statusOk, rankEvalResponse.Inspect().Response.StatusCode)
}

func (suite *OsAPITestSuite) Test_DocumentWithQuery() {
	osClient, _ := suite.database.GetOpensearchClient()
	assert.NotNil(suite.T(), osClient)
	client, _ := osClient.GetClient(suite.ctx)

	indexName := "query_index"
	docId := "1"
	suite.upsertDocument(indexName, docId, osClient)
	time.Sleep(3 * time.Second)

	updateBody := strings.NewReader(`{
	  "query": {
		"match_all": {}
	  }
	}`)
	updateReq := opensearchapi.UpdateByQueryReq{Indices: []string{indexName}, Body: updateBody}
	updateResponse, err := client.UpdateByQuery(suite.ctx, updateReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), updateResponse)
	assert.Equal(suite.T(), statusOk, updateResponse.Inspect().Response.StatusCode)
	assert.Equal(suite.T(), 1, updateResponse.Total)
	assert.Equal(suite.T(), 1, updateResponse.Updated)
	time.Sleep(3 * time.Second)

	deleteBody := strings.NewReader(`{
	  "query": {
		"match_all": {}
	  }
	}`)
	deleteReq := opensearchapi.DocumentDeleteByQueryReq{Indices: []string{indexName}, Body: deleteBody}
	deleteResponse, err := client.Document.DeleteByQuery(suite.ctx, deleteReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), deleteResponse)
	assert.Equal(suite.T(), statusOk, deleteResponse.Inspect().Response.StatusCode)
	assert.Equal(suite.T(), 1, deleteResponse.Total)
	assert.Equal(suite.T(), 1, deleteResponse.Deleted)

	getReq := opensearchapi.DocumentGetReq{
		Index:      indexName,
		DocumentID: docId,
	}
	getResponse, _ := client.Document.Get(suite.ctx, getReq)
	assert.Equal(suite.T(), statusNotFound, getResponse.Inspect().Response.StatusCode)
	assert.False(suite.T(), getResponse.Found)
}

func (suite *OsAPITestSuite) Test_Explain() {
	osClient, _ := suite.database.GetOpensearchClient()
	assert.NotNil(suite.T(), osClient)
	client, _ := osClient.GetClient(suite.ctx)

	indexName := "index_explain"
	docId := "1"
	suite.upsertDocument(indexName, docId, osClient)
	time.Sleep(3 * time.Second)

	body := strings.NewReader(`
	{
	  "query": {
		"match_all": {}
	  }
	}
	`)
	explainReq := opensearchapi.DocumentExplainReq{Index: indexName, DocumentID: docId, Body: body}
	explainResponse, err := client.Document.Explain(suite.ctx, explainReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), explainResponse)
	assert.Equal(suite.T(), statusOk, explainResponse.Inspect().Response.StatusCode)
}

func (suite *OsAPITestSuite) Test_FieldCaps() {
	osClient, _ := suite.database.GetOpensearchClient()
	assert.NotNil(suite.T(), osClient)
	client, _ := osClient.GetClient(suite.ctx)

	indexName := "index_field_caps"
	normalizeName, err2 := osClient.Normalize(suite.ctx, indexName)
	assert.Nil(suite.T(), err2)
	docId := "11"
	suite.upsertDocument(normalizeName, docId, osClient)

	fieldCapsReq := opensearchapi.IndicesFieldCapsReq{Indices: []string{normalizeName}, Params: opensearchapi.IndicesFieldCapsParams{Fields: []string{"title"}}}
	fieldCapsResponse, err := client.Indices.FieldCaps(suite.ctx, fieldCapsReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), fieldCapsResponse)
	inspect := fieldCapsResponse.Inspect()
	assert.Equal(suite.T(), statusOk, inspect.Response.StatusCode)

	parsedBody := suite.buildMapResponse(&inspect)
	indices := parsedBody["indices"].([]interface{})
	assert.Equal(suite.T(), 1, len(indices))
	assert.Equal(suite.T(), testPrefix+testDelimiter+indexName, indices[0])
}

func (suite *OsAPITestSuite) Test_SearchShards() {
	osClient, _ := suite.database.GetOpensearchClient()
	assert.NotNil(suite.T(), osClient)
	client, _ := osClient.GetClient(suite.ctx)

	indexName := "index_search_shards"
	docId := "11"
	suite.upsertDocument(indexName, docId, osClient)

	searchShardsReq := opensearchapi.SearchShardsReq{Indices: []string{indexName}}
	searchShardsResponse, err := client.SearchShards(suite.ctx, &searchShardsReq)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), searchShardsResponse)
	assert.Equal(suite.T(), statusOk, searchShardsResponse.Inspect().Response.StatusCode)
	assert.Equal(suite.T(), 1, len(searchShardsResponse.Shards))
}

func TestSuite(t *testing.T) {
	suite.Run(t, new(OsAPITestSuite))
}

func (suite *OsAPITestSuite) createTestIndex(indexName string, client osdbaas.OpensearchClient) {
	createReq := opensearchapi.IndicesCreateReq{Index: indexName}
	osclient, _ := client.GetClient(suite.ctx)
	creationResponse, err := osclient.Indices.Create(suite.ctx, createReq)
	assert.Nil(suite.T(), err)
	t := &testdata.AcknowledgedResponse{}
	inspect := creationResponse.Inspect()
	parsedBody := (*suite.buildResponseBody(&inspect, t)).(*testdata.AcknowledgedResponse)
	assert.True(suite.T(), parsedBody.Acknowledged)
}

func osDbaasResponseHandler(address string) []byte {
	url := fmt.Sprintf("http://%s", address)
	connectionProperties := map[string]interface{}{
		"username":       "admin",
		"password":       "admin",
		"url":            url,
		"resourcePrefix": testPrefix,
	}
	dbResponse := LogicalDb{
		Id:                   "123",
		ConnectionProperties: connectionProperties,
	}
	jsonResponse, _ := json.Marshal(dbResponse)
	return jsonResponse
}

func prepareTestContainer(t *testing.T, ctx context.Context, identifier string) (nat.Port, testcontainers.Container) {
	os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")

	env := map[string]string{
		"DISABLE_SECURITY_PLUGIN":     "true",
		"DISABLE_INSTALL_DEMO_CONFIG": "true",
		"discovery.type":              "single-node",
	}
	port, _ := nat.NewPort("tcp", opensearchPort)
	req := testcontainers.ContainerRequest{
		Image:        opensearchImage,
		ExposedPorts: []string{port.Port()},
		Env:          env,
		WaitingFor:   wait.ForListeningPort(port).WithStartupTimeout(120 * time.Second),
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

func (suite *OsAPITestSuite) createSampleDocument() *strings.Reader {
	return strings.NewReader(`{
     "title": "Moneyball",
     "director": "Bennett Miller",
     "year": 2011
 }`)
}

func (suite *OsAPITestSuite) upsertDocument(indexName string, docId string, client osdbaas.OpensearchClient) {
	document := suite.createSampleDocument()
	req := opensearchapi.DocumentCreateReq{
		Index:      indexName,
		DocumentID: docId,
		Body:       document,
	}
	osclient, _ := client.GetClient(suite.ctx)
	createResponse, err := osclient.Document.Create(suite.ctx, req)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), createResponse.Inspect().Response)
	assert.Equal(suite.T(), statusCreated, createResponse.Inspect().Response.StatusCode)
}

func (suite *OsAPITestSuite) buildMapResponse(responseInspect *opensearchapi.Inspect) map[string]interface{} {
	response := responseInspect.Response
	assert.NotNil(suite.T(), response)
	data, err := ioutil.ReadAll(response.Body)
	assert.Nil(suite.T(), err)
	var parsedBody map[string]interface{}
	err = json.Unmarshal(data, &parsedBody)
	assert.Nil(suite.T(), err)
	return parsedBody
}

func (suite *OsAPITestSuite) buildResponseBody(responseInspect *opensearchapi.Inspect, bodyType interface{}) *interface{} {
	response := responseInspect.Response
	assert.NotNil(suite.T(), response)
	data, err := ioutil.ReadAll(response.Body)
	assert.Nil(suite.T(), err)
	err = json.Unmarshal(data, bodyType)
	assert.Nil(suite.T(), err)
	return &bodyType
}

func (suite *OsAPITestSuite) checkGetResponse(getResponseInspect opensearchapi.Inspect, shouldBeFound bool) {
	getResponse := getResponseInspect.Response
	assert.NotNil(suite.T(), getResponse)

	getData, err := ioutil.ReadAll(getResponse.Body)
	assert.Nil(suite.T(), err)
	var getParsedBody testdata.GetResponse
	err = json.Unmarshal(getData, &getParsedBody)
	assert.Nil(suite.T(), err)
	if shouldBeFound {
		assert.Equal(suite.T(), statusOk, getResponse.StatusCode)
		assert.True(suite.T(), getParsedBody.Found)
	} else {
		assert.Equal(suite.T(), statusNotFound, getResponse.StatusCode)
		assert.False(suite.T(), getParsedBody.Found)
	}
}

func (suite *OsAPITestSuite) checkGetMappingResponse(body io.ReadCloser, indexName string) {
	response := suite.readBodyIntoMap(body)

	rawIndexMapping, isExists := response[indexName]
	assert.True(suite.T(), isExists)
	indexMapping := rawIndexMapping.(map[string]interface{})
	rawProperties, ok := indexMapping["mappings"]
	assert.True(suite.T(), ok)
	properties := rawProperties.(map[string]interface{})
	rawTitle, ok := properties["properties"]
	assert.True(suite.T(), ok)
	title := rawTitle.(map[string]interface{})
	rawType, ok := title["title"]
	assert.True(suite.T(), ok)
	titleType := rawType.(map[string]interface{})
	val, isTitleExists := titleType["type"]
	assert.True(suite.T(), isTitleExists)
	assert.Equal(suite.T(), "text", val)
}

func (suite *OsAPITestSuite) checkFieldMappingResponse(body io.ReadCloser, indexName string) {
	response := suite.readBodyIntoMap(body)

	rawIndexMapping, isExists := response[indexName]
	assert.True(suite.T(), isExists)
	indexMapping := rawIndexMapping.(map[string]interface{})
	rawProperties, ok := indexMapping["mappings"]
	assert.True(suite.T(), ok)
	properties := rawProperties.(map[string]interface{})
	rawFieldName, ok := properties["title"]
	assert.True(suite.T(), ok)
	fieldName := rawFieldName.(map[string]interface{})
	rawTitle, ok := fieldName["mapping"]
	assert.True(suite.T(), ok)
	title := rawTitle.(map[string]interface{})
	rawType, ok := title["title"]
	assert.True(suite.T(), ok)
	titleType := rawType.(map[string]interface{})
	val, isTitleExists := titleType["type"]
	assert.True(suite.T(), isTitleExists)
	assert.Equal(suite.T(), "text", val)
}

func (suite *OsAPITestSuite) readBodyIntoMap(body io.ReadCloser) map[string]interface{} {
	data, err := ioutil.ReadAll(body)
	assert.Nil(suite.T(), err)
	var response map[string]interface{}
	err = json.Unmarshal(data, &response)
	assert.Nil(suite.T(), err)
	return response
}
