package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/data/aztables"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/gorilla/mux"
	"html/template"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

const SizeLimit int = 1 << 20

// This program always uses a table name starting with "prog4"
const tableName string = "prog4"

type Server struct {
	cleared            bool
	opMutex            sync.RWMutex
	blobClient         azblob.BlockBlobClient
	tableClient        *aztables.Client
	tableServiceClient *aztables.ServiceClient
	rootTemplate       *template.Template
	log                *Logger
}

type Result struct {
	IsQuery  bool     `json:"is_query"`
	QueryRes []string `json:"query_res"`
	Success  bool     `json:"success"`
	Msg      string   `json:"msg"`
}

func initialize() (server *Server, err error) {
	server = &Server{}

	server.log = NewLogger(os.Stdout, INFO, "")

	server.rootTemplate = template.Must(template.ParseFiles("./html/index.gohtml"))
	blobCred, err := azblob.NewSharedKeyCredential(os.Getenv("PROG_4_AZURE_ACCOUNT"), os.Getenv("PROG_4_AZURE_KEY"))
	if err != nil {
		return nil, err
	}
	server.blobClient, err = azblob.NewBlockBlobClientWithSharedKey(os.Getenv("PROG_4_BLOB_URL")+"/"+"input.txt", blobCred, nil)
	if err != nil {
		return nil, err
	}

	tableCred, err := aztables.NewSharedKeyCredential(os.Getenv("PROG_4_AZURE_ACCOUNT"), os.Getenv("PROG_4_AZURE_KEY"))
	if err != nil {
		return nil, err
	}

	server.tableServiceClient, err = aztables.NewServiceClientWithSharedKey(os.Getenv("PROG_4_TABLE_URL"), tableCred, nil)
	if err != nil {
		return nil, err
	}

	f := fmt.Sprintf("TableName ge '%s'", tableName)
	pager := server.tableServiceClient.ListTables(&aztables.ListTablesOptions{
		Filter: &f,
	})

	hasPage := pager.NextPage(context.TODO())
	if !hasPage {
		server.log.Debug("no table found, creating new table")
		server.tableClient, err = server.createNewTable()
		if err != nil {
			return nil, err
		}
	} else {
		resp := pager.PageResponse()
		server.log.Debug("length of tables found:", len(resp.Tables))
		if len(resp.Tables) == 1 {
			server.log.Debug("name of the table found:", *resp.Tables[0].TableName)
			server.tableClient = server.tableServiceClient.NewClient(*resp.Tables[0].TableName)
		} else {
			// If there are more than one table, panic
			panic("too many tables starting with tableName const")
		}
	}

	return server, nil
}

func (server *Server) createNewTable() (*aztables.Client, error) {
	name := fmt.Sprintf("%s%d", tableName, time.Now().UnixNano())
	return server.tableServiceClient.CreateTable(context.TODO(), name, nil)
}

func (server *Server) clear(res *Result) {
	server.opMutex.Lock()
	defer server.opMutex.Unlock()

	if server.cleared {
		res.Success = true
		res.Msg = "cleared blob/table"
		return
	}

	// Delete table
	_, err := server.tableClient.Delete(context.TODO(), nil)
	if err != nil {
		server.log.Error(err)
		res.Msg = "error while deleting table"
		return
	}

	// Delete file
	_, err = server.blobClient.Delete(context.TODO(), nil)
	if err != nil {
		server.log.Error(err)
		res.Msg = "error while deleting blob object"
		return
	}

	// Create new empty table to use
	server.tableClient, err = server.createNewTable()
	if err != nil {
		server.log.Error(err)
		res.Msg = "error while creating table after deletion"
		return
	}

	server.cleared = true
	res.Success = true
	res.Msg = "cleared blob/table"
	return
}

func (server *Server) load(result *Result, urlStr string) {
	server.opMutex.Lock()
	defer server.opMutex.Unlock()
	server.cleared = false

	if _, err := url.ParseRequestURI(urlStr); err != nil {
		result.Msg = "invalid url"
		return
	}

	// Uploading directly from the URL works fine too,
	// but since load func downloads the file from the URL anyways, upload it in this function.
	//_, err := server.blobClient.StartCopyFromURL(context.TODO(), urlStr, nil)
	//if err != nil {
	//	server.log.Error(err)
	//	result.Msg = err.Error()
	//	return
	//}

	//Download from the URL
	resp, err := http.Get(urlStr)
	if err != nil || resp.StatusCode != http.StatusOK {
		server.log.Error(resp.StatusCode, err)
		result.Msg = fmt.Sprintf("status code: %d ", resp.StatusCode)
		if err != nil {
			result.Msg += "error: " + err.Error()
		}
		return
	}
	defer func() {
		if err = resp.Body.Close(); err != nil {
			server.log.Error(err)
		}
	}()
	if resp.ContentLength > int64(SizeLimit) {
		result.Msg = "size limit exceeded"
		return
	}

	var fileType *string
	if c := resp.Header.Values("Content-Type"); len(c) == 1 {
		fileType = to.StringPtr(c[0])
	} else {
		fileType = to.StringPtr("application/octet-stream")
	}

	tableOptions := &aztables.InsertEntityOptions{
		UpdateMode: aztables.MergeEntity,
	}

	blobHeaders := &azblob.BlobHTTPHeaders{
		BlobContentType: fileType,
	}

	var buffer bytes.Buffer
	scanner := bufio.NewScanner(io.TeeReader(resp.Body, &buffer))
	for scanner.Scan() {
		if buffer.Len() > SizeLimit {
			result.Msg = "size limit exceeded"
			return
		}

		tmpStr := scanner.Text()
		fields := strings.Fields(tmpStr)
		if len(fields) < 2 {
			server.log.Debug("invalid line:", tmpStr)
			continue
		}

		lastName := fields[0]
		firstName := fields[1]

		newPerson := aztables.EDMEntity{
			Entity: aztables.Entity{
				PartitionKey: lastName,
				RowKey:       firstName,
			},
			Properties: make(map[string]interface{}),
		}

		// Add any attributes (if any)
		for _, fragment := range fields[2:] {
			attribute := strings.SplitN(fragment, "=", 2)
			if len(attribute) != 2 {
				server.log.Debug(fragment)
				continue
			}
			newPerson.Properties[attribute[0]] = attribute[1]
		}

		// Convert to bytes
		newPersonBytes, err := json.Marshal(newPerson)
		if err != nil {
			server.log.Debug(err)
			continue
		}

		// Add or update entity
		_, err = server.tableClient.InsertEntity(context.TODO(), newPersonBytes, tableOptions)
		if err != nil {
			server.log.Debug(err)
			continue
		}

		server.log.Debug("Processed: ", tmpStr)
	}

	// UploadStreamToBlockBlob cannot be used until it's fixed
	// (To set the header, UploadStreamToBlockBlobOptions must be used but the SDK does not leverage it.)
	//_, err = server.blobClient.UploadStreamToBlockBlob(context.TODO(), &buffer, azblob.UploadStreamToBlockBlobOptions{
	//	HTTPHeaders: blobHeaders,
	//})

	_, err = server.blobClient.UploadBufferToBlockBlob(context.TODO(), buffer.Bytes(), azblob.HighLevelUploadToBlockBlobOption{
		HTTPHeaders: blobHeaders,
	})
	if err != nil {
		server.log.Error(err)
		result.Msg = "error while uploading to a blob"
		return
	}
	result.Success = true
	result.Msg = "loaded blob/table. Blob can be found <a href=\"" + server.blobClient.URL() + "\">here</a>"
	return
}

func (server *Server) query(result *Result, pk string, rk string) {
	server.opMutex.RLock()
	defer server.opMutex.RUnlock()

	var filter string
	result.IsQuery = true
	if pk != "" && rk != "" {
		filter = fmt.Sprintf("PartitionKey eq '%s' and RowKey eq '%s'", pk, rk)
	} else if pk != "" {
		filter = fmt.Sprintf("PartitionKey eq '%s'", pk)
	} else if rk != "" {
		filter = fmt.Sprintf("RowKey eq '%s'", rk)
	} else {
		result.Msg = "first and/or last name must be provided"
		return
	}

	q := server.tableClient.List(&aztables.ListEntitiesOptions{
		Filter: &filter,
	})

	// TODO: add page support
	hasPage := q.NextPage(context.TODO())
	if !hasPage {
		result.Success = true
		result.Msg = "no match found"
		return
	}
	for hasPage {
		var entity aztables.EDMEntity
		result.QueryRes = make([]string, len(q.PageResponse().Entities))
		for i, entityBytes := range q.PageResponse().Entities {
			if err := json.Unmarshal(entityBytes, &entity); err != nil {
				server.log.Debug("error unmarshalling result")
				continue
			}
			server.log.Debugf("pk: %s\trk: %s", entity.PartitionKey, entity.RowKey)
			result.QueryRes[i] = entity.PartitionKey + " " + entity.RowKey
			for key, val := range entity.Properties {
				result.QueryRes[i] += " " + key + "=" + val.(string)
			}
		}
		hasPage = q.NextPage(context.TODO())
	}

	result.Success = true
	return
}

func (server *Server) clearHandler(w http.ResponseWriter, _ *http.Request) {
	server.log.Debug("/clear called")
	result := Result{}
	defer func() {
		data, err := json.Marshal(&result)
		if err != nil {
			data = []byte{}
		}
		_, _ = w.Write(data)
	}()

	server.clear(&result)
}

func (server *Server) loadHandler(w http.ResponseWriter, r *http.Request) {
	server.log.Debug("/load called")
	result := Result{}
	defer func() {
		data, err := json.Marshal(&result)
		if err != nil {
			data = []byte{}
		}
		_, _ = w.Write(data)
	}()

	rawUrlStr := r.PostFormValue("url")
	if len(rawUrlStr) == 0 {
		result.Msg = "url cannot be empty"
		return
	}

	u, err := url.ParseRequestURI(rawUrlStr)
	if err != nil {
		result.Msg = "url is invalid"
		return
	}

	server.load(&result, u.String())
}

func (server *Server) queryHandler(w http.ResponseWriter, r *http.Request) {
	server.log.Debug("/q called")
	result := Result{
		IsQuery: true,
	}
	defer func() {
		data, err := json.Marshal(&result)
		if err != nil {
			data = []byte{}
		}
		_, _ = w.Write(data)
	}()

	vars := mux.Vars(r)
	server.query(&result, vars["first"], vars["last"])
}

func (server *Server) rootHandler(w http.ResponseWriter, _ *http.Request) {
	server.log.Debug("/ called")
	w.WriteHeader(http.StatusOK)
	err := server.rootTemplate.Execute(w, nil)
	if err != nil {
		server.log.Debug(err)
		return
	}
}

func main() {
	server, err := initialize()
	if err != nil {
		Fatal("Invalid credentials with error: " + err.Error())
	}

	r := mux.NewRouter()

	r.HandleFunc("/", server.rootHandler).Methods(http.MethodGet)
	r.HandleFunc("/q", server.queryHandler).Methods(http.MethodGet).Queries("first", "{first}", "last", "{last}")
	r.HandleFunc("/load", server.loadHandler).Methods(http.MethodPost)
	r.HandleFunc("/clear", server.clearHandler).Methods(http.MethodPost)

	_ = http.ListenAndServe(":8000", r)
}
