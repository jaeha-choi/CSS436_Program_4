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
)

const SizeLimit int = 1 << 20

type Server struct {
	blobClient   azblob.BlockBlobClient
	tableClient  *aztables.Client
	rootTemplate *template.Template
	log          *Logger
}

type Result struct {
	Success bool
	Msg     string
}

func initialize() (server *Server, err error) {
	server = &Server{}

	server.log = NewLogger(os.Stdout, DEBUG, "")

	server.rootTemplate = template.Must(template.ParseFiles("./html/index.gohtml"))
	blobCred, err := azblob.NewSharedKeyCredential(os.Getenv("PROG_4_AZURE_ACCOUNT"), os.Getenv("PROG_4_AZURE_KEY"))
	if err != nil {
		return nil, err
	}
	server.blobClient, err = azblob.NewBlockBlobClientWithSharedKey(os.Getenv("PROG_4_BLOB_URL")+"/"+"index.txt", blobCred, nil)
	if err != nil {
		return nil, err
	}
	tableCred, err := aztables.NewSharedKeyCredential(os.Getenv("PROG_4_AZURE_ACCOUNT"), os.Getenv("PROG_4_AZURE_KEY"))
	if err != nil {
		return nil, err
	}
	server.tableClient, err = aztables.NewClientWithSharedKey(os.Getenv("PROG_4_TABLE_URL"), tableCred, nil)
	if err != nil {
		return nil, err
	}

	return server, nil
}

// DONE
func (server *Server) clear() (errStr string) {
	// Delete table
	_, err := server.tableClient.Delete(context.TODO(), nil)
	if err != nil {
		server.log.Error(err)
		return "error while deleting table"
	}

	// Create empty table
	_, err = server.tableClient.Create(context.TODO(), nil)
	if err != nil {
		server.log.Error(err)
		return "error while creating table after deletion"
	}

	_, err = server.blobClient.Delete(context.TODO(), nil)
	if err != nil {
		server.log.Error(err)
		return "error while deleting blob object"
	}

	return ""
}

// DONE
func (server *Server) load(urlStr string) (errStr string) {
	//Download from the URL
	resp, err := http.Get(urlStr)
	if err != nil || resp.StatusCode != http.StatusOK {
		server.log.Error(resp.StatusCode, err)
		return err.Error()
	}
	defer func() {
		if err = resp.Body.Close(); err != nil {
			server.log.Error(err)
		}
	}()
	if resp.ContentLength > int64(SizeLimit) {
		return "size limit exceeded"
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
			return "size limit exceeded"
		}

		tmpStr := scanner.Text()
		fields := strings.Fields(tmpStr)
		if len(fields) < 2 {
			server.log.Debug("invalid line:", tmpStr)
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

		// Add any attributes
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
		return err.Error()
	}

	return ""
}

func (server *Server) resultWriter(w http.ResponseWriter, done bool, errMsg string) {
	res := Result{
		Success: done,
		Msg:     errMsg,
	}

	w.WriteHeader(http.StatusOK)
	err := server.rootTemplate.Execute(w, res)
	if err != nil {
		server.log.Error(err)
	}
}

func (server *Server) clearHandler(w http.ResponseWriter, r *http.Request) {
	// TODO: do stuff
	http.Redirect(w, r, "/success?o=clear", http.StatusPermanentRedirect)
}

func (server *Server) loadHandler(w http.ResponseWriter, r *http.Request) {
	var redirectUrl string
	defer func() {
		http.Redirect(w, r, redirectUrl, http.StatusPermanentRedirect)
	}()

	rawUrlStr := r.PostFormValue("url")
	if len(rawUrlStr) == 0 {
		redirectUrl = "/error?o=load"
		return
	}

	u, err := url.ParseRequestURI(rawUrlStr)
	if err != nil {
		redirectUrl = "/error?o=load"
		return
	}

	urlStr := u.String()
	//TODO: do stuff
	server.log.Debugf(urlStr)
	redirectUrl = "/success?o=load"
}

func (server *Server) queryHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	//TODO: do stuff
	fmt.Println("first:", vars["first"])
	fmt.Println("last:", vars["last"])
	server.resultWriter(w, true, "Query result...")
}

func (server *Server) errorHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		http.Redirect(w, r, "/", http.StatusPermanentRedirect)
		return
	}
	vars := mux.Vars(r)
	server.resultWriter(w, false, vars["operation"])
}

func (server *Server) successHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		http.Redirect(w, r, "/", http.StatusPermanentRedirect)
		return
	}
	vars := mux.Vars(r)
	server.resultWriter(w, true, "Successful "+vars["operation"])
}

func (server *Server) rootHandler(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	res := Result{
		Success: true,
		Msg:     "",
	}
	err := server.rootTemplate.Execute(w, res)
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
	r.HandleFunc("/success", server.successHandler).Methods(http.MethodGet, http.MethodPost).Queries("o", "{operation}")
	r.HandleFunc("/error", server.errorHandler).Methods(http.MethodGet, http.MethodPost).Queries("o", "{operation}")

	_ = http.ListenAndServe(":8000", r)
}
