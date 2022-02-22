package main

import (
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/data/aztables"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/gorilla/mux"
	"html/template"
	"log"
	"net/http"
	"net/url"
)

type Server struct {
	blobClient   azblob.ServiceClient
	tableClient  *aztables.ServiceClient
	rootTemplate *template.Template
}

type Result struct {
	Success bool
	Msg     string
}

func initialize() (server *Server, err error) {
	server = &Server{}
	server.rootTemplate = template.Must(template.ParseFiles("./html/index.gohtml"))
	//blobCred, err := azblob.NewSharedKeyCredential(os.Getenv("PROG_4_AZURE_ACCOUNT"), os.Getenv("PROG_4_AZURE_KEY"))
	//if err != nil {
	//	return nil, err
	//}
	//server.blobClient, err = azblob.NewServiceClientWithSharedKey(os.Getenv("PROG_4_BLOB_URL"), blobCred, nil)
	//if err != nil {
	//	return nil, err
	//}
	//tableCred, err := aztables.NewSharedKeyCredential(os.Getenv("PROG_4_AZURE_ACCOUNT"), os.Getenv("PROG_4_AZURE_KEY"))
	//if err != nil {
	//	return nil, err
	//}
	//server.tableClient, err = aztables.NewServiceClientWithSharedKey(os.Getenv("PROG_4_TABLE_URL"), tableCred, nil)
	//if err != nil {
	//	return nil, err
	//}
	return server, nil
}

func (server *Server) resultWriter(w http.ResponseWriter, done bool, errMsg string) {
	res := Result{
		Success: done,
		Msg:     errMsg,
	}

	w.WriteHeader(http.StatusOK)
	err := server.rootTemplate.Execute(w, res)
	if err != nil {
		log.Println(err)
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
	log.Println(urlStr)
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
	server.resultWriter(w, false, "Error: "+vars["operation"])
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
		log.Println(err)
		return
	}
}

func main() {
	server, err := initialize()
	if err != nil {
		log.Fatal("Invalid credentials with error: " + err.Error())
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
