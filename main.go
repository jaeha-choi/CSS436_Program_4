package main

import (
	"github.com/gorilla/mux"
	"net/http"
)

//func initialize() {
//	// Create a default Azure credential
//	credential, err := azidentity.NewDefaultAzureCredential(nil)
//	if err != nil {
//		log.Fatal("Invalid credentials with error: " + err.Error())
//	}
//	blobClient, err := azblob.NewServiceClient("", credential, nil)
//	if err != nil {
//		log.Fatal("Invalid credentials with error: " + err.Error())
//	}
//	tableClient, err := aztables.NewServiceClient("", credential, nil)
//	if err != nil {
//		log.Fatal("Invalid credentials with error: " + err.Error())
//	}
//}

func TestHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("first name: " + vars["first"] + " "))
	_, _ = w.Write([]byte("last name: " + vars["last"]))
}

func main() {
	r := mux.NewRouter()
	r.HandleFunc("/", TestHandler).Methods("GET").Queries("first", "{first}", "last", "{last}")
	r.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("Hello"))
	}).Methods("GET")
	_ = http.ListenAndServe(":8000", r)
}
