package main

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"testing"
)

var (
	page = `<!DOCTYPE html>
	<html lang="en">
	<head>
	<title>test title</title>
	<meta name="description" content="test description">
	<meta charset="UTF-8">
	<meta name="viewport" content="width=device-width, initial-scale=1">
	<title>wrong title</title>
	<meta name="description" content="wrong description">
	<style>
	body {]
	  font-family: Arial, Helvetica, sans-serif;
	}
	</style>
	</head>
	<body>
	
	<h1>My Website</h1>
	<p>A website created by me.</p>

	</body>
	</html>
	
	`
	file = `{"url": "http://test.url", "state": "checking", "categories": ["test_category"], "category_another": null, "for_main_page": false, "ctime": 1564574356}`
)

type HttpClientMock struct {
}

func (*HttpClientMock) Get(url string) (resp *http.Response, err error) {
	resp = &http.Response{}
	r := ioutil.NopCloser(bytes.NewReader([]byte(page)))
	return &http.Response{
		StatusCode: 200,
		Body:       r,
	}, nil
}

type FileMock struct {
	content []byte
}

func (f *FileMock) Write(p []byte) (n int, err error) {
	t := make([]byte, len(p))
	f.content = append(f.content, t...)
	copy(f.content, p)
	return 0, nil
}

func (*FileMock) Close() error {
	return nil
}

func TestGetPageData(t *testing.T) {
	pageParser := &PageParser{reader: strings.NewReader(page), url: "http://test.url"}

	data, _ := pageParser.GetPageData()
	expected := []byte("http://test.url\ttest title\ttest description")
	if bytes.Compare(data, expected) != 0 {
		t.Errorf("results not match\nGot: %v\nExpected: %v", string(data), string(expected))
	}
}

func TestParseURLs(t *testing.T) {
	reader := strings.NewReader(file)
	downloadURLs, err := NewWorkerPool(10)
	saveToFile, err := NewWorkerPool(10)
	if err != nil {
		t.Fatal("can't create worker pools")
	}

	dataChByCategory := make(map[string](chan []byte))
	client := &HttpClientMock{}

	fileMock := &FileMock{content: make([]byte, 0)}
	openFileMock := func(name string, flag int, perm os.FileMode) (io.WriteCloser, error) {
		return fileMock, nil
	}

	parseURLs(reader, downloadURLs, saveToFile, dataChByCategory, client, openFileMock)

	expected := []byte("http://test.url\ttest title\ttest description\n")
	if bytes.Compare(fileMock.content, expected) != 0 {
		t.Errorf("results not match\nGot: %v\nExpected: %v", fileMock.content, expected)
	}
}
