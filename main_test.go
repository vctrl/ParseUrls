package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
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

type WorkerPoolMock struct {
}

func (*WorkerPoolMock) AddTask(callback func()) error {
	callback()
	return nil
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

type FileBatchWriterMock struct {
}

func (fbw *FileBatchWriterMock) Write(f io.WriteCloser, data chan []byte, category string) {

}

func (fbw *FileBatchWriterMock) Open(name string, flag int, perm os.FileMode) (io.WriteCloser, error) {
	return nil, nil
}

func (fbw *FileBatchWriterMock) Query(f io.WriteCloser, ch chan []byte, category string) error {
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

func TestParseFile(t *testing.T) {
	reader := strings.NewReader(file)
	result := make(chan *URLData, 10)
	parseFile(reader, &WorkerPoolMock{}, result)

	expected := &URLData{URL: "http://test.url", Categories: []string{"test_category"}}
	fact := <-result
	if !reflect.DeepEqual(expected, fact) {
		t.Errorf("expected %v, got %v", expected, fact)
	}
}

func TestGroupByCategories(t *testing.T) {
	category1 := "test_category1"
	category2 := "test_category2"

	result := make(map[string](chan []byte))
	client := &HttpClientMock{}
	urlDownloader := &URLDownloader{Client: client, mu: &sync.Mutex{}, dataChByCategory: result}
	input1 := &URLData{URL: "http://test.url", Categories: []string{category1}}
	urlDownloader.Process(input1, &FileBatchWriterMock{})
	input2 := &URLData{URL: "http://test.url", Categories: []string{category1}}
	urlDownloader.Process(input2, &FileBatchWriterMock{})
	input3 := &URLData{URL: "http://test.url", Categories: []string{category2}}
	urlDownloader.Process(input3, &FileBatchWriterMock{})

	for _, ch := range result {
		close(ch)
	}

	i := 0
	for range result[category1] {
		i++
	}

	if i != 2 {
		t.Errorf("expected 2, but found %d in %s", i, category1)
	}

	i = 0
	for range result["test_category2"] {
		i++
	}

	if i != 1 {
		t.Errorf("expected 1, but found %d in %s", i, category2)
	}
}

func TestBatchWriter(t *testing.T) {
	f := &FileMock{}

	input := make(chan []byte, 10)
	fbw := &FileBatchWriter{BatchSize: 5, Category: "test_category", SaveToFile: &WorkerPoolMock{}}

	// wg for first 4 messages
	wg := sync.WaitGroup{}

	// wg1 for the last message
	wg1 := sync.WaitGroup{}
	wg1.Add(1)

	wg.Add(4)
	go func() {
		for i := 0; i < 4; i++ {
			input <- []byte(fmt.Sprintf("test_message %d", i))
			wg.Done()
		}

		time.Sleep(time.Second * 2)

		input <- []byte(fmt.Sprintf("test message 4"))
		time.Sleep(time.Second)
		wg1.Done()
	}()

	go func() {
		fbw.Write(f, input, "test_category")
	}()

	wg.Wait()

	// nothing
	if len(f.content) > 0 {
		t.Errorf("file should be empty, but was %v", f.content)
	}

	wg1.Wait()

	expected := []byte("test_message 0\ntest_message 1\ntest_message 2\ntest_message 3\ntest message 4\n")

	// one more message
	if !reflect.DeepEqual(f.content, expected) {
		t.Errorf("expected: %v, but was: %v", f.content, expected)
	}

	close(input)
}

func TestParseURLs(t *testing.T) {
	reader := strings.NewReader(file)

	parseURLFromFile, err := NewWorkerPool(10)
	downloadURLs, err := NewWorkerPool(10)
	saveToFile, err := NewWorkerPool(10)

	if err != nil {
		t.Fatal("can't create worker pools")
	}

	client := &HttpClientMock{}

	fileMock := &FileMock{content: make([]byte, 0)}

	URLDownloader := &URLDownloader{Client: client,
		dataChByCategory: make(map[string](chan []byte)),
		mu:               &sync.Mutex{},
	}

	openFileMock := func(name string, flag int, perm os.FileMode) (io.WriteCloser, error) {
		return fileMock, nil
	}

	fbw := NewFileBatchWriter(saveToFile, 20, openFileMock)

	process(reader,
		parseURLFromFile, downloadURLs, saveToFile,
		URLDownloader, fbw)

	expected := []byte("http://test.url\ttest title\ttest description\n")
	if bytes.Compare(fileMock.content, expected) != 0 {
		t.Errorf("results not match\nGot: %v\nExpected: %v", fileMock.content, expected)
	}
}
