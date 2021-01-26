package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

type URLData struct {
	URL        string   `json:"url"`
	Categories []string `json:"categories"`
}

type PageParser struct {
	reader io.Reader
	url    string
}

type TaskAdder interface {
	AddTask(func()) error
}

type HttpClient interface {
	Get(url string) (resp *http.Response, err error)
}

type OpenFileFunc func(name string, flag int, perm os.FileMode) (io.WriteCloser, error)

func (p *PageParser) GetPageData() ([]byte, error) {
	dataInBytes, err := ioutil.ReadAll(p.reader)
	title, err := p.getTitle(dataInBytes)
	if err != nil {
		return nil, err
	}

	description, err := p.getDescription(dataInBytes)

	if err != nil {
		// fmt.Println(err)
	}

	var buf bytes.Buffer

	buf.WriteString(p.url)
	buf.Write([]byte("\t"))
	buf.Write(title)
	buf.Write([]byte("\t"))
	buf.Write(description)

	return buf.Bytes(), nil
}

func (p *PageParser) getTitle(pageContent []byte) ([]byte, error) {
	return p.getPageTagValue(pageContent, []byte("<title>"), []byte("</title"),
		func(pageContent []byte, tagStartIndex int, closingTag []byte) (tagEndIndex int) {
			tagEndIndex = bytes.Index(pageContent, closingTag)
			return
		})
}

func (p *PageParser) getDescription(pageContent []byte) ([]byte, error) {
	return p.getPageTagValue(pageContent, []byte("<meta name=\"description\" content=\""), []byte(""),
		func(pageContent []byte, tagStartIndex int, closingTag []byte) (tagEndIndex int) {
			tagEndIndex = -1
			for i, b := range pageContent[tagStartIndex:] {
				if b == byte('"') {
					tagEndIndex = tagStartIndex + i
					break
				}
			}

			return
		})
}

func (p *PageParser) getPageTagValue(pageContent []byte, openingTag []byte, closingTag []byte,
	getEndIndex func([]byte, int, []byte) int) ([]byte, error) {
	// Find a substr
	tagStartIndex := bytes.Index(pageContent, openingTag)
	if tagStartIndex == -1 {
		return nil, fmt.Errorf("error parsing url %s: no %s element found", p.url, openingTag)
	}

	tagStartIndex += len(openingTag)

	// Find the index of the closing tag
	tagEndIndex := getEndIndex(pageContent, tagStartIndex, closingTag)

	if tagEndIndex == -1 {
		return nil, fmt.Errorf("error parsing url %s: no %s element found", p.url, closingTag)
	}

	if tagStartIndex >= len(pageContent) ||
		tagEndIndex >= len(pageContent) ||
		tagEndIndex < tagStartIndex {
		return nil, fmt.Errorf("error parsing url %s, tag start index %d, tag end index %d, page length%d",
			p.url, tagStartIndex, tagEndIndex, len(pageContent))
	}

	return pageContent[tagStartIndex:tagEndIndex], nil
}

type FileBatchWriter struct {
	// File       io.WriteCloser
	Data       chan []byte
	BatchSize  int
	Category   string
	SaveToFile TaskAdder
	OpenFile   OpenFileFunc
}

type OpenWriter interface {
	Write(f io.WriteCloser, data chan []byte, category string)
	Query(f io.WriteCloser, data chan []byte, category string) error
	Open(name string, flag int, perm os.FileMode) (io.WriteCloser, error)
}

func NewFileBatchWriter(wp *WorkerPool, batchSize int,
	openFile OpenFileFunc) *FileBatchWriter {
	return &FileBatchWriter{SaveToFile: wp, BatchSize: batchSize, OpenFile: openFile}
}

func (fbw *FileBatchWriter) Write(f io.WriteCloser, data chan []byte, category string) {
	var buf bytes.Buffer
	i := 0

	defer f.Close()

	for message := range data {
		fmt.Printf("%q: %q\n", category, message)
		buf.Write(message)
		buf.Write([]byte("\n"))

		i++
		if i%fbw.BatchSize == 0 {
			f.Write(buf.Bytes())
			buf.Reset()
		}
	}

	f.Write(buf.Bytes())
}

func (fbw *FileBatchWriter) Open(name string, flag int, perm os.FileMode) (io.WriteCloser, error) {
	f, err := fbw.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}

	return f, nil
}

func (fbw *FileBatchWriter) Query(f io.WriteCloser, ch chan []byte, category string) error {
	return fbw.SaveToFile.AddTask(func() {
		fbw.Write(f, ch, category)
	})
}

type URLDownloader struct {
	Client           HttpClient
	mu               *sync.Mutex
	dataChByCategory map[string](chan []byte)
}

func (downloader *URLDownloader) Download(input *URLData) ([]byte, error) {
	response, err := downloader.Client.Get(input.URL)
	if response != nil {
		defer response.Body.Close()
	}

	if err != nil {
		return nil, err
	}

	pageParser := &PageParser{reader: response.Body, url: input.URL}
	pageData, err := pageParser.GetPageData()

	if err != nil {
		return nil, err
	}

	return pageData, nil
}

func (d *URLDownloader) Process(urlData *URLData, fbw OpenWriter) error {
	fmt.Println("wtf?")
	pageData, err := d.Download(urlData)
	if err != nil {
		return err
	}

	d.QueryForSaving(urlData.Categories, pageData, fbw)
	return nil
}

func (downloader *URLDownloader) QueryForSaving(categories []string, pageData []byte, fbw OpenWriter) {
	if len(categories) == 0 {
		categories = []string{"empty"}
	}

	for _, category := range categories {
		if _, ok := downloader.dataChByCategory[category]; !ok {
			ch := make(chan []byte, 500)

			f, err := fbw.Open(fmt.Sprintf("results/%s.tsv", category), os.O_WRONLY|os.O_CREATE, 0666)
			if err != nil {
				fmt.Printf("error opening file: %v", err)
				continue
			}

			fbw.Query(f, ch, category)

			downloader.mu.Lock()
			downloader.dataChByCategory[category] = ch
			downloader.mu.Unlock()
		}

		downloader.dataChByCategory[category] <- pageData
	}
}

func main() {
	file, err := os.Open("500.jsonl")
	if err != nil {
		log.Fatalf("error while reading file: %v", err)
	}

	parseURLFromFile, err := NewWorkerPool(500)
	downloadURLs, err := NewWorkerPool(500)
	saveToFile, err := NewWorkerPool(500)

	if err != nil {
		log.Fatalf("error while starting worker pool: %v", err)
	}

	tr := &http.Transport{
		DisableKeepAlives:  true,
		IdleConnTimeout:    30 * time.Second,
		DisableCompression: true,
	}

	client := &http.Client{Timeout: time.Second * 30, Transport: tr}

	URLDownloader := &URLDownloader{Client: client,
		dataChByCategory: make(map[string](chan []byte)),
		mu:               &sync.Mutex{}}

	fbw := NewFileBatchWriter(saveToFile, 20, OpenFileFunc(func(name string, flag int, perm os.FileMode) (io.WriteCloser, error) {
		return os.OpenFile(name, flag, perm)
	}))

	process(file,
		parseURLFromFile, downloadURLs, saveToFile,
		URLDownloader, fbw)
}

func process(f io.Reader,
	parseURLFromFile *WorkerPool,
	downloadURLs *WorkerPool,
	saveToFile *WorkerPool,
	urlDownloader *URLDownloader,
	fbw *FileBatchWriter) {
	parsed := make(chan *URLData, 500)
	go parseFile(f, parseURLFromFile, parsed)

	for urlData := range parsed {
		downloadURLs.AddTask(func() {
			urlDownloader.Process(urlData, fbw)
			// fmt.Println(downloadURLs.Wg)
		})
	}

	parseURLFromFile.Wait()

	downloadURLs.Wait()

	for _, dataCh := range urlDownloader.dataChByCategory {
		close(dataCh)
	}

	saveToFile.Wait()

	saveToFile.Stop()
	downloadURLs.Stop()
	parseURLFromFile.Stop()
	fmt.Println("done")
}

func parseFile(f io.Reader, wp TaskAdder, result chan *URLData) {
	s := bufio.NewScanner(f)
	wg1 := &sync.WaitGroup{}
	for s.Scan() {
		wg1.Add(1)
		wg := &sync.WaitGroup{}
		wg.Add(1)
		wp.AddTask(func() {
			var urlData URLData
			err := json.Unmarshal(s.Bytes(), &urlData)
			wg.Done()
			if err != nil {
				wg1.Done()
				fmt.Printf("unmarshal error:%v\n", err.Error())
				return
			}

			result <- &urlData
			wg1.Done()
			// fmt.Println("done", wg1)
		})

		wg.Wait()
	}

	wg1.Wait()
	close(result)
}
