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
		fmt.Println(err)
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

func main() {
	f, err := os.Open("500.jsonl")
	if err != nil {
		log.Fatalf("error while reading file: %v", err)
	}

	downloadURLs, err := NewWorkerPool(500)
	saveToFile, err := NewWorkerPool(500)

	if err != nil {
		log.Fatalf("error while starting worker pool: %v", err)
	}

	dataChByCategory := make(map[string](chan []byte))

	tr := &http.Transport{
		DisableKeepAlives:  true,
		IdleConnTimeout:    30 * time.Second,
		DisableCompression: true,
	}

	client := &http.Client{Timeout: time.Second * 30, Transport: tr}

	openFile := func(name string, flag int, perm os.FileMode) (io.WriteCloser, error) {
		return os.OpenFile(name, os.O_WRONLY|os.O_CREATE, 0666)
	}

	parseURLs(f, downloadURLs, saveToFile, dataChByCategory, client, openFile)
}

func parseURLs(f io.Reader,
	downloadURLs *WorkerPool,
	saveToFile *WorkerPool,
	dataChByCategory map[string](chan []byte),
	client HttpClient,
	openFile OpenFileFunc) {
	mu := &sync.Mutex{}
	s := bufio.NewScanner(f)
	for s.Scan() {
		var urlData URLData
		err := json.Unmarshal(s.Bytes(), &urlData)
		if err != nil {
			fmt.Printf("unmarshal error:%v\n", err.Error())
			continue
		}

		downloadURLs.AddTask(func() {
			response, err := client.Get(urlData.URL)
			if response != nil {
				defer response.Body.Close()
			}

			if err != nil {
				fmt.Printf("http error: %v", err.Error())
				return
			}

			pageParser := &PageParser{reader: response.Body, url: urlData.URL}
			pageData, err := pageParser.GetPageData()

			if err != nil {
				fmt.Println(err.Error())
				return
			}

			var categories []string

			if len(urlData.Categories) == 0 {
				categories = []string{"empty"}
			} else {
				categories = urlData.Categories
			}

			for _, category := range categories {
				f, err := openFile(fmt.Sprintf("results/%s.tsv", category), os.O_WRONLY|os.O_CREATE, 0666)
				if err != nil {
					fmt.Printf("error opening file: %v", err)
					continue
				}

				createChannelIfNotExist(f, dataChByCategory, category, saveToFile, mu)
				dataChByCategory[category] <- pageData
			}
		})
	}

	downloadURLs.Wait()

	for _, dataCh := range dataChByCategory {
		close(dataCh)
	}

	saveToFile.Wait()

	downloadURLs.Stop()
	saveToFile.Stop()
}

func createChannelIfNotExist(f io.WriteCloser, dataChByCategory map[string](chan []byte), category string, wp *WorkerPool, mu *sync.Mutex) {
	if _, ok := dataChByCategory[category]; !ok {
		ch := make(chan []byte, 100)
		category := category

		wp.AddTask(func() { listen(f, category, ch) })
		mu.Lock()
		dataChByCategory[category] = ch
		mu.Unlock()
	}
}

func listen(f io.WriteCloser, category string, subscriber chan []byte) {
	var buf bytes.Buffer
	i := 0

	defer f.Close()

	for message := range subscriber {
		fmt.Printf("%q: %q\n", category, message)
		buf.Write(message)
		buf.Write([]byte("\n"))

		i++
		if i%20 == 0 {
			f.Write(buf.Bytes())
			buf.Reset()
		}
	}

	f.Write(buf.Bytes())
}
