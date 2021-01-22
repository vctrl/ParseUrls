package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
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

	s := bufio.NewScanner(f)

	tr := &http.Transport{
		DisableKeepAlives:  true,
		IdleConnTimeout:    30 * time.Second,
		DisableCompression: true,
	}

	client := &http.Client{Timeout: time.Second * 30, Transport: tr}

	mu := &sync.Mutex{}

	i := 0
	for s.Scan() && i < 10 {
		i++
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
				fmt.Printf("http error:%v", err.Error())
				return
			}

			pageTitle, err := getPageTitle(response, urlData.URL)

			if err != nil {
				fmt.Printf("error parsing page:%v", err.Error())
				return
			}

			var categories []string

			if len(urlData.Categories) == 0 {
				categories = []string{"empty"}
			} else {
				categories = urlData.Categories
			}

			for _, category := range categories {
				createChannelIfNotExist(dataChByCategory, category, saveToFile, mu)
				dataChByCategory[category] <- pageTitle
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

func listen(category string, subscriber chan []byte) {
	var buf bytes.Buffer
	i := 0
	f, err := os.OpenFile(fmt.Sprintf("results/%s.tsv", category), os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		fmt.Printf("error opening file: %v", err)
		return
	}

	defer f.Close()

	for message := range subscriber {
		// fmt.Printf("%q: %q\n", category, message)
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

func getPageTitle(response *http.Response, url string) ([]byte, error) {
	dataInBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	pageContent := dataInBytes

	// Find a substr
	titleStartIndex := bytes.Index(pageContent, []byte("<title>"))
	if titleStartIndex == -1 {
		fmt.Println("No title element found")
		return nil, fmt.Errorf("error parsing url %s: no title element found", url)
	}

	titleStartIndex += 7

	// Find the index of the closing tag
	titleEndIndex := bytes.Index(pageContent, []byte("</title>"))

	if titleEndIndex == -1 {
		fmt.Println("No closing tag for title found.")
		return nil, fmt.Errorf("error parsing url %s: no closing tag for title found", url)
	}

	if titleStartIndex >= len(pageContent) ||
		titleEndIndex >= len(pageContent) ||
		titleEndIndex < titleStartIndex {
		return nil, fmt.Errorf("error parsing url %s, title tag start index %d, title tag end index %d, page length%d",
			url, titleStartIndex, titleEndIndex, len(pageContent))
	}

	return pageContent[titleStartIndex:titleEndIndex], nil
}

func createChannelIfNotExist(dataChByCategory map[string](chan []byte), category string, wp *WorkerPool, mu *sync.Mutex) {
	if _, ok := dataChByCategory[category]; !ok {
		ch := make(chan []byte, 100)
		category := category

		wp.AddTask(func() { listen(category, ch) })
		mu.Lock()
		dataChByCategory[category] = ch
		mu.Unlock()
	}
}
