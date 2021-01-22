package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
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

	dataChByCategory := make(map[string](chan string))

	s := bufio.NewScanner(f)

	tr := &http.Transport{
		DisableKeepAlives:  true,
		IdleConnTimeout:    30 * time.Second,
		DisableCompression: true,
	}

	client := &http.Client{Timeout: time.Second * 30, Transport: tr}

	mu := &sync.Mutex{}
	for s.Scan() {
		var urlData URLData
		err := json.Unmarshal(s.Bytes(), &urlData)
		if err != nil {
			fmt.Printf("%v\n", err.Error())
			continue
		}

		downloadURLs.AddTask(func() {
			response, err := client.Get(urlData.URL)
			if response != nil {
				defer response.Body.Close()
			}

			if err != nil {
				fmt.Println(err.Error())
				return
			}

			pageTitle, err := getPageTitle(response, urlData.URL)

			if err != nil {
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

func listen(category string, subscriber chan string) {
	var sb strings.Builder
	i := 0
	f, err := os.OpenFile(fmt.Sprintf("results/%s.tsv", category), os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {

	}
	for message := range subscriber {
		// fmt.Printf("%q: %q\n", category, message)
		sb.WriteString(fmt.Sprintf("%s\n", message))
		i++
		if i%20 == 0 {
			f.WriteString(sb.String())
			sb.Reset()
		}
	}

	f.WriteString(sb.String())
}

func getPageTitle(response *http.Response, url string) (string, error) {
	dataInBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return "", err
	}

	pageContent := string(dataInBytes)

	// Find a substr
	titleStartIndex := strings.Index(pageContent, "<title>")
	if titleStartIndex == -1 {
		fmt.Println("No title element found")
		return "", errors.New("No title element found")
	}

	titleStartIndex += 7

	// Find the index of the closing tag
	titleEndIndex := strings.Index(pageContent, "</title>")

	if titleEndIndex == -1 {
		fmt.Println("No closing tag for title found.")
		return "", errors.New("no closing tag for title found")
	}

	if titleStartIndex >= len(pageContent) ||
		titleEndIndex >= len(pageContent) ||
		titleEndIndex < titleStartIndex {
		return "", fmt.Errorf("error while parsing url %s, %d %d %d", url, titleStartIndex, titleEndIndex, len(pageContent))
	}

	return pageContent[titleStartIndex:titleEndIndex], nil
}

func createChannelIfNotExist(dataChByCategory map[string](chan string), category string, wp *WorkerPool, mu *sync.Mutex) {
	if _, ok := dataChByCategory[category]; !ok {
		ch := make(chan string, 100)
		category := category

		wp.AddTask(func() { listen(category, ch) })
		mu.Lock()
		dataChByCategory[category] = ch
		mu.Unlock()
	}
}
