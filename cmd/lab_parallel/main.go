package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type BlockingQueue struct {
	Data chan []byte
}

const ChunkSize = 1024 * 10

func Producer(q *BlockingQueue, inputFile string) error {
	const op = "main.Producer"

	file, err := os.Open(inputFile)
	if err != nil {
		return fmt.Errorf("%s %v", op, err)
	}

	reader := bufio.NewReader(file)

	buf := make([]byte, ChunkSize)
	for {
		n, err := reader.Read(buf)
		if n == 0 {
			break
		}

		if err != nil {
			return fmt.Errorf("%s %v", op, err)
		}

		chunk := make([]byte, n)
		copy(chunk, buf[:n])
		q.Data <- chunk

	}

	close(q.Data)

	return nil
}

func Consumer(results chan []byte, q *BlockingQueue, wg *sync.WaitGroup, id int) {
	defer wg.Done()

	for row := range q.Data {
		fmt.Printf("[%s] Consumer #%d: Processing chunk of size %d bytes\n", time.Now().Format("15:04:05.000"), id, len(row))

		processed := []byte(strings.ToUpper(string(row)))

		results <- processed
		fmt.Printf("[%s] Consumer #%d: Finished processing chunk\n", time.Now().Format("15:04:05.000"), id)
	}
}

func Writer(outputFile string, results chan []byte, done chan bool) {
	file, err := os.Create(outputFile)
	if err != nil {
		fmt.Printf("failed to create output file: %v\n", err)
		done <- false
		return
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for result := range results {
		_, _ = writer.Write(result)
	}
	writer.Flush()
	done <- true

}

func main() {
	client := http.Client{}
	resp, err := client.Get("https://ru.wikipedia.org/wiki/Ядро_операционной_системы")
	if err != nil {
		log.Fatalf("Failed to get url %v", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("failed to read response body %v", err)
	}

	inputFile := "input.txt"

	outputFile := "output.txt"

	file, err := os.Create(inputFile)
	if err != nil {
		log.Fatalf("Failed to create file %v", err)
	}

	file.Write(data)

	fileSize, err := file.Stat()
	if err != nil {
		log.Fatalf("Failed to create file %v", err)
	}

	cntBlocks := fileSize.Size() / ChunkSize
	fmt.Println(cntBlocks)

	blockQ := BlockingQueue{
		Data: make(chan []byte, cntBlocks),
	}

	done := make(chan bool)

	go func() {
		if err := Producer(&blockQ, inputFile); err != nil {
			fmt.Printf("error while producing data %v\n", err)
		}
	}()

	wg := &sync.WaitGroup{}

	results := make(chan []byte)

	numConsumers := 4
	for i := 0; i < numConsumers; i++ {
		wg.Add(1)
		go Consumer(results, &blockQ, wg, i)
	}

	go Writer(outputFile, results, done)

	go func() {
		wg.Wait()
		close(results)
	}()

	if <-done {
		fmt.Println("File processing complete.")
	} else {
		fmt.Println("File processing failed.")
	}

}
