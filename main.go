package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"syscall"
	"os/signal"
)

type Node struct {
	data  []string
	left  *Node
	right *Node
}
type Tree struct {
	root *Node
}

var (
	sorted         [][]string
	dir            = flag.String("d", "", "Specifies a directory where it must read input files from")
	inputFileName  = flag.String("i", "", "Use a file with the name file-name as an input")
	outputFileName = flag.String("o", "", "Use a file with the name file-name as an output")
	headerFlag     = flag.Bool("h", false, "Remove headers from sorting")
	reverseFlag    = flag.Bool("r", false, "Sort input lines in reverse order")
	fieldFlag      = flag.Int("f", 0, "Sort input lines by value number N")
	algorithmFlag  = flag.Int("a", 1, "Sorting algorithm: 1 - built in, 2 - Tree Sort")
)

func main() {
	sigchnl := make(chan os.Signal, 1)
	signal.Notify(sigchnl)
	go func() {
		for {
			s := <-sigchnl
			handler(s)
		}
	}()
	
	contChan := make(chan []string)
	flag.Parse()

	if isFlagPassed("d") && isFlagPassed("i") {
		log.Fatal("ERROR: You can't use -d and -i flags at the same time")
	} else if isFlagPassed("d") {
		fnChan := readDir(dir)
		contChan = fileReadinStage(fnChan, 3)
	} else {
		contChan = input()
	}

	sortContent(contChan, *headerFlag, *fieldFlag, *reverseFlag, *algorithmFlag)
	output(sorted)
}

func handler(signal os.Signal) {
	if signal == syscall.SIGTERM {
		fmt.Println("Got kill signal. ")
		fmt.Println("Program will terminate now.")
		os.Exit(0)
	} else if signal == syscall.SIGINT {
		fmt.Println("Got CTRL+C signal.")
		fmt.Println("Closing.")
		os.Exit(0)
	} else {
		fmt.Println("Ignoring signal: ", signal)
	}
}

func readDir(dir *string) chan string {
	fnames := make(chan string)
	go func() {
		if *dir != "" {
			files, err := os.ReadDir(*dir)
			if err != nil {
				log.Fatal(err)
			}
			for _, file := range files {
				fnames <- file.Name()
			}
		}
		close(fnames)
	}()
	return fnames
}

func fileReadinStage(fnames chan string, n int) (allLines chan []string) {
	lines := make([]chan []string, n)
	allLines = make(chan []string)

	// process files with n goroutines
	for i := 0; i < n; i++ {
		readFiles(fnames, lines[i])
	}
	wg := &sync.WaitGroup{}
	for i := range lines {
		wg.Add(1)
		go func(ch chan []string) {
			for line := range ch {
				allLines <- line
			}
			wg.Done()
		}(lines[i])
	}
	go func() {
		wg.Wait()
		close(allLines)
	}()

	return allLines
}

func readFiles(fnames chan string, lines chan []string) {
	lines = make(chan []string)
	go func() {
		for fn := range fnames {
			f, err := os.Open(fn)
			if err != nil {
				log.Fatal(err)
			}
			content := readContent(f)
			for _, line := range content {
				fmt.Println(line)
				lines <- line
			}
		}
	}()
}

func isFlagPassed(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

func input() chan []string {
	var readfrom *os.File
	if isFlagPassed("i") {
		f, err := os.Open(*inputFileName)
		if err != nil {
			log.Fatal(err)
		}
		readfrom = f
	} else {
		readfrom = os.Stdin
	}

	content := readContent(readfrom)
	lines := make(chan []string)

	go func() {
		for _, line := range content {
			lines <- line
		}
		close(lines)
	}()

	return lines
}

func output(text [][]string) {
	if isFlagPassed("o") {
		f, err := os.Create(*outputFileName)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Fprintln(f, text)
		fmt.Printf("Output is written to file %s\n", *outputFileName)
		defer f.Close()
	} else {
		fmt.Printf("Result: %v\n", text)
	}
}

func readContent(readfrom *os.File) (content [][]string) {
	n := 0
	s := bufio.NewScanner(readfrom)

	if s.Err() != nil {
		log.Fatal(s.Err())
	}

	for s.Scan() {
		line := s.Text()
		row := strings.Split(line, ",")
		if line == "" {
			break
		}
		if n == 0 {
			n = len(row)
		}
		if n != len(row) {
			log.Fatal("ERROR: The number of columns is not equal to the number of rows")
		}
		content = append(content, row)
	}
	return content
}

func sortContent(contentCh chan []string, header bool, field int, reverse bool, sortAlgorithm int) {
	buff := [][]string{}

	for line := range contentCh {
		buff = append(buff, line)
	}

	h := 0
	if header {
		h = 1
	}
	switch sortAlgorithm {
	case 1:
		sort.Slice(buff[h:], func(i, j int) bool {
			if reverse {
				return buff[i+h][field] > buff[j+h][field]
			}
			return buff[i+h][field] < buff[j+h][field]
		})
		sorted = buff
	case 2:
		// tree sort
		t := &Tree{}
		for i := h; i < len(buff); i++ {
			t.insert(buff[i], field)
		}
		t.root.rewriteTree()
	}
}

func (t *Tree) insert(data []string, field int) *Tree {
	if t.root == nil {
		t.root = &Node{data: data, left: nil, right: nil}
	} else {
		t.root.insert(data, field)
	}
	return t
}

func (n *Node) insert(data []string, field int) {
	if n == nil {
		return
	} else if data[field] <= n.data[field] {
		if n.left == nil {
			n.left = &Node{data: data, left: nil, right: nil}
		} else {
			n.left.insert(data, field)
		}
	} else {
		if n.right == nil {
			n.right = &Node{data: data, left: nil, right: nil}
		} else {
			n.right.insert(data, field)
		}
	}
}

func (node *Node) rewriteTree() {
	if node.left != nil {
		node.left.rewriteTree()
	}
	sorted = append(sorted, node.data)
	if node.right != nil {
		node.right.rewriteTree()
	}
}
