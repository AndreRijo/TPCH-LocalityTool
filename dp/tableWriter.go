package dp

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	colSeparator = '|'
)

//Table is only one file
func WriteTable(fileLoc string, table [][]string) {
	locParts := strings.Split(fileLoc, "/")
	fullLoc := ""
	for i := 0; i < len(locParts)-1; i++ {
		fullLoc += locParts[i] + "/"
	}
	os.MkdirAll(fullLoc, 0777)

	file, err := os.Create(fileLoc)
	if err == nil {
		writer := bufio.NewWriter(file)
		var sb strings.Builder
		for _, line := range table {
			writeLine(writer, sb, line)
		}
		writer.Flush()
	} else {
		fmt.Println("[TableWriter]Error writting table to", fileLoc)
	}
	file.Close()
}

func writeLine(writer *bufio.Writer, sb strings.Builder, line []string) {
	if len(line) == 0 {
		return
	}
	for i := 0; i < len(line); i++ {
		sb.WriteString(line[i])
		sb.WriteRune('|')
	}
	sb.WriteString("\n")
	writer.WriteString(sb.String())
	sb.Reset()
}

//Updates are multiple small files
func WriteUpdates(fileLoc string, nOrders, nUpdateFiles int, itemsTable [][][]string) {
	locParts := strings.Split(fileLoc, "/")
	fullLoc := ""
	for i := 0; i < len(locParts)-1; i++ {
		fullLoc += locParts[i] + "/"
	}
	os.MkdirAll(fullLoc, 0777)

	//Just split equally the number of orders. Write the lineitems of said orders right away too.
	//We do not need to re-write orders though... hmm... maybe read orders files again and write items?
	//I believe we can just divide nOrders/nFiles and write the items right away.
	perFile := nOrders / nUpdateFiles
	nFile := int64(1)
	fmt.Println("NOrders:", nOrders, "NUpdateFiles:", nUpdateFiles, "PerFile:", perFile)
	for i := 0; i < nOrders; i += perFile {
		if nFile%int64(nUpdateFiles/10) == 0 {
			fmt.Printf("%d out of %d update files have been written.\n", nFile, nUpdateFiles)
		}
		writeItemUpdsFile(fileLoc+strconv.FormatInt(nFile, 10), itemsTable[i:i+perFile])
		nFile++
	}
}

func writeItemUpdsFile(fileLoc string, items [][][]string) {
	file, err := os.Create(fileLoc)
	if err == nil {
		writer := bufio.NewWriter(file)
		var sb strings.Builder
		for _, orderItems := range items {
			for _, item := range orderItems {
				writeLine(writer, sb, item)
			}
		}
		writer.Flush()
	} else {
		fmt.Println("[TableWriter]Error writting update file to", fileLoc, "(err:", err, ")")
	}
	file.Close()
}
