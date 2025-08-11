package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"machDB/src/internal/query"
)

func main() {
	fmt.Println("Interpreter DB CLI")
	inter := query.NewInterpreter("./data")

	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}
		if strings.ToLower(line) == "exit" {
			break
		}

		lex := query.NewLexer(line)
		parser := query.NewParser(lex)

		cmd, err := parser.ParseCommand()
		if err != nil {
			fmt.Println("Parse error:", err)
			continue
		}

		err = inter.Execute(cmd)
		if err != nil {
			fmt.Println("Error ejecutando comando:", err)
		}
	}
}
