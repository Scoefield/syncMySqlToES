package main

import (
	"fmt"
	"github/guanhg/syncDB-search/config"
)

func main() {
	cfg := config.LoadJsonFileConfig()

	fmt.Println(cfg)
}
