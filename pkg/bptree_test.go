package pkg

import (
	"fmt"
	"strconv"
	"testing"
)

func TestAdd(t *testing.T) {
	tree := CreateBPlusTree()

	for i := 0; i < 16; i++ {
		key := strconv.Itoa(i)
		tree.Add(key, int64(i))
	}

	fmt.Println(tree)

	for _, n := range tree.nodes {
		fmt.Println(n)
	}

	fmt.Println()

	fmt.Println(tree.Find("0"))
	fmt.Println(tree.Find("14"))
	fmt.Println(tree.Find("12"))
	fmt.Println(tree.Find("6"))
}
