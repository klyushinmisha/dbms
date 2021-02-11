package pkg

import (
	"log"
	"os"
	"testing"
)

func fileScopedExec(name string, exec func(*os.File) error) error {
	file, err := os.Create(name)
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		closeErr := file.Close()
		if closeErr != nil {
			log.Fatalln(closeErr)
		}
		removeErr := os.Remove(name)
		if removeErr != nil {
			log.Fatalln(removeErr)
		}
	}()
	return exec(file)
}

func TestBPlusTree_Insert(t *testing.T) {
	execErr := fileScopedExec("somefile.bin", func(file *os.File) error {
		tree := MakeBPlusTree(file)
		tree.Init()
		keys := []string{"A", "B", "C", "D", "E", "F", "G", "H"}
		for _, k := range keys {
			err := tree.Insert(k, 0xABCD)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if execErr != nil {
		log.Panic(execErr)
	}
}

func TestBPlusTree_Find(t *testing.T) {
	execErr := fileScopedExec("somefile.bin", func(file *os.File) error {
		tree := MakeBPlusTree(file)
		tree.Init()
		keys := []string{"A", "B", "C", "D", "E", "F", "G", "H"}
		for i, k := range keys {
			err := tree.Insert(k, AddrType(0xABCD+i))
			if err != nil {
				return err
			}
		}
		for _, k := range keys {
			_, err := tree.Find(k)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if execErr != nil {
		log.Panic(execErr)
	}
}

func TestBPlusTree_Find_Non_Existing(t *testing.T) {
	execErr := fileScopedExec("somefile.bin", func(file *os.File) error {
		tree := MakeBPlusTree(file)
		tree.Init()
		keys := []string{"A", "B", "C", "D", "E", "F", "G", "H"}
		for i, k := range keys {
			err := tree.Insert(k, AddrType(0xABCD+i))
			if err != nil {
				return err
			}
		}
		keys = []string{"Z", "Y", "X", "W", "K", "N", "M"}
		for _, k := range keys {
			_, err := tree.Find(k)
			if err != ErrKeyNotFound {
				return err
			}
		}
		return nil
	})
	if execErr != nil {
		log.Panic(execErr)
	}
}

// TODO: fix fails for some t size cases
// update some keys on the way to the root
func TestBPlusTree_Delete(t *testing.T) {
	execErr := fileScopedExec("somefile.bin", func(file *os.File) error {
		tree := MakeBPlusTree(file)
		tree.Init()
		keys := []string{"A", "B", "C", "D", "E", "F", "G", "H"}
		for i, k := range keys {
			err := tree.Insert(k, AddrType(0xABCD+i))
			if err != nil {
				return err
			}
		}
		for _, k := range keys {
			_, err := tree.Find(k)
			if err != nil {
				return err
			}
		}
		for i, k := range keys {
			err := tree.Delete(k)
			if err != nil {
				return err
			}
			_, err = tree.Find(k)
			if err != ErrKeyNotFound {
				log.Panicf("Found deleted key %s", k)
			}
			if i == len(keys)-1 {
				break
			}
			for _, k2 := range keys[i+1:] {
				_, err = tree.Find(k2)
				if err != nil {
					log.Panicf("Not found untouched key %s during %s delete", k2, k)
				}
			}
		}
		return nil
	})
	if execErr != nil {
		log.Panic(execErr)
	}
}
