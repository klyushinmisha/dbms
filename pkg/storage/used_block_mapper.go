package storage

import (
	"dbms/pkg/utils"
	"io"
	"io/ioutil"
	"log"
	"os"
)

// TODO: add chunks loading
type UsedBlockMapper struct {
	file             *os.File
	pageSize         int
	usedBlocksValues []byte
}

func (mapper *UsedBlockMapper) TranslatePagePosition(pagePos int64) (int, int) {
	var mapperPos, bitNumber int
	mapperPos = int(pagePos) / mapper.pageSize
	bitNumber = mapperPos & 0b111
	mapperPos >>= 3
	return mapperPos, bitNumber
}

func (mapper *UsedBlockMapper) TranslateMapperPosition(mapperPos int, bitNumber int) int64 {
	mapperPos <<= 3
	mapperPos |= bitNumber & 0b111
	return int64(mapperPos * mapper.pageSize)
}

func (mapper *UsedBlockMapper) GetUsed(pagePos int64) bool {
	mapperPos, bitNumber := mapper.TranslatePagePosition(pagePos)
	return utils.BitArray(mapper.usedBlocksValues[mapperPos]).Get(bitNumber)
}

func (mapper *UsedBlockMapper) SetUsed(pagePos int64, used bool) {
	mapperPos, bitNumber := mapper.TranslatePagePosition(pagePos)
	posDiff := mapperPos - len(mapper.usedBlocksValues)
	if posDiff > 0 {
		log.Panic("broken allocation: diff can't be more than actual size")
	}
	if posDiff == 0 {
		var value utils.BitArray
		value.Set(used, bitNumber)
		mapper.usedBlocksValues = append(mapper.usedBlocksValues, byte(value))
	} else {
		pValue := &mapper.usedBlocksValues[mapperPos]
		arr := utils.BitArray(*pValue)
		arr.Set(used, bitNumber)
	}
}

func LoadUsedBlockMapperFromFile(file *os.File, pageSize int) *UsedBlockMapper {
	var mapper UsedBlockMapper
	var readErr error
	mapper.file = file
	mapper.pageSize = pageSize
	mapper.usedBlocksValues, readErr = ioutil.ReadAll(mapper.file)
	if readErr != nil {
		log.Panic("failed to load used blocks freeSpaceMapper")
	}
	return &mapper
}

func (mapper *UsedBlockMapper) Dump() {
	_, seekErr := mapper.file.Seek(0, io.SeekStart)
	if seekErr != nil {
		log.Panic(seekErr)
	}
	_, writeErr := mapper.file.Write(mapper.usedBlocksValues)
	if writeErr != nil {
		log.Panic("failed to dump free space freeSpaceMapper")
	}
}
