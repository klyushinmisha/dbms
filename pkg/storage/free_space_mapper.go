package storage

import (
	"io"
	"io/ioutil"
	"log"
	"os"
)

// TODO: add chunks loading
type FreeSpaceMapper struct {
	file            *os.File
	pageSize        int
	freeSpaceValues []byte
}

func (mapper *FreeSpaceMapper) TranslatePagePosition(pagePos int64) int {
	return int(pagePos) / mapper.pageSize
}

func (mapper *FreeSpaceMapper) TranslateMapperPosition(mapperPos int) int64 {
	return int64(mapperPos * mapper.pageSize)
}

func (mapper *FreeSpaceMapper) CalculateLevel(freeSpace int) byte {
	return byte(float32(freeSpace) / float32(mapper.pageSize) * 255)
}

func (mapper *FreeSpaceMapper) GetLevel(pagePos int64) byte {
	return mapper.freeSpaceValues[mapper.TranslatePagePosition(pagePos)]
}

func (mapper *FreeSpaceMapper) SetLevel(pagePos int64, freeSpace int) {
	mapperPos := mapper.TranslatePagePosition(pagePos)
	posDiff := mapperPos - len(mapper.freeSpaceValues)
	if posDiff > 0 {
		log.Panic("broken allocation: diff can't be more than actual size")
	}
	level := mapper.CalculateLevel(freeSpace)
	if posDiff == 0 {
		mapper.freeSpaceValues = append(mapper.freeSpaceValues, level)
	} else {
		mapper.freeSpaceValues[mapperPos] = level
	}
}

func (mapper *FreeSpaceMapper) FindFirstFit(requiredSpace int) int64 {
	requiredLevel := mapper.CalculateLevel(requiredSpace)
	for mapperPos, level := range mapper.freeSpaceValues {
		if level > requiredLevel {
			return mapper.TranslateMapperPosition(mapperPos)
		}
	}
	return -1
}

func LoadFreeSpaceMapperFromFile(file *os.File, pageSize int) *FreeSpaceMapper {
	var mapper FreeSpaceMapper
	var readErr error
	mapper.file = file
	mapper.pageSize = pageSize
	mapper.freeSpaceValues, readErr = ioutil.ReadAll(mapper.file)
	if readErr != nil {
		log.Panic("failed to load free space freeSpaceMapper")
	}
	return &mapper
}

func (mapper *FreeSpaceMapper) Dump() {
	_, seekErr := mapper.file.Seek(0, io.SeekStart)
	if seekErr != nil {
		log.Panic(seekErr)
	}
	_, writeErr := mapper.file.Write(mapper.freeSpaceValues)
	if writeErr != nil {
		log.Panic("failed to dump free space freeSpaceMapper")
	}
}
