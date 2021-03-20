package storage

import (
	"io"
	"io/ioutil"
	"log"
	"os"
)

// TODO: add chunks loading
// FSM is a free space mapper
type FSM struct {
	file            *os.File
	pageSize        int
	freeSpaceValues []byte
}

func (m *FSM) TranslatePagePosition(pagePos int64) int {
	return int(pagePos) / m.pageSize
}

func (m *FSM) TranslateMapperPosition(mapperPos int) int64 {
	return int64(mapperPos * m.pageSize)
}

func (m *FSM) CalculateLevel(freeSpace int) byte {
	return byte(float32(freeSpace) / float32(m.pageSize) * 255)
}

func (m *FSM) GetLevel(pagePos int64) byte {
	return m.freeSpaceValues[m.TranslatePagePosition(pagePos)]
}

func (m *FSM) SetLevel(pagePos int64, freeSpace int) {
	mapperPos := m.TranslatePagePosition(pagePos)
	posDiff := mapperPos - len(m.freeSpaceValues)
	if posDiff > 0 {
		log.Panic("broken allocation: diff can't be more than actual size")
	}
	level := m.CalculateLevel(freeSpace)
	if posDiff == 0 {
		m.freeSpaceValues = append(m.freeSpaceValues, level)
	} else {
		m.freeSpaceValues[mapperPos] = level
	}
}

func (m *FSM) FindFirstFit(requiredSpace int) int64 {
	requiredLevel := m.CalculateLevel(requiredSpace)
	for mapperPos, level := range m.freeSpaceValues {
		if level > requiredLevel {
			return m.TranslateMapperPosition(mapperPos)
		}
	}
	return -1
}

func LoadFreeSpaceMapperFromFile(file *os.File, pageSize int) *FSM {
	var mapper FSM
	var readErr error
	mapper.file = file
	mapper.pageSize = pageSize
	mapper.freeSpaceValues, readErr = ioutil.ReadAll(mapper.file)
	if readErr != nil {
		log.Panic("failed to load free space fsm")
	}
	return &mapper
}

func (m *FSM) Dump() {
	_, seekErr := m.file.Seek(0, io.SeekStart)
	if seekErr != nil {
		log.Panic(seekErr)
	}
	_, writeErr := m.file.Write(m.freeSpaceValues)
	if writeErr != nil {
		log.Panic("failed to dump free space fsm")
	}
}
