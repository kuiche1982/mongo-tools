package main

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/mongodb/mongo-tools/common/db"
	"go.mongodb.org/mongo-driver/bson"
)

func main() {
	start := time.Now()
	files := LoadFiles("./data/new_abc")
	restoreFiles := BuildRestoreUnit(files)
	var wg sync.WaitGroup
	wg.Add(len(restoreFiles))
	for _, file := range restoreFiles {
		f := file
		go func() {
			defer wg.Done()
			ch := f.ReaBson()
			docCount := 0
			errCount := 0
			for v := range ch {
				// d := v
				var vi interface{}
				if err := bson.Unmarshal([]byte(v), &vi); err == nil {
					// jdata, err := json.Marshal(vi)
					// fmt.Println(string(jdata), err)
					docCount++
					continue
				}
				errCount++
			}
			fmt.Printf("%s: %d read, %d error\n", f.CollectioName, docCount, errCount)
		}()
	}
	wg.Wait()
	end := time.Now()
	duration := end.Sub(start)
	fmt.Printf("\ncost: %s\n", duration.String())
}

// ======== LoadFileGroup ===========
type MongoRestoreUnit struct {
	Bson          MongoFile
	Meta          MongoFile
	CollectioName string
}

func (mru *MongoRestoreUnit) ReaBson() <-chan bson.Raw {
	docChan := make(chan bson.Raw, 20)
	fileName := mru.Bson.Name
	if fileName == "" {
		close(docChan)
		return docChan
	}
	fullPath := path.Join(mru.Meta.Path, fileName)

	f, err := os.Open(fullPath)
	if err != nil {
		close(docChan)
		return docChan
	}

	var rc io.ReadCloser = f
	if mru.Meta.IsZip() {
		gzipReader, err := gzip.NewReader(f)
		if err != nil {
			f.Close()
			close(docChan)
			return docChan
		}
		rc = gzipReader
	}
	bsonSource := db.NewDecodedBSONSource(db.NewBSONSource(rc))

	// stream documents for this collection on docChan
	go func() {
		defer close(docChan)
		defer f.Close()
		defer bsonSource.Close()
		for {
			doc := bsonSource.LoadNext()
			if doc == nil {
				break
			}
			rawBytes := make([]byte, len(doc))
			copy(rawBytes, doc)
			docChan <- bson.Raw(rawBytes)
		}
	}()
	return docChan
}

func (mru *MongoRestoreUnit) LoadCollectionName() {
	if mru.CollectioName != "" {
		return
	}
	mru.CollectioName = strings.ReplaceAll(strings.ReplaceAll(mru.Bson.Name, ".bson.gz", ""), ".bson", "")
	fileName := mru.Meta.Name
	if fileName == "" {
		return
	}
	fullPath := path.Join(mru.Meta.Path, fileName)

	f, err := os.Open(fullPath)
	if err != nil {
		return
	}

	defer f.Close()
	var rc io.ReadCloser = f
	if mru.Meta.IsZip() {
		gzipReader, err := gzip.NewReader(f)
		if err != nil {
			return
		}
		rc = gzipReader
	}
	defer rc.Close()
	data, err := ioutil.ReadAll(rc)
	if err != nil {
		return
	}
	type Meta struct {
		CollectionName string `json:"collectionName"`
	}
	m := &Meta{}
	err = json.Unmarshal(data, m)
	if err != nil {
		return
	}
	if m.CollectionName != "" {
		mru.CollectioName = m.CollectionName
	}
}

func BuildRestoreUnit(files []MongoFile) []MongoRestoreUnit {
	bsonFiles := FindBsonFiles(files)
	result := []MongoRestoreUnit{}
	for _, bsonFile := range bsonFiles {
		metaFile := FindMetaFile(files, bsonFile.Name)
		mru := MongoRestoreUnit{
			Bson: bsonFile,
			Meta: metaFile,
		}
		mru.LoadCollectionName()
		result = append(result, mru)

	}
	return result
}

// ========================= findFiles ============
type MongoFile struct {
	Path string
	Name string
}

func (mf MongoFile) IsZip() bool {
	return strings.HasSuffix(mf.Name, ".gz")
}

func (mf MongoFile) IsBson() bool {
	return strings.HasSuffix(mf.Name, ".bson") ||
		strings.HasSuffix(mf.Name, ".bson.gz")
}

func (mf MongoFile) IsMeta() bool {
	return strings.HasSuffix(mf.Name, ".metadata.json") ||
		strings.HasSuffix(mf.Name, ".metadata.json.gz")
}

func LoadFiles(path string) []MongoFile {
	fileInfos := []MongoFile{}
	if fs, err := os.Stat(path); err == nil && !fs.IsDir() {
		return fileInfos
	}
	entries, err := ioutil.ReadDir(path)
	if err != nil {
		return fileInfos
	}
	for _, entry := range entries {
		fileInfos = append(fileInfos, MongoFile{
			Path: path,
			Name: entry.Name(),
		})
	}
	return fileInfos
}

func FindBsonFiles(files []MongoFile) []MongoFile {
	bsonFiles := []MongoFile{}
	for _, file := range files {
		if file.IsBson() {
			bsonFiles = append(bsonFiles, file)
		}
	}
	return bsonFiles
}

func FindMetaFile(files []MongoFile, bsonFile string) MongoFile {
	expMetaName := strings.ReplaceAll(bsonFile, ".bson.gz", ".metadata.json.gz")
	expMetaName = strings.ReplaceAll(expMetaName, ".bson", ".metadata.json")
	for _, file := range files {
		if file.Name == expMetaName {
			return file
		}
	}
	return MongoFile{Name: "", Path: ""}
}
