// Copyright (C) MongoDB, Inc. 2014-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

// Main package for the mongorestore tool.
package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/mongodb/mongo-tools/common/log"
	"github.com/mongodb/mongo-tools/common/signals"
	"github.com/mongodb/mongo-tools/common/util"
	"github.com/mongodb/mongo-tools/mongorestore"
	"go.mongodb.org/mongo-driver/bson"

	"os"
)

var (
	VersionStr = "built-without-version-string"
	GitCommit  = "build-without-git-commit"
)

func main() {
	start := time.Now()
	opts := mongorestore.CreateChannelRestoreOption("/Users/mobvista/gopath/src/kuik8srampup/s3read/data/new_adn", VersionStr, GitCommit)
	docChan := make(chan bson.Raw)
	// printDoneChan := make(chan struct{})
	var wg sync.WaitGroup
	var readThread = 10

	for i := 0; i < readThread; i++ {
		wg.Add(1)
		// prints the output
		go func() {
			var failed, passed uint64
			defer func() {
				// close(printDoneChan)
				wg.Done()
			}()
			for d := range docChan {

				var i interface{}
				if err := bson.Unmarshal(d, &i); err != nil {
					// fmt.Printf("unmarshal bson error %v \n", err)
					failed++
					continue
				}
				passed++
				// data, err := json.Marshal(i)
				// if err != nil {
				// 	fmt.Printf("marshal json error %v \n", err)
				// 	continue
				// }
				// fmt.Printf("another json %s \n", string(data))

			}
			// fmt.Printf("docChan closed with %d unmarshal succeeded, %d unmarshal failed\n", passed, failed)
		}()

	}
	restore, err := mongorestore.New(opts, docChan)
	if err != nil {
		log.Logvf(log.Always, err.Error())
		os.Exit(util.ExitFailure)
	}
	defer restore.Close()

	finishedChan := signals.HandleWithInterrupt(restore.HandleInterrupt)
	defer close(finishedChan)

	result := restore.Restore()
	if result.Err != nil {
		log.Logvf(log.Always, "Failed: %v", result.Err)
	}
	// wait till doc printed
	// <-printDoneChan
	wg.Wait()
	if restore.GetToolOptions().WriteConcern.Acknowledged() {
		log.Logvf(log.Always, "%v document(s) restored successfully. %v document(s) failed to restore.", result.Successes, result.Failures)
	} else {
		log.Logvf(log.Always, "done")
	}
	end := time.Now()
	duration := end.Sub(start)
	fmt.Printf("cost: %s", duration.String())
	if result.Err != nil {
		os.Exit(util.ExitFailure)
	}
	os.Exit(util.ExitSuccess)
}
