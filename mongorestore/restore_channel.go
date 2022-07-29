// Copyright (C) MongoDB, Inc. 2014-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package mongorestore

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/mongodb/mongo-tools/common/archive"
	"github.com/mongodb/mongo-tools/common/auth"
	"github.com/mongodb/mongo-tools/common/db"
	"github.com/mongodb/mongo-tools/common/intents"
	"github.com/mongodb/mongo-tools/common/log"
	"github.com/mongodb/mongo-tools/common/options"
	"github.com/mongodb/mongo-tools/common/progress"
	"github.com/mongodb/mongo-tools/common/util"
	"github.com/mongodb/mongo-tools/mongorestore/ns"

	"go.mongodb.org/mongo-driver/bson"
)

type ChannelRestore struct {
	*MongoRestore
	DocChan chan<- bson.Raw
}

// Restore runs the mongorestore program.
func (restore *ChannelRestore) Restore() Result {
	var target archive.DirLike
	var err error
	err = restore.ParseAndValidateOptions()
	if err != nil {
		log.Logvf(log.DebugLow, "got error from options parsing: %v", err)
		return Result{Err: err}
	}

	// Build up all intents to be restored
	restore.manager = intents.NewIntentManager()
	if restore.InputOptions.Archive == "" && restore.InputOptions.OplogReplay {
		restore.manager.SetSmartPickOplog(true)
	}

	if restore.InputOptions.Archive != "" {
		if restore.archive == nil {
			archiveReader, err := restore.getArchiveReader()
			if err != nil {
				return Result{Err: err}
			}
			restore.archive = &archive.Reader{
				In:      archiveReader,
				Prelude: &archive.Prelude{},
			}
			defer restore.archive.In.Close()
		}
		err = restore.archive.Prelude.Read(restore.archive.In)
		if err != nil {
			return Result{Err: err}
		}
		log.Logvf(log.DebugLow, `archive format version "%v"`, restore.archive.Prelude.Header.FormatVersion)
		log.Logvf(log.DebugLow, `archive server version "%v"`, restore.archive.Prelude.Header.ServerVersion)
		log.Logvf(log.DebugLow, `archive tool version "%v"`, restore.archive.Prelude.Header.ToolVersion)
		target, err = restore.archive.Prelude.NewPreludeExplorer()
		if err != nil {
			return Result{Err: err}
		}
	} else if restore.TargetDirectory != "-" {
		var usedDefaultTarget bool
		if restore.TargetDirectory == "" {
			restore.TargetDirectory = "dump"
			log.Logv(log.Always, "using default 'dump' directory")
			usedDefaultTarget = true
		}
		target, err = newActualPath(restore.TargetDirectory)
		if err != nil {
			if usedDefaultTarget {
				log.Logv(log.Always, util.ShortUsage("mongorestore"))
			}
			return Result{Err: fmt.Errorf("mongorestore target '%v' invalid: %v", restore.TargetDirectory, err)}
		}
		// handle cases where the user passes in a file instead of a directory
		if !target.IsDir() {
			log.Logv(log.DebugLow, "mongorestore target is a file, not a directory")
			err = restore.handleBSONInsteadOfDirectory(restore.TargetDirectory)
			if err != nil {
				return Result{Err: err}
			}
		} else {
			log.Logv(log.DebugLow, "mongorestore target is a directory, not a file")
		}
	}
	if restore.ToolOptions.Namespace.Collection != "" &&
		restore.OutputOptions.NumParallelCollections > 1 &&
		restore.OutputOptions.NumInsertionWorkers == 1 &&
		!restore.OutputOptions.MaintainInsertionOrder {
		// handle special parallelization case when we are only restoring one collection
		// by mapping -j to insertion workers rather than parallel collections
		log.Logvf(log.DebugHigh,
			"setting number of insertions workers to number of parallel collections (%v)",
			restore.OutputOptions.NumParallelCollections)
		restore.OutputOptions.NumInsertionWorkers = restore.OutputOptions.NumParallelCollections
	}
	if restore.InputOptions.Archive != "" {
		if int(restore.archive.Prelude.Header.ConcurrentCollections) > restore.OutputOptions.NumParallelCollections {
			restore.OutputOptions.NumParallelCollections = int(restore.archive.Prelude.Header.ConcurrentCollections)
			log.Logvf(log.Always,
				"setting number of parallel collections to number of parallel collections in archive (%v)",
				restore.archive.Prelude.Header.ConcurrentCollections,
			)
		}
	}

	// Create the demux before intent creation, because muted archive intents need
	// to register themselves with the demux directly
	if restore.InputOptions.Archive != "" {
		restore.archive.Demux = archive.CreateDemux(restore.archive.Prelude.NamespaceMetadatas, restore.archive.In)
	}

	switch {
	case restore.InputOptions.Archive != "":
		log.Logvf(log.Always, "preparing collections to restore from")
		err = restore.CreateAllIntents(target)
	case restore.ToolOptions.Namespace.DB != "" && restore.ToolOptions.Namespace.Collection == "":
		log.Logvf(log.Always,
			"building a list of collections to restore from %v dir",
			target.Path())
		err = restore.CreateIntentsForDB(
			restore.ToolOptions.Namespace.DB,
			target,
		)
	case restore.ToolOptions.Namespace.DB != "" && restore.ToolOptions.Namespace.Collection != "" && restore.TargetDirectory == "-":
		log.Logvf(log.Always, "setting up a collection to be read from standard input")
		err = restore.CreateStdinIntentForCollection(
			restore.ToolOptions.Namespace.DB,
			restore.ToolOptions.Namespace.Collection,
		)
	case restore.ToolOptions.Namespace.DB != "" && restore.ToolOptions.Namespace.Collection != "":
		log.Logvf(log.Always, "checking for collection data in %v", target.Path())
		err = restore.CreateIntentForCollection(
			restore.ToolOptions.Namespace.DB,
			restore.ToolOptions.Namespace.Collection,
			target,
		)
	default:
		log.Logvf(log.Always, "preparing collections to restore from")
		err = restore.CreateAllIntents(target)
	}
	if err != nil {
		return Result{Err: fmt.Errorf("error scanning filesystem: %v", err)}
	}

	if restore.isMongos && restore.manager.HasConfigDBIntent() && restore.ToolOptions.Namespace.DB == "" {
		return Result{Err: fmt.Errorf("cannot do a full restore on a sharded system - " +
			"remove the 'config' directory from the dump directory first")}
	}

	if restore.InputOptions.OplogFile != "" {
		err = restore.CreateIntentForOplog()
		if err != nil {
			return Result{Err: fmt.Errorf("error reading oplog file: %v", err)}
		}
	}
	if restore.InputOptions.OplogReplay && restore.manager.Oplog() == nil {
		return Result{Err: fmt.Errorf("no oplog file to replay; make sure you run mongodump with --oplog")}
	}
	if restore.manager.GetOplogConflict() {
		return Result{Err: fmt.Errorf("cannot provide both an oplog.bson file and an oplog file with --oplogFile, " +
			"nor can you provide both a local/oplog.rs.bson and a local/oplog.$main.bson file")}
	}

	conflicts := restore.manager.GetDestinationConflicts()
	if len(conflicts) > 0 {
		for _, conflict := range conflicts {
			log.Logvf(log.Always, "%s", conflict.Error())
		}
		return Result{Err: fmt.Errorf("cannot restore with conflicting namespace destinations")}
	}

	if restore.OutputOptions.DryRun {
		log.Logvf(log.Always, "dry run completed")
		return Result{}
	}

	demuxFinished := make(chan interface{})
	var demuxErr error
	if restore.InputOptions.Archive != "" {
		namespaceChan := make(chan string, 1)
		namespaceErrorChan := make(chan error)
		restore.archive.Demux.NamespaceChan = namespaceChan
		restore.archive.Demux.NamespaceErrorChan = namespaceErrorChan

		go func() {
			demuxErr = restore.archive.Demux.Run()
			close(demuxFinished)
		}()
		// consume the new namespace announcement from the demux for all of the special collections
		// that get cached when being read out of the archive.
		// The first regular collection found gets pushed back on to the namespaceChan
		// consume the new namespace announcement from the demux for all of the collections that get cached
		for {
			ns, ok := <-namespaceChan
			// the archive can have only special collections. In that case we keep reading until
			// the namespaces are exhausted, indicated by the namespaceChan being closed.
			log.Logvf(log.DebugLow, "received %v from namespaceChan", ns)
			if !ok {
				break
			}
			dbName, collName := util.SplitNamespace(ns)
			ns = dbName + "." + strings.TrimPrefix(collName, "system.buckets.")
			intent := restore.manager.IntentForNamespace(ns)
			if intent == nil {
				return Result{Err: fmt.Errorf("no intent for collection in archive: %v", ns)}
			}
			if intent.IsSystemIndexes() ||
				intent.IsUsers() ||
				intent.IsRoles() ||
				intent.IsAuthVersion() {
				log.Logvf(log.DebugLow, "special collection %v found", ns)
				namespaceErrorChan <- nil
			} else {
				// Put the ns back on the announcement chan so that the
				// demultiplexer can start correctly
				log.Logvf(log.DebugLow, "first non special collection %v found."+
					" The demultiplexer will handle it and the remainder", ns)
				namespaceChan <- ns
				break
			}
		}
	}

	// If restoring users and roles, make sure we validate auth versions
	if restore.ShouldRestoreUsersAndRoles() {
		log.Logv(log.Info, "comparing auth version of the dump directory and target server")
		restore.authVersions.Dump, err = restore.GetDumpAuthVersion()
		if err != nil {
			return Result{Err: fmt.Errorf("error getting auth version from dump: %v", err)}
		}
		restore.authVersions.Server, err = auth.GetAuthVersion(restore.SessionProvider)
		if err != nil {
			return Result{Err: fmt.Errorf("error getting auth version of server: %v", err)}
		}
		err = restore.ValidateAuthVersions()
		if err != nil {
			return Result{Err: fmt.Errorf(
				"the users and roles collections in the dump have an incompatible auth version with target server: %v",
				err)}
		}
	}

	err = restore.LoadIndexesFromBSON()
	if err != nil {
		return Result{Err: fmt.Errorf("restore error: %v", err)}
	}

	err = restore.PopulateMetadataForIntents()
	if err != nil {
		return Result{Err: fmt.Errorf("restore error: %v", err)}
	}

	err = restore.preFlightChecks()
	if err != nil {
		return Result{Err: fmt.Errorf("restore error: %v", err)}
	}

	// Restore the regular collections
	if restore.InputOptions.Archive != "" {
		restore.manager.UsePrioritizer(restore.archive.Demux.NewPrioritizer(restore.manager))
	} else if restore.OutputOptions.NumParallelCollections > 1 {
		// 3.0+ has collection-level locking for writes, so it is most efficient to
		// prioritize by collection size. Pre-3.0 we try to avoid inserting into collections
		// in the same database simultaneously due to the database-level locking.
		// Up to 4.2, foreground index builds take a database-level lock for the entire build,
		// but this prioritizer is not used for index builds so we don't need to worry about that here.
		if restore.serverVersion.GTE(db.Version{3, 0, 0}) {
			restore.manager.Finalize(intents.LongestTaskFirst)
		} else {
			restore.manager.Finalize(intents.MultiDatabaseLTF)
		}
	} else {
		// use legacy restoration order if we are single-threaded
		restore.manager.Finalize(intents.Legacy)
	}

	result := restore.RestoreIntents()
	if result.Err != nil {
		return result
	}

	// Restore users/roles
	if restore.ShouldRestoreUsersAndRoles() {
		err = restore.RestoreUsersOrRoles(restore.manager.Users(), restore.manager.Roles())
		if err != nil {
			return result.withErr(fmt.Errorf("restore error: %v", err))
		}
	}

	// Restore oplog
	if restore.InputOptions.OplogReplay {
		err = restore.RestoreOplog()
		if err != nil {
			return result.withErr(fmt.Errorf("restore error: %v", err))
		}
	}

	if !restore.OutputOptions.NoIndexRestore {
		err = restore.RestoreIndexes()
		if err != nil {
			return result.withErr(err)
		}
	}

	if restore.InputOptions.Archive != "" {
		<-demuxFinished
		return result.withErr(demuxErr)
	}

	return result
}

// ParseAndValidateOptions returns a non-nil error if user-supplied options are invalid.
func (restore *ChannelRestore) ParseAndValidateOptions() error {
	// Can't use option pkg defaults for --objcheck because it's two separate flags,
	// and we need to be able to see if they're both being used. We default to
	// true here and then see if noobjcheck is enabled.
	log.Logv(log.DebugHigh, "checking options")
	if restore.InputOptions.Objcheck {
		restore.objCheck = true
		log.Logv(log.DebugHigh, "\tdumping with object check enabled")
	} else {
		log.Logv(log.DebugHigh, "\tdumping with object check disabled")
	}

	if restore.ToolOptions.Namespace.DB == "" && restore.ToolOptions.Namespace.Collection != "" {
		return fmt.Errorf("cannot restore a collection without a specified database")
	}

	if restore.ToolOptions.Namespace.DB != "" {
		if err := util.ValidateDBName(restore.ToolOptions.Namespace.DB); err != nil {
			return fmt.Errorf("invalid db name: %v", err)
		}
	}
	if restore.ToolOptions.Namespace.Collection != "" {
		if err := util.ValidateCollectionGrammar(restore.ToolOptions.Namespace.Collection); err != nil {
			return fmt.Errorf("invalid collection name: %v", err)
		}
	}
	if restore.InputOptions.RestoreDBUsersAndRoles && restore.ToolOptions.Namespace.DB == "" {
		return fmt.Errorf("cannot use --restoreDbUsersAndRoles without a specified database")
	}
	if restore.InputOptions.RestoreDBUsersAndRoles && restore.ToolOptions.Namespace.DB == "admin" {
		return fmt.Errorf("cannot use --restoreDbUsersAndRoles with the admin database")
	}

	var err error

	// if restore.InputOptions.OplogFile != "" {
	// 	if !restore.InputOptions.OplogReplay {
	// 		return fmt.Errorf("cannot use --oplogFile without --oplogReplay enabled")
	// 	}
	// 	if restore.InputOptions.Archive != "" {
	// 		return fmt.Errorf("cannot use --oplogFile with --archive specified")
	// 	}
	// }

	// // check if we are using a replica set and fall back to w=1 if we aren't (for <= 2.4)
	// nodeType, err := restore.SessionProvider.GetNodeType()
	// if err != nil {
	// 	return fmt.Errorf("error determining type of connected node: %v", err)
	// }

	// log.Logvf(log.DebugLow, "connected to node type: %v", nodeType)

	// // deprecations with --nsInclude --nsExclude
	// if restore.ToolOptions.Namespace.DB != "" || restore.ToolOptions.Namespace.Collection != "" {
	// 	if filepath.Ext(restore.TargetDirectory) != ".bson" {
	// 		log.Logvf(log.Always, deprecatedDBAndCollectionsOptionsWarning)
	// 	}

	// }
	if len(restore.NSOptions.ExcludedCollections) > 0 ||
		len(restore.NSOptions.ExcludedCollectionPrefixes) > 0 {
		log.Logvf(log.Always, "the --excludeCollections and --excludeCollectionPrefixes options "+
			"are deprecated and will not exist in the future; use --nsExclude instead")
	}
	if restore.InputOptions.OplogReplay {
		if len(restore.NSOptions.NSInclude) > 0 || restore.ToolOptions.Namespace.DB != "" {
			return fmt.Errorf("cannot use --oplogReplay with includes specified")
		}
		if len(restore.NSOptions.NSExclude) > 0 || len(restore.NSOptions.ExcludedCollections) > 0 ||
			len(restore.NSOptions.ExcludedCollectionPrefixes) > 0 {
			return fmt.Errorf("cannot use --oplogReplay with excludes specified")
		}
		if len(restore.NSOptions.NSFrom) > 0 {
			return fmt.Errorf("cannot use --oplogReplay with namespace renames specified")
		}
	}

	includes := restore.NSOptions.NSInclude
	if restore.ToolOptions.Namespace.DB != "" && restore.ToolOptions.Namespace.Collection != "" {
		includes = append(includes, ns.Escape(restore.ToolOptions.Namespace.DB)+"."+
			restore.ToolOptions.Namespace.Collection)
	} else if restore.ToolOptions.Namespace.DB != "" {
		includes = append(includes, ns.Escape(restore.ToolOptions.Namespace.DB)+".*")
	}
	if len(includes) == 0 {
		includes = []string{"*"}
	}
	restore.includer, err = ns.NewMatcher(includes)
	if err != nil {
		return fmt.Errorf("invalid includes: %v", err)
	}

	if len(restore.NSOptions.ExcludedCollections) > 0 && restore.ToolOptions.Namespace.Collection != "" {
		return fmt.Errorf("--collection is not allowed when --excludeCollection is specified")
	}
	if len(restore.NSOptions.ExcludedCollectionPrefixes) > 0 && restore.ToolOptions.Namespace.Collection != "" {
		return fmt.Errorf("--collection is not allowed when --excludeCollectionsWithPrefix is specified")
	}
	excludes := restore.NSOptions.NSExclude
	for _, col := range restore.NSOptions.ExcludedCollections {
		excludes = append(excludes, "*."+ns.Escape(col))
	}
	for _, colPrefix := range restore.NSOptions.ExcludedCollectionPrefixes {
		excludes = append(excludes, "*."+ns.Escape(colPrefix)+"*")
	}
	restore.excluder, err = ns.NewMatcher(excludes)
	if err != nil {
		return fmt.Errorf("invalid excludes: %v", err)
	}

	if len(restore.NSOptions.NSFrom) != len(restore.NSOptions.NSTo) {
		return fmt.Errorf("--nsFrom and --nsTo arguments must be specified an equal number of times")
	}
	restore.renamer, err = ns.NewRenamer(restore.NSOptions.NSFrom, restore.NSOptions.NSTo)
	if err != nil {
		return fmt.Errorf("invalid renames: %v", err)
	}

	if restore.OutputOptions.NumInsertionWorkers < 0 {
		return fmt.Errorf(
			"cannot specify a negative number of insertion workers per collection")
	}

	if restore.OutputOptions.MaintainInsertionOrder {
		restore.OutputOptions.StopOnError = true
		restore.OutputOptions.NumInsertionWorkers = 1
	}

	if restore.OutputOptions.PreserveUUID {
		if !restore.OutputOptions.Drop {
			return fmt.Errorf("cannot specify --preserveUUID without --drop")
		}

		ok, err := SupportsCollectionUUID(restore.SessionProvider)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("target host does not support --preserveUUID")
		}
	}

	// a single dash signals reading from stdin
	if restore.TargetDirectory == "-" {
		if restore.InputOptions.Archive != "" {
			return fmt.Errorf(
				"cannot restore from \"-\" when --archive is specified")
		}
		if restore.ToolOptions.Namespace.Collection == "" {
			return fmt.Errorf("cannot restore from stdin without a specified collection")
		}
	}
	if restore.InputReader == nil {
		restore.InputReader = os.Stdin
	}

	return nil
}

func (restore *ChannelRestore) RestoreIndexes() error {
	// Only data needed
	return nil
}

func (restore *ChannelRestore) RestoreIndexesForNamespace(namespace *options.Namespace) error {
	// Only data needed
	return nil
}

func (restore *ChannelRestore) PopulateMetadataForIntents() error {
	intents := restore.manager.NormalIntents()

	for _, intent := range intents {
		var metadata *Metadata
		if intent.MetadataFile != nil {
			err := intent.MetadataFile.Open()
			if err != nil {
				return fmt.Errorf("could not open metadata file %v: %v", intent.MetadataLocation, err)
			}
			defer intent.MetadataFile.Close()

			log.Logvf(log.Always, "reading metadata for %v from %v", intent.Namespace(), intent.MetadataLocation)
			metadataJSON, err := ioutil.ReadAll(intent.MetadataFile)
			if err != nil {
				return fmt.Errorf("error reading metadata from %v: %v", intent.MetadataLocation, err)
			}
			metadata, err = restore.MetadataFromJSON(metadataJSON)
			if err != nil {
				return fmt.Errorf("error parsing metadata from %v: %v", intent.MetadataLocation, err)
			}
			if metadata != nil {
				intent.Options = metadata.Options.Map()
			}
		}
	}
	return nil
}

// RestoreIntents iterates through all of the intents stored in the IntentManager, and restores them.
func (restore *ChannelRestore) RestoreIntents() Result {
	log.Logvf(log.DebugLow, "restoring up to %v collections in parallel", restore.OutputOptions.NumParallelCollections)

	if restore.OutputOptions.NumParallelCollections > 0 {
		resultChan := make(chan Result)

		// start a goroutine for each job thread
		for i := 0; i < restore.OutputOptions.NumParallelCollections; i++ {
			go func(id int) {
				var workerResult Result
				log.Logvf(log.DebugHigh, "starting restore routine with id=%v", id)
				var ioBuf []byte
				for {
					intent := restore.manager.Pop()
					if intent == nil {
						log.Logvf(log.DebugHigh, "ending restore routine with id=%v, no more work to do", id)
						resultChan <- workerResult // done
						return
					}
					if fileNeedsIOBuffer, ok := intent.BSONFile.(intents.FileNeedsIOBuffer); ok {
						if ioBuf == nil {
							ioBuf = make([]byte, db.MaxBSONSize)
						}
						fileNeedsIOBuffer.TakeIOBuffer(ioBuf)
					}
					result := restore.RestoreIntent(intent)
					result.log(intent.Namespace())
					workerResult.combineWith(result)
					if result.Err != nil {
						resultChan <- workerResult.withErr(fmt.Errorf("%v: %v", intent.Namespace(), result.Err))
						return
					}
					restore.manager.Finish(intent)
					if fileNeedsIOBuffer, ok := intent.BSONFile.(intents.FileNeedsIOBuffer); ok {
						fileNeedsIOBuffer.ReleaseIOBuffer()
					}

				}
			}(i)
		}

		var totalResult Result
		// wait until all goroutines are done or one of them errors out
		for i := 0; i < restore.OutputOptions.NumParallelCollections; i++ {
			result := <-resultChan
			totalResult.combineWith(result)
			if totalResult.Err != nil {
				return totalResult
			}
		}
		close(restore.DocChan) // all doc sent, close the channel
		return totalResult
	}

	var totalResult Result
	// single-threaded
	for {
		intent := restore.manager.Pop()
		if intent == nil {
			break
		}
		result := restore.RestoreIntent(intent)
		result.log(intent.Namespace())
		totalResult.combineWith(result)
		if result.Err != nil {
			return totalResult.withErr(fmt.Errorf("%v: %v", intent.Namespace(), result.Err))
		}
		restore.manager.Finish(intent)
	}
	return totalResult
}

// RestoreIntent attempts to restore a given intent into MongoDB.
func (restore *ChannelRestore) RestoreIntent(intent *intents.Intent) Result {
	// collectionExists, err := restore.CollectionExists(intent.DB, intent.C)
	// if err != nil {
	// 	return Result{Err: fmt.Errorf("error reading database: %v", err)}
	// }

	// if !restore.OutputOptions.Drop && collectionExists {
	// 	log.Logvf(log.Always, "restoring to existing collection %v without dropping", intent.Namespace())
	// }

	// if restore.OutputOptions.Drop {
	// 	if collectionExists {
	// 		if strings.HasPrefix(intent.C, "system.") {
	// 			log.Logvf(log.Always, "cannot drop system collection %v, skipping", intent.Namespace())
	// 		} else {
	// 			log.Logvf(log.Always, "dropping collection %v before restoring", intent.Namespace())
	// 			err = restore.DropCollection(intent)
	// 			if err != nil {
	// 				return Result{Err: err} // no context needed
	// 			}
	// 			collectionExists = false
	// 		}
	// 	} else {
	// 		log.Logvf(log.DebugLow, "collection %v doesn't exist, skipping drop command", intent.Namespace())
	// 	}
	// }

	// logMessageSuffix := "using options from metadata"

	// // first create the collection with options from the metadata file
	// uuid := intent.UUID
	// options := bsonutil.MtoD(intent.Options)
	// if len(options) == 0 {
	// 	logMessageSuffix = "with no metadata"
	// }

	// var isClustered bool
	// if v := intent.Options["clusteredIndex"]; v != nil {
	// 	isClustered = true
	// }

	// // Clustered collections already have their _id index.
	// if !isClustered {
	// 	// The only way to specify options on the idIndex is at collection creation time.
	// 	IDIndex := restore.indexCatalog.GetIndex(intent.DB, intent.C, "_id_")
	// 	if IDIndex != nil {
	// 		// Remove the index version (to use the default) unless otherwise specified.
	// 		// If preserving UUID, we have to create a collection via
	// 		// applyops, which requires the "v" key.
	// 		if !restore.OutputOptions.KeepIndexVersion && !restore.OutputOptions.PreserveUUID {
	// 			delete(IDIndex.Options, "v")
	// 		}
	// 		IDIndex.Options["ns"] = intent.Namespace()

	// 		// If the collection has an idIndex, then we are about to create it, so
	// 		// ignore the value of autoIndexId.
	// 		for j, opt := range options {
	// 			if opt.Key == "autoIndexId" {
	// 				options = append(options[:j], options[j+1:]...)
	// 			}
	// 		}

	// 		options = append(options, bson.E{"idIndex", *IDIndex})
	// 	}
	// }

	// if restore.OutputOptions.NoOptionsRestore {
	// 	log.Logv(log.Info, "not restoring collection options")
	// 	logMessageSuffix = "with no collection options"
	// 	options = nil
	// }

	// if !collectionExists {
	// 	log.Logvf(log.Info, "creating collection %v %s", intent.Namespace(), logMessageSuffix)
	// 	log.Logvf(log.DebugHigh, "using collection options: %#v", options)
	// 	err = restore.CreateCollection(intent, options, uuid)
	// 	if err != nil {
	// 		return Result{Err: fmt.Errorf("error creating collection %v: %v", intent.Namespace(), err)}
	// 	}
	// 	restore.addToKnownCollections(intent)
	// } else {
	// 	log.Logvf(log.Info, "collection %v already exists - skipping collection create", intent.Namespace())
	// }

	var result Result
	if intent.BSONFile != nil {
		err := intent.BSONFile.Open()
		if err != nil {
			return Result{Err: err}
		}
		defer intent.BSONFile.Close()

		log.Logvf(log.Always, "restoring %v from %v", intent.DataNamespace(), intent.Location)

		bsonSource := db.NewDecodedBSONSource(db.NewBSONSource(intent.BSONFile))
		defer bsonSource.Close()

		result = restore.RestoreCollectionToDB(intent.DB, intent.DataCollection(), bsonSource, intent.BSONFile, intent.Size, intent.Type)
		if result.Err != nil {
			result.Err = fmt.Errorf("error restoring from %v: %v", intent.Location, result.Err)
			return result
		}
	}

	return result
}

// RestoreCollectionToDB pipes the given BSON data into the database.
// Returns the number of documents restored and any errors that occurred.
func (restore *ChannelRestore) RestoreCollectionToDB(dbName, colName string,
	bsonSource *db.DecodedBSONSource, file PosReader, fileSize int64, collectionType string) Result {

	var termErr error
	// session, err := restore.SessionProvider.GetSession()
	// if err != nil {
	// 	return Result{Err: fmt.Errorf("error establishing connection: %v", err)}
	// }

	// collection := session.Database(dbName).Collection(colName)

	documentCount := int64(0)
	watchProgressor := progress.NewCounter(fileSize)
	if restore.ProgressManager != nil {
		name := fmt.Sprintf("%v.%v", dbName, colName)
		restore.ProgressManager.Attach(name, watchProgressor)
		defer restore.ProgressManager.Detach(name)
	}

	maxInsertWorkers := restore.OutputOptions.NumInsertionWorkers

	docChan := make(chan bson.Raw, insertBufferFactor)
	resultChan := make(chan Result, maxInsertWorkers)

	// stream documents for this collection on docChan
	go func() {
		for {
			doc := bsonSource.LoadNext()
			if doc == nil {
				break
			}

			if restore.terminate {
				log.Logvf(log.Always, "terminating read on %v.%v", dbName, colName)
				termErr = util.ErrTerminated
				close(docChan)
				return
			}

			rawBytes := make([]byte, len(doc))
			copy(rawBytes, doc)
			docChan <- bson.Raw(rawBytes)
			documentCount++
		}
		close(docChan)
	}()

	log.Logvf(log.DebugLow, "using %v insertion workers", maxInsertWorkers)

	for i := 0; i < maxInsertWorkers; i++ {
		go func() {
			var result Result

			// bulk := db.NewUnorderedBufferedBulkInserter(collection, restore.OutputOptions.BulkBufferSize).
			// 	SetOrdered(restore.OutputOptions.MaintainInsertionOrder)
			// if collectionType != "timeseries" {
			// 	bulk.SetBypassDocumentValidation(restore.OutputOptions.BypassDocumentValidation)
			// }
			for rawDoc := range docChan {
				if restore.objCheck {
					result.Err = bson.Unmarshal(rawDoc, &bson.D{})
					if result.Err != nil {
						resultChan <- result
						return
					}
				}
				restore.DocChan <- rawDoc
				// result.combineWith(NewResultFromBulkResult(bulk.InsertRaw(rawDoc)))
				// result.Err = db.FilterError(restore.OutputOptions.StopOnError, result.Err)
				// if result.Err != nil {
				// 	resultChan <- result
				// 	return
				// }
				result.combineWith(Result{Successes: 1, Err: nil, Failures: 0})
				watchProgressor.Set(file.Pos())
			}
			// flush the remaining docs
			// result.combineWith(NewResultFromBulkResult(bulk.Flush()))
			resultChan <- result
		}()

		// sleep to prevent all threads from inserting at the same time at start
		time.Sleep(10 * time.Millisecond)
	}

	var totalResult Result
	var finalErr error

	// wait until all insert jobs finish
	for done := 0; done < maxInsertWorkers; done++ {
		totalResult.combineWith(<-resultChan)
		if finalErr == nil && totalResult.Err != nil {
			finalErr = totalResult.Err
			restore.terminate = true
		}
	}

	if finalErr != nil {
		totalResult.Err = finalErr
	} else if err := bsonSource.Err(); err != nil {
		totalResult.Err = fmt.Errorf("reading bson input: %v", err)
	} else if termErr != nil {
		totalResult.Err = termErr
	}
	return totalResult
}
