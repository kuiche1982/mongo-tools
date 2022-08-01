# Purpose
Read mongodump output  and send it to a channel, so thirdparty apps could unmarshal at will, e.g. read from the channel and restore data to redis/in-process DB.  

# some howtos

aws s3 cp s3://xxxx/ /Users/xxxx/data --recursive
restore 
./mongorestore --dir=/xxxx/data/abc --gzip --authenticationDatabase=admin --uri=mongodb://root:example@localhost:27017/ -d test

```
$ ./restore --dir=/xxxx/data/abc --gzip --authenticationDatabase=admin --uri=mongodb://root:example@localhost:27017/ -d test
2022-07-29T09:56:48.983+0800	The --db and --collection flags are deprecated for this use-case; please use --nsInclude instead, i.e. with --nsInclude=${DATABASE}.${COLLECTION}
2022-07-29T09:56:48.983+0800	building a list of collections to restore from /xxxx/data/abc dir
2022-07-29T09:56:48.984+0800	reading metadata for test.aaa from /xxxx/data/abc/aaa.metadata.json.gz
2022-07-29T09:56:48.996+0800	restoring test.aaa from /xxxx/data/abc/aaa.bson.gz
2022-07-29T09:56:51.337+0800	finished restoring test.aaa (78529 documents, 0 failures)
2022-07-29T09:56:51.337+0800	restoring indexes for collection test.aaa from metadata
2022-07-29T09:56:51.337+0800	index: &idx.IndexDocument{Options:primitive.M{"name":"cpId_1", "ns":"aaa.aaa", "unique":true, "v":1}, Key:primitive.D{primitive.E{Key:"cpId", Value:1}}, PartialFilterExpression:primitive.D(nil)}
2022-07-29T09:56:51.338+0800	index: &idx.IndexDocument{Options:primitive.M{"background":true, "name":"updated_-1", "ns":"aaa.aaa", "v":1}, Key:primitive.D{primitive.E{Key:"updated", Value:-1}}, PartialFilterExpression:primitive.D(nil)}
2022-07-29T09:56:51.519+0800	78529 document(s) restored successfully. 0 document(s) failed to restore
```

extract "collectionName" from .metadata.json.gz

restore bson.gz

