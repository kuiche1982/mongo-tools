MongoDB Tools

# Purpose Aug 1, 2022
Modified mongorestore/readToChannel 

Read mongodump output  and send it to a channel, so thirdparty apps could unmarshal at will, e.g. read from the channel and restore data to redis/in-process DB. 

# Fork from Aug 1, 2022
https://github.com/mongodb/mongo-tools