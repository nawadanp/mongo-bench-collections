package main

import (
	"flag"
	"log"
	"strconv"
	"time"

	"github.com/globalsign/mgo"
)

type params struct {
	MongoURI       string
	MongoTimeoutMS int
	Databases      int
	Collections    int
}

const (
	appName = "mongo-bench-collections"
)

func parseFlags() params {
	flagMongoURI := flag.String("mongo-uri", "mongodb://127.0.0.1:27017", "MongoDB URI - Should be a mongos. Default: 'mongodb://127.0.0.1:27017'")
	flagMongoTimeoutMS := flag.Int("mongo-timeout", 5000, "MongoDB Connection timeout in ms. Default: 5000ms")
	flagDatabases := flag.Int("databases", 1, "Number of databases to create. Default: 1")
	flagCollections := flag.Int("collections", 1, "Number of collections per database. Default: 1")
	flag.Parse()
	var config params
	config.MongoURI = *flagMongoURI
	config.MongoTimeoutMS = *flagMongoTimeoutMS
	config.Databases = *flagDatabases
	config.Collections = *flagCollections
	log.Printf("%s started with the following parameters:\nMongoDB URI: %s\nMongoDB Connect Timeout: %d\nNumber of databases to create: %d\nNumber of collections per database:%d\n", appName, config.MongoURI, config.MongoTimeoutMS, config.Databases, config.Collections)
	return config
}
func main() {
	config := parseFlags()

	dialInfo, err := mgo.ParseURL(config.MongoURI)
	if err != nil {
		log.Fatalf("Unable to parse mongo-uri: %s", err)
	}
	if dialInfo.AppName == "" {
		dialInfo.AppName = appName
	}
	dialInfo.Timeout = time.Duration(config.MongoTimeoutMS) * time.Millisecond
	session, err := mgo.DialWithInfo(dialInfo)
	if err != nil {
		log.Fatalf("could not connect to %s: %s\n", config.MongoURI, err)
	}
	defer session.Close()
	log.Printf("Connected to %s", config.MongoURI)

	var coll mgo.CollectionInfo

	for database := 1; database <= config.Databases; database++ {
		dbname := "db" + strconv.Itoa(database)
		for collection := 1; collection <= config.Collections; collection++ {
			collectionName := "col" + strconv.Itoa(collection)
			col := session.DB(dbname).C(collectionName)
			startTime := time.Now()
			err := col.Create(&coll)
			if err != nil {
				log.Printf("Create collection error: %s\n", err)
			} else {
				log.Printf("%s - %s : Created in %dms", dbname, collectionName, time.Since(startTime)/time.Millisecond)
			}
		}
	}

}
