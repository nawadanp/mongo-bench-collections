package main

import (
	"flag"
	"log"
	"os"
	"strconv"
	"time"

	"encoding/csv"

	"github.com/globalsign/mgo"
)

type params struct {
	MongoURI       string
	MongoTimeoutMS int
	Databases      int
	Collections    int
	OutputFile     string
}

const (
	appName = "mongo-bench-collections"
)

func parseFlags() params {
	flagMongoURI := flag.String("mongo-uri", "mongodb://127.0.0.1:27017", "MongoDB URI - Should be a mongos. Default: 'mongodb://127.0.0.1:27017'")
	flagMongoTimeoutMS := flag.Int("mongo-timeout", 5000, "MongoDB Connection timeout in ms. Default: 5000ms")
	flagDatabases := flag.Int("databases", 1, "Number of databases to create. Default: 1")
	flagCollections := flag.Int("collections", 1, "Number of collections per database. Default: 1")
	flagOutputFile := flag.String("out", "output.csv", "Output file. Default: output.csv")
	flag.Parse()
	var config params
	config.MongoURI = *flagMongoURI
	config.MongoTimeoutMS = *flagMongoTimeoutMS
	config.Databases = *flagDatabases
	config.Collections = *flagCollections
	config.OutputFile = *flagOutputFile
	log.Printf("%s started with the following parameters:\nMongoDB URI: %s\nMongoDB Connect Timeout: %d\nNumber of databases to create: %d\nNumber of collections per database:%d\nOutput file: %s\n", appName, config.MongoURI, config.MongoTimeoutMS, config.Databases, config.Collections, config.OutputFile)
	return config
}
func main() {
	config := parseFlags()

	createdDatabasesTotal := 0
	createdCollectionsTotal := 0
	initTime := time.Now()

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

	file, err := os.Create(config.OutputFile)
	defer file.Close()
	if err != nil {
		log.Fatalf("Unable to open the output file: %s", err)
	}
	output := csv.NewWriter(file)
	output.Comma = ';'

	line := []string{"dbname", "collectionName", "Total databases created", "Total collections created", "Collections created on this database", "Operation time", "Total Execution Time"}
	output.Write(line)

	var coll mgo.CollectionInfo

	for database := 1; database <= config.Databases; database++ {
		dbname := "db" + strconv.Itoa(database)
		createdDatabasesTotal++
		createdCollections := 0
		for collection := 1; collection <= config.Collections; collection++ {
			createdCollectionsTotal++
			createdCollections++
			collectionName := "col" + strconv.Itoa(collection)
			col := session.DB(dbname).C(collectionName)
			startTime := time.Now()
			err := col.Create(&coll)
			if err != nil {
				line := []string{dbname, collectionName, strconv.Itoa(createdDatabasesTotal), strconv.Itoa(createdCollectionsTotal), strconv.Itoa(createdCollections), "Existing", time.Since(startTime).String(), time.Since(initTime).String()}
				output.Write(line)
			} else {
				line := []string{dbname, collectionName, strconv.Itoa(createdDatabasesTotal), strconv.Itoa(createdCollectionsTotal), strconv.Itoa(createdCollections), "Created", time.Since(startTime).String(), time.Since(initTime).String()}
				output.Write(line)
			}
			output.Flush()
		}
	}

}
