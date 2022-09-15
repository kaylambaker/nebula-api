// go run parser.go [file_path] [semster]
// example: go run parser.go "Fall 2019.csv" 19F

package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"parser/configs"
	"path/filepath"
	"reflect"

	"os"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type Class struct {
	subject           string
	catalogNumber     string
	section           string
	gradeDistribution bson.A
}

func getCollection(client *mongo.Client, database string, collection string) (returnCollection *mongo.Collection) {
	return (client.Database(database).Collection(collection))
}

func DBConnect(URI string) (client *mongo.Client) {
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(URI))
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	if err := client.Ping(context.TODO(), readpref.Primary()); err != nil {
		fmt.Println(err)
		panic(err)
	}
	return client
}

func csvToClassesSlice(csvFile *os.File, logFile *os.File) (classes []Class) {
	reader := csv.NewReader(csvFile)
	records, err := reader.ReadAll() // records is [][]strings
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	// look for the subject column and w column
	subjectCol := -1
	wCol := -1
	for j := 0; j < len(records[0]); j++ {
		if strings.Compare(records[0][j], "Subject") == 0 {
			subjectCol = j
		}
		if strings.Compare(records[0][j], "W") == 0 || strings.Compare(records[0][j], "Total W") == 0 || strings.Compare(records[0][j], "W Total") == 0 {
			wCol = j
		}
		if wCol == -1 || subjectCol == -1 {
			continue
		} else {
			break
		}
	}
	if wCol == -1 {
		logFile.WriteString("could not find W column")
	}
	catalogNumberCol := subjectCol + 1
	sectionCol := subjectCol + 2

	for i := 1; i < len(records); i++ {
		// convert grade distribution from string to int
		var tempSlice bson.A
		for j := 0; j < 13; j++ {
			var tempInt int
			fmt.Sscan(records[i][3+subjectCol+j], &tempInt)
			tempSlice = append(tempSlice, tempInt)
		}
		// add w number to the grade_distribution slice
		var tempInt int
		if wCol != -1 {
			fmt.Sscan(records[i][wCol], &tempInt)
		}
		tempSlice = append(tempSlice, tempInt)
		// add new class to classes slice
		classes = append(classes,
			Class{
				subject:           records[i][subjectCol],
				catalogNumber:     records[i][catalogNumberCol],
				section:           records[i][sectionCol],
				gradeDistribution: tempSlice,
			})
	}
	return classes
}

// inserts grades into mongodb
func insertGrades(sectionsCollection *mongo.Collection, coursesCollection *mongo.Collection, classes []Class, academicSession string, logFile *os.File) {
	for i := 0; i < len(classes); i++ {
		var result bson.D
		var err error
		err = coursesCollection.FindOne(context.TODO(), bson.D{{"course_number", classes[i].catalogNumber}, {"subject_prefix", classes[i].subject}}).Decode(&result)
		// if class is not in courses section
		if err != nil {
			// log that class could not be found
			logFile.WriteString("could not find course " + classes[i].subject + " " + classes[i].catalogNumber + "\n")
			fmt.Println("could not find course " + classes[i].subject + " " + classes[i].catalogNumber)
			fmt.Println(err)
		} else {
			mymap := result.Map()
			section_ids := reflect.ValueOf(mymap["sections"])
			for j := 0; j < section_ids.Len(); j++ {
				result = nil
				idStr := fmt.Sprint(section_ids.Index(j).Elem())
				idStr = strings.Split(idStr, "\"")[1]
				idObj, _ := primitive.ObjectIDFromHex(idStr) // can't use id string, ahs to be object
				err := sectionsCollection.FindOne(context.TODO(), bson.D{{"_id", idObj}}).Decode(&result)
				if err != nil {
					fmt.Println(err)
					continue
				}
				academicSessionE := result.Map()["academic_session"].(primitive.D)[0] // so i can access academic_session.name
				// if right section and academic session insert grade distribution and exit j loop
				if result.Map()["section_number"] == classes[i].section && academicSessionE.Value == academicSession {
					_, err := sectionsCollection.UpdateOne(context.TODO(), bson.M{"_id": idObj}, bson.D{{"$set", bson.D{{"grade_distribution", classes[i].gradeDistribution}}}})
					if err != nil {
						logFile.WriteString("could not modify " + classes[i].subject + " " + classes[i].catalogNumber + "." + classes[i].section + ", object id " + idStr + "\n")
						fmt.Println(err)
					} else {
						fmt.Println("added " + classes[i].subject + " " + classes[i].catalogNumber + "." + classes[i].section + " grade distribution")
					}
					break
				} else {
					continue
				}
			}
		}
	}
}

func main() {
	URI := configs.EnvMongoURI()
	helpMsg := "usage: \ngo run parser.go -file [file_path] -semester [academic_session]\nexamples: \ngo run parser.go -file \"Fall 2019.csv\" -semester 19F\ngo run parser.go -file \"Summer 2019.csv\" -semester 19U"
	fileFlag := flag.String("file", "", "csv grade file to be parsed")
	semesterFlag := flag.String("semester", "", "semester of the grades")
	helpFlag := flag.Bool("help", false, "if user needs help")
	flag.Parse()
	if *helpFlag == true || *fileFlag == "" || *semesterFlag == "" {
		fmt.Println(helpMsg)
		os.Exit(1)
	}
	csvPath := *fileFlag
	academicSession := *semesterFlag
	csvFile, err := os.Open(csvPath)
	if err != nil {
		fmt.Println("could not open file " + csvPath)
		fmt.Println(err)
		os.Exit(1)
	}

	// create logs directory
	if _, err := os.Stat("logs"); err != nil {
		os.Mkdir("logs", os.ModePerm)
	}
	// create log file [name of csv].log in logs directory
	logFileName := filepath.Base(csvPath)
	logFile, err := os.Create("logs/" + logFileName + ".log")
	if err != nil {
		fmt.Println("could not create log file")
		fmt.Println(err)
		os.Exit(1)
	}

	// put class data from csv into classes slice
	classes := csvToClassesSlice(csvFile, logFile)
	client := DBConnect(URI)

	// insert grades into mongodb
	insertGrades(getCollection(client, "combinedDB", "sections"), getCollection(client, "combinedDB", "courses"), classes, academicSession, logFile)
}
