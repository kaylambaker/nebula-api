// go run parser.go [file_path] [semster]
// example: go run parser.go "Fall 2019.csv" 19F

package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"parser/configs"
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

func main() {
	URI := configs.EnvMongoURI()
	if len(os.Args) != 3 {
		fmt.Println("usage: \ngo build parser.go\n./parser.exe [file_path] [academic_session]\nexample: \ngo build parser.go\n./parser.exe \"Fall 2019.csv\" 19F")
	}
	filePath := os.Args[1]
	academicSession := os.Args[2]
	var classes []Class
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("could not open file " + filePath)
		fmt.Println(err)
		return
	}
	reader := csv.NewReader(file)
	records, err := reader.ReadAll() // records is [][]strings
	if err != nil {
		fmt.Println(err)
		return
	}
	// create logs directory
	if _, err := os.Stat("logs"); err != nil {
		os.Mkdir("logs", os.ModePerm)
	}
	str := strings.Split(filePath, "/")
	if len(str) == 1 {
		str = strings.Split(filePath, "\\")
	}
	logFile, err := os.Create("logs/" + strings.Split(str[len(str)-1], ".")[0] + ".log")
	if err != nil {
		fmt.Println("could not create log file")
		fmt.Println(err)
		return
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
	// connect to monodb //
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(URI))
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	if err := client.Ping(context.TODO(), readpref.Primary()); err != nil {
		fmt.Println(err)
		panic(err)
	}
	coursesCollection := client.Database("combinedDB").Collection("courses")
	sectionsCollection := client.Database("combinedDB").Collection("sections")
	// -- //
	for i := 0; i < len(classes); i++ {
		var result bson.D
		err = coursesCollection.FindOne(context.TODO(), bson.D{{"course_number", classes[i].catalogNumber}, {"subject_prefix", classes[i].subject}}).Decode(&result)
		// if class is not in courses section
		if err != nil {
			// log that class could not be found
			logFile.WriteString("could not find course " + classes[i].subject + " " + classes[i].catalogNumber + "\n")
			fmt.Println("could not find course " + classes[i].subject + " " + classes[i].catalogNumber)
		} else {
			mymap := result.Map()
			section_ids := reflect.ValueOf(mymap["sections"])
			for j := 0; j < section_ids.Len(); j++ {
				result = nil
				idStr := fmt.Sprint(section_ids.Index(j).Elem())
				idStr = strings.Split(idStr, "\"")[1]
				idObj, _ := primitive.ObjectIDFromHex(idStr)
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
