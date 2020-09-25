package main

//Download HTTP resources using go routines and workers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const HTTP_TIMEOUT = 10

var messages chan string

type LatLon struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

type XYZ struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
	Z float64 `json:"z"`
}

type Quaternion struct {
	Q0 float64 `json:"q0"`
	Q1 float64 `json:"q1"`
	Q2 float64 `json:"q2"`
	Q3 float64 `json:"q3"`
}

type NasaImageDesc struct {
	Identifier            string     `json:"identifier"`
	Caption               string     `json:"caption"`
	Image                 string     `json:"image"`
	Version               string     `json:"version"`
	Centroid_coordinates  LatLon     `json:"centroid_coordinates"`
	Dscovr_j2000_position XYZ        `json:"dscovr_j2000_position"`
	Lunar_j2000_position  XYZ        `json:"lunar_j2000_position"`
	Sun_j2000_position    XYZ        `json:"sun_j2000_position"`
	Attitude_quaternions  Quaternion `json:"attitude_quaternions"`
	Date                  string     `json:"date"`
}

func getGID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}

func getYearMonthDay(basename string) (string) {
	//epic_1b_20200923001751
	splits := strings.Split(basename, "_")
	last_part := splits[len(splits)-1]
	year := last_part[0:4]
	month := last_part[4:6]
	day := last_part[6:8]
	return year + "/" + month + "/" + day
}

func processImages(client *http.Client, output_directory string, image_directory string, apikey string, url []NasaImageDesc) {
	id := getGID()
	var output []string
	for i := 0; i < len(url); i++ {
		imageURL := "https://api.nasa.gov/EPIC/archive/natural/" + getYearMonthDay(url[i].Image) + "/png/" + url[i].Image + ".png?api_key=" + apikey
		bytesFile, errJson := json.Marshal(url[i])
		if errJson != nil {
			messages <- "ERROR " + strconv.FormatUint(id, 10) + " - Serializing JSON for " + url[i].Image + " [ " + errJson.Error() + " ] "
		}
		err := ioutil.WriteFile(output_directory + "/" +url[i].Image+".json", bytesFile, 0777)
		if err != nil {
			messages <- "ERROR " + strconv.FormatUint(id, 10) + " - Writing info " + url[i].Image + " [ " + err.Error() + " ] "
		} else {
			messages <- "SUCCESS " + strconv.FormatUint(id, 10) + " - Writing info " + url[i].Image
		}
		messages <- strconv.FormatUint(id, 10) + " - Downloading " + imageURL
		output = append(output, processOneImage(client, id, image_directory, url[i].Image + ".png", imageURL))
	}
	for i := 0; i < len(output); i++ {
		messages <- output[i]
	}
}

func processOneImage(client *http.Client, id uint64, output_directory string, basename string, url string) string {
	response, err := client.Get(url)
	if err != nil {
		return "ERROR " + strconv.FormatUint(id, 10) + " - Downloading " + url
	}

	contents, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return err.Error()
	}
	defer response.Body.Close()

	err = ioutil.WriteFile(output_directory+"/"+basename, contents, 0777)

	if err != nil {
		return "ERROR " + strconv.FormatUint(id, 10) + " - Downloading " + url + " [ " + err.Error() + " ] "
	}
	return strconv.FormatUint(id, 10) + " - Completed " + url
}

func min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}

func main() {
	workernum := 3
	apikey := os.Getenv("NASA_KEY")

	if len(apikey) == 0 {
		apikey = "<INSERT_API_KEY>"
	}
	url := "https://api.nasa.gov/EPIC/api/natural?api_key=" + apikey
	client := http.Client{
		Timeout: HTTP_TIMEOUT * time.Second,
	}
	response, err := client.Get(url)
	if err != nil {
		log.Fatal("ERROR Downloading " + url + " -  " + err.Error())
	}

	defer response.Body.Close()
	contents, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Fatal("ERROR Reading BODY Of " + url + " -  " + err.Error())
	}

	messages = make(chan string, 1)

	err = os.Mkdir("./data", os.ModePerm)
	if err != nil && !strings.Contains(err.Error(), "file exists") {
		log.Fatal("Error " + err.Error())
	}


	err = os.Mkdir("./images", os.ModePerm)
	if err != nil && !strings.Contains(err.Error(), "file exists") {
		log.Fatal("Error " + err.Error())
	}

	var entries []NasaImageDesc
	_ = json.Unmarshal(contents, &entries)

	for i := 0; i < len(entries); i += workernum {
		chunks := entries[i:min(i+workernum, len(entries))]
		go processImages(&client,"./data", "./images", apikey, chunks)
	}

	//Number of messages is well known: 3 per worker. First: writing JSON data. Second: Progress download. Third: OK/Error download
	for idx := 0; idx < len(entries)*3; idx++ {
		fmt.Println(<-messages)
	}

	fmt.Println("OK")
}
