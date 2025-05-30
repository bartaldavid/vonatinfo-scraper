package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type ResponseBody struct {
	D struct {
		Result struct {
			CreationTime string `json:"@CreationTime"`
			Trains       struct {
				Train []Train `json:"Train"`
			} `json:"Trains"`
		} `json:"result"`
	} `json:"d"`
}

type Train struct {
	Delay       int     `json:"@Delay"`
	Lat         float32 `json:"@Lat"`
	Lon         float32 `json:"@Lon"`
	Relation    string  `json:"@Relation"`
	Line        string  `json:"@Line"`
	TrainNumber string  `json:"@TrainNumber"`
	ElviraID    string  `json:"@ElviraID"`
	Menetvonal  string  `json:"@Menetvonal"`
}

func main() {
	err := initializeDatabase()
	if err != nil {
		log.Fatal("Error initializing database:", err)
	}

	data, err := fetchData()
	if err != nil {
		log.Fatal("Error fetching data:", err)
	}

	start := time.Now()
	err = saveData(data)
	if err != nil {
		log.Fatal("Error saving data:", err)
	}
	elapsed := time.Since(start)
	fmt.Printf("Data saving took %s\n", elapsed)

	fmt.Println("Data processing completed successfully.")

}

func fetchData() (ResponseBody, error) {
	url := "https://vonatinfo.mav-start.hu/map.aspx/getData"

	body := `{"a": "TRAINS", "jo": {"history": false, "id": false}}`

	req, err := http.NewRequest("POST", url, bytes.NewBufferString(body))
	if err != nil {
		fmt.Println("Error creating request:", err)
		return ResponseBody{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println("Error fetching data:", err)
		return ResponseBody{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Println("Error: received non-200 response code:", resp.StatusCode)
		return ResponseBody{}, fmt.Errorf("non-200 response code: %d", resp.StatusCode)
	}
	var responseBody ResponseBody
	// Decode the response body into the ResponseBody struct
	// You can use a JSON decoder here, e.g.:
	err = json.NewDecoder(resp.Body).Decode(&responseBody)
	if err != nil {
		fmt.Println("Error decoding response body:", err)
		return ResponseBody{}, err
	}

	return responseBody, nil
}

func initializeDatabase() error {
	sqlQuery, err := os.ReadFile("../initial-schema.sql")
	if err != nil {
		log.Fatal("Error reading SQL file:", err)
	}

	db, err := sql.Open("sqlite3", "./test.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(string(sqlQuery))
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Database initialized successfully")
	return nil
}

func saveData(data ResponseBody) error {
	db, err := sql.Open("sqlite3", "./test.db")
	if err != nil {
		log.Fatal("Error opening database:", err)
	}
	defer db.Close()

	tx, _ := db.Begin()
	stmt, err := tx.Prepare("INSERT INTO train_position (timestamp, lat, lon, relation, train_number, menetvonal, elvira_id) VALUES (?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		log.Fatal("Error preparing statement:", err)
	}
	defer stmt.Close()

	for _, train := range data.D.Result.Trains.Train {
		timeStamp, err := time.Parse("2006.01.02 15:04:05", data.D.Result.CreationTime)
		if err != nil {
			log.Fatal("Error parsing timestamp:", err)
		}
		timeStampInt := timeStamp.Unix()

		if err != nil {
			log.Fatal("Error parsing timestamp:", err)
		}
		_, err = stmt.Exec(
			timeStampInt, train.Lat, train.Lon,
			train.Relation, train.TrainNumber, train.Menetvonal, train.ElviraID)
		if err != nil {
			log.Fatal("Error inserting data:", err)
		}
	}
	tx.Commit()
	log.Println("Data saved successfully")
	return nil
}
