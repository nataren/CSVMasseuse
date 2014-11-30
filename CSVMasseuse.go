package main

import (
	"net/http"
	"fmt"
	"errors"
	"encoding/csv"
	"flag"
	"log"
	"os"
	"bufio"
	"strconv"
	"github.com/kellydunn/golang-geo"
	"github.com/olivere/elastic"
)

type OutpatientService struct {
	APC string // The service
	ProviderId string
	ProviderName string
	ProviderStreetAddress string
	ProviderCity string
	ProviderState string
	ProviderZipCode string
	ProviderHRR string // HRR = Hospital Referral Region
	OutpatientServices int64
	AverageEstimatedSubmittedCharges float64
	AverageTotalPayments float64
	Latitude float64
	Longitude float64
}

func GetOutpatientService (record []string) (*OutpatientService, error) {
	outpatientServices, osErr := strconv.ParseInt(record[8], 10, 64)
	if osErr != nil {
		return nil, errors.New("Unable to parse OutpatientServices value")
	}
	averageEstimatedSubmittedCharges, estimatedSubmittedChargesErr := strconv.ParseFloat(record[9], 64)
	if estimatedSubmittedChargesErr != nil {
		return nil, errors.New("Unable to parse Average Estimated Submitted Charges value")
	}
	averageTotalPayments, totalPaymentsErr := strconv.ParseFloat(record[10], 64)
	if totalPaymentsErr != nil {
		return nil, errors.New("Unable to parse Average Total Payments")
	}
	return &OutpatientService {
		APC: record[0],
		ProviderId: record[1],
		ProviderName: record[2],
		ProviderStreetAddress: record[3],
		ProviderCity: record[4],
		ProviderState: record[5],
		ProviderZipCode: record[6],
		ProviderHRR: record[7],
		OutpatientServices: outpatientServices,
		AverageEstimatedSubmittedCharges: averageEstimatedSubmittedCharges,
		AverageTotalPayments: averageTotalPayments,
	}, nil
}

func main() {

	// Read command arguments
	var dataFile string
	flag.StringVar(&dataFile, "files", "", "Comma separated list of files to massage")
	var recordsToProcess int
	flag.IntVar(&recordsToProcess, "records", 0, "Number of records to process")
	var searchHostname string
	flag.StringVar(&searchHostname, "search-hostname", "", "The search engine's hostname")
	var searchPort string
	flag.StringVar(&searchPort, "search-port", "", "The search engine's port")
	flag.Parse()
	if dataFile == "" {
		log.Println("No file was specified")
		return
	}
	if recordsToProcess == 0 {
		log.Println("No records to process, will exit")
		return
	}
	if searchHostname == "" {
		log.Println("No searchHostname provided, will exit")
		return
	}
	if searchPort ==  "" {
		log.Println("No searchPort provided, will exit")
		return
	}
	file, err := os.Open(dataFile)
	if err != nil {
		log.Printf("Error trying to read file '%s'", dataFile)
		return
	}

	// Read the values
	csvReader := csv.NewReader(bufio.NewReader(file))
	csvReader.Read()
	geoCoder := &geo.GoogleGeocoder {}

	// New csv writer
/*
	newFile, fileCreateErr := os.Create(fmt.Sprintf("gps_%s", dataFile))
	if fileCreateErr != nil {
		log.Println("Could not create new csv file with GPS information")
		return
	}
	csvWriter := csv.NewWriter(bufio.NewWriter(newFile))
*/

	client, err := elastic.NewClient(http.DefaultClient, fmt.Sprintf("%s:%s", searchHostname, searchPort))
	if err != nil {
		log.Printf("Could not connect to ES: %s", err)
		return
	}
	fmt.Println(client)

	// Init the ES index
	exists, err := client.IndexExists("healthadvisor").Do()
	if err != nil {
		panic(err)
	}
	if !exists {
		log.Println("Will create index 'healthadvisor'")

		// Create index
		createIndex, err := client.CreateIndex("healthadvisor").Do()
		if err != nil {
			panic(err)
		}
		if !createIndex.Acknowledged {
			log.Println("The index %s has not been acknowledged", "healthadvisor")
		}
	}

	// Get GPS locations for some of the locations
	for i := 0; i < recordsToProcess; i++ {
		record, recErr := csvReader.Read()
		if recErr != nil {
			log.Println("Error trying to read the first record from the dataSet")
			continue
		}
		outs, err := GetOutpatientService(record)
		if err != nil {
			log.Println("Could not parse record as an OutpatientService")
			continue
		}
		
		// Get the Latitude and Longitude for the addresses
		address := fmt.Sprintf("%s, %s, %s, %s", outs.ProviderStreetAddress, outs.ProviderCity, outs.ProviderState, outs.ProviderZipCode)
		geoCode, geoErr := geoCoder.Geocode(address)
		if geoErr != nil {
			log.Printf("There was an error retrieving GPS information for %s", address)
			continue
		}
		outs.Latitude = geoCode.Lat()
		outs.Longitude = geoCode.Lng()
		indexResult, err := client.Index().
			Index("healthadvisor").
			Type("service").
			Id(string(i)).
			BodyJson(outs).
			Do()
		if err != nil {
			log.Println("Could not index %s", outs)
			continue
		}
		log.Println(indexResult)

/*
		csvWriter.Write([]string {
			outs.APC,
			outs.ProviderId,
			outs.ProviderName,
			outs.ProviderStreetAddress,
			outs.ProviderCity,
			outs.ProviderState,
			outs.ProviderZipCode,
			outs.ProviderHRR,
			fmt.Sprintf("%d", outs.OutpatientServices),
			fmt.Sprintf("%f", outs.AverageEstimatedSubmittedCharges),
			fmt.Sprintf("%f", outs.AverageTotalPayments),
			fmt.Sprintf("%f", geoCode.Lat()),
			fmt.Sprintf("%f", geoCode.Lng()),
		})
*/
	}

/*
	csvWriter.Flush()
	flushErr := csvWriter.Error()
	if flushErr != nil {
		log.Println("There was an error flushing the file")
	}
*/

	flushResult, err := client.Flush().Index("healthadvisor").Do()
	if err != nil {
		panic(err)
	}
	log.Println(flushResult)
}
