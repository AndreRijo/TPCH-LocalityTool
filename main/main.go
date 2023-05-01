package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"tpch_data_processor/dp"
)

func main() {
	lp := loadFlags()
	if !parametersCheck(lp) {
		fmt.Println("One or more parameters are invalid, program will exit")
		os.Exit(0)
	}
	fmt.Println("Parameters are valid.")
	dp.ProcessData(lp)

}

func loadFlags() dp.LoadParameters {
	dataLoc := flag.String("data_loc", "none", "location of the data to process")
	//headerLoc := flag.String("header_loc", "none", "location of the headers")
	orderLocality := flag.Float64("order_rate", -1, "rate (0.2-1) to which an entire order should be with local items")
	itemLocality := flag.Float64("item_rate", -1, "rate (0-1) to which an item in an order should be local, for random orders")
	oneRemRate := flag.Float64("one_rem_rate", -1, "rate (0-1) to which an order should have 1 remote item")
	twoRemRate := flag.Float64("two_rem_rate", -1, "rate (0-1) to which an order should have 2 remote items of the same region")
	twoDiffRegRemRate := flag.Float64("two_diff_rem_rate", -1, "rate (0-1) to which an order should have 2 remote items of different regions")
	sf := flag.Float64("sf", math.MinInt64, "sf for the data")
	nUpdFiles := flag.Int64("n_upd_files", -1, "Number of update files.")

	flag.Parse()
	close := false

	if !isStringFlagValid(dataLoc) {
		fmt.Println("Dataloc undefined.")
		close = true
	}
	/*if !isStringFlagValid(headerLoc) {
		fmt.Println("Headerloc undefined.")
		close = true
	}*/
	if !isFloatFlagValid(orderLocality) {
		fmt.Println(*orderLocality)
		fmt.Println("Rate of order undefined.")
		close = true
	}
	if !isFloatFlagValid(itemLocality) {
		fmt.Println("Rate of item undefined")
		close = true
	}
	if !isFloatFlagValid(sf) {
		fmt.Println("SF underfined.")
		close = true
	}
	if !isFloatFlagValid(oneRemRate) {
		fmt.Println("Rate of one remote item undefined.")
		close = true
	}
	if !isFloatFlagValid(twoRemRate) {
		fmt.Println("Rate of two remote items (same region) undefined.")
		close = true
	}
	if !isFloatFlagValid(twoDiffRegRemRate) {
		fmt.Println("Rate of two remote items (diff region) undefined.")
		close = true
	}
	if !isIntFlagValid(nUpdFiles) {
		fmt.Println("Number of update files", nUpdFiles)
	}

	if close {
		fmt.Println("Program will exit due to required parameters not being fully defined.")
		os.Exit(0)
	}
	return dp.LoadParameters{DataLoc: *dataLoc, Sf: *sf, OLocRate: *orderLocality, ILocRate: *itemLocality,
		OneRemRate: *oneRemRate, TwoRemRate: *twoRemRate, TwoRemDiffRate: *twoDiffRegRemRate, NUpdFiles: int(*nUpdFiles)}
}

//Checks if parameters have valid values
func parametersCheck(lp dp.LoadParameters) bool {
	check := true
	fmt.Printf("DataLoc: %s\n Sf: %f; OLocRate: %f; ILocRate: %f\n"+
		"OneRemRate: %f; TwoRemRate: %f; twoRemDiffRegionsRate: %f\n", lp.DataLoc, lp.Sf,
		lp.OLocRate, lp.ILocRate, lp.OneRemRate, lp.TwoRemRate, lp.TwoRemDiffRate)
	if lp.Sf <= 0 {
		check = false
		fmt.Println("Invalid SF:", lp.Sf)
	}
	if lp.OLocRate <= 0 || lp.OLocRate >= 1 {
		check = false
		fmt.Println("Invalid OLocRate:", lp.OLocRate)
	}
	if lp.ILocRate <= 0 || lp.ILocRate >= 1 {
		check = false
		fmt.Println("Invalid ILocRate:", lp.ILocRate)
	}
	if lp.OneRemRate <= 0 || lp.OneRemRate >= 1 {
		check = false
		fmt.Println("Invalid OneRemRate:", lp.OLocRate)
	}
	if lp.TwoRemRate <= 0 || lp.TwoRemRate >= 1 {
		check = false
		fmt.Println("Invalid TwoRemRate (same region):", lp.TwoRemRate)
	}
	if lp.TwoRemDiffRate <= 0 || lp.TwoRemDiffRate >= 1 {
		check = false
		fmt.Println("Invalid TwoRemDiffRate (diff region):", lp.TwoRemDiffRate)
	}
	if lp.NUpdFiles < 0 {
		check = false
		fmt.Println("Invalid NUpdFiles:", lp.NUpdFiles)
	}
	return check
}

func isStringFlagValid(value *string) bool {
	return *value != "none" && *value != "" && *value != " "
}

func isFloatFlagValid(value *float64) bool {
	return *value != -1
}

func isIntFlagValid(value *int64) bool {
	return *value != math.MinInt64
}
