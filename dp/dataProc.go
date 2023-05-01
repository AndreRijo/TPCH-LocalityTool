package dp

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"
	"tpch_client/src/client"

	//"tpch_client/src/tpch"
	tpch "potionDB/tpch_helper"
)

//Parameters given by the command line
type LoadParameters struct {
	DataLoc                                                        string
	Sf, OLocRate, ILocRate, OneRemRate, TwoRemRate, TwoRemDiffRate float64
	NUpdFiles                                                      int
}

const (
	header = "tpch_headers/tpch_headers_full.txt"
)

var (
	lp LoadParameters

	updEntries          []int
	updCompleteFilename [3]string

	headerLoc, tableFolder, updFolder string

	tables  [][][]string //Base, unprocessed, tables
	toRead  [][]int8     //Fields of each table that should be read
	keys    [][]int      //Fields that compose the primary key of each table
	headers [][]string   //The name of each field in each table

	procTables     *tpch.Tables
	partToRegToSup [][][]int32 //partkey -> []region -> []suppkey
	nRegions       int

	rng *rand.Rand
)

func ProcessData(loadP LoadParameters) {
	//Prepare variables
	lp = loadP
	fmt.Println("Doing preparatory work...")
	prepVars()
	fixTableEntries()

	//First, read base data
	fmt.Println("Reading data...")
	readBaseData()
	fmt.Println("Processing data...")
	processBaseData()
	preparePartToRegToSupTable()
	//Now, modify base data
	fmt.Println("Modifying data locality...")
	modifyData()
	checkData()
	fmt.Println("Writting modified data...")
	//Write base data
	modTableFolder := tableFolder + "mod/"
	WriteTable(modTableFolder+client.TableNames[tpch.LINEITEM]+client.TableExtension, itemsToString())

	if lp.NUpdFiles == 0 {
		fmt.Println("Done! (did not process updates as n_upd_files is 0)")
		return //No updates
	}
	//Read update data
	fmt.Println("Reading and processing update data...")
	readAndProcessUpdates()
	//Modify update data
	fmt.Println("Modifying update data locality...")
	modifyData()
	checkData()
	//Write update data
	fmt.Println("Writting updates...")
	updModFilename := updFolder + "mod/" + client.UpdsNames[1] + client.UpdExtension
	WriteUpdates(updModFilename, getTableEntries(tpch.ORDERS), lp.NUpdFiles, itemsToStringPerOrder())
	fmt.Println("Done!")

}

func readBaseData() {
	headers, keys, toRead = tpch.ReadHeaders(headerLoc, len(client.TableNames))
	tables = make([][][]string, len(client.TableNames))
	//Force these to be read first
	readTable(tpch.REGION)
	readTable(tpch.NATION)
	readTable(tpch.SUPPLIER)
	readTable(tpch.CUSTOMER)
	readTable(tpch.ORDERS)
	//Order is irrelevant now
	readTable(tpch.LINEITEM)
	readTable(tpch.PARTSUPP)
	readTable(tpch.PART)

	procTables.NationsByRegion = tpch.CreateNationsByRegionTable(procTables.Nations, procTables.Regions)
}

func processBaseData() {
	processTable(tpch.REGION)
	processTable(tpch.NATION)
	processTable(tpch.SUPPLIER)
	processTable(tpch.CUSTOMER)
	processTable(tpch.ORDERS)
	processTable(tpch.LINEITEM)
	processTable(tpch.PARTSUPP)
	processTable(tpch.PART)
}

func readAndProcessUpdates() {
	updPartsRead := [][]int8{toRead[tpch.ORDERS], toRead[tpch.LINEITEM]}
	startUpdFiles := 1
	ordersUpds, lineItemUpds, _, _, itemSizesPerOrder := tpch.ReadUpdatesPerOrder(updCompleteFilename[:], updEntries[:], client.UpdParts[:], updPartsRead, startUpdFiles, lp.NUpdFiles)
	createOrdersTable(ordersUpds)
	createItemsTable(lineItemUpds, itemSizesPerOrder)
}

func createOrdersTable(ordersUpds [][]string) {
	orders := make([]*tpch.Orders, len(ordersUpds)+1) //The original orders table has the first entry empty
	for i, orderSlice := range ordersUpds {
		orders[i+1] = procTables.CreateOrder(orderSlice)
	}
	procTables.Orders = orders
	fmt.Println("NOrders:", len(ordersUpds), "Last orderID:", orders[len(orders)-1].O_ORDERKEY)
}

func createItemsTable(itemsUpds [][]string, itemSizesPerOrder []int) {
	items := make([][]*tpch.LineItem, len(itemSizesPerOrder))
	j := 0
	for i, size := range itemSizesPerOrder {
		orderItems := itemsUpds[j : j+size]
		j += size
		items[i] = procTables.CreateLineitemsOfOrder(orderItems)
	}
	procTables.LineItems = items
}

func prepVars() {
	nRegions = client.TableEntries[tpch.REGION]
	scaleFactorS := strconv.FormatFloat(lp.Sf, 'f', -1, 64)
	tableFolder, updFolder = lp.DataLoc+fmt.Sprintf(client.TableFormat, scaleFactorS), lp.DataLoc+fmt.Sprintf(client.UpdFormat, scaleFactorS)
	updCompleteFilename = [3]string{updFolder + client.UpdsNames[0] + client.UpdExtension, updFolder + client.UpdsNames[1] + client.UpdExtension,
		updFolder + client.UpdsNames[2] + client.DeleteExtension}
	headerLoc = lp.DataLoc + header
	//A certain % of the items ordered are already local. 20% for 5 regions
	lp.ILocRate -= (1.0 / float64(client.TableEntries[tpch.REGION]))
	procTables = &tpch.Tables{}
	procTables.InitConstants(false)

	//preparing odds for easier access
	//oneRemRate, twoRemRate, twoRemDiffRate
	lp.TwoRemRate += lp.OneRemRate
	lp.TwoRemDiffRate += lp.TwoRemRate
}

func readTable(tableN int) {
	fmt.Println("Reading", client.TableNames[tableN], tableN)
	nEntries := client.TableEntries[tableN]
	if client.TableUsesSF[tableN] {
		nEntries = int(float64(nEntries) * lp.Sf)
	}
	tables[tableN] = tpch.ReadTable(tableFolder+client.TableNames[tableN]+client.TableExtension, client.TableParts[tableN], nEntries, toRead[tableN])
}

func processTable(tableN int) {
	switch tableN {
	case tpch.CUSTOMER:
		procTables.CreateCustomers(tables)
	case tpch.LINEITEM:
		procTables.CreateLineitems(tables)
	case tpch.NATION:
		procTables.CreateNations(tables)
	case tpch.ORDERS:
		procTables.CreateOrders(tables)
	case tpch.PART:
		procTables.CreateParts(tables)
	case tpch.REGION:
		procTables.CreateRegions(tables)
	case tpch.PARTSUPP:
		procTables.CreatePartsupps(tables)
	case tpch.SUPPLIER:
		procTables.CreateSuppliers(tables)
	}
}

func preparePartToRegToSupTable() {
	nRegions := client.TableEntries[tpch.REGION]
	//partkey -> []region -> []suppkey
	partToRegToSup = make([][][]int32, getTableEntries(tpch.PART)+1) //First entry is empty
	for i := range partToRegToSup {
		partToRegToSup[i] = make([][]int32, nRegions)
	}

	//Go through all partSups and fill in
	//suppkeyToRegionkey
	partSups := procTables.PartSupps[1:]
	for _, partSup := range partSups {
		partKey, regKey := partSup.PS_PARTKEY, procTables.SuppkeyToRegionkey(int64(partSup.PS_SUPPKEY))
		partToRegToSup[partKey][regKey] = append(partToRegToSup[partKey][regKey], partSup.PS_SUPPKEY)
	}
}

func checkAllLocal(order *tpch.Orders, index int) {
	items := procTables.LineItems[index]
	custReg := procTables.Custkey32ToRegionkey(order.O_CUSTKEY)
	for _, item := range items {
		suppReg := procTables.SuppkeyToRegionkey(int64(item.L_SUPPKEY))
		if custReg != suppReg {
			fmt.Println("[LOCAL]")
			fmt.Println("Order", *order, "is not fully local!")
			fmt.Println("CustReg:", custReg, "ItemReg:", suppReg)
			os.Exit(0)
		}
	}
}

func checkOneRemote(order *tpch.Orders, index int) {
	items := procTables.LineItems[index]
	custReg := procTables.Custkey32ToRegionkey(order.O_CUSTKEY)
	nRemote := 0
	for _, item := range items {
		suppReg := procTables.SuppkeyToRegionkey(int64(item.L_SUPPKEY))
		if custReg != suppReg {
			nRemote++
		}
	}
	if nRemote != 1 {
		fmt.Println("[ONE_REMOTE]")
		fmt.Println("Order", *order, "is not 'one remote'!")
		fmt.Println("Number of remotes:", nRemote, "Number of entries:", len(items))
		os.Exit(0)
	}
}

func checkTwoRemoteSame(order *tpch.Orders, index int) {
	items := procTables.LineItems[index]
	custReg := int(procTables.Custkey32ToRegionkey(order.O_CUSTKEY))
	regs := make([]int, len(procTables.Regions))
	for _, item := range items {
		regs[procTables.SuppkeyToRegionkey(int64(item.L_SUPPKEY))]++
	}
	nRemote, nDiffRemote := 0, 0
	for i, nReg := range regs {
		if i == custReg {
			//Don't count local
			continue
		}
		if nReg > 0 {
			nDiffRemote++
			nRemote += nReg
		}
	}
	if nRemote == 1 && len(items) == 1 {
		return //OK case
	}
	if nRemote != 2 || nDiffRemote != 1 {
		fmt.Println("[TWO_REMOTE_SAME]")
		fmt.Println("Order", *order, "is not 'two remote same'!")
		fmt.Println("Number of remotes:", nRemote, "Number of different remote regions:",
			nDiffRemote, "Number of entries:", len(items))
		os.Exit(0)
	}
}

func checkTwoRemoteDiff(order *tpch.Orders, index int) {
	items := procTables.LineItems[index]
	custReg := int(procTables.Custkey32ToRegionkey(order.O_CUSTKEY))
	regs := make([]int, len(procTables.Regions))
	for _, item := range items {
		regs[procTables.SuppkeyToRegionkey(int64(item.L_SUPPKEY))]++
	}
	nRemote, nDiffRemote := 0, 0
	for i, nReg := range regs {
		if i == custReg {
			//Don't count local
			continue
		}
		if nReg > 0 {
			nDiffRemote++
			nRemote += nReg
		}
	}
	if nRemote == 1 && len(items) == 1 {
		return //OK case
	}
	if nRemote != 2 || nDiffRemote != 2 {
		fmt.Println("[TWO_REMOTE_DIFF]")
		fmt.Println("Order", *order, "is not 'two remote diff'!")
		fmt.Println("Number of remotes:", nRemote, "Number of different remote regions:",
			nDiffRemote, "Number of entries:", len(items))
		fmt.Println("CustReg:", custReg)
		for _, item := range items {
			fmt.Println("SuppReg:", procTables.SuppkeyToRegionkey(int64(item.L_SUPPKEY)), "Item:", *item)
		}
		os.Exit(0)
	}
}

func modifyData() {
	orders := procTables.Orders[1:]
	rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	for i, order := range orders {
		allOrderLocRnd := rng.Float64()
		if (i+1)%10000 == 0 {
			fmt.Println(i, "orders done")
		}
		if allOrderLocRnd < lp.OLocRate {
			//fmt.Println("[AllLocal]")
			allItemToLocal(order, i)
			checkAllLocal(order, i)
		} else {
			locTypeRnd := rng.Float64()
			if locTypeRnd < lp.OneRemRate {
				//fmt.Println("[OneRemote]")
				oneItemToRemote(order, i)
				checkOneRemote(order, i)
			} else if locTypeRnd < lp.TwoRemRate {
				//fmt.Println("[TwoSameRemote]")
				twoItemToRemoteSameReg(order, i)
				checkTwoRemoteSame(order, i)
			} else if locTypeRnd < lp.TwoRemDiffRate {
				//fmt.Println("[TwoDiffRemote]")
				twoItemToRemoteDiffReg(order, i)
				checkTwoRemoteDiff(order, i)
			} else {
				//fmt.Println("[Random]")
				itemToLocalWithOdds(order, i)
			}
		}
	}
}

func allItemToLocal(order *tpch.Orders, index int) {
	items := procTables.LineItems[index]
	orderReg := procTables.OrderkeyToRegionkey(order.O_ORDERKEY)
	for _, item := range items {
		updateLineItemToLocal(item, orderReg)
	}
}

//Note: No need to update L_EXTENDEDPRICE or any other field if supplier changes as...
//L_EXTENDEDPRICE = L_QUANTITY * P_RETAILPRICE. Discount and tax are randoms.
func itemToLocalWithOdds(order *tpch.Orders, index int) {
	items := procTables.LineItems[index]
	orderReg := procTables.OrderkeyToRegionkey(order.O_ORDERKEY)
	for _, item := range items {
		itemToLocalRnd := rng.Float64()
		if itemToLocalRnd < lp.ILocRate {
			updateLineItemToLocal(item, orderReg)
		}
		//else, do nothing, keeps local or remote
	}
}

/*
	25% - all local except 1 remote
	25% - all local except 2 remote, same region
	25% - all local except 2 remote, random region
	25% - random (50% chance local, 50% chance remote of any region)

	Maybe strategy can be like this?
	All except 1 remote - just find one remote, keep it like that, change all others to local
	2 remote - search if there's two remote with same region. If yes, keep both and change all other to local
			   otherwise, pick first remote's region and force second's remote to be the same, all others local.
	2 remote - search if there's two remote with diff region. If yes, keep both and change all others to local
				otherwise, pick first two remote, change 2nd to a different region
	random - use existing code
*/

func twoItemToRemoteDiffReg(order *tpch.Orders, index int) {
	orderReg := procTables.OrderkeyToRegionkey(order.O_ORDERKEY)
	items := procTables.LineItems[index]
	if len(items) == 1 {
		//Applies logic to single item order item instead
		oneItemToRemote(order, index)
		return
	}
	_, firstRemoteReg, secondRemote := twoItemRemoteAux(order, items, orderReg)
	updateLineItemToDiffRemote(items[secondRemote], orderReg, firstRemoteReg)
}

func twoItemToRemoteSameReg(order *tpch.Orders, index int) {
	orderReg := procTables.OrderkeyToRegionkey(order.O_ORDERKEY)
	items := procTables.LineItems[index]
	if len(items) == 1 {
		//Applies logic to single item order item instead
		oneItemToRemote(order, index)
		return
	}
	_, firstRemoteReg, secondRemote := twoItemRemoteAux(order, items, orderReg)
	//Convert second remote to first remote's region. This works by setting orderReg to the first item's reg
	updateLineItemToLocal(items[secondRemote], firstRemoteReg)
}

//Searches for two candidates to be converted to remote. Also updates all others to local.
//Pre: there must be at least two items in the order
func twoItemRemoteAux(order *tpch.Orders, items []*tpch.LineItem, orderReg int8) (firstRemote, firstRemoteReg, secondRemote int8) {
	firstRemote, firstRemoteReg, secondRemote = int8(-1), int8(-1), int8(-1)
	i := int8(0)
	for _, item := range items {
		i++
		supReg := procTables.SuppkeyToRegionkey(int64(item.L_SUPPKEY))
		if orderReg != supReg {
			firstRemote, firstRemoteReg = i-1, supReg //Found first remote, break
			break
		}
	}
	//Search for second
	for ; i < int8(len(items)); i++ {
		supReg := procTables.SuppkeyToRegionkey(int64(items[i].L_SUPPKEY))
		if orderReg != supReg {
			secondRemote = i //Found second remote, point it
			break
		}
	}
	if firstRemote == -1 {
		firstRemote = int8(rng.Intn(len(items)))
		updateLineItemToRemote(items[firstRemote], orderReg)
		firstRemoteReg = procTables.SuppkeyToRegionkey(int64(items[firstRemote].L_SUPPKEY))
	}
	if secondRemote == -1 {
		for secondRemote == -1 || secondRemote == firstRemote {
			secondRemote = int8(rng.Intn(len(items)))
		}
	}
	//Set others to local
	for ; i < int8(len(items)); i++ {
		updateLineItemToLocal(items[i], orderReg)
	}
	return
}

func oneItemToRemote(order *tpch.Orders, index int) {
	orderReg := procTables.OrderkeyToRegionkey(order.O_ORDERKEY)
	items := procTables.LineItems[index]
	foundRemote, i := false, 0
	for _, item := range items {
		i++
		supReg := procTables.SuppkeyToRegionkey(int64(item.L_SUPPKEY))
		if orderReg != supReg {
			foundRemote = true //Found remote to keep, break
			break
		}
	}
	if !foundRemote {
		//Forcing one random position to be remote. All others are already local
		rnd := rng.Intn(len(items))
		updateLineItemToRemote(items[rnd], orderReg)
	}
	for ; i < len(items); i++ {
		//Force local
		updateLineItemToLocal(items[i], orderReg)
	}
}

func updateLineItemToLocal(item *tpch.LineItem, orderReg int8) {
	supReg := procTables.SuppkeyToRegionkey(int64(item.L_SUPPKEY))
	if supReg == orderReg {
		//Already local, nothing to do
		return
	}
	//Change to local
	partKey := item.L_PARTKEY
	done := false
	//First iteration tries to find a local supplier for the same part.
	//If there's no such supplier, pick random parts until we find a part with a local supplier

	for !done {
		if updateItemIfHasSup(item, partKey, orderReg) {
			done = true
		} else {
			//Try with a random part
			partKey = int32(1 + rng.Intn(len(partToRegToSup)-1)) //Adjustements as IDs go from 1 to N
		}
	}
}

func updateLineItemToRemote(item *tpch.LineItem, orderReg int8) {
	supReg := procTables.SuppkeyToRegionkey(int64(item.L_SUPPKEY))
	if supReg != orderReg {
		//Already remote, nothing to do
		return
	}
	//Change to remote
	partKey := item.L_PARTKEY
	done := false
	//First iteration tries to find a remote supplier for the same part.
	//If there's no such supplier (rare), pick random parts until we find a part with a remote supplier

	for !done {
		for reg := int8(0); reg < int8(nRegions); reg++ {
			if reg == orderReg {
				continue
			}
			if updateItemIfHasSup(item, partKey, reg) {
				done = true
				break
			}
			//Try next region
		}
		//Try with a random part
		partKey = int32(1 + rng.Intn(len(partToRegToSup)-1)) //Adjustements as IDs go from 1 to N
	}
}

func updateLineItemToDiffRemote(item *tpch.LineItem, orderReg int8, diffFromReg int8) {
	supReg := procTables.SuppkeyToRegionkey(int64(item.L_SUPPKEY))
	if supReg != orderReg && supReg != diffFromReg {
		//No change needed
		return
	}

	//Change to remote
	partKey := item.L_PARTKEY
	done := false
	//First iteration tries to find a remote supplier for the same part.
	//If there's no such supplier (rare), pick random parts until we find a part with a remote supplier

	for !done {
		for reg := int8(0); reg < int8(nRegions); reg++ {
			if reg == orderReg || reg == diffFromReg {
				continue
			}
			if updateItemIfHasSup(item, partKey, reg) {
				done = true
				break
			}
			//Try next region
		}
		//Try with a random part
		partKey = int32(1 + rng.Intn(len(partToRegToSup)-1)) //Adjustements as IDs go from 1 to N
	}
}

//Returns true if the item was updated
func updateItemIfHasSup(item *tpch.LineItem, partKey int32, reg int8) bool {
	if partKey < 0 {
		fmt.Println("Warning - negative partKey!!!", partKey)
	}
	if reg < 0 || reg > 5 {
		fmt.Println("Warning - invalid reg!!!", reg)
	}
	suppliers := partToRegToSup[partKey][reg]
	if suppliers != nil && len(suppliers) > 0 {
		//Has a supplier in another region! Choose one randomly
		item.L_PARTKEY, item.L_SUPPKEY = partKey, suppliers[rng.Intn(len(suppliers))]
		return true
	}
	return false
}

/*
Locality plan:

Things to consider:
	- When changing regions of a lineitem, there must be a matching partsupp
		- Can do like this:
			- check if partsupp exists for item (how?)
				- if it does, then change to any supp of that region
				- otherwise, pick any other part at random
	- Apply two % rules:
		- a) % of the order being "local-only"
		- b) % of each lineitem being local
			- Have to consider that by default 20% are already local
	- No efficiency is needed for this process - this is done outside of tests.

I may need extra structures?
	- Structure of partsupp by partkey -> [region] -> [suppkey]
	- I think that is about it

*/

/*
	Extra details:
	Process both base data and updates right away? Seperately? For now, consider just base data.
*/

func fixTableEntries() {
	updEntries = make([]int, 3)
	switch lp.Sf {
	case 0.01:
		client.TableEntries[tpch.LINEITEM] = 60175
		//updEntries = []int{10, 37, 10}
		updEntries = []int{15, 41, 16}
	case 0.1:
		client.TableEntries[tpch.LINEITEM] = 600572
		//updEntries = []int{150, 592, 150}
		//updEntries = []int{150, 601, 150}
		updEntries = []int{151, 601, 150}
	case 0.2:
		client.TableEntries[tpch.LINEITEM] = 1800093
		updEntries = []int{300, 1164, 300} //NOTE: FAKE VALUES!
	case 0.3:
		client.TableEntries[tpch.LINEITEM] = 2999668
		updEntries = []int{450, 1747, 450} //NOTE: FAKE VALUES!
	case 1:
		client.TableEntries[tpch.LINEITEM] = 6001215
		//updEntries = []int{1500, 5822, 1500}
		//updEntries = []int{1500, 6001, 1500}
		updEntries = []int{1500, 6010, 1500}
	}
}

//All items in a row
func itemsToString() (itemsString [][]string) {
	itemsString = make([][]string, client.TableEntries[tpch.LINEITEM])
	allItems := procTables.LineItems
	i := 0
	for _, orderItems := range allItems {
		for _, item := range orderItems {
			itemsString[i] = item.ToStringSlice()
			i++
		}
	}
	return
}

//Items grouped by order
func itemsToStringPerOrder() (itemsString [][][]string) {
	itemsString = make([][][]string, getTableEntries(tpch.ORDERS))
	allItems := procTables.LineItems
	fmt.Println(len(itemsString), len(allItems))
	for i, orderItems := range allItems {
		itemsString[i] = make([][]string, len(orderItems))
		for j, item := range orderItems {
			itemsString[i][j] = item.ToStringSlice()
		}
	}
	return
}

//Checks if the data was properly converted
func checkData() {
	orders := procTables.Orders[1:]
	items := procTables.LineItems
	nOrders := float64(len(orders))
	fullLocal, oneRem, twoRemSame, twoRemDiff, random := float64(0), float64(0), float64(0), float64(0), float64(0)
	for i, order := range orders {
		orderItems := items[i]
		nItems := len(orderItems)
		regMap := make([]int, 5)
		for _, item := range orderItems {
			regMap[procTables.SuppkeyToRegionkey(int64(item.L_SUPPKEY))]++
		}
		nLocal := regMap[procTables.OrderkeyToRegionkey(order.O_ORDERKEY)]
		if nLocal == nItems {
			fullLocal++
		} else if nLocal == nItems-1 {
			oneRem++
		} else if nLocal == nItems-2 {
			nRegs := 0
			for _, amount := range regMap {
				//If there's only 2 entries with values > 0, then it's same region (local + 1 remote)
				if amount > 0 {
					nRegs++
				}
			}
			if nRegs == 2 {
				twoRemSame++
			} else {
				twoRemDiff++
			}
		} else {
			random++
		}
	}
	fmt.Println("[DATACHECK]")
	fmt.Println("Note: The following counters are estimates. They're unlikely to reflect the actual odds applied due to randomness in small orders.")
	fmt.Println("E.g: an order with 3 items in all different regions, can be either of type '2 remote with diff regions' or 'random'")
	fmt.Println("The program will count such examples as '2 remote with diff regions'. The priority order is: local -> 2 remote same -> 2 remote diff -> random")
	fmt.Println("Total orders:", nOrders)
	fmt.Printf("Local: %f (rate: %f)\n", fullLocal, fullLocal/nOrders)
	fmt.Printf("One rem: %f (rate: %f)\n", oneRem, oneRem/nOrders)
	fmt.Printf("Two rem same region: %f (rate: %f)\n", twoRemSame, twoRemSame/nOrders)
	fmt.Printf("Two rem diff region: %f (rate: %f)\n", twoRemDiff, twoRemDiff/nOrders)
	fmt.Printf("Random (>= 3 remote): %f (rate: %f)\n", random, random/nOrders)
}

func getTableEntries(entryType int) int {
	if client.TableUsesSF[entryType] {
		return int(float64(client.TableEntries[entryType]) * lp.Sf)
	}
	return client.TableEntries[entryType]
}
