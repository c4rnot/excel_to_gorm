package main

import (
	"fmt"
	"log"
	"os"

	"github.com/c4rnot/excel_to_gorm"
	"github.com/tealeg/xlsx/v3"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Apple struct {
	gorm.Model // include ID, CretedAt, UpdatedAt, DeletedAt
	Name       string
	Diameter   float64
	Popularity float64
	Origin     string
	Discovered uint
	ForCooking bool
	ForEating  bool
}

var (
	colMap = map[string]int{
		"Name":       1,
		"Diameter":   2,
		"Popularity": 3,
		"Origin":     4,
		"Discovered": 5,
		"ForCooking": 6,
		"ForEating":  7,
	}

	//fileName = "apples.csv"
	fileName = "apples.xlsx"
)

type Orange struct {
	gorm.Model         // include ID, CretedAt, UpdatedAt, DeletedAt
	Name       string  `xtg:"col:Name"`
	Diameter   float64 `xtg:"col:diameter"`
	Popularity float64 `xtg:"col:Liked By"`
}

type Yield struct {
	gorm.Model         // include ID, CretedAt, UpdatedAt, DeletedAt
	Name       string  `xtg:"col:Name"`
	Product    string  `xtg:"mapConst:product"`
	Year       int     `xtg:"intcols:colname"`
	Yield      float64 `xtg:"intcols:value"`
}

type PestLoss struct {
	gorm.Model         // include ID, CretedAt, UpdatedAt, DeletedAt
	Name       string  `xtg:"col:Name"`
	Cause      string  `xtg:"melt:colname"`
	Loss       float64 `xtg:"melt:value"`
}

type BiggestExporter struct {
	gorm.Model      // include ID, CretedAt, UpdatedAt, DeletedAt
	Country         string
	Type            string `xtg:"melt:colname"`
	ExportCode      int    `xtg:"melt:value"`
	Year            int    `xtg:"intcols:colname"`
	BiggestExporter string `xtg:"intcols:value"`
}

func main() {

	// open the file to read and defer its closure
	wb, err := xlsx.OpenFile(fileName)
	if err != nil {
		log.Fatal(err)
	}

	// connect to the DB
	dsn := os.Getenv("DATABASE_URL")
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		CreateBatchSize: 1000,
	})
	if err != nil {
		log.Fatal("failed to connect database ", err)
	}

	// Ensure the expected schema is applied to the database
	err = db.AutoMigrate(&Apple{})
	if err != nil {
		log.Fatal("could not migrate schema ", err)
	}
	db.AutoMigrate(&Apple{})
	db.AutoMigrate(&Orange{})
	db.AutoMigrate(&Yield{})
	db.AutoMigrate(&PestLoss{})
	db.AutoMigrate(&BiggestExporter{})

	// ** Read excel to database
	// *******************************

	params := excel_to_gorm.Params{
		ColMap:          colMap,
		FirstRowHasData: false,
	}
	// note, you need to typecast the returned interface so that Gorm knows what it is
	apples, err := excel_to_gorm.WorkbookToSlice(wb, "colMap-apples", &Apple{}, params)
	if err != nil {
		fmt.Println(err.Error())
	}
	db.Model(&Apple{}).Create(apples) //PASS

	params = excel_to_gorm.Params{
		FirstRowHasData: false,
	}
	sh, ok := wb.Sheet["oranges"]
	if !ok {
		fmt.Println("Could not open sheet: oranges")
	}
	oranges, err := excel_to_gorm.WorksheetToSlice(sh, &Orange{}, params)
	if err != nil {
		fmt.Println(err.Error())
	}
	db.Model(&Orange{}).Create(oranges) //PASS

	params = excel_to_gorm.Params{
		FirstRowHasData: false,
		ConstMap:        map[string]string{"product": "apple"},
	}
	sh, ok = wb.Sheet["intcols-yield-by-year"]
	if !ok {
		fmt.Println("Could not open sheet: intcols-yield-by-year")
	}
	yields, err := excel_to_gorm.WorksheetToSlice(sh, &Yield{}, params)
	//yields = yields.([]Yield)
	if err != nil {
		fmt.Println(err.Error())
	}
	db.Model(&Yield{}).Create(yields) //PASS

	params = excel_to_gorm.Params{
		FirstRowHasData: false,
	}
	sh, ok = wb.Sheet["melt-pest-losses"]
	if !ok {
		fmt.Println("Could not open sheet: melt-pest-losses")
	}
	pestLosses, err := excel_to_gorm.WorksheetToSlice(sh, &PestLoss{}, params)
	//pestLosses = pestLosses.([]PestLoss)
	if err != nil {
		fmt.Println(err.Error())
	}
	db.Model(&PestLoss{}).Create(pestLosses) //FAIL

	params = excel_to_gorm.Params{
		FirstRowHasData: false,
	}
	sh, ok = wb.Sheet["melt-pest-losses"]
	if !ok {
		fmt.Println("Could not open sheet: melt-pest-losses")
	}
	biggestExporters, err := excel_to_gorm.WorksheetToSlice(sh, &BiggestExporter{}, params)
	biggestExporters = biggestExporters.([]BiggestExporter)
	if err != nil {
		fmt.Println(err.Error())
	}
	db.Model(&BiggestExporter{}).Create(biggestExporters) // FAIL

}
