package excel_to_gorm

import (
	"errors"
	"fmt"
	"log"
	"math"
	"reflect"
	"strings"

	"github.com/c4rnot/csv_to_gorm"
	"github.com/tealeg/xlsx/v3"
	//"github.com/c4rnot/xlsx/v3"
)

/*
*Instructions:
* =============
*
*  use struct tags to inform xtg how to handle the translation of excel to the variable.
* xtg tags have teh following form:
* `xtg:<instruction>:<parameter>,<instruction>:<parameter;parameter;parameter>`
*
* mapConst : parameter is the key to the Params constMap.  Value becomes the constant associated with the key
* col: The column name associated with this field
* intcols:colname  xtg will parse all columns whose column names  can parse as an integer.  A separate database record is created for each one
* intcols:value  This field is the value associated with the column headed by an integer.
* melt:colname  takes all colums not declared with col: and creates a separate record for each
* melt:value  takes value associated with colums not declared with col:
* ignore:  takes a ; separated list of strings.  These columns are ignored for melt
 */

type Tag struct {
	HasTag         bool
	HasColanme     bool
	Colname        string
	IsIntColsHead  bool
	IsIntColsValue bool
	IsMapConst     bool
	ConstMapKey    string
	IsMeltHead     bool
	IsMeltValue    bool
	Ignore         []string
}

type Params struct {
	colMap          map[string]int    // maps fieldnames to column numbers(starting at 1).  Overrides tagnames if mapping present
	ConstMap        map[string]string // maps from tagname mapConst:Mapfrom to a string constant to be parsed into the field
	FirstRowHasData bool
	ErrorOnNaN      bool
	//ErrorOnInf bool
}

func parseTag(field reflect.StructField) (Tag, error) {
	var tag Tag

	value, ok := field.Tag.Lookup("xtg")
	if !ok || value == "" {
		tag.HasTag = false
		return tag, nil
	}
	subTags := strings.Split(value, ",")

	for _, subTag := range subTags {
		subTagElements := strings.Split(subTag, ":")
		switch subTagElements[0] {
		case "col":
			tag.HasColanme = true
			if len(subTagElements) < 2 {
				return tag, errors.New("column name missing for field: " + field.Name + ". should be in the form col:<colname>")
			}
			tag.Colname = subTagElements[1]
		case "mapConst":
			tag.IsMapConst = true
			if len(subTagElements) < 2 {
				return tag, errors.New("constant map key is missing for field : " + field.Name + ". should be in the form mapConst:<mapkey>")
			}
			tag.ConstMapKey = subTagElements[1]
		case "intcols":
			if len(subTagElements) < 2 {
				return tag, errors.New("whether field is heading or value field : " + field.Name + ". should be in the form intcols:colname or intcols:value")
			}
			if strings.ToLower(subTagElements[1]) == "colname" {
				tag.IsIntColsHead = true
				tag.IsIntColsValue = false
			} else {
				tag.IsIntColsHead = false
				tag.IsIntColsValue = true
			}
		case "melt":
			if len(subTagElements) < 2 {
				return tag, errors.New("whether field is heading or value field : " + field.Name + ". should be in the form melt:colname or melt:value")
			}
			if strings.ToLower(subTagElements[1]) == "colname" {
				tag.IsMeltHead = true
				tag.IsMeltValue = false
			} else {
				tag.IsMeltHead = false
				tag.IsMeltValue = true
			}
		case "ignore":
			if len(subTagElements) == 1 {
				continue
			}
			ignoreStrings := strings.Split(subTagElements[1], ";")
			tag.Ignore = ignoreStrings
		}

	}
	return tag, nil
}

func ExcelFileToSlice(fileName string, sheetName string, model interface{}, params Params) (interface{}, error) {
	modelTyp := reflect.ValueOf(model).Elem().Type()
	objSlice := reflect.Zero(reflect.SliceOf(modelTyp))

	wb, err := xlsx.OpenFile(fileName)
	if err != nil {
		return objSlice.Interface(), errors.New("could not open file: " + fileName)
	}
	result, err := WorkbookToSlice(wb, sheetName, model, params)
	return result, err
}

// allows calling function to keep file open
// calling function needs to import "github.com/tealeg/xlsx/v3" and pass a pointer to an xlsx.File
// eg.: wb, err := xlsx.OpenFile(fileName)
func WorkbookToSlice(wb *xlsx.File, sheetName string, model interface{}, params Params) (interface{}, error) {
	modelTyp := reflect.ValueOf(model).Elem().Type()
	objSlice := reflect.Zero(reflect.SliceOf(modelTyp))

	sh, ok := wb.Sheet[sheetName]
	if !ok {
		return objSlice.Interface(), errors.New("could not find sheet:  " + sheetName)
	}
	defer sh.Close()
	result, err := WorksheetToSlice(sh, model, params)
	return result, err
}

// allows calling function to keep sheet open
// calling function needs to close the sheet
// calling function needs to import "github.com/tealeg/xlsx/v3" and pass a pointer to an xlsx.Sheet
// eg.: sh, ok := wb.Sheet[sheetName]
func WorksheetToSlice(sh *xlsx.Sheet, model interface{}, params Params) (interface{}, error) {

	// map of column headings to 1 based column numbers (for consistency with csv_to_gorm)
	var lclColMap map[string]int
	var hasIntCols bool
	var intColHdgs []string
	var hasMelt bool
	var meltColHdgs []string
	var ignore []string

	// determine what type of model we are trying to fill records of
	modelTyp := reflect.ValueOf(model).Elem().Type()
	modelNumFlds := modelTyp.NumField()

	// make an empty slice to hold the records to be uploaded to the db.

	objSlice := reflect.Zero(reflect.SliceOf(modelTyp))

	err := sh.ForEachRow(func(r *xlsx.Row) error {

		// Get headings from first row if necessary
		if r.GetCoordinate() == 0 {
			if !params.FirstRowHasData {
				lclColMap = mapHeadingToCol(r)
				intColHdgs = getIntCols(r)

				// check if there is an intcol tag, as a db entry has to be made for each int col
				for fldIx := 0; fldIx < modelNumFlds; fldIx++ {
					tag, err := parseTag(modelTyp.Field(fldIx))
					if err != nil {
						return fmt.Errorf("could not parse tag for sheetname:  "+sh.Name+". ", err)
					}
					if tag.IsIntColsHead || tag.IsIntColsValue {
						hasIntCols = true
					}
					if tag.IsMeltHead || tag.IsMeltValue {
						hasMelt = true
					}
					if len(tag.Ignore) > 0 {
						ignore = append(ignore, tag.Ignore...)
					}
				}
				if !hasMelt {
					meltColHdgs = []string{}
				}
				meltColHdgs = getMeltCols(r, lclColMap, ignore, hasIntCols, intColHdgs)
				return nil
			}
			// any other special first row code here
		}

		// create the new item to add to the database
		dbRecordPtr := reflect.New(modelTyp)

		if hasIntCols && !hasMelt {
			for _, intColHdg := range intColHdgs {
				// create the new item to add to the database
				dbRecordPtr = reflect.New(modelTyp)

				// for each field in the model
				for fldIx := 0; fldIx < modelNumFlds; fldIx++ {
					fld := modelTyp.Field(fldIx)
					fldName := fld.Name
					fldType := fld.Type
					tag, _ := parseTag(fld)

					paramsCol := params.colMap[fldName]
					//lclCol := lclColMap
					if paramsCol >= r.Sheet.MaxCol {
						log.Fatal("Column supplied in map is out of range")
					}
					// if a parameter column maps to the field
					if paramsCol > 0 {
						cell := r.GetCell(paramsCol - 1)
						dbRecordPtr.Elem().Field(fldIx).Set(CellToType(cell, fldType, params))
					} else {
						switch {
						case tag.IsMapConst:
							constString := params.ConstMap[tag.ConstMapKey]
							// trying to convert empty strings to numbers in csv_to_gorm will bomb!
							if constString == "" && fldType.Name() != "string" {
								return fmt.Errorf("tag constant: " + tag.ConstMapKey + " missing for sheet:  " + sh.Name + ". ")
							} else {
								dbRecordPtr.Elem().Field(fldIx).Set(csv_to_gorm.StringToType(constString, fldType))
							}
						case tag.IsIntColsHead:
							dbRecordPtr.Elem().Field(fldIx).Set(csv_to_gorm.StringToType(intColHdg, fldType))
						case tag.IsIntColsValue:
							dbRecordPtr.Elem().Field(fldIx).Set(CellToType(r.GetCell(lclColMap[intColHdg]-1), fldType, params))
						case tag.HasColanme:
							if lclColMap[tag.Colname] == 0 {
								fmt.Println("Could not find column header " + tag.Colname + " in sheet: " + sh.Name)
								return fmt.Errorf("Could not find column header " + tag.Colname + " in sheet: " + sh.Name)
							}
							dbRecordPtr.Elem().Field(fldIx).Set(CellToType(r.GetCell(lclColMap[tag.Colname]-1), fldType, params))
						}
					}
				}
				// add the record to the slice of records
				objSlice = reflect.Append(objSlice, dbRecordPtr.Elem())
			}

		} else if hasMelt && !hasIntCols {
			for _, meltColHdg := range meltColHdgs {
				// create the new item to add to the database
				dbRecordPtr = reflect.New(modelTyp)

				// for each field in the model
				for fldIx := 0; fldIx < modelNumFlds; fldIx++ {
					fld := modelTyp.Field(fldIx)
					fldName := fld.Name
					fldType := fld.Type
					tag, _ := parseTag(fld)

					paramsCol := params.colMap[fldName]
					if paramsCol >= r.Sheet.MaxCol {
						log.Fatal("Column supplied in map is out of range")
					}
					// if a parameter column maps to the field
					if paramsCol > 0 {
						cell := r.GetCell(paramsCol - 1)
						dbRecordPtr.Elem().Field(fldIx).Set(CellToType(cell, fldType, params))
					} else {
						switch {
						case tag.IsMapConst:
							constString := params.ConstMap[tag.ConstMapKey]
							// trying to convert empty strings to numbers in csv_to_gorm will bomb!
							if constString == "" && fldType.Name() != "string" {
								return fmt.Errorf("tag constant: " + tag.ConstMapKey + " missing for sheet:  " + sh.Name + ". ")
							} else {
								dbRecordPtr.Elem().Field(fldIx).Set(csv_to_gorm.StringToType(constString, fldType))
							}
						case tag.IsMeltHead:
							dbRecordPtr.Elem().Field(fldIx).Set(csv_to_gorm.StringToType(meltColHdg, fldType))
						case tag.IsMeltValue:
							dbRecordPtr.Elem().Field(fldIx).Set(CellToType(r.GetCell(lclColMap[meltColHdg]-1), fldType, params))
						case tag.HasColanme:
							if lclColMap[tag.Colname] == 0 {
								fmt.Println("Could not find column header " + tag.Colname + " in sheet: " + sh.Name)
								return fmt.Errorf("Could not find column header " + tag.Colname + " in sheet: " + sh.Name)
							}
							dbRecordPtr.Elem().Field(fldIx).Set(CellToType(r.GetCell(lclColMap[tag.Colname]-1), fldType, params))
						}
					}
				}
				// add the record to the slice of records
				// objArry.Index(recordIx).Set(reflect.ValueOf(dbRecordPtr.Elem().Interface()))
				objSlice = reflect.Append(objSlice, dbRecordPtr.Elem())
			}

		} else if hasMelt && hasIntCols {
			for _, meltColHdg := range meltColHdgs {
				for _, intColHdg := range intColHdgs {
					// create the new item to add to the database
					dbRecordPtr = reflect.New(modelTyp)

					// for each field in the model
					for fldIx := 0; fldIx < modelNumFlds; fldIx++ {
						fld := modelTyp.Field(fldIx)
						fldName := fld.Name
						fldType := fld.Type
						tag, _ := parseTag(fld)

						paramsCol := params.colMap[fldName]
						if paramsCol >= r.Sheet.MaxCol {
							log.Fatal("Column supplied in map is out of range")
						}
						// if a parameter column maps to the field
						if paramsCol > 0 {
							cell := r.GetCell(paramsCol - 1)
							dbRecordPtr.Elem().Field(fldIx).Set(CellToType(cell, fldType, params))
						} else {
							switch {
							case tag.IsMapConst:
								constString := params.ConstMap[tag.ConstMapKey]
								// trying to convert empty strings to numbers in csv_to_gorm will bomb!
								if constString == "" && fldType.Name() != "string" {
									return fmt.Errorf("tag constant: " + tag.ConstMapKey + " missing for sheet:  " + sh.Name + ". ")
								} else {
									dbRecordPtr.Elem().Field(fldIx).Set(csv_to_gorm.StringToType(constString, fldType))
								}
							case tag.IsIntColsHead:
								dbRecordPtr.Elem().Field(fldIx).Set(csv_to_gorm.StringToType(meltColHdg, fldType))
							case tag.IsIntColsValue:
								dbRecordPtr.Elem().Field(fldIx).Set(CellToType(r.GetCell(lclColMap[meltColHdg]-1), fldType, params))
							case tag.IsIntColsHead:
								dbRecordPtr.Elem().Field(fldIx).Set(csv_to_gorm.StringToType(intColHdg, fldType))
							case tag.IsIntColsValue:
								dbRecordPtr.Elem().Field(fldIx).Set(CellToType(r.GetCell(lclColMap[intColHdg]-1), fldType, params))
							case tag.HasColanme:
								if lclColMap[tag.Colname] == 0 {
									fmt.Println("Could not find column header " + tag.Colname + " in sheet: " + sh.Name)
									return fmt.Errorf("Could not find column header " + tag.Colname + " in sheet: " + sh.Name)
								}
								dbRecordPtr.Elem().Field(fldIx).Set(CellToType(r.GetCell(lclColMap[tag.Colname]-1), fldType, params))
							}
						}
					}
				}
				// add the record to the slice of records
				// objArry.Index(recordIx).Set(reflect.ValueOf(dbRecordPtr.Elem().Interface()))
				objSlice = reflect.Append(objSlice, dbRecordPtr.Elem())
			}

		} else {
			// for each field in the model
			for fldIx := 0; fldIx < modelNumFlds; fldIx++ {
				fld := modelTyp.Field(fldIx)
				fldName := fld.Name
				fldType := fld.Type
				tag, _ := parseTag(fld)

				paramsCol := params.colMap[fldName]
				//lclCol := lclColMap
				if paramsCol >= r.Sheet.MaxCol {
					log.Fatal("Column supplied in map is out of range")
				}
				// if a parameter column maps to the field
				if paramsCol > 0 {
					cell := r.GetCell(paramsCol - 1)
					dbRecordPtr.Elem().Field(fldIx).Set(CellToType(cell, fldType, params))
				} else {
					switch {
					case tag.IsMapConst:
						constString := params.ConstMap[tag.ConstMapKey]
						// trying to convert empty strings to numbers in csv_to_gorm will bomb!
						if constString == "" && fldType.Name() != "string" {
							return fmt.Errorf("tag constant: " + tag.ConstMapKey + " missing for sheet:  " + sh.Name + ". ")
						} else {
							dbRecordPtr.Elem().Field(fldIx).Set(csv_to_gorm.StringToType(constString, fldType))
						}
					case tag.HasColanme:
						if lclColMap[tag.Colname] == 0 {
							fmt.Println("Could not find column header " + tag.Colname + " in sheet: " + sh.Name)
							// TODO - How to handle the error?
						}
						dbRecordPtr.Elem().Field(fldIx).Set(CellToType(r.GetCell(lclColMap[tag.Colname]-1), fldType, params))
					}
				}
			}
			// add the record to the slice of records
			// objArry.Index(recordIx).Set(reflect.ValueOf(dbRecordPtr.Elem().Interface()))
			objSlice = reflect.Append(objSlice, dbRecordPtr.Elem())
		}

		return nil
	})
	if err != nil {
		return objSlice.Interface(), err
	}

	return objSlice.Interface(), nil

}

// get the first row of a worksheet, whixch is assumed to be the column heading names
func GetHeadings(fileName string, sheetName string) ([]string, error) {
	var headings []string
	wb, err := xlsx.OpenFile(fileName)
	if err != nil {
		return []string{""}, errors.New("could not open file: " + fileName)
	}

	sh, ok := wb.Sheet[sheetName]
	if !ok {
		return []string{""}, errors.New("could find sheet:  " + sheetName)
	}
	defer sh.Close()

	row1, _ := sh.Row(0)

	row1.ForEachCell(func(c *xlsx.Cell) error {
		heading := c.String()
		headings = append(headings, heading)
		return nil
	})

	return headings, nil
}

func mapHeadingToCol(r *xlsx.Row) map[string]int {
	colMap := make(map[string]int, r.Sheet.MaxCol)

	r.ForEachCell(func(c *xlsx.Cell) error {
		header := c.String()
		if header != "" {
			ColNo, _ := c.GetCoordinates()
			colMap[header] = ColNo + 1
		}
		return nil
	})
	return colMap
}

func getIntCols(r *xlsx.Row) []string {
	var intCols []string
	r.ForEachCell(func(c *xlsx.Cell) error {
		// converts strings.ParseFloat to int
		f, err := c.Float()
		if err == nil {
			if math.Abs(math.Round(f)-f) < 0.000001 {
				intCols = append(intCols, c.Value)
			}
		}
		return nil
	})
	return intCols
}

func getMeltCols(r *xlsx.Row, colMap map[string]int, ignoreHdgs []string, hasIntCols bool, intColHdgs []string) []string {
	var definedCols []string
	var meltCols []string
	r.ForEachCell(func(c *xlsx.Cell) error {
		for definedCol := range colMap {
			definedCols = append(definedCols, definedCol)
		}
		// check if column heading is
		heading := c.String()
		if heading == "" {
			return nil
		}
		_, isDefined := find(definedCols, heading)
		if isDefined {
			return nil
		}
		_, isIgnored := find(ignoreHdgs, heading)
		if isIgnored {
			return nil
		}
		_, isIntCol := find(intColHdgs, heading)
		if isIntCol && hasIntCols {
			return nil
		}

		// if we've not exited by now, we are a melt column
		meltCols = append(meltCols, heading)
		return nil
	})
	return meltCols
}

func GetSheetNames(fileName string) ([]string, error) {
	var sheets []string
	wb, err := xlsx.OpenFile(fileName)
	if err != nil {
		return sheets, errors.New("could not open file: " + fileName)
	}
	for _, sh := range wb.Sheets {
		sheets = append(sheets, sh.Name)
	}
	return sheets, nil
}

func GetSheetNameMap(fileName string) (map[string]int, error) {
	var sheetMap map[string]int
	wb, err := xlsx.OpenFile(fileName)
	if err != nil {
		return sheetMap, errors.New("could not open file: " + fileName)
	}

	sheetMap = make(map[string]int, len(wb.Sheets))

	for i, sh := range wb.Sheets {
		sheetMap[sh.Name] = i
	}
	return sheetMap, nil
}

// takes an excel cell and converts it to a reflect.Value of a given type (supplied as a reflect.Type)
// used internally, but exposed as it may have uses elsewhere
func CellToType(c *xlsx.Cell, outType reflect.Type, params Params) reflect.Value {
	var cellString string
	switch outType.Kind() {
	case reflect.String:
		cellString = c.Value
		return reflect.ValueOf(cellString)
	case reflect.Bool:
		cellString = c.Value
		//fmt.Println("Step a: bool")
		var firstLetter string
		if len(cellString) < 1 {
			firstLetter = ""
		} else {
			firstLetter = cellString[0:1]
		}
		if c.Bool() || strings.ContainsAny(firstLetter, "YyTt1") || strings.Contains(strings.ToLower(cellString), "true") || strings.Contains(strings.ToLower(cellString), "yes") {
			return reflect.ValueOf(true)
		} else {
			return reflect.ValueOf(false)
		}
	case reflect.Int, reflect.Uint, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		result := reflect.New(reflect.Type(outType))

		i, err := c.Int64()
		if err != nil {
			log.Fatal("CellToType could not convert "+c.Value+" to integer: ", err)
		} else {
			if outType.Kind() == reflect.Int || outType.Kind() == reflect.Int64 || outType.Kind() == reflect.Int32 || outType.Kind() == reflect.Int16 || outType.Kind() == reflect.Int8 {
				result.Elem().SetInt(int64(i))
				return result.Elem()
			} else {
				result.Elem().SetUint(uint64(i))
				return result.Elem()
			}
		}
	case reflect.Float32, reflect.Float64:
		resultPtr := reflect.New(reflect.Type(outType))

		// Postgres numeric doesnt support inf yet.  Convert to MaxFloat
		strVal := c.String()
		if strings.Contains(strVal, "inf") {

			if strings.Contains(strVal, "-") {
				if outType.Kind() == reflect.Float64 {
					f := -math.MaxFloat64
					resultPtr.Elem().SetFloat(f)
				} else {
					f := -math.MaxFloat32
					resultPtr.Elem().SetFloat(f)
				}
			} else {
				if outType.Kind() == reflect.Float64 {
					f := math.MaxFloat64
					resultPtr.Elem().SetFloat(f)
				} else {
					f := math.MaxFloat32
					resultPtr.Elem().SetFloat(f)
				}
			}
			return resultPtr.Elem()
		}

		f, err := c.Float()
		if err != nil {
			if params.ErrorOnNaN {
				log.Fatal("CellToType could not convert "+c.Value+" to float: ", err)
			}
			// if it's not a
			f = math.NaN()
		}

		resultPtr.Elem().SetFloat(f)
		return resultPtr.Elem()
	default:
		switch outType.String() {
		case "time.Time":
			dt, err := c.GetTime(false)
			if err != nil {
				log.Fatal("CellToType could not convert "+c.Value+" to date/time: ", err)
			}
			return reflect.ValueOf(dt)

		default:
			log.Fatal("CellToType has recieved a ", outType, " and does not kow how to handle it")

		}

	}
	return reflect.ValueOf(errors.New("CellToType could not convert type " + outType.Name()))
}

// Find takes a slice and looks for an element in it. If found it will
// return it's key, otherwise it will return -1 and a bool of false.
func find(slice []string, val string) (int, bool) {
	for i, item := range slice {
		if item == val {
			return i, true
		}
	}
	return -1, false
}
