package main

import (
	"fmt"
//	"log"
	"os"
//	"bufio"
	"time"
//	"encoding/csv"
	"encoding/json"
	"encoding/gob"
//	"io"
//	"io/ioutil"
	"flag"
//	"strconv"
//	"github.com/aunum/gold/pkg/v1/common/num"
	"github.com/aunum/gold/pkg/v1/common/require"
	"github.com/aunum/gold/pkg/v1/dense"
	"github.com/aunum/goro/pkg/v1/layer"
	m "github.com/aunum/goro/pkg/v1/model"
	"github.com/aunum/log"

	g "gorgonia.org/gorgonia"
	"gorgonia.org/tensor"

	"github.com/go-gota/gota/dataframe"
//	"github.com/go-gota/gota/series"
	"gonum.org/v1/gonum/mat"


)

const	(
	NUMVENUES 	=	86
	NUMRUNNING	=	13
	NUMCOND		=	4
	NUMAGE		=	24
	NUMRTYPE	=	4
	NUMGROUND	=	4
	NUMCLASS	=	14
	NUMSTARTERS	=	5
	NUMRACETYPE	=	19
	NUMWINDDIR	=	8
	MAXWIND		=	120.0
	VERMAJ		=	0
	VERMIN		=	1
	VERPATCH	=	1

)	


// T1 : theta: [-0.56  -0.44  -0.62  -0.05  ...  2.23   1.18   3.08   7.64]  Iter: 19999 Cost: 0.223 Accuracy: 0.78  
// T2 : theta: [-0.18   0.92   0.53   0.14  ...  1.92   1.28   2.44   7.64]  Iter: 19999 Cost: 0.617 Accuracy: 0.48
/*	Input fields
	Venue			string								=> 	86 columms
	Starters		int									=>	5 columns
	Distance 		float64 (in furlongs)				=>	1 column, divide by 35
	Going 			string								=> 	13 columns
	Conds 			string 	"",Handicap,Listed,Stakes	=>	4 columns
	Ages 			string								=>	24 columns
	RTypes 			string								=>	4 columns
	Ground 			string								=> 	4 columns
	Class 			string can be ""					=>	14 columns
	RaceTypes 		[]int								=>	19 columns
	WindDir 		string								=>	2x8 columns cross product with wind and gust
	Wind			float64
	Gust			float64
*/

	
type RacesRecord	struct	{
	IdRace		int64
	IdVenue 	int
	Starters 	int
	Distance	float64
	IdRunning	int
	IdCond		int
	IdAge		int
	IdRType		int
	IdGround	int
	IdClass		int
	WindSpeed	float64
	WindGust	float64
	WindDir		int
	WindQuarter	int
	WinTime		float64
}


var 	(
	numcols 	=1923
	ModelName	string
	InFileName 	string
	OutFileName	string
	HeaderFile	string
	Debug		bool
	Columns		=	[]string{"Venue","Distance","Going","RTypes","Surface","WindDir","Wind","Gust"}

	Venues	map[string]int=map[string]int{"Ludlow":1,"Lingfield":2,"Taunton":3,"Newcastle":4,"Clonmel":5,"Southwell":6,"Newbury":7,"Doncaster":8,"Dundalk":9,"Kelso":10,"Navan":11,
					"Sedgefield":12,"Huntingdon":13,"Leopardstown":14,"Wexford":15,"Wetherby":16,"Wolverhampton":17,"Sandown Park":18,"Fontwell Park":19,"Catterick Bridge":20,
					"Kempton Park":21,"Carlisle":22,"Thurles":23,"Wincanton":24,"Plumpton":25,"Leicester":26,"Musselburgh":27,"Limerick":28,"Ffos Las":29,"Exeter":30,
					"Chelmsford City":31,"Chepstow":32,"Market Rasen":33,"Fairyhouse":34,"Fakenham":35,"Bangor-On-Dee":36,"Uttoxeter":37,"Warwick":38,"Naas":39,
					"Punchestown":40,"Tramore":41,"Cheltenham":42,"Ayr":43,"Hereford":44,"Cork":45,"Ascot":46,"Haydock Park":47,"Down Royal":48,"Gowran Park":49,
					"Stratford-on-Avon":50,"Hexham":51,"Aintree":52,"Downpatrick":53,"Laytown":54,"Redcar":55,"Nottingham":56,"Worcester":57,"Newmarket":58,"Killarney":59,
					"Chester":60,"Tipperary":61,"Pontefract":62,"Brighton":63,"Galway":64,"York":65,"Curragh":66,"Newton Abbot":67,"Goodwood":68,"Yarmouth":69,"Bath":70,"Sligo":71,
					"Hamilton Park":72,"Salisbury":73,"Kilbeggan":74,"Thirsk":75,"Perth":76,"Windsor":77,"Epsom Downs":78,"Ballinrobe":79,"Beverley":80,"Listowel":81,
					"Ripon":82,"Roscommon":83,"Bellewstown":84,"Cartmel":85,"Towcester":86}
	Runnings	map[string]int	=	map[string]int{"Firm":1,"Good":2,"Good to Firm":3,"Good to Soft":4,"Good to Yielding":5,"Heavy":6,"Slow":7,"Soft":8,"Soft to Heavy":9,
					"Standard":10,"Standard to Slow":11,"Yielding":12,"Yielding to Soft":13}
	Conds		map[string]int	=	map[string]int{"":1,"Handicap":2,"Listed":3,"Stakes":4}
	Ages 		map[string]int	=	map[string]int{"10yo+":1,"2yo+":2,"2yo3":3,"2yoO":4,"3yo+":5,"3yo4":6,"3yo5":7,"3yo6":8,"3yoO":9,"4yo+":10,"4yo5":11,"4yo6":12,"4yo7":13,"4yo8":14,"4yoO":15,
					"5yo+":16,"5yo6":17,"5yo7":18,"5yo8":19,"5yoO":20,"6yo+":21,"6yo7":22,"7yo+":23,"8yo+":24}
	RTypes		map[string]int	=	map[string]int{"Chase":1,"Flat":2,"Hurdle":3,"National Hunt Flat":4}
	Grounds		map[string]int	=	map[string]int{"Allweather":1,"Sand":2,"Polytrack":3,"Turf":4}
	Classes		map[string]int	=	map[string]int{"":1,"Class 1":2,"Class 2":3,"Class 3":4,"Class 4":5,"Class 5":6,"Class 6":7,"Class 7":8,"D.I":9,"Grade A":10,"Grade B":11,
					"Premier Handicap":12,"Q.R.":13,"Qualifier":14}
	WindDir	map[string]int	=	map[string]int{"N":1,"NE":2,"E":3,"SE":4,"S":5,"SW":6,"W":7,"NW":8}
	Starters 	=	[]string{"","St2-6","St7-10","St11-15","St16-24","St24+"}
	RaceTracks  	[]string 		// slice of venueid|furlongs|rtype to column number
	NumRaceTracks 	int
	MaxDistance 	float64
	testX			*tensor.Dense	// testX contains the expanded data to be used to predict times
	inputX 			dataframe.DataFrame 	// inputX contains the unexpanded input data or nil
)


type matrix struct {
	dataframe.DataFrame
}

func (m matrix) At(i, j int) float64 {
	return m.Elem(i, j).Float()
}

func (m matrix) T() mat.Matrix {
	return mat.Transpose{Matrix: m}
}


func main() {
	fmt.Printf("Predict v%d.%d.%d\n",VERMAJ,VERMIN,VERPATCH)

	sMN:=flag.String("mn","model.gob","Filename of the model, default model.gob")
	sIN:=flag.String("in","input.csv","Filename of the input dataset, default input.csv")
	sOUT:=flag.String("out","output.csv","Filename of the output(results) file, default output.csv")
	sHeaderFile:=flag.String("headers","headers.json","Filename containing header definitions, default headers.json")
	fMaxDistance:=flag.Float64("md",35.0,"Max Distance in furlongs default 35")
	bRaw:=flag.Bool("raw",true,"If true, process the headers as contained in the input file (default true), ignores headers.json")
	bDebug:=flag.Bool("debug",false,"If true, save the raw csv file as debug.csv which created from the compressed input file.")
	flag.Parse()
	ModelName=*sMN
	InFileName=*sIN
	OutFileName=*sOUT
	HeaderFile=*sHeaderFile
	MaxDistance=*fMaxDistance
	Debug=*bDebug
	
	// build inputs
	xi := m.NewInput("x", []int{1,numcols})
	
	yi := m.NewInput("y", []int{1})
	
	
	// build model
	model, err := m.NewSequential("sp")
	require.NoError(err)

	model.AddLayers(
		layer.FC{Input: numcols, Output: numcols*2 ,Name: "L0", NoBias: false},
//		layer.Dropout{},
		layer.FC{Input: numcols*2, Output: 20 ,Name: "L1", NoBias: true},
		layer.FC{Input: 20, Output: 1 ,Name: "O0", NoBias: true, Activation:layer.NewLinear()},
	)

//	optimizer := g.NewRMSPropSolver(g.WithBatchSize(float64(batchSize)),g.WithLearnRate(LearnRate),g.WithL1Reg(0.2))
	err = model.Compile(xi, yi,
//		m.WithOptimizer(optimizer),
		m.WithLoss(m.MSE),
//		m.WithLoss(m.NewPseudoHuberLoss(0.2)),
//		m.WithBatchSize(batchSize),
	)
	require.NoError(err)
	err=loadmodel(ModelName,model)
	require.NoError(err)
	
	predfile,err:=os.Create(OutFileName)
	if err!=nil 	{
		log.Fatal("Failed to create output file ",OutFileName," : ",err)
	}
	defer predfile.Close()
	
	numCols:=0	// set to number of columns in input file
	numRows:=0	// set to number of rows in input file
	if *bRaw	{
		// raw (expanded) input file
		df,headers:=rawinputs(InFileName)
		numRows, numCols = df.Dims()
		if numCols!=numcols 	{
			log.Fatal("Input file has ",numCols," fields, model requires ",numcols)
		}
		fmt.Println("Read ",numRows," rows of ",numCols," columns")
		for h:=0;h<len(headers);h++	{
			predfile.WriteString(fmt.Sprintf("%s,",headers[h]))
		}	
		predfile.WriteString("Target\n")
		testX=tensor.FromMat64(mat.DenseCopyOf(&matrix{df}),tensor.As(g.Float32))
	}	else 	{
		// Condensed input file which needs expanding. testX is always raw (expanded)
		df,dfinput,headers:=squashedinputs(InFileName)
		numRows, numCols = dfinput.Dims()
		nc:=df.Shape()[1]
		if nc!=numcols 	{
			log.Fatal("Input file has ",nc," fields, model requires ",numcols)
		}
		fmt.Println("Read ",numRows," rows of ",numCols," columns which expand to ",nc," columns")
		for h:=0;h<len(headers);h++	{
			predfile.WriteString(fmt.Sprintf("%s,",headers[h]))
		}	
		predfile.WriteString("Target\n")
		testX=df.(*tensor.Dense)
//		testX=tensor.FromMat64(mat.DenseCopyOf(&matrix{df}),tensor.As(g.Float32))
		inputX=dfinput
		testxrow,testxcol:=inputX.Dims()
		fmt.Println("Shape of compressed testX: ",testxrow,testxcol)
	}
	
	
	fmt.Println("Shape testX: ",testX.Shape())
	time.Sleep(10*time.Second)
	for row:=0;row<numRows;row++	{
		xi, err := testX.Slice(dense.MakeRangedSlice(row, row+1))
		require.NoError(err)
		xi.Reshape(1,numcols)
		yHat, err := model.Predict(xi)
		require.NoError(err)
		if *bRaw 	{
			for c:=0;c<numCols;c++	{
				d,err:=xi.At(0,c)
				require.NoError(err)
				predfile.WriteString(fmt.Sprintf("%.2f,",d.(float32)))
			}
		} 	else 	{
			for c:=0;c<numCols;c++	{
				v:=inputX.Elem(row,c)
				
				switch {
				case c==0:	predfile.WriteString(fmt.Sprintf("%s,",v.String())) 			//venue
				case c==1:	predfile.WriteString(fmt.Sprintf("%.1f,",v.Float()))			//distance
				case c>=2 && c<=5:	predfile.WriteString(fmt.Sprintf("%s,",v.String()))		//going,RTypes,Surface,WindDir
				case c>5:	predfile.WriteString(fmt.Sprintf("%.2f,",v.Float()))			//wind,gust
				}
			}
		}
//		fmt.Println("yHat data: ",yHat.Data())
		
		d:=yHat.Data().([]float32)
//		fmt.Sprintf("%.2f\n",d[0])
		predfile.WriteString(fmt.Sprintf("%.2f\n",d[0]))
	}	
	predfile.Close()
}


func 	Expand(id,max int,scale float32)	(columns []float32)	{
	if id>max 	{
		log.Fatal("Id ",id," is greater than max ",max)
	}
	for c:=1;c<=max;c++	{
		if c==id	{
			columns=append(columns,scale)
		}	else 	{
			columns=append(columns,0)
		}
	}
	return
}

func loadmodel(filename string,model *m.Sequential ) error {
	f, err := os.Open(filename)
	if err != nil {
			return err
	}
	defer f.Close()
	enc := gob.NewDecoder(f)
	learnnodes:=model.Learnables()
	for _, node := range learnnodes {
			err := enc.Decode(node.Value())
			if err != nil {
					return err
			}
	}
	err=model.SetLearnables(learnnodes)
	return err
}

func 	rawinputs(filename string) 	(df dataframe.DataFrame,headers []string)	{
	f, err := os.Open(filename)
	if err != nil {
		log.Fatal("Failed to open input file ",filename," : ",err)
	}
	defer f.Close()
	
	fmt.Printf("Reading input file %s...",filename)
	df=dataframe.ReadCSV(f)
	headers=df.Names()
//	for h:=0;h<len(headers);h++	{
//		fmt.Printf("Header %d %s,",h,headers[h])
//	}
	fmt.Printf("\n") 
	return
}

func 	squashedinputs(filename string) 	(df tensor.Tensor,dfinput dataframe.DataFrame,headers []string)	{
	var 	allrows		[]float32
	var		validrows	[]int

	fmt.Printf("Reading header file %s...",HeaderFile)

	headerjson,err:=os.ReadFile(HeaderFile)
	if err!=nil	{
		log.Fatal("Failed to read header file ",HeaderFile," : ",err)
	}
	err=json.Unmarshal(headerjson,&RaceTracks)
	require.NoError(err)
	NumRaceTracks=len(RaceTracks)
	fmt.Printf("Loaded RaceTracks map, %d entries\n",NumRaceTracks)


	fmt.Printf("Reading input file %s...",filename)
	f, err := os.Open(filename)
	if err != nil {
		log.Fatal("Failed to open input file ",filename," : ",err)
	}
	defer f.Close()
	dfinput=dataframe.ReadCSV(f)
	headers=dfinput.Names()
	if len(headers)!=len(Columns)	{
		log.Fatal("Processing input file, missing columns, headers should be ",Columns)
	}
	for h:=0;h<len(headers);h++	{
		fmt.Printf("Header %d %s,",h,headers[h])
		if headers[h]!=Columns[h] 	{
			log.Fatalf("Processing input file, invalid column %d, headers should be %v\n",h,Columns)
		}
	}
	fmt.Printf("\n")
	numRows, _ := dfinput.Dims()

	line:=0
	for row:=0;row<numRows;row++	{
		var 	race	RacesRecord
		var 	allcolumns	[]float32
		ok:=true
		venue:=dfinput.Elem(row,0).String()
		if race.IdVenue,ok=Venues[venue]; !ok	{
			log.Fatal("Venue ",venue," not a valid venue")
		}
		dist,err:=dfinput.Elem(row,1).Int()
		if err!=nil || dist==0	{
			log.Fatal("Distance ",dist," not a valid distance, skipping")
			continue
		}	
		race.Distance=float64(dist)/220
		running:=dfinput.Elem(row,2).String()
		if race.IdRunning,ok=Runnings[running];!ok 	{
			log.Fatal("Going ",running," not a valid Value :",Runnings)
		}
		rtypes:=dfinput.Elem(row,3).String()
		if race.IdRType,ok=RTypes[rtypes];!ok 	{
			log.Fatal("RTypes ",rtypes," not a valid Value :",RTypes)
		}
		ground:=dfinput.Elem(row,4).String()
		if race.IdGround,ok=Grounds[ground];!ok 	{
			log.Fatal("Surface ",ground," not a valid Value :",Grounds)
		}
		windir:=dfinput.Elem(row,5).String()
		if race.WindQuarter,ok=WindDir[windir];!ok 	{
			log.Fatal("WindDir ",windir," not a valid Value :",WindDir)
		}
		race.WindSpeed=dfinput.Elem(row,6).Float()
		race.WindGust=dfinput.Elem(row,7).Float()
		
	
		key:=fmt.Sprintf("%s:%.1f:%d",venue,race.Distance,race.IdRType)
		if venuecolumn,ok:=FindRaceTrack(key); !ok	{
			fmt.Printf("Skipping Venue %s, Distance(f) %.1f, RType %s not a valid combination\n",venue,race.Distance,rtypes)
		}	else 	{
			fmt.Printf("Found %s|%.1f|%s [%s] = column %d\n",venue,race.Distance,rtypes,key,venuecolumn)
			
			allcolumns=append(allcolumns,Expand(venuecolumn,NumRaceTracks,1)...)
			allcolumns=append(allcolumns,float32(race.Distance/(MaxDistance)))
			allcolumns=append(allcolumns,Expand(race.IdRunning,NUMRUNNING,1)...)
			allcolumns=append(allcolumns,Expand(race.IdRType,NUMRTYPE,1)...)
			allcolumns=append(allcolumns,Expand(race.IdGround,NUMGROUND,1)...)
			allcolumns=append(allcolumns,Expand(race.WindQuarter,NUMWINDDIR,float32(race.WindSpeed/MAXWIND))...)
			allcolumns=append(allcolumns,Expand(race.WindQuarter,NUMWINDDIR,float32(race.WindGust/MAXWIND))...)
			allrows=append(allrows,allcolumns...)
			validrows=append(validrows,row)
			line++
		}
	}

	df=tensor.New(tensor.WithBacking(allrows), tensor.WithShape(line,numcols))
	dfinput=dfinput.Subset(validrows)
	
	if Debug 	{
		debugfile, err := os.Create("debug.csv")
		if err != nil {
			log.Fatal("Failed to create debug file :",err)
		}
		defer debugfile.Close()
		df.(*tensor.Dense).WriteCSV(debugfile)
	}
	return
}

func 	FindRaceTrack(key	string)		(index int, ok bool)	{
	for r:=0;r<NumRaceTracks;r++	{
		if RaceTracks[r]==key 	{
			return r,true
		}
	}
	return 0,false
}