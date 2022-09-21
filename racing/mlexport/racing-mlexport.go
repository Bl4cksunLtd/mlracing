package main

import (
	"fmt"
	"log"
	"math"
	"database/sql"
	"encoding/json"
	"os"
//	"time"
	_ "github.com/go-sql-driver/mysql"
	"flag"
	
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"

)

/* 	racing-mlexport 		-	exports data to be used to train the main racing prediction model
		Parameters are:
		*	-u			Username to connect to the database
		*	-p 			Password used to connect to the database
			-db			Database connection string 									(default "sectionals?parseTime=true")
			-file 		Training data is saved to this csv file						(default report.csv)
			-headers	Json file containing the std time one hit categories 		(default headers.json)
			-md			Max distance in furlongs 									(default 35.0)
			-min		used to scale furlong times, minimum 						(default 0)
			-max 		used to scale furlong times, maximum						(default 0)
			-rt 		Limit to this race type (e.g. Flat)							(default any)
			-st 		Scale win times as minutes (true) rather than seconds 		(default true)
			-ems 		Estimate Missing Sectionals if true will attempt to estimate
						sectional figures for races that don't have them. If true any
						runs without sectionals are ignored.						(default true)
			-mhs 		Only include runs with sectional information				(default true)
			-years 		Limit data selection to a range of years 					(default 2020)
		(* must be supplied)	
		
		The headers file contains the names of the one hit encoded categories and is used by the racing-predict program when it's inputs 
		need one hit encoding to ensure the categories match.
		Distances are converted to furlongs and scaled based on the max distance, which defaults to 35f, so one mile race (8f) would 
		be scaled to 0.2286.
		Furlong times can be scalled using the -min and -max parameters. Scaled time=(furlong time-min)/(max-min)
		By default all race types are exported, but this can be limited using the -rt parameter. For instance setting this to 
		"'Flat','National Hunt Flat'" would limit data to all non-jump races.
		Only a subset of races include sectional data, if -ems is true, then values are estimated based on winning time and 
		lengths behind the winner when looking at historical races. If false, then any run without sectional data is ignored.
		If mhs is true, then any only runs with sectional data is selected.
		Data is exported based on the date for the years specified (defaults to 2020), specifying -years="2018,2019,2020" would limit data
		to 2018-2020.

*/

var 	(
	MaxIter 	int
	DBName 		string
	FileName	string
	DB 			*sql.DB
	Venues		= []string{"","Ludlow","Lingfield","Taunton","Newcastle","Clonmel","Southwell","Newbury","Doncaster","Dundalk","Kelso","Navan",
					"Sedgefield","Huntingdon","Leopardstown","Wexford","Wetherby","Wolverhampton","Sandown Park","Fontwell Park","Catterick Bridge",
					"Kempton Park","Carlisle","Thurles","Wincanton","Plumpton","Leicester","Musselburgh","Limerick","Ffos Las","Exeter",
					"Chelmsford City","Chepstow","Market Rasen","Fairyhouse","Fakenham","Bangor-On-Dee","Uttoxeter","Warwick","Naas",
					"Punchestown","Tramore","Cheltenham","Ayr","Hereford","Cork","Ascot","Haydock Park","Down Royal","Gowran Park",
					"Stratford-on-Avon","Hexham","Aintree","Downpatrick","Laytown","Redcar","Nottingham","Worcester","Newmarket","Killarney",
					"Chester","Tipperary","Pontefract","Brighton","Galway","York","Curragh","Newton Abbot","Goodwood","Yarmouth","Bath","Sligo",
					"Hamilton Park","Salisbury","Kilbeggan","Thirsk","Perth","Windsor","Epsom Downs","Ballinrobe","Beverley","Listowel",
					"Ripon","Roscommon","Bellewstown","Cartmel","Towcester"}
	Runnings	=	[]string{"","Firm","Good","Good to Firm","Good to Soft","Good to Yielding","Heavy","Slow","Soft","Soft to Heavy",
					"Standard","Standard to Slow","Yielding","Yielding to Soft"}
	Conds		=	[]string{""," ","Handicap","Listed","Stakes"}
	Ages 		=	[]string{"","10yo+","2yo+","2yo3","2yoO","3yo+","3yo4","3yo5","3yo6","3yoO","4yo+","4yo5","4yo6","4yo7","4yo8","4yoO",
					"5yo+","5yo6","5yo7","5yo8","5yoO","6yo+","6yo7","7yo+","8yo+"}
	RTypes		=	[]string{"","Chase","Flat","Hurdle","National Hunt Flat"}
	Grounds		=	[]string{"","Allweather","Sand","Polytrack","Turf"}
	Classes		=	[]string{""," ","Class 1","Class 2","Class 3","Class 4","Class 5","Class 6","Class 7","D.I","Grade A","Grade B",
					"Premier Handicap","Q.R.","Qualifier"}
	WindStrs	=	[]string{"","N Str","NE Str","E Str","SE Str","S Str","SW Str","W Str","NW Str"}
	WindGusts	=	[]string{"","N Gust","NE Gust","E Gust","SE Gust","S Gust","SW Gust","W Gust","NW Gust"}
	Starters 	=	[]string{"","St2-6","St7-10","St11-15","St16-24","St24+"}
	RaceTracks		[]string
	NumRaceTracks	int
	RunnersDF	*dataframe.DataFrame
	
	FurlongAdjustments = map[int][]float64{	5: {0.031293995,	-0.01930003,	-0.016444745,	-0.011445335,	0.003520834},
											6: {0.008818549,	-0.011131109,	-0.010983735,	-0.008681728,	0.002652181},
											7: {0.003991506,	-0.006042652,	-0.008849022,	-0.008170615,	0.000128213},
											8: {0.002709,	-0.005309263,	-0.009190481,	-0.00803817,	-0.00084261},
											9: {0.002232262,	-0.002298472,	-0.005133499,	-0.007303215,	-0.00405557},
											10: {0.002331378,	-0.005330065,	-0.009511137,	-0.008134687,	-0.003062679},
											11: {-0.009471398,	0.020794987,	-0.008492882,	0.015796209,	0.021670549},
											12: {0.001366433,	-0.00410579,	-0.007333233,	-0.007095492,	-0.0034708},
											13: {0.001334396,	-0.005609111,	-0.008628523,	-0.00709966,	-0.004022776},
											14: {0.000565496,	-0.005010692,	-0.00623634,	-0.006218911,	-0.00301555},
											15: {0.00087468,	-0.006518873,	-0.006460128,	-0.004979973,	-0.001915898},
											16: {-0.000212162,	-0.003964839,	-0.00544565,	-0.004815813,	-0.002150688},
											17: {-0.000341512,	-0.0045335,	-0.004859721,	-0.004436974,	-0.000973587},
											18: {-0.000927423,	-0.005257741,	-0.004636975,	-0.003183598,	0.001114627},
											19: {-0.000315915,	-0.00429841,	-0.004065611,	-0.003553282,	-0.000399104},
											20: {-0.000579708,	-0.003458316,	-0.004141549,	-0.003391351,	-0.001256175},
											21: {-0.000263819,	-0.004023298,	-0.004003417,	-0.002847687,	-0.000514316},
											22: {-0.000439698,	-0.003778423,	-0.003652573,	-0.002141597,	0.0000267084},
											23: {-0.000547557,	-0.003441571,	-0.004212571,	-0.002924912,	-0.001732523},
											24: {-0.000282567,	-0.002735405,	-0.002923576,	-0.002367055,	-0.000916652},
											25: {-0.000109192,	-0.003340793,	-0.002841067,	-0.002373409,	-0.000717194},
											26: {-0.000458267,	-0.002734952,	-0.002831119,	-0.001908512,	0.000239978},
											27: {1.73981E-05,	-0.00288678,	-0.002333636,	-0.004033772,	0.0000110897},
											28: {0.000268397,	-0.002481938,	-0.002591242,	-0.001046238,	0.001082588},
											29: {-0.000470875,	-0.002349119,	-0.003665281,	-0.003575133,	-0.001147144},
											30: {1.51265E-06,	-0.004204991,	-0.004721023,	-0.004653388,	-0.003040161},
											31: {-0.00034143,	-0.002122503,	-0.001365718,	-0.000322832,	0.000177384},
											32: {-0.000578635,	-0.002989732,	-0.001646414,	-0.000463652,	-0.001143339},
											33: {-0.000606546,	-0.002725664,	-0.001409937,	-0.000940034,	0.000845597},
											34: {-0.000647299,	-0.003197521,	-0.001672684,	-0.001643846,	-0.000234363}}
	
)

const	(
	NUMVENUES 	=	86
	NUMCOURSES	=	1869
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
	MAXRUNNERS	=	20				// only process the first 20 horses in a race in finishing order
	STARTOFTIME	=	736695			// treat 1st jan 2017 as the first day
	MAXWEIGHTLBS =	185				// scale weight in lbs by this amount
	LENGTH2YARDS =	2.625
	VERMAJ		=	0
	VERMIN		=	0
	VERPATCH	=	1
)	
	
/*
Fields from the races table:
	idraces					
	venues_idvenues			
	starters				
	distance
	idrunning
	idcond
	idage
	idrtype
	idground
	idclass
	windspeed
	windgust
	winddir
	wintime

Fields from racestypes where the races_idraces matches the idraces
	racetypes_idracetypes
*/
	
type RacesRecord	struct	{
	IdRace		int
	IdVenue 	int
	IdTrack		int
	DaysSince	int 		// number of days from 1st jan 2017 (STARTOFTIME) for the race
	Starters 	int
	Distance	int
	Furlongs	float64
	RaceTypes 	string
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
	StdTime 	float64
}
	
type	RunnerRecord	struct	{
	IdRunner 	int
	IdRace 		int
	IdSelection	int
	IdJockey	int
	IdTrainer	int
	DaysSince 	int				// number of days from 1st jan 2017 (STARTOFTIME) for the race
	Number 		int
	Scratched	bool
	Draw		int
	Position	int				// needs dropping before training
	Weight		int
	Lengths		float64			// needs dropping before training distance behind
	Age			int
	Rating		int
	Odds 		float64			// might need dropping before training
	Start 		float64			// needs dropping before training
	AvgStart	float64
	SDStart		float64
	MinStart	float64
	MaxStart	float64
	F4 			float64			// needs dropping before training
	AvgF4		float64
	SDF4		float64
	MinF4		float64
	MaxF4		float64
	F3			float64			// needs dropping before training
	AvgF3		float64
	SDF3		float64
	MinF3		float64
	MaxF3		float64
	F2			float64			// needs dropping before training
	AvgF2		float64
	SDF2		float64
	MinF2		float64
	MaxF2		float64
	F1 			float64			// needs dropping before training
	AvgF1		float64
	SDF1		float64
	MinF1		float64
	MaxF1		float64
	Real		float64			// Furlong times are real (true or 1) or estimated from wintime-lengths behind
	Finish		float64			// needs dropping before training
	AvgF		float64			// average finish/furlongs time for horse
	SDF			float64			// standard deviation for AvgF
	MinF		float64			// min finish/furlongs for horse
	MaxF		float64			// max finish/furlongs for horse
	AvgFinishJ	float64
	AvgFinishD	float64
	AvgFinishV	float64
	AvgPosJ		float64
	AvgPosD		float64
	AvgPosV		float64
	HStrike		float64
	HVStrike	float64 		// horse strike rate at this venue
	HDStrike	float64			// horse strike rate at this distance
	JStrike		float64
	TStrike 	float64
	H7Strike	float64			// horses strike rate over last 7 runs
	J7Strike	float64			// jockeys strike rate over last 7 days
	T7Strike 	float64			// trainers strike rate over last 7 days
	TimeLastRun	int				// how many days since the horse last ran
	TimeLastWin	int				// how many days since last win
	RunsLastWin	int				// how many runs since last win
	CnDWinner	bool			// course and distance winner
}


func 	main()	{
	fmt.Printf("mlexport v%d.%d.%d\n",VERMAJ,VERMIN,VERPATCH)
	sDB:=flag.String("db","sectionals?parseTime=true","Database connection string")
	sFileName:=flag.String("file","report.csv","Output file name")
	sHeaderFile:=flag.String("headers","headers.json","Filename to save header map")
	fMaxDistance:=flag.Float64("md",35.0,"Max Distance in furlongs")
	iMin:=flag.Int("min",0,"If specified, min is subtracted from the furlong time, used for scaling, both min and max must be non zero")
	iMax:=flag.Int("max",0,"If specified, max is used to scale the furlong time. Scaled time=(furlong time-min)/(max-min)")
	sRType:=flag.String("rt","","list of RTypes to limit results by, e.g. 'Turf,National Hunt Flat' (Default is any)")
	bST:=flag.Bool("st",true,"Scale Win Time as minutes (true) or seconds (false) Default Minutes")
	bEMS:=flag.Bool("ems",true,"Estimate missing sectionals (true) or ignore races with missing sectionals")
	bMHS:=flag.Bool("mhs",false,"Runs must have sectional information or are ignored")
	sUser:=flag.String("u","","DB Username")
	sPass:=flag.String("p","","DB Password")
	sYear:=flag.String("years","2021","years to export e.g. \"2018,2019,2020\" ")
	iMaxYear:=flag.Int("maxyear",2021,"Include all runs up to and including this year")
	flag.Parse()
	if *sUser=="" || *sPass==""		{
		log.Fatal("Username or password missing, specify with -u and -p options.")
	}
	DBName=fmt.Sprintf("%s:%s@/%s",*sUser,*sPass,*sDB)
	FileName=*sFileName
	
	fn,err:=os.Create(FileName)
	if err!=nil	{
		log.Fatal("Failed to create file ",FileName," : ",err)
	}
	defer fn.Close()
	
	db, err := sql.Open("mysql", DBName)
	if err != nil {
		log.Fatal("(InitDatabase) Failed to open mysql database : ", err)
	}
	defer db.Close()
	DB=db
	
	fmt.Println("Max Distance:",*fMaxDistance)
	fmt.Println("Min/Max     :",*iMin,"/",*iMax)
	fmt.Println("ST          :",*bST)
	fmt.Println("EMS         :",*bEMS)
	fmt.Println("MHS         :",*bMHS)
	fmt.Println("Years       :",*sYear)
	fmt.Println("Max Year    :",*iMaxYear)
	

	fmt.Printf("Reading header file %s...",*sHeaderFile)
	headerjson,err:=os.ReadFile(*sHeaderFile)
	if err!=nil	{
		log.Fatal("Failed to read header file ",*sHeaderFile," : ",err)
	}
	if err=json.Unmarshal(headerjson,&RaceTracks);err!=nil 	{
		log.Fatal("Failed to unmarshall header file ",*sHeaderFile," : ",err)
	}
	NumRaceTracks=len(RaceTracks)
	fmt.Printf("Loaded RaceTracks map, %d entries\n",NumRaceTracks)

	// now read in all the race data skipping any that are not present in the RaceTracks slice as it means there is no 
	// standard time for that race.
	
	racedf:=GrabRaceDataFrames(*sYear,*sRType,*bMHS)
	fmt.Println("Race dataframe ",racedf.Nrow()," rows : ",racedf.Names())
	fmt.Println(racedf)
	
	// now go through the runners and sectional table to retrieve all valid runs.
	// if ems is false, then ignore races and runs that do not have sectional data otherwise
	// attempt to estimate from patterns, wintime and distance behind the winner.
	
	RunnersDF=GrabRunnerDataFrames(*iMaxYear,*sRType,*bEMS,*bMHS)
	fmt.Println("Runner dataframe ",RunnersDF.Nrow()," rows : ",RunnersDF.Names())
	fmt.Println(RunnersDF)
	
//	fmt.Println("Joining races and runs together.....")
//	innerdf:=racedf.InnerJoin(runnersdf,"IdRace")
//	fmt.Println("Combined Inner dataframe ",innerdf.Nrow()," rows : ",innerdf.Names())
//	fmt.Println(innerdf)

	fmt.Println("Processing Races...")
	for r:=0;r<racedf.Nrow();r++	{
		idrace,err:=racedf.Elem(r,0).Int()
		if err!=nil	{
			log.Fatal("racedf.Elem failed to return int:",err)
		}
		dayssince,err:=racedf.Elem(r,3).Int()
		if err!=nil 	{
			log.Fatal("racedf.elem failed getting days since :",err)
		}
		runners:=RunnersDF.FilterAggregation(
						dataframe.And, 
						dataframe.F{Colname:"IdRace", Comparator: series.Eq,Comparando:  idrace},
						dataframe.F{Colname:"IdSelection",Comparator: series.Neq,Comparando: 0},
		)
		fmt.Printf("RaceId: %d %d Runners %d DaysSince: \n",idrace,runners.Nrow(),dayssince)
		for run:=0;run<runners.Nrow();run++		{
			idselection,err:=runners.Elem(run,2).Int()
			if err!=nil 	{
				log.Fatal("Runners.Elem failed: ",err)
			}
			fmt.Printf("Run: %d IdSelection : %d ",run,idselection)
			runs:=RunnersDF.FilterAggregation(
						dataframe.And, 
						dataframe.F{Colname:"IdRace", Comparator: series.Neq,Comparando:  idrace},
						dataframe.F{Colname:"Scratched",Comparator: series.Eq,Comparando: false},
						dataframe.F{Colname:"Position",Comparator: series.Neq,Comparando: 0},
						dataframe.F{Colname:"IdSelection",Comparator: series.Eq,Comparando: idselection},
						dataframe.F{Colname:"DaysSince",Comparator: series.Less,Comparando: dayssince},
			)
			numotherruns:=runs.Nrow()
			if numotherruns>0	{
				RealCol:=runs.Col("Real")
				sectionals:=int(RealCol.Sum())
//				if err!=nil	{
//					log.Fatal("RealCol.Sum failed :",err)
//				}
				estimated:=numotherruns-sectionals
				f4col:=runs.Col("F4")
				f4max:=f4col.Max()
				f4min:=f4col.Min()
				f4med:=f4col.Median()
				f4std:=f4col.StdDev()
				f3col:=runs.Col("F3")
				f3max:=f3col.Max()
				f3min:=f3col.Min()
				f3med:=f3col.Median()
				f3std:=f3col.StdDev()
				f2col:=runs.Col("F2")
				f2max:=f2col.Max()
				f2min:=f2col.Min()
				f2med:=f2col.Median()
				f2std:=f2col.StdDev()
				f1col:=runs.Col("F1")
				f1max:=f1col.Max()
				f1min:=f1col.Min()
				f1med:=f1col.Median()
				f1std:=f1col.StdDev()
				startcol:=runs.Col("Start")
				startmax:=startcol.Max()
				startmin:=startcol.Min()
				startmed:=startcol.Median()
				startstd:=startcol.StdDev()
				finishcol:=runs.Col("Finish")
				finishmax:=finishcol.Max()
				finishmin:=finishcol.Min()
				finishmed:=finishcol.Median()
				finishstd:=finishcol.StdDev()
				fmt.Printf("%d/%d/%d E/R/T Runs Start: %.1f/%.1f/%.1f/%.1f F4 : %.1f/%.1f/%.1f/%.1f F3: %.1f/%.1f/%.1f/%.1f F2: %.1f/%.1f/%.1f/%.1f F1: %.1f/%.1f/%.1f/%.1f Finish: %.1f/%.1f/%.1f/%.1f\n",
						estimated,sectionals,numotherruns,startmin,startmax,startmed,startstd,f4min,f4max,f4med,f4std,
						f3min,f3max,f3med,f3std,
						f2min,f2max,f2med,f2std,
						f1min,f1max,f1med,f1std,
						finishmin,finishmax,finishmed,finishstd)
			} else {
				fmt.Printf("0 previous runs\n")
			}
			 
		}
	}
	fmt.Println("all races processed")
//	racedf.Rapply(ProcessRace)
	
//	fmt.Println("Saving ",FileName)
//	innerdf.WriteCSV(fn,dataframe.WriteHeader(true))
}

func 	Expand(id,max int,scale float64)	(columns []string)	{
	if id>max 	{
		log.Fatal("Id ",id," is greater than max ",max)
	}
	for c:=1;c<=max;c++	{
		if c==id	{
			columns=append(columns,fmt.Sprintf("%.4f",scale))
		}	else 	{
			columns=append(columns,"0")
		}
	}
	return
}

func 	CalcWindQuarter(dir 	int)	(wq int)	{
	dirf:=float64(dir)
	switch	{
	case 	dirf<22.5:	wq=1
	case 	dirf>=22.5 && dirf<67.5:	wq=2
	case 	dirf>=67.5 && dirf<112.5:	wq=3
	case 	dirf>=112.5 && dirf<157.5:	wq=4
	case 	dirf>=157.5 && dirf<202.5:	wq=5
	case 	dirf>=202.5 && dirf<247.5:	wq=6
	case 	dirf>=247.5 && dirf<292.5:	wq=7
	case 	dirf>=292.5 && dirf<337.5:	wq=8
	case 	dirf>=337.5:				wq=1
	}
	return
}
	
	
	
	

func 	GrabRaceTypeTitles()	(results [NUMRACETYPE+1]string)	{
	rows, err := DB.Query("SELECT idracetypes,rtname FROM racetypes order by idracetypes ")
	if err != nil {
		DB.Close()
		log.Fatal("(GrabRaceTypeTitles) DB Query: ", err)
	}
	var 	racetype 	string
	var		idracetype 	int
	for rows.Next() {
		if err := rows.Scan(&idracetype,&racetype); err != nil {
			// Check for a scan error.
			rows.Close()
			DB.Close()
			log.Fatal("(GrabRaceTypeTitles) Rows Scan failed: ", err)
		}
		log.Println("(GrabRaceTypeTitles) found ",idracetype,racetype)
		results[idracetype]=racetype
	}
	rows.Close()
	return results
}


func 	GrabRaceTypes(raceid int64)	(results []string)	{
	for i:=0;i<=NUMRACETYPE;i++	{
		results=append(results,"0")
	}
	rows, err := DB.Query("SELECT racetypes_idracetypes FROM racestypes where races_idraces=?",raceid)
	if err != nil {
		DB.Close()
		log.Fatal("(GrabRaceTypes)DB Query: ", err)
	}
	var 	id 	int
	for rows.Next() {
		if err := rows.Scan(&id); err != nil {
			// Check for a scan error.
			rows.Close()
			DB.Close()
			log.Fatal("(GrabRaceTypes) Rows Scan failed: ", err)
		}
		results[id]="1"
	}
	rows.Close()
	return results[1:]
}


func 	FindRaceTrack(key	string)		(index int, ok bool)	{
	for r:=0;r<NumRaceTracks;r++	{
		if RaceTracks[r]==key 	{
			return r,true
		}
	}
	return 0,false
}	

func 	GrabRaceTypeIds(raceid int)	(results string)	{
	rows, err := DB.Query("SELECT racetypes_idracetypes FROM racestypes where races_idraces=?",raceid)
	if err != nil {
		DB.Close()
		log.Fatal("(GrabRaceTypeIds)DB Query: ", err)
	}
	var 	id 	int
	for rows.Next() {
		if err := rows.Scan(&id); err != nil {
			// Check for a scan error.
			rows.Close()
			DB.Close()
			log.Fatal("(GrabRaceTypeIds) Rows Scan failed: ", err)
		}
		if len(results)==0	{
			results=fmt.Sprintf("%d",id)
		}	else 	{
			results=fmt.Sprintf("%s,%d",results,id)
		}
	}
	rows.Close()
	return 
}

func 	GrabRaceDataFrames(year 	string, rtype string,mhs bool)	dataframe.DataFrame	{
	var 	races 	[]RacesRecord
	
	mhsquery:=""
	if mhs 	{
		mhsquery="and idraces in (select races_idraces  from sectionals.sectionals,sectionals.runners where runners_idrunners=idrunners)"
	}
	query:=fmt.Sprintf("SELECT idraces,TO_DAYS(starttime)-%d,venues_idvenues,distance,round(distance/220,1),starters,idrunning,idrtype,idground,idclass,idage,idcond,windspeed,windgust,winddir,stdtime,wintime "+
			"FROM races,weather,stdracetimes "+ 
			"where weather.races_idraces=idraces and stdracetimes.races_idraces=idraces and year(starttime) in (%s) %s "+
			"and distance!=0 and wintime!=0 and idrunning!=0 and idcond!=0 and idage!=0 and idrtype!=0 and idground!=0 and idclass!=0 and windspeed<120 and "+
			"round(wintime/(distance/220),1)<19 and round(wintime/(distance/220),1)>= 10 ",STARTOFTIME,year,mhsquery)
	if rtype!=""	{
		query=fmt.Sprintf("SELECT idraces,TO_DAYS(starttime)-%d,venues_idvenues,distance,round(distance/220,1),starters,idrunning,idrtype,idground,idclass,idage,idcond,windspeed,windgust,winddir,stdtime,wintime "+
			"FROM races,weather,stdracetimes "+ 
			"where weather.races_idraces=idraces and stdracetimes.races_idraces=idraces and year(starttime) in (%s) "+
			"and distance!=0 and wintime!=0 and idrunning!=0 and idcond!=0 and idage!=0 and idrtype!=0 and idground!=0 and idclass!=0 and windspeed<120 and "+
			"round(wintime/(distance/220),1)<19 and round(wintime/(distance/220),1)>= 10 and rtype in (%s) ",
									STARTOFTIME,year,mhsquery,rtype)
	}
	
	fmt.Printf("(GrabRacesDataframe)Query: %s\n",query)
	rows,err:=DB.Query(query)
	if err != nil {
		DB.Close()
		log.Fatal("(GrabRacesDataframe)DB Query failed: ", err)
	}
	defer rows.Close()
	fmt.Println("(GrabRacesDataframe)Reading race records....")
	rownum:=0		
	for rows.Next() {
		race:=RacesRecord{}
		if err := rows.Scan(&race.IdRace,&race.DaysSince,&race.IdVenue,&race.Distance,&race.Furlongs,&race.Starters,&race.IdRunning,&race.IdRType,
							&race.IdGround,&race.IdClass,&race.IdAge,&race.IdCond,&race.WindSpeed,&race.WindGust,&race.WindDir,&race.StdTime,&race.WinTime); err != nil {
			// Check for a scan error.
			rows.Close()
			DB.Close()
			log.Fatal("(GrabRacesDataframe)Rows Scan failed: ", err)
		}
		// grab the racetypes for this race id
		race.WindQuarter=CalcWindQuarter(race.WindDir)
		// expand out the rows
			
		key:=fmt.Sprintf("%s:%.1f:%d",Venues[race.IdVenue],race.Furlongs,race.IdRType)
		if tracknum,ok:=FindRaceTrack(key);ok	{
			race.IdTrack=tracknum
		}	else 	{
			fmt.Printf("Failed to find category %s, skipping\n",key)
			continue
		}
		race.RaceTypes=GrabRaceTypeIds(race.IdRace)
		rownum++
		races=append(races,race)
		fmt.Printf("Row: %d   Id: %d  Std:%.3f                      \r",rownum,race.IdRace,race.StdTime)
	}
	fmt.Printf("\n")
	rerr := rows.Close()
	if rerr != nil {
		rows.Close()
		DB.Close()
		log.Fatal("(GrabRacesDataframe)Rows Close: ", err)
	}
	return dataframe.LoadStructs(races)
}


func 	GrabRunnerDataFrames(year 	int, rtype string, ems 	bool, mhs bool)	*dataframe.DataFrame	{
	var 	runs 	[]RunnerRecord
	var 	distance 	int
//	var 	Furlongs 			float64
	var 	weight,position 				string
	var 	f4,f3,f2,f1,finish 				sql.NullFloat64
	var 	HaveRunner[40]					bool

/*	query:=fmt.Sprintf("SELECT idraces,venues_idvenues,distance,round(distance/220,1),starters,idrunning,idrtype,idground,idclass,idage,idcond,windspeed,windgust,winddir,stdtime,wintime "+
			"FROM races,weather,stdracetimes "+ 
			"where weather.races_idraces=idraces and stdracetimes.races_idraces=idraces and year(starttime) in (%s) "+
			"and distance!=0 and wintime!=0 and idrunning!=0 and idcond!=0 and idage!=0 and idrtype!=0 and idground!=0 and idclass!=0 and windspeed<120 and "+
			"round(wintime/(distance/220),1)<19 and round(wintime/(distance/220),1)>= 10 ",year)
	if rtype!=""	{
		query=fmt.Sprintf("SELECT idraces,venues_idvenues,distance,round(distance/220,1),starters,idrunning,idrtype,idground,idclass,idage,idcond,windspeed,windgust,winddir,stdtime,wintime "+
			"FROM races,weather,stdracetimes "+ 
			"where weather.races_idraces=idraces and stdracetimes.races_idraces=idraces and year(starttime) in (%s) "+
			"and distance!=0 and wintime!=0 and idrunning!=0 and idcond!=0 and idage!=0 and idrtype!=0 and idground!=0 and idclass!=0 and windspeed<120 and "+
			"round(wintime/(distance/220),1)<19 and round(wintime/(distance/220),1)>= 10 and rtype in (%s) ",
									year,rtype)
	}
*/
	lr:="left "
	if mhs 	{
		lr="right "
	}
	query:=fmt.Sprintf("select idrunners,races_idraces,TO_DAYS(starttime)-%d,wintime,distance,selections_idselections,jockeys_idjockeys,trainers_idtrainers,runners.number,scratched,draw,position,iposition,weight,weightlbs,lengths,runners.age,runners.rating,odds,f4,f3,f2,f1,finish "+
						"from runners "+
						"%s join sectionals on runners_idrunners=idrunners "+
						"left join races on races_idraces=races.idraces "+
						"where year(starttime) <= %d "+ 
						"order by races_idraces,iposition ",STARTOFTIME,lr,year)
	if rtype!="" 	{
		query=fmt.Sprintf("select idrunners,races_idraces,TO_DAYS(starttime)-%d,wintime,distance,selections_idselections,jockeys_idjockeys,trainers_idtrainers,runners.number,scratched,draw,position,iposition,weight,weightlbs,lengths,runners.age,runners.rating,odds,f4,f3,f2,f1,finish "+
							"from runners "+
							"%s join sectionals on runners_idrunners=idrunners "+
							"left join races on races_idraces=races.idraces "+
							"where year(starttime) <= %d and rtype in (%s) "+ 
							"order by races_idraces,iposition ",STARTOFTIME,lr,year,rtype)
	}
	fmt.Printf("(GrabRunnerDataFrames)Query: %s\n",query)
	rows,err:=DB.Query(query)
	if err != nil {
		DB.Close()
		log.Fatal("(GrabRunnerDataFrames)DB Query failed: ", err)
	}
	defer rows.Close()
	fmt.Println("(GrabRunnerDataFrames)Reading race records....")
	rownum:=0	
	finished:=false
	lengthsbehind:=float64(0)
	currentraceId:=-1
	numrunners:=0
	avgfurtime:=float64(0)
	firstrun:=true
	for rows.Next() {
		runner:=RunnerRecord{}
		wintime:=float64(0)
		if err := rows.Scan(&runner.IdRunner,&runner.IdRace,&runner.DaysSince,&wintime,&distance,&runner.IdSelection,&runner.IdJockey,&runner.IdTrainer,
					&runner.Number,&runner.Scratched,&runner.Draw,&position,&runner.Position,&weight,&runner.Weight,&runner.Lengths,
					&runner.Age,&runner.Rating,&runner.Odds,&f4,&f3,&f2,&f1,&finish); err != nil {
			// Check for a scan error.
			rows.Close()
			DB.Close()
			log.Fatal("(GrabRunnerDataFrames)Rows Scan failed: ", err)
		}
		if firstrun 	{
			currentraceId=runner.IdRace
			firstrun=false
		}
		// ignore races where the wintime or distance was incorrectly posted or scraped
		if wintime==0	|| distance==0	|| runner.Number==0 {
			continue
		}
		runner.Real=1
		finished=false
		avgfurtime=0
		distancef:=float64(distance)
		furlongs:=int(math.Round(distancef/220))
		if runner.IdRace!=currentraceId		{
			// new race so add extra horses so all races have MAXRUNNERS in race
/*			for h:=0;numrunners<MAXRUNNERS;h++	{
				if !HaveRunner[h]	{
					// havent used runner number h+1 so add a blank record
					rownum++
					runs=append(runs,RunnerRecord{IdRunner:0,IdRace:currentraceId,Number:h+1,Scratched:true})
					HaveRunner[h]=true
					numrunners++
				}
			} */
			// new race so reset lengthsbehind
			currentraceId=runner.IdRace
			lengthsbehind=0
			numrunners=0
			// set flag to say we havent processed any runners yet from this race
			for h:=0;h<40;h++	{
				HaveRunner[h]=false
			}
		}
		if runner.Position==0 && position=="NA"		{
			runner.Scratched=true
		}
		
		if runner.Position!=0 && runner.Position<=40	{
			// valid finishing position so add in the lengths behind
			lengthsbehind+=runner.Lengths
			dist:=distancef-(lengthsbehind*LENGTH2YARDS)
			avgfurtime=wintime*220/dist
			finished=true
		}
		HaveRunner[runner.Number-1]=true	// mark this horse number as processed
		if finish.Valid 	{
			runner.Finish=finish.Float64
		}	
		if runner.Finish==0 && finished 	{
			runner.Finish=wintime+avgfurtime*((lengthsbehind*LENGTH2YARDS)/220)
		}
//		fmt.Printf("RaceId: %d CRI %d WT: %.1f Pos: %d H:%d EMS %v Finished %v avgfurtime %.2f lb %.1f RL %.1f\n",
//				runner.IdRace,currentraceId,wintime,runner.Position,runner.Number,ems,finished,avgfurtime,lengthsbehind,
//				runner.Lengths)
	//	time.Sleep(1*time.Second)
		if f4.Valid 	{
			runner.F4=f4.Float64
		}	else 	{
			if ems 	&&	finished	{
				runner.F4=runner.Finish*FurlongAdjustments[furlongs][1]+avgfurtime
				runner.Real=0
			}	else 	{
				runner.F4=0
			}
		}
		if f3.Valid 	{
			runner.F3=f3.Float64
		}	else 	{
			if ems 	&&	finished	 	{
				runner.F3=runner.Finish*FurlongAdjustments[furlongs][2]+avgfurtime
				runner.Real=0
			}	else 	{
				runner.F3=0
			}
		}
		if f2.Valid 	{
			runner.F2=f2.Float64
		}	else 	{
			if ems 	&&	finished	 	{
				runner.F2=runner.Finish*FurlongAdjustments[furlongs][3]+avgfurtime
				runner.Real=0
			}	else 	{
				runner.F2=0
			}
		}
		
		if f1.Valid 	{
			runner.F1=f1.Float64
		}	else 	{
			if ems 	&&	finished	 	{
				runner.F1=runner.Finish*FurlongAdjustments[furlongs][4]+avgfurtime
				runner.Real=0
			}	else 	{
				runner.F1=0
			}
		}
		
		if finished 	{
			// calculate the avg start furlong time
			runner.Start=(runner.Finish-(runner.F1+runner.F2+runner.F3+runner.F4))/((distancef/220)-4)
		}
/*		key:=fmt.Sprintf("%s:%.1f:%d",Venues[IdVenue],Furlongs,IdRType)
		if tracknum,ok:=FindRaceTrack(key);ok	{
			runner.IdTrack=tracknum
		}	else 	{
			fmt.Printf("Failed to find category %s, skipping\n",key)
			continue
		} */
		rownum++
		runs=append(runs,runner)
		numrunners++
		fmt.Printf("Row: %d   RaceId: %d  RunnerId:%d EMS:%v F4:%.3f F3:%3f F2:%.3f F1: %.3f Finish:%.2f           \r",rownum,runner.IdRace,runner.IdRunner,
									runner.Real,runner.F4,runner.F3,runner.F2,runner.F1,runner.Finish)
	}
	fmt.Printf("\n")
	rerr := rows.Close()
	if rerr != nil {
		rows.Close()
		DB.Close()
		log.Fatal("(GrabRunnerDataFrames)Rows Close: ", err)
	}
	fmt.Println("RUNS: ",len(runs))
	df:=dataframe.LoadStructs(runs)
	return &df
}

func	ProcessRace(s series.Series) series.Series {
	floats := s.Float()
	fmt.Printf("RaceId: %d ",int(floats[0]))
	runners:=RunnersDF.Filter(dataframe.F{Colname:"IdRace", Comparator: series.Eq,Comparando:  floats[0]})
	fmt.Printf(" %d Runners :",runners.Nrow())
	fmt.Println(runners)
	runners.Rapply(ProcessRunner)
	fmt.Printf("\n")
	return series.Floats(floats)
}

func	ProcessRunners(s series.Series) series.Series {
	fmt.Println(s.Int())
	return s
}

func 	ProcessRunner(s series.Series)	series.Series	{
	fmt.Println("Runner series: ",s.Name)
	floats:=s.Float()
	fmt.Println(floats)
	fmt.Printf("%d (",int(floats[2]))
	runs:=RunnersDF.FilterAggregation(
		dataframe.And,
		dataframe.F{Colname:"IdSelection",Comparator: series.Eq,Comparando:  floats[2]},
		dataframe.F{Colname:"IdRunner",Comparator: series.Neq,Comparando: floats[0]},
	)
	fmt.Printf("%d Runs),",runs.Nrow())
	fmt.Println(runs)
	return series.Floats(floats)
}