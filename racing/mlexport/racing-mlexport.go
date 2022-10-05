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
	// venues runs from 1 to NumRaceTracks
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
	Conds		=	[]string{"","Normal","Handicap","Listed","Stakes"}  // make cond[1]="Normal"
	Ages 		=	[]string{"","10yo+","2yo+","2yo3","2yoO","3yo+","3yo4","3yo5","3yo6","3yoO","4yo+","4yo5","4yo6","4yo7","4yo8","4yoO",
					"5yo+","5yo6","5yo7","5yo8","5yoO","6yo+","6yo7","7yo+","8yo+"}
	RTypes		=	[]string{"","Chase","Flat","Hurdle","National Hunt Flat"}
	Grounds		=	[]string{"","Allweather","Sand","Polytrack","Turf"}
	Classes		=	[]string{"","NoClass","Class 1","Class 2","Class 3","Class 4","Class 5","Class 6","Class 7","D.I","Grade A","Grade B",
					"Premier Handicap","Q.R.","Qualifier"}
	WindStrs	=	[]string{"","N Str","NE Str","E Str","SE Str","S Str","SW Str","W Str","NW Str"}
	WindGusts	=	[]string{"","N Gust","NE Gust","E Gust","SE Gust","S Gust","SW Gust","W Gust","NW Gust"}
	Starters 	=	[]string{"","St2-6","St7-10","St11-15","St16-24","St24+"}
	RaceTracks		[]string
	NumRaceTracks	int
	RecentDays		int
	RecentRuns		int
	MaxDistance		float64
	RunnersDF	dataframe.DataFrame
	CSVfile			*os.File
	
	DaysCategorySizes = []int{2,2,2,4,10,10,10,10,10,10,10,10,10,20,20,20,20,20,20,20,20,20,20}		// 23+1 categories
	
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
	RECENTDAYS	=	30
	RECENTRUNS	=	7
	NUMVENUES 	=	87
	NUMCOURSES	=	1869
	NUMRUNNING	=	13
	NUMCOND		=	4
	NUMAGE		=	24
	NUMDRAW		=	25				// allow for draws from 0 to 20, 0=> no draw
	NUMHORSEAGE	=	10				// number of categories for a horse's age ranges from 2 to 11
	NUMRTYPE	=	4
	NUMGROUND	=	4
	NUMCLASS	=	14
	NUMSTARTERS	=	5
	NUMRACETYPE	=	19
	NUMWINDDIR	=	8
	NUMDAYCATS 	= 	23				// number of day categories
	NUMRUNSLAST = 	20
	MAXWIND		=	120.0
	MAXRATING	=	100.0
	MAXWEIGHT	=	200.0
	MAXRUNNERS	=	20				// only process the first 20 horses in a race in finishing order
	MAXDAYS		=	300				// when categorising number of days old, ignore 0 and place any greater than this in a bucket
	MAXRUNS 	=	100				// scale number of runs using this
	MAXRECENTRUNS = 10				// scale number of recent runs using this
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
	raceTypes 	[]string	// not included in the datafame
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

	
type	DFRunnerRecord	struct	{
	IdRunner 	int
	IdRace 		int
	IdSelection	int
	IdVenue		int				// from races
	Distance	int				// from races
	IdTrack		int				// from races
	IdJockey	int
	IdTrainer	int
	DaysSince 	int				// number of days from 1st jan 2017 (STARTOFTIME) for the race
	Number 		int
	Scratched	bool
	Draw		int
	Position	int				// needs dropping before training
	Weight		int
	Lengths		float64			// lengths behind next horse, needs dropping before training distance behind
	LB			float64			// lengths behind the winner, needs dropping before training distance behind
	Age			int
	Rating		int
	Odds 		float64			// might need dropping before training
	Start		float64			// needs dropping before training
	F4 			float64			// needs dropping before training
	F3			float64			// needs dropping before training
	F2			float64			// needs dropping before training
	F1 			float64			// needs dropping before training
	Real		float64			// Furlong times are real (true or 1) or estimated from wintime-lengths behind
	Finish		float64			// needs dropping before training
	HasHistory	bool			// This is set to true when previous runs are found, otherwise historic values are based on standard
								// times. When written out to the csv file this value is reversed and written as FirstRun.
}

	
type	RunnerRecord	struct	{
	DFRunnerRecord
	Count		int				
	AvgPos		float64			// average position of all previous runs
	MedPos		float64			// Med position of all previous runs
	SDPos 		float64			// standard deviation of position in all previous runs
	MinPos		float64			// minimum position of all previous runs (ignoring 0)
	MaxPos		float64			// maximum position of all previous runs
	AvgLB		float64			// average lengths behind of all previous runs
	MedLB		float64			// median lengths behind of all previous runs
	SDLB 		float64			// standard deviation of lengths behind in all previous runs
	MinLB		float64			// minimum lengths behind of all previous runs
	MaxLB		float64			// maximum lengths behind of all previous runs
	AvgOdds		float64			// average odds of all previous runs
	MedOdds		float64			// Med odds of all previous runs
	SDOdds	 	float64			// standard deviation of odds in all previous runs
	MinOdds		float64			// minimum odds of all previous runs (ignoring 0)
	MaxOdds		float64			// maximum odds of all previous runs
	AvgStart	float64
	MedStart	float64
	SDStart		float64
	MinStart	float64
	MaxStart	float64
	AvgF4		float64
	MedF4		float64
	SDF4		float64
	MinF4		float64
	MaxF4		float64
	AvgF3		float64
	MedF3		float64
	SDF3		float64
	MinF3		float64
	MaxF3		float64
	AvgF2		float64
	MedF2		float64
	SDF2		float64
	MinF2		float64
	MaxF2		float64
	AvgF1		float64
	MedF1		float64
	SDF1		float64
	MinF1		float64
	MaxF1		float64
	AvgF		float64			// average finish/furlongs time for horse
	MedF		float64			// average finish/furlongs time for horse
	SDF			float64			// standard deviation for AvgF
	MinF		float64			// min finish/furlongs for horse
	MaxF		float64			// max finish/furlongs for horse
	AvgReal		float64
	HStrike		float64			// horse strike rate
	HDNFStrike	float64			// horse DNF strike rate
	HVStrike	float64 		// horse strike rate at this venue
	HDStrike	float64			// horse strike rate at this distance (nearest furlong)
	HTStrike	float64			// horse strije rate at this distance and venue
	HJStrike	float64			// horse strike rate with this jockey
	HPlaceStrike	float64			// horse strike rate for placing top 3
	HVPlaceStrike	float64 		// horse strike rate for placing top 3 at this venue
	HDPlaceStrike	float64			// horse strike rate for placing top 3 at this distance (nearest furlong)
	HTPlaceStrike	float64			// horse strike rate for placing top 3 at this distance and venue
	HJPlaceStrike	float64			// horse strike rate for placing top 3 with this jockey
	JStrike		float64				// jockey strike rate
	JVStrike	float64				// jockey strike rate at this venue
	JDStrike	float64				// jockey strike rate at this distance (nearest furlong)
	JTStrike	float64				// jockey strike rate at this track (venue and distance)
	JOStrike 	float64				// jockey strike rate with this trainer
	JPStrike	float64				// jockey strike rate for placing top 3 at this distance (nearest furlong)
	JVPStrike	float64				// jockey strike rate at this venue for placing top 3
	JDPStrike	float64				// jockey strike rate at this distance for placing top 3
	JTPStrike	float64				// jockey strike rate at this track (venue and distance) for placing top 3
	JOPStrike 	float64				// jockey strike rate with this trainer for placing top 3
	JDNF		float64				// jocket dnf strike rate
	TStrike		float64				// trainer strike rate
	TVStrike	float64				// trainer strike rate at this venue
	TDStrike	float64				// trainer strike rate at this distance (nearest furlong)
	TTStrike	float64				// trainer strike rate at this track (venue and distance)
	TOStrike 	float64				// trainer strike rate with this jockey
	TPStrike	float64				// trainer strike rate for top3 
	TVPStrike	float64				// trainer strike rate for top3 at this venue
	TDPStrike	float64				// trainer strike rate for top3 at this distance (nearest furlong)
	TTPStrike	float64				// trainer strike rate for top3 at this track (venue and distance)
	TOPStrike 	float64				// trainer strike rate with this jockey for a top3
	TDNF		float64				// trainer dnf strike rate
	HRStrike	float64			// horse strike rate in recent times
	HRPlaceStrike	float64		// horse place strike rate in recent times
	HRDNFStrike	float64			// horse DNF strike rate in recent times
	HRRStrike	float64			// horse strike rate in recent runs
	HRRPlaceStrike	float64		// horse place strike rate in recent runs
	HRRDNFStrike	float64		// horse DNF strike rate in recent runs
	JRStrike	float64			// jockey strike rate in recent times
	JRVStrike	float64			// jockey strike rate in recent times at this venue
	JRTStrike	float64			// jockey strike rate in recent times at this track (C & D)
	JRDStrike	float64			// jockey strike rate in recent times at this distance
	JROStrike	float64			// jockey strike rate in recent times with this trainer
	JRDNF		float64			// jockey DNF strike rate in recent times
	JRPStrike	float64			// jockey place strike rate in recent times
	JRVPStrike	float64			// jockey place strike rate in recent times at this venue
	JRTPStrike	float64			// jockey place strike rate in recent times at this track (C & D)
	JRDPStrike	float64			// jockey place strike rate in recent times at this distance
	JROPStrike	float64			// jockey place strike rate in recent times with this trainer
	JRRStrike	float64			// jockey strike rate in recent runs
	JRRPStrike	float64			// jockey place strike rate in recent runs
	JRRVStrike	float64			// jockey strike rate in recent runs at this venue
	JRRTStrike	float64			// jockey strike rate in recent runs at this track (C & D)
	JRRDStrike	float64			// jockey strike rate in recent runs at this distance
	JRROStrike	float64			// jockey strike rate in recent runs with this trainer
	JRRVPStrike	float64			// jockey place strike rate in recent runs at this venue
	JRRTPStrike	float64			// jockey place strike rate in recent runs at this track (C & D)
	JRRDPStrike	float64			// jockey place strike rate in recent runs at this distance
	JRROPStrike	float64			// jockey place strike rate in recent runs with this trainer
	JRRDNF		float64			// jockey DNF strike rate in recent RUNS
	TRStrike	float64			// trainer strike rate in recent times
	TRVStrike	float64			// trainer strike rate in recent times at this venue
	TRTStrike	float64			// trainer strike rate in recent times at this track (C & D)
	TRDStrike	float64			// trainer strike rate in recent times at this distance
	TROStrike	float64			// trainer strike rate in recent times with this jockey
	TRDNF		float64			// trainer DNF strike rate in recent times
	TRPStrike	float64			// trainer place strike rate in recent times
	TRVPStrike	float64			// trainer place strike rate in recent times at this venue
	TRTPStrike	float64			// trainer place strike rate in recent times at this track (C & D)
	TRDPStrike	float64			// trainer place strike rate in recent times at this distance
	TROPStrike	float64			// trainer place strike rate in recent times with this jockey
	TRRStrike	float64			// trainer strike rate in recent runs
	TRRPStrike	float64			// trainer place strike rate in recent runs
	TRRVStrike	float64			// trainer strike rate in recent runs at this venue
	TRRTStrike	float64			// trainer strike rate in recent runs at this track (C & D)
	TRRDStrike	float64			// trainer strike rate in recent runs at this distance
	TRROStrike	float64			// trainer strike rate in recent runs with this trainer
	TRRVPStrike	float64			// trainer place strike rate in recent runs at this venue
	TRRTPStrike	float64			// trainer place strike rate in recent runs at this track (C & D)
	TRRDPStrike	float64			// trainer place strike rate in recent runs at this distance
	TRROPStrike	float64			// trainer place strike rate in recent runs with this jockey
	TRRDNF		float64			// trainer DNF strike rate in recent RUNS
	TimeLastRun		int				// how many days since the horse last ran
	TimeLastWin		int				// how many days since last win
	TimeLastPlace	int				// how many days since last in top 3
	RunsLastWin		int				// how many runs since last win
	RunsLastPlace	int				// how many runs since last Place
	CnDWinner	bool			// course and distance winner
	HNRuns			int				// number of runs
	VNRuns			int				// number of runs with same venue
	DNRuns			int				// number of runs with same distance
	TNRuns			int				// number of runs with same track (C & D)
	JNRuns			int				// number of runs with same jockey
	HRNRuns			int				// number of runs in recent time
	VRNRuns			int				// number of runs with same venue in recent time
	DRNRuns			int				// number of runs with same distance in recent time
	TRNRuns			int				// number of runs with same track (C & D) in recent time
	JRNRuns			int				// number of runs with same jockey in recent time
	HRRNRuns		int				// number of runs in recent runs
	VRRNRuns		int				// number of runs with same venue in recent runs
	DRRNRuns		int				// number of runs with same distance in recent runs
	TRRNRuns		int				// number of runs with same track (C & D) in recent runs
	JRRNRuns		int				// number of runs with same jockey in recent runs
	JNum			int				// number of jockey runs
	JVNum			int				// number jockey runs on this venue
	JDNum			int				// number of jockey runs at this distance
	JTNum			int				// number of jockey runs at this track (C & D)
	JONum			int 			// number of jockey/trainer runs 
	JRNum			int				// number of jockey runs IN recent time
	JRVNum			int				// number jockey runs on this venue IN recent time
	JRDNum			int				// number of jockey runs at this distance IN recent time
	JRTNum			int				// number of jockey runs at this track (C & D) IN recent time
	JRONum			int 			// number of jockey/trainer runs in recent times
	JRRNum			int				// number of jockey runs IN recent runs
	JRRVNum			int				// number jockey runs on this venue IN recent runs
	JRRDNum			int				// number of jockey runs at this distance IN recent runs
	JRRTNum			int				// number of jockey runs at this track (C & D) IN recent runs
	JRRONum			int 			// number of jockey jockey/trainer runs in recent runs
	TNum			int				// number of trainer runs
	TVNum			int				// number trainer runs on this venue
	TDNum			int				// number of trainer runs at this distance
	TTNum			int				// number of trainer runs at this track (C & D)
	TONum			int 			// number of trainer/trainer runs 
	TRNum			int				// number of trainer runs IN recent time
	TRVNum			int				// number trainer runs on this venue IN recent time
	TRDNum			int				// number of trainer runs at this distance IN recent time
	TRTNum			int				// number of trainer runs at this track (C & D) IN recent time
	TRONum			int 			// number of trainer/trainer runs in recent times
	TRRNum			int				// number of trainer runs IN recent runs
	TRRVNum			int				// number trainer runs on this venue IN recent runs
	TRRDNum			int				// number of trainer runs at this distance IN recent runs
	TRRTNum			int				// number of trainer runs at this track (C & D) IN recent runs
	TRRONum			int 			// number of trainer jockey/trainer runs in recent runs

}

func 	main()	{
	fmt.Printf("mlexport v%d.%d.%d\n",VERMAJ,VERMIN,VERPATCH)
	sDB:=flag.String("db","sectionals?parseTime=true","Database connection string")
	sFileName:=flag.String("file","report.csv","Output file name")
	sHeaderFile:=flag.String("headers","headers.json","Filename to save header map")
	fMaxDistance:=flag.Float64("md",35.0,"Max Distance in furlongs")
	iMin:=flag.Int("min",0,"If specified, min is subtracted from the furlong time, used for scaling, both min and max must be non zero (Default 0)")
	iMax:=flag.Int("max",0,"If specified, max is used to scale the furlong time. Scaled time=(furlong time-min)/(max-min) Default 0")
	sRType:=flag.String("rt","","list of RTypes to limit results by, e.g. 'Turf,National Hunt Flat' (Default is any)")
	bST:=flag.Bool("st",true,"Scale Win Time as minutes (true) or seconds (false) Default Minutes")
	bEMS:=flag.Bool("ems",true,"Estimate missing sectionals (true) or ignore races with missing sectionals")
	bMHS:=flag.Bool("mhs",false,"Runs must have sectional information or are ignored (default false)")
	sUser:=flag.String("u","","DB Username")
	sPass:=flag.String("p","","DB Password")
	bExpand:=flag.Bool("expand",true,"Expand categories out as one hit columns")
	sYear:=flag.String("years","2021","years to export e.g. \"2018,2019,2020\" ")
	iMaxYear:=flag.Int("maxyear",2021,"Include all runs up to and including this year")
	iRecentDays:=flag.Int("rd",RECENTDAYS,"How many days to go back looking for recent form")
	iRecentRuns:=flag.Int("rr",RECENTRUNS,"How many runs to go back looking for recent form")
	flag.Parse()
	if *sUser=="" || *sPass==""		{
		log.Fatal("Username or password missing, specify with -u and -p options.")
	}
	DBName=fmt.Sprintf("%s:%s@/%s",*sUser,*sPass,*sDB)
	FileName=*sFileName
	
	db, err := sql.Open("mysql", DBName)
	if err != nil {
		log.Fatal("(InitDatabase) Failed to open mysql database : ", err)
	}
	defer db.Close()
	DB=db
	
	
	CSVfile,err=os.Create(FileName)
	if err!=nil	{
		log.Fatal("Failed to create file ",FileName," : ",err)
	}
	defer CSVfile.Close()
	
	MaxDistance=*fMaxDistance
	RecentDays=*iRecentDays
	RecentRuns=*iRecentRuns
	fmt.Println("Max Distance:",MaxDistance)
	fmt.Println("Min/Max     :",*iMin,"/",*iMax)
	fmt.Println("ST          :",*bST)
	fmt.Println("EMS         :",*bEMS)
	fmt.Println("MHS         :",*bMHS)
	fmt.Println("RecentDays  :",*iRecentDays)
	fmt.Println("RecentRuns  :",*iRecentRuns)
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
	fmt.Printf("Loaded RaceTracks map, %d entries. %d Venues\n",NumRaceTracks,len(Venues))

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
	
/*	fmt.Println("Joining races and runs together.....")
	RunnersDF=racedf.LeftJoin(GrabRunnerDataFrames(*iMaxYear,*sRType,*bEMS,*bMHS),"IdRace")
	fmt.Println("Combined Inner dataframe ",RunnersDF.Nrow()," rows : ",RunnersDF.Names())
	fmt.Println(RunnersDF) */

	// write out the headers if we are in expanded mode
	if *bExpand	{
		WriteHeaders(CSVfile)
	}
			
	fmt.Println("Processing Races...")
	for r:=0;r<racedf.Nrow();r++	{
//if r==4 	{
//	return
//	log.Fatal("STOPPING")
//}
		record:=Record{}
		idrace,err:=racedf.Elem(r,0).Int()
		ErrorNotNil(err,"RaceId ")
		record.IdRace=idrace
		record.IdVenue,err=racedf.Elem(r,1).Int()			//IdVenue 	int
		ErrorNotNil(err,"IdVenue ")
		record.IdTrack,err=racedf.Elem(r,2).Int()			//IdTrack		int
		ErrorNotNil(err,"IdTrack ")
		record.DaysSince,err=racedf.Elem(r,3).Int()			//DaysSince	int 		// number of days from 1st jan 2017 (STARTOFTIME) for the race
		ErrorNotNil(err,"DaysSince ")
		record.Starters,err=racedf.Elem(r,4).Int()			//Starters 	int
		ErrorNotNil(err,"Starters ")
		record.Distance,err=racedf.Elem(r,5).Int()			//Distance	int
		ErrorNotNil(err,"Distance ")
		record.Furlongs=racedf.Elem(r,6).Float()			//Furlongs	float64
//		record.RaceTypes=racedf.Elem(r,7).String()			//RaceTypes 	string
		record.IdRunning,err=racedf.Elem(r,7).Int()			//IdRunning	int
		ErrorNotNil(err,"IdRunning ")
		record.IdCond,err=racedf.Elem(r,8).Int()			//IdCond		int
		ErrorNotNil(err,"IdCond ")
		record.IdAge,err=racedf.Elem(r,9).Int()			//IdAge		int
		ErrorNotNil(err,"IdAge ")
		record.IdRType,err=racedf.Elem(r,10).Int()			//IdRType		int
		ErrorNotNil(err,"IdRType ")
		record.IdGround,err=racedf.Elem(r,11).Int()			//IdGround	int
		ErrorNotNil(err,"IdGround ")
		record.IdClass,err=racedf.Elem(r,12).Int()			//IdClass		int
		ErrorNotNil(err,"IdClass ")
		record.WindSpeed=racedf.Elem(r,13).Float()			//WindSpeed	float64
		record.WindGust=racedf.Elem(r,14).Float()			//WindGust	float64
		record.WindDir,err=racedf.Elem(r,15).Int()			//WindDir		int
		ErrorNotNil(err,"WindDir ")
		record.WindQuarter,err=racedf.Elem(r,16).Int()			//WindQuarter	int
		ErrorNotNil(err,"WindQuarter ")
		record.WinTime=racedf.Elem(r,17).Float()			//WinTime		float64
		record.StdTime=racedf.Elem(r,18).Float()			//StdTime 	float64
		
		// grab the race types for this race, returned as a slice of 1 or 0 based on categories
		record.raceTypes=GrabRaceTypes(idrace)			
	
		
		if record.ProcessRace()	{
			if *bExpand	{
				record.ExpandRace()
			}	else 	{
				record.WriteRace()
			}
		}
	}
	fmt.Println("all races processed")

}

func 	Expand(id,max int,scale float64)	(columns []string)	{
	if id>max 	{
		log.Panic("Id ",id," is greater than max ",max)
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

func 	Limit(v,max int)		int	{
	if v>max 	{
		v=max
	}
	return v
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


func 	GrabRaceTypes(raceid int)	(results []string)	{
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

func ErrorNotNil(err error, msg string)	{
	if err!=nil 	{
		log.Fatal(msg," Failed: ",err)
	}
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
		// grab the wind details for this race id
		race.WindQuarter=CalcWindQuarter(race.WindDir)
		// expand out the rows
			
		key:=fmt.Sprintf("%s:%.1f:%d",Venues[race.IdVenue],race.Furlongs,race.IdRType)
		if tracknum,ok:=FindRaceTrack(key);ok	{
			race.IdTrack=tracknum
		}	else 	{
			fmt.Printf("Failed to find category %s, skipping\n",key)
			continue
		}

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


func 	GrabRunnerDataFrames(year 	int, rtype string, ems 	bool, mhs bool)	dataframe.DataFrame	{
	var 	runs 	[]DFRunnerRecord
	var 	distance 	int
//	var 	Furlongs	 			float64
	var 	weight,position 				string
	var 	f4,f3,f2,f1,finish,isStdtime	sql.NullFloat64
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
	query:=fmt.Sprintf("select idrunners,runners.races_idraces,TO_DAYS(starttime)-%d,wintime,distance,round(distance/220,1),venues_idvenues,idrtype,selections_idselections,jockeys_idjockeys,trainers_idtrainers,runners.number,scratched,draw,position,iposition,weight,weightlbs,lengths,runners.age,runners.rating,odds,f4,f3,f2,f1,finish,stdtime "+
						"from runners "+
						"%s join sectionals on runners_idrunners=idrunners "+
						"left join races on runners.races_idraces=races.idraces "+
						"left join stdracetimes on stdracetimes.races_idraces=runners.races_idraces "+
						"where year(starttime) <= %d "+ 
						"order by races_idraces,iposition ",STARTOFTIME,lr,year)
	if rtype!="" 	{
		query=fmt.Sprintf("select idrunners,runners.races_idraces,TO_DAYS(starttime)-%d,wintime,distance,round(distance/220,1),venues_idvenues,idrtype,selections_idselections,jockeys_idjockeys,trainers_idtrainers,runners.number,scratched,draw,position,iposition,weight,weightlbs,lengths,runners.age,runners.rating,odds,f4,f3,f2,f1,finish,stdtime "+
							"from runners "+
							"%s join sectionals on runners_idrunners=idrunners "+
							"left join races on races_idraces=races.idraces "+
							"left join stdracetimes on stdracetimes.races_idraces=runners.races_idraces "+
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
	idvenue:=0
	idrtype:=0
	furlongs1:=float64(0)
	avgfurtime:=float64(0)
	firstrun:=true
	missingstd:=0
	for rows.Next() {
		runner:=DFRunnerRecord{}
		stdtime:=float64(0)
		wintime:=float64(0)
		if err := rows.Scan(&runner.IdRunner,&runner.IdRace,&runner.DaysSince,&wintime,&distance,&furlongs1,&idvenue,&idrtype,&runner.IdSelection,&runner.IdJockey,&runner.IdTrainer,
					&runner.Number,&runner.Scratched,&runner.Draw,&position,&runner.Position,&weight,&runner.Weight,&runner.Lengths,
					&runner.Age,&runner.Rating,&runner.Odds,&f4,&f3,&f2,&f1,&finish,&isStdtime); err != nil {
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
		if isStdtime.Valid 	{
			stdtime=isStdtime.Float64
		}	else 	{
		//	fmt.Printf("Skipping due to missing standard time                                   \r")
			missingstd++
			continue
		}
		runner.IdVenue=idvenue
		runner.Distance=distance
		key:=fmt.Sprintf("%s:%.1f:%d",Venues[idvenue],furlongs1,idrtype)
		if tracknum,ok:=FindRaceTrack(key);ok	{
			runner.IdTrack=tracknum
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
			runner.LB=lengthsbehind
		}
		HaveRunner[runner.Number-1]=true	// mark this horse number as processed
		if finish.Valid 	{
			runner.Finish=finish.Float64
		}	
		if runner.Finish==0 && finished 	{
			runner.Finish=wintime+avgfurtime*((lengthsbehind*LENGTH2YARDS)/220)
		}
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
		runner.Odds=runner.Odds/1000
		rownum++
		if stdtime!=0	{
			if runner.Start!=0	{
				runner.Start=stdtime/runner.Start
			}
			if runner.Finish!=0	{
				runner.Finish=(distancef*stdtime/220)/runner.Finish
			}
			if runner.F4!=0	{
				runner.F4=stdtime/runner.F4
			}
			if runner.F3!=0	{
				runner.F3=stdtime/runner.F3
			}
			if runner.F2!=0	{
				runner.F2=stdtime/runner.F2
			}
			if runner.F1!=0	{
				runner.F1=stdtime/runner.F1
			}
		}	else 	{
			missingstd++
			continue
		}
		runs=append(runs,runner)
		numrunners++
		fmt.Printf("Row: %d   RaceId: %d  RunnerId:%d EMS:%.0f Start:%.3f F4:%.3f F3:%3f F2:%.3f F1: %.3f Finish:%.2f           \r",rownum,runner.IdRace,runner.IdRunner,
									runner.Real,runner.Start,runner.F4,runner.F3,runner.F2,runner.F1,runner.Finish)
	}
	fmt.Printf("\nSkipped %d/%d records due to missing standard times\n",missingstd,len(runs))
	rerr := rows.Close()
	if rerr != nil {
		rows.Close()
		DB.Close()
		log.Fatal("(GrabRunnerDataFrames)Rows Close: ", err)
	}
	df:=dataframe.LoadStructs(runs)
	return df
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

func 	StarterCategory(starter int)	(starters int)	{
	switch	{
	case starter<2:	return 0
	case starter>=2 && starter<=6:		starters=1
	case starter>=7 && starter<=10:	starters=2
	case starter>=11 && starter<=15:	starters=3
	case starter>=16 && starter<=24:	starters=4
	case starter>24:							starters=5
	}
	return
}

func 	DaysCategory(days int)	int	{
	if 	days==0 	{
		return	0
	}
	if days>MAXDAYS	{
		days=MAXDAYS
	}
	startofcat:=0
	for category:=0;category<NUMDAYCATS;category++	{
		startofcat+=DaysCategorySizes[category]
		if days<startofcat	{
			return category+1				// return 1 to 23
		}
	}
	return NUMDAYCATS
}

func 	RunsCategory(runs int)	int	{
	if runs==0	{
		return 0
	}
	if runs>NUMRUNSLAST	{
		runs=NUMRUNSLAST
	}
	return runs
}
	
func 	(r *RunnerRecord)FinishCategory()	(results []string) 	{
	for p:=0;p<4;p++	{
		results=append(results,"0")
	}
	if r.Position==0	&& (r.Scratched || r.IdSelection==0)	{
		return
	}
	switch 	{
	case r.Position==0:	results[3]="1"
	case r.Position==1:	results[0]="1"
	case r.Position==2 || r.Position==3:	results[1]="1"
	case r.Position>3:	results[2]="1"
	}
	return
}

func 	ScaledNumber(num ,max float64)	float64	{
	if num>max	{
		num=max
	}
	return num/max
}

func 	WriteHeaders(fn	*os.File)	{
// write header

			
	for t:=0;t<NumRaceTracks;t++	{
		fn.WriteString(fmt.Sprintf("%s,",RaceTracks[t]))
	}
	for v:=1;v<NUMVENUES;v++	{
		fn.WriteString(fmt.Sprintf("%s,",Venues[v]))
	}
	for s:=1;s<=NUMSTARTERS;s++	{
		fn.WriteString(fmt.Sprintf("%s,",Starters[s]))
	}
	fn.WriteString("Dist,")
	for i:=1;i<=NUMRUNNING;i++	{
		fn.WriteString(fmt.Sprintf("%s,",Runnings[i]))
	}
	for i:=1;i<=NUMCOND;i++	{
		fn.WriteString(fmt.Sprintf("%s,",Conds[i]))
	}
	for i:=1;i<=NUMAGE;i++	{
		fn.WriteString(fmt.Sprintf("%s,",Ages[i]))
	} 
	for i:=1;i<=NUMRTYPE;i++	{
		fn.WriteString(fmt.Sprintf("%s,",RTypes[i]))
	}
	for i:=1;i<=NUMGROUND;i++	{
		fn.WriteString(fmt.Sprintf("%s,",Grounds[i]))
	}
	for i:=1;i<=NUMCLASS;i++	{
		fn.WriteString(fmt.Sprintf("%s,",Classes[i]))
	}
	racetypetitles:=GrabRaceTypeTitles()
	for i:=1;i<=NUMRACETYPE;i++	{
		fn.WriteString(racetypetitles[i]+",")
	}
	for i:=1;i<=NUMWINDDIR;i++	{
		fn.WriteString(fmt.Sprintf("%s,",WindStrs[i]))
	}
	for i:=1;i<=NUMWINDDIR;i++	{
		fn.WriteString(fmt.Sprintf("%s,",WindGusts[i]))
	}
	fn.WriteString("StdTime,")
	for r:=1;r<=MAXRUNNERS;r++	{
		// write out the headers for each horse
		fn.WriteString(fmt.Sprintf("Include%d,Scratched%d,",r,r))
		for draw:=0;draw<NUMDRAW;draw++	{
			fn.WriteString(fmt.Sprintf("Draw%d-%d,",draw,r))
		}
		fn.WriteString(fmt.Sprintf("Weight%d,",r))
		for age:=0;age<NUMHORSEAGE;age++	{
			fn.WriteString(fmt.Sprintf("Age%d-%d,",age+2,r))
		}
		fn.WriteString(fmt.Sprintf("Rating%d,Odds%d,FirstRun%d,Real%d,",r,r,r,r))
		fn.WriteString(fmt.Sprintf("AvgReal%d,AvgPos%d,MedPos%d,SDPos%d,MinPos%d,MaxPos%d,AvgLB%d,MedLB%d,SDLB%d,MinLB%d,MaxLB%d,",
						r,r,r,r,r,r,r,r,r,r,r))
		fn.WriteString(fmt.Sprintf("AvgOdds%d,MedOdds%d,SDOdds%d,MinOdds%d,MaxOdds%d,AvgStart%d,MedStart%d,SDStart%d,MinStart%d,MaxStart%d,",
						r,r,r,r,r,r,r,r,r,r))
		fn.WriteString(fmt.Sprintf("AvgF4-%d,MedF4-%d,SDF4-%d,MinF4-%d,MaxF4-%d,",r,r,r,r,r))
		fn.WriteString(fmt.Sprintf("AvgF3-%d,MedF3-%d,SDF3-%d,MinF3-%d,MaxF3-%d,",r,r,r,r,r))
		fn.WriteString(fmt.Sprintf("AvgF2-%d,MedF2-%d,SDF2-%d,MinF2-%d,MaxF2-%d,",r,r,r,r,r))
		fn.WriteString(fmt.Sprintf("AvgF1-%d,MedF1-%d,SDF1-%d,MinF1-%d,MaxF1-%d,",r,r,r,r,r))
		fn.WriteString(fmt.Sprintf("AvgF%d,MedF%d,SDF%d,MinF%d,MaxF%d,",r,r,r,r,r))
		fn.WriteString(fmt.Sprintf("HStrike%d,HVStrike%d,HDStrike%d,HTStrike%d,HJStrike%d,",r,r,r,r,r))
		fn.WriteString(fmt.Sprintf("HPStrike%d,HVPStrike%d,HDPStrike%d,HTPStrike%d,HJPStrike%d,HDNFStrike%d,",r,r,r,r,r,r))
		fn.WriteString(fmt.Sprintf("JStrike%d,JVStrike%d,JDStrike%d,JTStrike%d,JOStrike%d,JPStrike%d,JVPStrike%d,JDPStrike%d,JTPStrike%d,JOPStrike%d,JDNF%d,",r,r,r,r,r,r,r,r,r,r,r))
		fn.WriteString(fmt.Sprintf("TStrike%d,TVStrike%d,TDStrike%d,TTStrike%d,TOStrike%d,TPStrike%d,TVPStrike%d,TDPStrike%d,TTPStrike%d,TOPStrike%d,TDNF%d,",r,r,r,r,r,r,r,r,r,r,r))
		fn.WriteString(fmt.Sprintf("HRStrike%d,HRPStrike%d,HRDNFStrike%d,HRRStrike%d,HRRPStrike%d,HRRDNFStrike%d,",r,r,r,r,r,r))
		fn.WriteString(fmt.Sprintf("JRStrike%d,JRVStrike%d,JRDStrike%d,JRTStrike%d,JROStrike%d,JRPStrike%d,JRVPStrike%d,JRDPStrike%d,JRTPStrike%d,JROPStrike%d,JRDNF%d,",r,r,r,r,r,r,r,r,r,r,r))
		fn.WriteString(fmt.Sprintf("JRRStrike%d,JRRVStrike%d,JRRDStrike%d,JRRTStrike%d,JRROStrike%d,JRRPStrike%d,JRRVPStrike%d,JRRDPStrike%d,JRRTPStrike%d,JRROPStrike%d,JRRDNF%d,",r,r,r,r,r,r,r,r,r,r,r))
		fn.WriteString(fmt.Sprintf("TRStrike%d,TRPStrike%d,TRRStrike%d,TRRPStrike%d,",r,r,r,r))
		st:=0
		for c:=0;c<NUMDAYCATS;c++	{
			st+=DaysCategorySizes[c]
			fn.WriteString(fmt.Sprintf("TLastRun%d-%d,",st,r))
		}
		st=0
		for c:=0;c<NUMDAYCATS;c++	{
			st+=DaysCategorySizes[c]
			fn.WriteString(fmt.Sprintf("TLastWin%d-%d,",st,r))
		}
		st=0
		for c:=0;c<NUMDAYCATS;c++	{
			st+=DaysCategorySizes[c]
			fn.WriteString(fmt.Sprintf("TLastPlace%d-%d,",st,r))
		}
		for c:=0;c<NUMRUNSLAST;c++	{
			fn.WriteString(fmt.Sprintf("RLastWin%d-%d,",c+1,r))
		}
		for c:=0;c<NUMRUNSLAST;c++	{
			fn.WriteString(fmt.Sprintf("RLastPlace%d-%d,",c+1,r))
		}
		fn.WriteString(fmt.Sprintf("HNRuns%d,VNRuns%d,DNRuns%d,TNRuns%d,JNRuns%d,",r,r,r,r,r))
		fn.WriteString(fmt.Sprintf("HRNRuns%d,VRNRuns%d,DRNRuns%d,TRNRuns%d,JRNRuns%d,",r,r,r,r,r))
		fn.WriteString(fmt.Sprintf("HRRNRuns%d,VRRNRuns%d,DRRNRuns%d,TRRNRuns%d,JRRNRuns%d,",r,r,r,r,r))
		fn.WriteString(fmt.Sprintf("JNum%d,JVNum%d,JDNum%d,JTNum%d,JONum%d,",r,r,r,r,r))
		fn.WriteString(fmt.Sprintf("JRNum%d,JRVNum%d,JRDNum%d,JRTNum%d,JRONum%d,",r,r,r,r,r))
		fn.WriteString(fmt.Sprintf("JRRNum%d,JRRVNum%d,JRRDNum%d,JRRTNum%d,JRRONum%d,",r,r,r,r,r))
		fn.WriteString(fmt.Sprintf("TNum%d,TVNum%d,TDNum%d,TTNum%d,TONum%d,",r,r,r,r,r))
		fn.WriteString(fmt.Sprintf("TRNum%d,TRVNum%d,TRDNum%d,TRTNum%d,TRONum%d,",r,r,r,r,r))
		fn.WriteString(fmt.Sprintf("TRRNum%d,TRRVNum%d,TRRDNum%d,TRRTNum%d,TRRONum%d,",r,r,r,r,r))
	}
	for r:=1;r<=MAXRUNNERS;r++	{
		fn.WriteString(fmt.Sprintf("First%d,Placed%d,Finshed%d,DNF%d,LB%d,Start%d,Finish%d,",r,r,r,r,r,r,r))
	}
	fn.WriteString("\n")
}	
	
		