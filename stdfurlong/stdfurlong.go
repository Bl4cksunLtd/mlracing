package main

import (
	"fmt"
	"database/sql"
	"encoding/json"
	"encoding/gob"
	"os"
	"time"
	_ "github.com/go-sql-driver/mysql"
	"github.com/go-gota/gota/dataframe"
	"github.com/aunum/gold/pkg/v1/common/require"
//	"github.com/aunum/gold/pkg/v1/dense"
	"github.com/aunum/goro/pkg/v1/layer"
	m "github.com/aunum/goro/pkg/v1/model"
	"github.com/aunum/log"

//	g "gorgonia.org/gorgonia"
	"gorgonia.org/tensor"
	"gonum.org/v1/gonum/mat"

	"flag"
)

/* 	Stdfurlong 	-	Sets a races "standard furlong time"
		Parameters are:
		*	-u	 		sql username 
		*	-p	 		sql password
			-db			Database connection string 									(default "sectionals?parseTime=true")
			-mn 		name of the model file 										(default model.gob)
			-headers	name of the json file containing the field headers 			(default headers.json)
			-md 		Max distance in furlongs, used to scale distances 			(default 35.0)
			-min		used to scale furlong times, minimum 						(default 0)
			-max 		used to scale furlong times, maximum						(default 0)
			-start	 	update starting on this date 								(defaults to yesterday)
			-end		update ending on this date, 								(defaults to yesterday)
			-today		if true update std furlong times for all of todays runs.	(default false)
		(* must be supplied)	
		
		The -mn, -headers,-min,-max should match the parameters that were used to train the standard race times model.
		This program can be used to update/store the standard furlong times for historical races by specifying the start and end 
		dates or called repeatedly (e.g. hourly) during the day to update the times for todays races based on the latest weather. 
		Due to the way the weather is currently obtained, this will produce times for the first race of each meeting in the day 
		and for any race starting in the next 90 minutes of an hour (assuming it is run each hour just after a weather update).
		
*/


var 	(
	numcols 	=1924
	MaxIter 	int
	DBName 		string
	ModelName	string
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
	VERMAJ		=	0
	VERMIN		=	1
	VERPATCH	=	0
)	

	
type RacesRecord	struct	{
	IdRace		int64
	IdVenue 	int
	Starters 	int
	Distance	int
	Furlongs	float64
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
	

type matrix struct {
	dataframe.DataFrame
}

func (m matrix) At(i, j int) float64 {
	return m.Elem(i, j).Float()
}

func (m matrix) T() mat.Matrix {
	return mat.Transpose{Matrix: m}
}

func 	main()	{
	fmt.Printf("stdfurlong v%d.%d.%d\n",VERMAJ,VERMIN,VERPATCH)
	startdate:=time.Now().Add(-24*time.Hour).Format("2006-01-02")
	sDB:=flag.String("db","sectionals?parseTime=true","Database connection string")
	sMN:=flag.String("mn","model.gob","Filename of the model, default model.gob")
	sHeaderFile:=flag.String("headers","headers.json","Filename containing header definitions, default headers.json")
	fMaxDistance:=flag.Float64("md",35.0,"Max Distance in furlongs default 35")
	iMin:=flag.Int("min",10,"If specified, min is subtracted from the furlong time, used for scaling, both min and max must be non zero")
	iMax:=flag.Int("max",20,"If specified, max is used to scale the furlong time. Scaled time=(furlong time-min)/(max-min)")
	sUser:=flag.String("u","","DB Username")
	sPass:=flag.String("p","","DB Password")
	sStartDate:=flag.String("start",startdate,"update starting on this date, defaults to yesterday")
	sEndDate:=flag.String("end",startdate,"update ending on this date, defaults to yesterday")
	bToday:=flag.Bool("today",false,"update times for all of todays runs.")
	flag.Parse()
	if *sUser=="" || *sPass==""		{
		log.Fatal("Username or password missing, specify with -u and -p options.")
	}
	DBName=fmt.Sprintf("%s:%s@/%s",*sUser,*sPass,*sDB)

	ModelName=*sMN
	if *bToday 	{
		startdate=time.Now().Format("2006-01-02")
		*sStartDate=startdate
		*sEndDate=startdate
	}
	log.Infof("Processing records from %s to %s\n",*sStartDate,*sEndDate)
// build inputs
	xi := m.NewInput("x", []int{1,numcols})
	
	yi := m.NewInput("y", []int{1})
	
	
	// build model
	model, err := m.NewSequential("sp")
	require.NoError(err)

	model.AddLayers(
		layer.FC{Input: numcols, Output: numcols*2 ,Name: "L0", NoBias: true},
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
	fmt.Println("Model loaded",err)
	require.NoError(err)

	fmt.Printf("Reading header file %s...",*sHeaderFile)
	headerjson,err:=os.ReadFile(*sHeaderFile)
	if err!=nil	{
		log.Fatal("Failed to read header file ",*sHeaderFile," : ",err)
	}
	err=json.Unmarshal(headerjson,&RaceTracks)
	require.NoError(err)
	NumRaceTracks=len(RaceTracks)
	fmt.Printf("Loaded RaceTracks map, %d entries\n",NumRaceTracks)

	// open the database
	db, err := sql.Open("mysql", DBName)
	if err != nil {
		log.Fatal("(InitDatabase) Failed to open mysql database : ", err)
	}
	defer db.Close()
	DB=db
	var 	race 	RacesRecord
	
	
	query:=fmt.Sprintf("SELECT idraces,venues_idvenues,distance,round(distance/220,1),idrunning,idrtype,idground,windspeed,windgust,winddir "+
			"FROM races,weather "+
			"where races_idraces=idraces "+
			"and date(starttime)>='%s' and date(starttime)<='%s' "+
			"and distance!=0 and idrunning!=0 and idrtype!=0 and idground!=0 and windspeed<120 order by forecast desc,starttime",
			*sStartDate,*sEndDate)
	
	fmt.Printf("Query: %s\n",query)
	rows,err:=db.Query(query)
	if err != nil {
		db.Close()
		log.Fatal("DB Query: ", err)
	}

	rownum:=0		
	for rows.Next() {
		if err := rows.Scan(&race.IdRace,&race.IdVenue,&race.Distance,&race.Furlongs,&race.IdRunning,&race.IdRType,
							&race.IdGround,&race.WindSpeed,&race.WindGust,&race.WindDir); err != nil {
			// Check for a scan error.
			rows.Close()
			db.Close()
			log.Fatal("Rows Scan failed: ", err)
		}
		// grab the racetypes for this race id
		race.WindQuarter=CalcWindQuarter(race.WindDir)
		// expand out the rows
		var 	allcolumns	[]float32
		scaleddistance:=float64(race.Distance)/(*fMaxDistance*220)
			
		key:=fmt.Sprintf("%s:%.1f:%d",Venues[race.IdVenue],race.Furlongs,race.IdRType)
		if tracknum,ok:=FindRaceTrack(key);ok	{
			allcolumns=append(allcolumns,Expand(tracknum+1,NumRaceTracks,1)...)
		}	else 	{
			fmt.Printf("Failed to find category %s, skipping\n",key)
			continue
		}
		allcolumns=append(allcolumns,float32(scaleddistance))
		allcolumns=append(allcolumns,Expand(race.IdRunning,NUMRUNNING,1)...)
		allcolumns=append(allcolumns,Expand(race.IdRType,NUMRTYPE,1)...)
		allcolumns=append(allcolumns,Expand(race.IdGround,NUMGROUND,1)...)
		allcolumns=append(allcolumns,Expand(race.WindQuarter,NUMWINDDIR,race.WindSpeed/MAXWIND)...)
		allcolumns=append(allcolumns,Expand(race.WindQuarter,NUMWINDDIR,race.WindGust/MAXWIND)...)
		
			
		rownum++
		xi:=tensor.New(tensor.WithBacking(allcolumns), tensor.WithShape(1,numcols))
		if err!=nil	{
			log.Error("xi failed: ",err)
		}
		require.NoError(err) 
		
		
		yHat, err := model.Predict(xi)
		if err!=nil	{
			log.Error("yHat failed: ",err)
		}
		require.NoError(err) 

		d:=yHat.Data().([]float32)
		speed:=(float32(*iMin))+d[0]*float32(*iMax-*iMin)
		wt:=float32(race.Distance)*speed/220
		id:=StoreStdTime(race.IdRace,speed)
		fmt.Printf("Row: %d  Id:%d D:%.3f SP: %.03f WT: %.03f (rowid:%d)\r",rownum,race.IdRace,d[0],speed,wt,id) 
	}
	fmt.Printf("\n")
	rerr := rows.Close()
	if rerr != nil {
		rows.Close()
		db.Close()
		log.Fatal("Rows Close: ", err)
	}
}

func 	Expand(id,max int,scale float64)	(columns []float32)	{
	if id>max 	{
		log.Fatal("Id ",id," is greater than max ",max)
	}
	for c:=1;c<=max;c++	{
		if c==id	{
			columns=append(columns,float32(scale))
		}	else 	{
			columns=append(columns,0)
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
		log.Info("(GrabRaceTypeTitles) found ",idracetype,racetype)
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


func loadmodel(filename string,model *m.Sequential ) error {
	f, err := os.Open(filename)
	if err != nil {
			log.Errorv("Failed to open model ",err)
			return err
	}
	defer f.Close()
	enc := gob.NewDecoder(f)
	learnnodes:=model.Learnables()
	for _, node := range learnnodes {
			err := enc.Decode(node.Value())
			if err != nil {
					log.Errorv("enc.Decode failed: ",err)
					log.Errorv("with ",node.Value())
					return err
			}
	}
	err=model.SetLearnables(learnnodes)
	return err
}

func 	StoreStdTime(raceid int64,stdtime float32)	(id int64)	{
	// first check if the raceid is already in the stdracetimes table
	row:=DB.QueryRow("select idstdracetimes from stdracetimes where races_idraces=?",raceid)
	if err := row.Scan(&id); err != nil {
		if err!=sql.ErrNoRows	{
			log.Fatal("(StoreStdTime) QueryRow :",err)
		}	
		result,sterr:=DB.Exec("insert into stdracetimes (races_idraces,stdtime) values (?,?)",raceid,stdtime)
		if sterr	!=	nil	{
			log.Fatal("(StoreStdTime) Exec failed ",sterr)
		}
		id,err:=result.LastInsertId()
		if err!=nil	{
			log.Fatal("(StoreStdTime) LastInsertId failed ",err)
		}
		return id
	}	else	{
		// do update instead
		_,sterr:=DB.Exec("update stdracetimes set stdtime=? where idstdracetimes=?",stdtime,id)
		if sterr	!=	nil	{
			log.Fatal("(StoreStdTime) Exec update failed ",sterr)
		}
	}
	return id
}