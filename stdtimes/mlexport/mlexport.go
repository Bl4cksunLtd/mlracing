package main

import (
	"fmt"
	"log"
	"database/sql"
	"encoding/json"
	"os"
	_ "github.com/go-sql-driver/mysql"
	"flag"
)

/* 	Mlexport 		-	exports data to be used to train the standard race time model
		Parameters are:
		*	-u			Username to connect to the database
		*	-p 			Password used to connect to the database
			-db			Database connection string 									(default "sectionals?parseTime=true")
			-file 		File to save output to 										(default report.csv)
			-headers	name of json file to save the one hit categories 			(default headers.json)
			-save		Should the headers json be saved (true) or loaded			(default false)
			-md			Max distance in furlongs 									(default 35.0)
			-min		used to scale furlong times, minimum 						(default 0)
			-max 		used to scale furlong times, maximum						(default 0)
			-rt 		Limit to this race type (e.g. Flat)							(default any)
			-st 		Scale win times as minutes (true) rather than seconds 		(default true)
			-sdl 		Standard deviation lower limit 								(default 1.0)
			-sdu		Standard deviation upper limit 								(default 1.0)
			-sv 		Scale venue category by distance in furlongs 				(default false)
			-cc 		Course categories, use venue|distance|racetype categories	(default false)
			-years 		Limit data selection to a range of years 					(default 2020)
		(* must be supplied)	
		
		The headers file contains the names of the one hit encoded categories and is used by the predict program when it's inputs 
		need one hit encoding to ensure the categories match.
		Distances are converted to furlongs and scaled based on the max distance, which defaults to 35f, so one mile race (8f) would 
		be scalled to 0.2286.
		Furlong times can be scalled using the -min and -max parameters. Scaled time=(furlong time-min)/(max-min)
		By default all race types are exported, but this can be limited using the -rt parameter. For instance setting this to 
		"'Flat','National Hunt Flat'" would limit data to all non-jump races.
		To avoid including outlier and erronous data, the -sdl and -sdu parameters can be used to restrict the range of races included.
		Data is selected where wintime/furlongs>=avgtime-(sdl)*stddevtime and wintime/furlongs<=avgtime+(sdu)*stddevtime. Default 
		values select races where the furlong time is within 65% of the average.
		By default matching venue categories are set to 1, but if -sv is true then they are scalled based on the distance in furlongs.
		The output file is one hit encoded based on venue name, but is -cc is true then the category names are based on an amalgamation 
		of venue name, race distance in furlongs (to 1 decimal place) and race type. This better handles the impact of wind although is 
		not perfect.
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
	VERMAJ		=	1
	VERMIN		=	0
	VERPATCH	=	0
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
	

func 	main()	{
	fmt.Printf("mlexport v%d.%d.%d\n",VERMAJ,VERMIN,VERPATCH)
	sDB:=flag.String("db","sectionals?parseTime=true","Database connection string")
	sFileName:=flag.String("file","report.csv","Output file name")
	sHeaderFile:=flag.String("headers","headers.json","Filename to save header map")
	bSaveHeader:=flag.Bool("save",false,"Save (true) or load (false) header file")
	fMaxDistance:=flag.Float64("md",35.0,"Max Distance in furlongs")
	iMin:=flag.Int("min",0,"If specified, min is subtracted from the furlong time, used for scaling, both min and max must be non zero")
	iMax:=flag.Int("max",0,"If specified, max is used to scale the furlong time. Scaled time=(furlong time-min)/(max-min)")
	sRType:=flag.String("rt","","list of RTypes to limit results by, e.g. 'Turf,National Hunt Flat' (Default is any)")
	bST:=flag.Bool("st",true,"Scale Win Time as minutes (true) or seconds (false) Default Minutes")
	fSDL:=flag.Float64("sdl",1.0,"Low standard deviation multiplier, 1.0=65%% sample range")
	fSDU:=flag.Float64("sdu",1.0,"Upper standard deviation multiplier, 1.0=65%% sample range")
	bSV:=flag.Bool("sv",false,"Scale Venue category by distance")
	bCC:=flag.Bool("cc",false,"Use track of race as category (ie venue|distance|rtype) or just venue (default - use venue)")
	sUser:=flag.String("u","","DB Username")
	sPass:=flag.String("p","","DB Password")
	sYear:=flag.String("years","2020","years to export e.g. \"2018,2019,2020\" ")
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
	var 	race 	RacesRecord
	
	if *bCC	{	
		if *bSaveHeader 	{
			// grab headers from the database and save as a json file.
			// find out how many venue|distance|rtypes there are and populate the map
			rows,err:=db.Query("select venues_idvenues,vname,round(distance/220,1),idrtype from races,venues "+ 
								"where venues_idvenues=idvenues and distance!=0 and wintime!=0 and idrunning!=0 and idcond!=0 and "+
								"idage!=0 and idrtype!=0 and idground!=0 and idclass!=0 and "+
								"round(wintime/(distance/220),1)<19 and round(wintime/(distance/220),1)>= 10 "+
								"group by venues_idvenues,round(distance/220,1),idrtype "+
								"order by venues_idvenues,round(distance/220,1),idrtype")
			if err != nil {
				db.Close()
				log.Fatal("DB Query failed retrieving categories: ", err)
			}
			var idvenue			int64
			var furlongs		float64
			var rtype 			int
			var vname 			string
			for rows.Next() {
				if err := rows.Scan(&idvenue,&vname,&furlongs,&rtype); err != nil {
					// Check for a scan error.
					rows.Close()
					db.Close()
					log.Fatal("Category Rows Scan failed: ", err)
				}
				key:=fmt.Sprintf("%s:%.1f:%d",vname,furlongs,rtype)
//				fn.WriteString(fmt.Sprintf("%s,",key))
				RaceTracks=append(RaceTracks,key)
				NumRaceTracks++
			}
			rows.Close()
			fnh,err:=os.Create(*sHeaderFile)
			if err!=nil	{
				log.Fatal("Failed to create header file file ",*sHeaderFile," : ",err)
			}
			defer fnh.Close()
			data, _ := json.Marshal(&RaceTracks)
			fnh.Write(data)
			fmt.Printf("Found %d racetrack/distance combinations\n",NumRaceTracks)
		}	else 	{
			// load the headers from the json file
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
		}
		for h:=0;h<NumRaceTracks;h++	{
			fn.WriteString(fmt.Sprintf("%s,",RaceTracks[h]))
		}
	}	else 	{
		for v:=1;v<=NUMVENUES;v++	{
			fn.WriteString(fmt.Sprintf("%s,",Venues[v]))
			NumRaceTracks++
		}
	}
	
	NumAdditional:=0
	query:=fmt.Sprintf("SELECT idraces,venues_idvenues,distance,round(distance/220,1),idrunning,idrtype,idground,windspeed,windgust,winddir,wintime "+
			"FROM races,weather,( "+
				"SELECT round(distanceyards/220) as furlongs,type,avg(wintime/round(distanceyards/220)) as avgtime,stddev(wintime/round(distanceyards/220)) as stddevtime "+
				"FROM sectionals.allraces "+
				"where distanceyards!=0 and wintime!=0 and wintime/round(distanceyards/220)>=10 and wintime/round(distanceyards/220)<=20 "+
				"group by round(distanceyards/220),type) as sd "+
			"where races_idraces=idraces and round(distance/220)=furlongs and rtype=type "+
			"and year(starttime) in (%s) "+
			"and distance!=0 and wintime!=0 and idrunning!=0 and idcond!=0 and idage!=0 and idrtype!=0 and idground!=0 and idclass!=0 and windspeed<120 and "+
			"wintime/furlongs>=avgtime-?*stddevtime and wintime/furlongs<=avgtime+?*stddevtime ",*sYear)
	if *sRType!=""	{
		query=fmt.Sprintf("SELECT idraces,venues_idvenues,distance,round(distance/220,1),idrunning,idrtype,idground,windspeed,windgust,winddir,wintime "+
			"FROM races,weather,( "+
				"SELECT round(distanceyards/220) as furlongs,type,avg(wintime/round(distanceyards/220)) as avgtime,stddev(wintime/round(distanceyards/220)) as stddevtime "+
				"FROM sectionals.allraces "+
				"where distanceyards!=0 and wintime!=0 and wintime/round(distanceyards/220)>=10 and wintime/round(distanceyards/220)<=20 "+
				"group by round(distanceyards/220),type) as sd "+
			"where races_idraces=idraces and round(distance/220)=furlongs and rtype=type "+
			"and year(starttime) in (%s) "+
			"and distance!=0 and wintime!=0 and idrunning!=0 and idcond!=0 and idage!=0 and idrtype!=0 and idground!=0 and idclass!=0 and windspeed<120 and "+
			"wintime/furlongs>=avgtime-?*stddevtime and wintime/furlongs<=avgtime+?*stddevtime  and rtype in (%s) ",
									*sYear,*sRType)
	}
	
	fmt.Printf("Query: %s\n",query)
	rows,err:=db.Query(query,*fSDL,*fSDU)
	if err != nil {
		db.Close()
		log.Fatal("DB Query: ", err)
	}

	// write header
	fn.WriteString("Dist,")
	NumAdditional++
	for i:=1;i<=NUMRUNNING;i++	{
		fn.WriteString(fmt.Sprintf("%s,",Runnings[i]))
		NumAdditional++
	}
	for i:=1;i<=NUMRTYPE;i++	{
		fn.WriteString(fmt.Sprintf("%s,",RTypes[i]))
		NumAdditional++
	}
	for i:=1;i<=NUMGROUND;i++	{
		fn.WriteString(fmt.Sprintf("%s,",Grounds[i]))
		NumAdditional++
	}
	for i:=1;i<=NUMWINDDIR;i++	{
		fn.WriteString(fmt.Sprintf("%s,",WindStrs[i]))
		NumAdditional++
	}
	for i:=1;i<=NUMWINDDIR;i++	{
		fn.WriteString(fmt.Sprintf("%s,",WindGusts[i]))
		NumAdditional++
	}
	fn.WriteString("SP,WT\n")
	NumAdditional+=2
	
	fmt.Printf("Number of Race/track categories %d, %d additional fields, total %d\n",NumRaceTracks,NumAdditional,NumRaceTracks+NumAdditional)
	
	rownum:=0		
	for rows.Next() {
		if err := rows.Scan(&race.IdRace,&race.IdVenue,&race.Distance,&race.Furlongs,&race.IdRunning,&race.IdRType,
							&race.IdGround,&race.WindSpeed,&race.WindGust,&race.WindDir,&race.WinTime); err != nil {
			// Check for a scan error.
			rows.Close()
			db.Close()
			log.Fatal("Rows Scan failed: ", err)
		}
		// grab the racetypes for this race id
		race.WindQuarter=CalcWindQuarter(race.WindDir)
		// expand out the rows
		var 	allcolumns	[]string
		var 	wt 			float64
		scaleddistance:=float64(race.Distance)/(*fMaxDistance*220)
		venuevalue:=float64(1)
		if *bSV 	{
			venuevalue=scaleddistance
		}
			
		if !*bCC	{
			allcolumns=append(allcolumns,Expand(race.IdVenue,NUMVENUES,venuevalue)...)
		}	else 	{
			key:=fmt.Sprintf("%s:%.1f:%d",Venues[race.IdVenue],race.Furlongs,race.IdRType)
			if tracknum,ok:=FindRaceTrack(key);ok	{
				allcolumns=append(allcolumns,Expand(tracknum+1,NumRaceTracks,1)...)
			}	else 	{
				fmt.Printf("Failed to find category %s, skipping\n",key)
				continue
			}
		}
		allcolumns=append(allcolumns,fmt.Sprintf("%.3f",scaleddistance))
		allcolumns=append(allcolumns,Expand(race.IdRunning,NUMRUNNING,1)...)
		allcolumns=append(allcolumns,Expand(race.IdRType,NUMRTYPE,1)...)
		allcolumns=append(allcolumns,Expand(race.IdGround,NUMGROUND,1)...)
		allcolumns=append(allcolumns,Expand(race.WindQuarter,NUMWINDDIR,race.WindSpeed/MAXWIND)...)
		allcolumns=append(allcolumns,Expand(race.WindQuarter,NUMWINDDIR,race.WindGust/MAXWIND)...)
		if *bST	{
			wt=race.WinTime/60
		}	else 	{	
			wt=race.WinTime
		}
		speed:=race.WinTime/(float64(race.Distance)/220)
		if *iMin!=0 && *iMax!=0	{
			// skip races that would result in a divide by zero
			if *iMax-*iMin==0 	{
				continue
			}
			// scale the speed to between 0 and 1
			speed=(speed-float64(*iMin))/float64(*iMax-*iMin)
			if speed < 0 	{
				speed=0
			}
			if speed>1	{
				speed=1
			}
		}
		// dump the columns to the csv file
		for c:=0;c<len(allcolumns);c++	{
			_,err=fn.WriteString(allcolumns[c]+",")
		}
		fn.WriteString(fmt.Sprintf("%.5f,%.5f\n",speed,wt))
		rownum++
		fmt.Printf("Row: %d   SP: %.03f WT: %.03f\r",rownum,speed,wt)
	}
	fmt.Printf("\n")
	rerr := rows.Close()
	if rerr != nil {
		rows.Close()
		db.Close()
		log.Fatal("Rows Close: ", err)
	}
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