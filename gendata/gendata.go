package main

import (
	"fmt"
	"log"
	"database/sql"
	"os"
	_ "github.com/go-sql-driver/mysql"
	"flag"
)

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
	TurfRunning =	[]string{"Firm","Good to Firm","Good","Good to Soft","Soft","Soft to Heavy","Heavy"}
	AllWRunning =	[]string{"Slow","Standard to Slow","Standard"}
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
	RaceTrackMap  map[string]int 		// map of venue|furlongs|rtype to column number
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

func 	main()	{
	fmt.Printf("Gendata v%d.%d.%d\n",VERMAJ,VERMIN,VERPATCH)

	sDB:=flag.String("db","sectionals?parseTime=true","Database connection string")
	sFileName:=flag.String("file","report.csv","Output file name ")
	sRType:=flag.String("rt","'Flat'","RTypes to limit results by, e.g. \"'Flat','Chase','National Hunt Flat'\" ")
	sGType:=flag.String("gt","Turf","Ground types to limit results by, e.g. \"'Allweather','Turf'\" ")
	sUser:=flag.String("u","","DB Username")
	sPass:=flag.String("p","","DB Password")
	flag.Parse()
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
	
	// find out how many venue|distance|rtypes there are and populate the map
	query:=fmt.Sprintf("select venues_idvenues,vname,round(distance/220,1),distance,rtype,ground from races,venues "+ 
						"where venues_idvenues=idvenues and countries_idcountry=1 and distance!=0 and wintime!=0 and idrunning!=0 and idcond!=0 and "+
						"idage!=0 and idrtype!=0 and idground!=0 and idclass!=0 and "+
						"round(wintime/(distance/220),1)<19 and round(wintime/(distance/220),1)>= 10 and "+
						"rtype in (%s) and ground in (%s) "+ 
						"group by vname,round(distance/220,1),distance,rtype,ground "+
						"order by vname,round(distance/220,1),distance,rtype,ground",*sRType,*sGType)
	rows,err:=db.Query(query)
	if err != nil {
		db.Close()
		log.Fatal("DB Query (",query,") failed retrieving categories: ", err)
	}
	var idvenue			int64
	var distance 		int
	var furlongs		float64
	var rtype 			string
	var vname 			string
	var ground 			string
	fn.WriteString("Venue,Distance,Going,RTypes,Surface,WindDir,Wind,Gust\n")

	for rows.Next() {
		if err := rows.Scan(&idvenue,&vname,&furlongs,&distance,&rtype,&ground); err != nil {
			// Check for a scan error.
			rows.Close()
			db.Close()
			log.Fatal("Category Rows Scan failed: ", err)
		}
		switch (ground)	{
		case 	"Turf":
			for g:=0;g<len(TurfRunning);g++	{
				fn.WriteString(fmt.Sprintf("%s,%d,%s,%s,%s,N,0,0\n",
					vname,distance,TurfRunning[g],rtype,ground))
			}
		default: 
			for g:=0;g<len(AllWRunning);g++	{
				fn.WriteString(fmt.Sprintf("%s,%d,%s,%s,%s,N,0,0\n",
					vname,distance,AllWRunning[g],rtype,ground))
			}
		}
	}
	rows.Close()
}	