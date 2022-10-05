package main

import (
	"fmt"
	"log"
	"math"
	"strconv"
	"time"
	
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"

)


type 	Record	struct	{
	RacesRecord
	Runners 	[MAXRUNNERS]*RunnerRecord
	runners 	map[int]*RunnerRecord
}

var 	(
	FirstSave bool = true
	Blank RunnerRecord
)

func	(r *Record)ProcessRace()	bool	{
	var 	jp,tp 	Performance
	for n:=0;n<MAXRUNNERS;n++	{
		r.Runners[n]=&Blank
	}
	// get the details about the runners in this race	
	runners:=RunnersDF.FilterAggregation(
					dataframe.And, 
					dataframe.F{Colname:"IdRace", Comparator: series.Eq,Comparando:  r.IdRace},
					dataframe.F{Colname:"IdSelection",Comparator: series.Neq,Comparando: 0},
	).Arrange(dataframe.Sort("Odds"))
//	fmt.Printf("RaceId: %d %d Runners %d DaysSince: \n",r.IdRace,runners.Nrow(),r.DaysSince)
//	fmt.Println("Runners: ",runners)
	m:=runners.Maps()
	
//	runners.WriteJSON(CSVfile)
	if runners.Nrow()==0	{
		log.Fatal("Runners.Nrow() is 0")
	}
	if r.IdRace==19003	{
		fmt.Println("RUNNERS: ",runners)
		fmt.Println("RUNNERSMAPs: ",m)
	}
	r.runners=make(map[int]*RunnerRecord)
	nrunners:=runners.Nrow()
	for n:=0;n<nrunners;n++	{
		tr:=m[n]
		rr:=RunnerRecord{
			DFRunnerRecord: 	DFRunnerRecord{
				IdRunner: tr["IdRunner"].(int),
				IdRace: 	tr["IdRace"].(int),
				IdVenue:	tr["IdVenue"].(int),
				Distance:	tr["Distance"].(int),
				IdTrack:	tr["IdTrack"].(int),
				IdSelection:	tr["IdSelection"].(int),
				IdJockey:	tr["IdJockey"].(int),
				IdTrainer:	tr["IdTrainer"].(int),
				DaysSince:	tr["DaysSince"].(int),
				Number:	tr["Number"].(int),
				Scratched:	tr["Scratched"].(bool),
				Draw:	tr["Draw"].(int),
				Position:	tr["Position"].(int),
				Weight:	tr["Weight"].(int),
				Lengths:	tr["Lengths"].(float64),
				LB:		tr["LB"].(float64),
				Age:	tr["Age"].(int),
				Rating:	tr["Rating"].(int),
				Odds:	tr["Odds"].(float64),
				Start:	tr["Start"].(float64),
				F4:	tr["F4"].(float64),
				F3:	tr["F3"].(float64),
				F2:	tr["F2"].(float64),
				F1:	tr["F1"].(float64),
				Real:	tr["Real"].(float64),
				Finish:	tr["Finish"].(float64),
			},
		}
		r.runners[rr.IdSelection]=&rr
		if n<MAXRUNNERS	{
			r.Runners[n]=&rr
		}
	}	
	
	// need to somehow deal with horses with zero previous runs
	
	runs:=RunnersDF.FilterAggregation(
				dataframe.And, 
				dataframe.F{Colname:"IdRace", Comparator: series.Neq,Comparando:  r.IdRace},
				dataframe.F{Colname:"Scratched",Comparator: series.Eq,Comparando: false},
				dataframe.F{Colname:"IdSelection",Comparator: series.In,Comparando: runners.Col("IdSelection")},
				dataframe.F{Colname:"DaysSince",Comparator: series.Less,Comparando: r.DaysSince},
	).Arrange(dataframe.Sort("DaysSince"))
//	runs.WriteCSV(CSVfile)
	if r.IdRace==19003 	{
		fmt.Println("RUNS: ",runs)
		fmt.Println("COL: ",runners.Col("IdSelection"))
		time.Sleep(20*time.Second)
	}
	groups:=runs.Filter(dataframe.F{Colname:"Position",Comparator: series.Neq,Comparando: 0}).GroupBy("IdSelection")
	finishedmap:=groups.GetGroups()
	if len(finishedmap)>0	{
		if r.IdRace==19003	{
			fmt.Println("GROUPS: ",groups)
			fmt.Println("FINISHEDMAP: ",finishedmap)
		}
		for k,v:=range finishedmap	{
			fmt.Println("      ",k," Row ",v.Nrow())
			if r.IdRace==19003	{
				time.Sleep(20*time.Second)
			}
			
		}
		runnerRuns := groups.Aggregation([]dataframe.AggregationType{
			dataframe.Aggregation_COUNT,dataframe.Aggregation_MEAN,
			dataframe.Aggregation_MAX, dataframe.Aggregation_MIN,dataframe.Aggregation_MEDIAN,dataframe.Aggregation_MEAN,dataframe.Aggregation_STD, // Position
			dataframe.Aggregation_MAX, dataframe.Aggregation_MIN,dataframe.Aggregation_MEDIAN,dataframe.Aggregation_MEAN,dataframe.Aggregation_STD, // LB
			dataframe.Aggregation_MAX, dataframe.Aggregation_MIN,dataframe.Aggregation_MEDIAN,dataframe.Aggregation_MEAN,dataframe.Aggregation_STD, // odds
			dataframe.Aggregation_MAX, dataframe.Aggregation_MIN,dataframe.Aggregation_MEDIAN,dataframe.Aggregation_MEAN,dataframe.Aggregation_STD, // f4
			dataframe.Aggregation_MAX, dataframe.Aggregation_MIN,dataframe.Aggregation_MEDIAN,dataframe.Aggregation_MEAN,dataframe.Aggregation_STD, // f3
			dataframe.Aggregation_MAX, dataframe.Aggregation_MIN,dataframe.Aggregation_MEDIAN,dataframe.Aggregation_MEAN,dataframe.Aggregation_STD, // f2
			dataframe.Aggregation_MAX, dataframe.Aggregation_MIN,dataframe.Aggregation_MEDIAN,dataframe.Aggregation_MEAN,dataframe.Aggregation_STD, // f1
			dataframe.Aggregation_MAX, dataframe.Aggregation_MIN,dataframe.Aggregation_MEDIAN,dataframe.Aggregation_MEAN,dataframe.Aggregation_STD, // start
			dataframe.Aggregation_MAX, dataframe.Aggregation_MIN,dataframe.Aggregation_MEDIAN,dataframe.Aggregation_MEAN,dataframe.Aggregation_STD}, // finish
			[]string{"IdSelection","Real","Position","Position","Position","Position","Position",
						"LB","LB","LB","LB","LB",
						"Odds","Odds","Odds","Odds","Odds",
						"F4","F4","F4","F4","F4",
						"F3","F3","F3","F3","F3",
						"F2","F2","F2","F2","F2",
						"F1","F1","F1","F1","F1",
						"Start","Start","Start","Start","Start",
						"Finish","Finish","Finish","Finish","Finish"})
		mruns:=runnerRuns.Maps()
		
		
		for n:=0;n<runnerRuns.Nrow();n++	{
			rn:=mruns[n]
			idselection:=rn["IdSelection"].(int)
			r.runners[idselection].HasHistory=true
			r.runners[idselection].Count=int(rn["IdSelection_COUNT"].(float64))
			if rn["Real_MEAN"]!=nil	{
				r.runners[idselection].AvgReal=rn["Real_MEAN"].(float64)
			}
			r.runners[idselection].AvgPos=rn["Position_MEAN"].(float64)
			r.runners[idselection].MedPos=rn["Position_MEDIAN"].(float64)
			if rn["Position_STD"]!=nil	{
				r.runners[idselection].SDPos=rn["Position_STD"].(float64)
			}
			r.runners[idselection].MinPos=rn["Position_MIN"].(float64)
			r.runners[idselection].MaxPos=rn["Position_MAX"].(float64)
			r.runners[idselection].AvgLB=rn["LB_MEAN"].(float64)
			r.runners[idselection].MedLB=rn["LB_MEDIAN"].(float64)
			if rn["LB_STD"]!=nil	{
				r.runners[idselection].SDLB=rn["LB_STD"].(float64)
			}
			r.runners[idselection].MinLB=rn["LB_MIN"].(float64)
			r.runners[idselection].MaxLB=rn["LB_MAX"].(float64)
			r.runners[idselection].AvgOdds=rn["Odds_MEAN"].(float64)
			r.runners[idselection].MedOdds=rn["Odds_MEDIAN"].(float64)
			if rn["Odds_STD"]!=nil	{
				r.runners[idselection].SDOdds=rn["Odds_STD"].(float64)
			}
			r.runners[idselection].MinOdds=rn["Odds_MIN"].(float64)
			r.runners[idselection].MaxOdds=rn["Odds_MAX"].(float64)
			r.runners[idselection].AvgStart=rn["Start_MEAN"].(float64)
			r.runners[idselection].MedStart=rn["Start_MEDIAN"].(float64)
			if rn["Start_STD"]!=nil	{
				r.runners[idselection].SDStart=rn["Start_STD"].(float64)
			}
			r.runners[idselection].MinStart=rn["Start_MIN"].(float64)
			r.runners[idselection].MaxStart=rn["Start_MAX"].(float64)
			r.runners[idselection].AvgF4=rn["F4_MEAN"].(float64)
			r.runners[idselection].MedF4=rn["F4_MEDIAN"].(float64)
			if rn["F4_STD"]!=nil	{
				r.runners[idselection].SDF4=rn["F4_STD"].(float64)
			}
			r.runners[idselection].MinF4=rn["F4_MIN"].(float64)
			r.runners[idselection].MaxF4=rn["F4_MAX"].(float64)
			r.runners[idselection].AvgF3=rn["F3_MEAN"].(float64)
			r.runners[idselection].MedF3=rn["F3_MEDIAN"].(float64)
			if rn["F3_STD"]!=nil	{
				r.runners[idselection].SDF3=rn["F3_STD"].(float64)
			}
			r.runners[idselection].MinF3=rn["F3_MIN"].(float64)
			r.runners[idselection].MaxF3=rn["F3_MAX"].(float64)
			r.runners[idselection].AvgF2=rn["F2_MEAN"].(float64)
			r.runners[idselection].MedF2=rn["F2_MEDIAN"].(float64)
			if rn["F2_STD"]!=nil	{
				r.runners[idselection].SDF2=rn["F2_STD"].(float64)
			}
			r.runners[idselection].MinF2=rn["F2_MIN"].(float64)
			r.runners[idselection].MaxF2=rn["F2_MAX"].(float64)
			r.runners[idselection].AvgF1=rn["F1_MEAN"].(float64)
			r.runners[idselection].MedF1=rn["F1_MEDIAN"].(float64)
			if rn["F1_STD"]!=nil	{
				r.runners[idselection].SDF1=rn["F1_STD"].(float64)
			}
			r.runners[idselection].MinF1=rn["F1_MIN"].(float64)
			r.runners[idselection].MaxF1=rn["F1_MAX"].(float64)
			r.runners[idselection].AvgF=rn["Finish_MEAN"].(float64)
			r.runners[idselection].MedF=rn["Finish_MEDIAN"].(float64)
			if rn["Finish_STD"]!=nil	{
				r.runners[idselection].SDF=rn["Finish_STD"].(float64)
			}
			r.runners[idselection].MinF=rn["Finish_MIN"].(float64)
			r.runners[idselection].MaxF=rn["Finish_MAX"].(float64)
		}
	}
/*	fmt.Println("RunnerRuns: ",runnerRuns.Select([]string{"IdSelection","IdSelection_COUNT","Real_MEAN",
					"Odds_MAX","Odds_MIN","Odds_MEAN","Odds_STD",
					"Position_MAX","Position_MIN","Position_MEAN","Position_STD",
					"LB_MAX","LB_MIN","LB_MEAN","LB_STD",
					"F1_MAX","F1_MIN","F1_MEAN","F1_STD",
					"F2_MAX","F2_MIN","F2_MEAN","F2_STD",
					"F3_MAX","F3_MIN","F3_MEAN","F3_STD",
					"F4_MAX","F4_MIN","F4_MEAN","F4_STD"}	)) 
	fmt.Println("RunnerRuns: ",runnerRuns) */
	
	// having aggregated the historic runs, process the historic runs for each horse to calculate the
	// strike rates and course/distance values
	groupmap:=runs.GroupBy("IdSelection").GetGroups()
	
	for idstr,historicruns:=range groupmap	{
		idselection,_:=strconv.Atoi(idstr)
//		fmt.Printf("Id: %d:\n",idselection)
		NDNF:=0
		NRuns:=0
		NWins:=0
		NTop3:=0
		NSameJockey:=0
		NSameVenue:=0
		NSameDistance:=0
		NSameTrack:=0
		NJockeyWins:=0
		NVenueWins:=0
		NDistanceWins:=0
		NTrackWins:=0
		NJockeyPlaces:=0
		NVenuePlaces:=0
		NDistancePlaces:=0
		NTrackPlaces:=0
		DaysSinceLastWin:=0
		DaysSinceLastRace:=0
		DaysSinceLastPlace:=0
		RunsSinceLastWin:=0
		RunsSinceLastPlace:=0
		historicmaps:=historicruns.Maps()
		maxruns:=len(historicmaps)
		for rnum:=maxruns;rnum>0;rnum--		{
			run:=historicmaps[rnum-1]
			NRuns++
			pos:=run["Position"].(int)
			idjockey:=run["IdJockey"].(int)
			idvenue:=run["IdVenue"].(int)
			idtrack:=run["IdTrack"].(int)
			dayssince:=r.DaysSince-run["DaysSince"].(int)
			distance:=run["Distance"].(int)
			if idjockey==r.runners[idselection].IdJockey	{
				NSameJockey++
			}
			if idvenue==r.runners[idselection].IdVenue 	{
				NSameVenue++
			}
			if idtrack==r.runners[idselection].IdTrack	{
				NSameTrack++
			}
			if math.Round(float64(distance)/220)==math.Round(float64(r.runners[idselection].Distance)/220)	{
				NSameDistance++
			}
			if pos==0	{
				NDNF++
			}
			if pos==1 	{
				NWins++
				if idjockey==r.runners[idselection].IdJockey	{
					NJockeyWins++
				}
				if idvenue==r.runners[idselection].IdVenue 	{
					NVenueWins++
				}
				if idtrack==r.runners[idselection].IdTrack	{
					NTrackWins++
					r.runners[idselection].CnDWinner=true
				}
				if math.Round(float64(distance)/220)==math.Round(float64(r.runners[idselection].Distance)/220)	{
					NDistanceWins++
				}
				if DaysSinceLastWin==0 /*|| DaysSinceLastWin>dayssince*/	{
					DaysSinceLastWin=dayssince
				}
				if RunsSinceLastWin==0	{
					RunsSinceLastWin=NRuns
				}
			}
			
			if pos>1 && pos<=3 	{
				NTop3++
				if idjockey==r.runners[idselection].IdJockey	{
					NJockeyPlaces++
				}
				if idvenue==r.runners[idselection].IdVenue 	{
					NVenuePlaces++
				}
				if idtrack==r.runners[idselection].IdTrack	{
					NTrackPlaces++
				}
				if math.Round(float64(distance)/220)==math.Round(float64(r.runners[idselection].Distance)/220)	{
					NDistancePlaces++
				}
				if DaysSinceLastPlace==0 /*|| DaysSinceLastPlace>dayssince*/	{
					DaysSinceLastPlace=dayssince
				}
				if RunsSinceLastPlace==0	{
					RunsSinceLastPlace=NRuns
				}
			}
			if DaysSinceLastRace==0 || DaysSinceLastRace>dayssince	{
				DaysSinceLastRace=dayssince
			}
			if dayssince<=RecentDays	{
				r.runners[idselection].HRStrike=float64(NWins)/float64(NRuns)
				r.runners[idselection].HRPlaceStrike=float64(NTop3)/float64(NRuns)
				r.runners[idselection].HRDNFStrike=float64(NDNF)/float64(NRuns)
				r.runners[idselection].HRNRuns=NRuns
				r.runners[idselection].VRNRuns=NSameVenue
				r.runners[idselection].DRNRuns=NSameDistance
				r.runners[idselection].TRNRuns=NSameTrack
				r.runners[idselection].JRNRuns=NSameJockey
			}
			if NRuns==RecentRuns	{
				r.runners[idselection].HRRStrike=float64(NWins)/float64(NRuns)
				r.runners[idselection].HRRPlaceStrike=float64(NTop3)/float64(NRuns)
				r.runners[idselection].HRRDNFStrike=float64(NDNF)/float64(NRuns)
				r.runners[idselection].HRRNRuns=NRuns
				r.runners[idselection].VRRNRuns=NSameVenue
				r.runners[idselection].DRRNRuns=NSameDistance
				r.runners[idselection].TRRNRuns=NSameTrack
				r.runners[idselection].JRRNRuns=NSameJockey
			}
			
//			fmt.Printf(" Run: %d Age: %d Wins: %d Places: %d DNF: %d SameJ: %d SameV: %d SameD: %d DaysLastWin: %d DaysLastPlace: %d RunsLastWin: %d\n",
//						NRuns,dayssince,NWins,NTop3,NDNF,NSameJockey,NSameVenue,NSameDistance,DaysSinceLastWin,DaysSinceLastPlace,RunsSinceLastWin)
		}
		r.runners[idselection].HStrike=float64(NWins)/float64(NRuns)
		r.runners[idselection].HPlaceStrike=float64(NTop3)/float64(NRuns)
		r.runners[idselection].HDNFStrike=float64(NDNF)/float64(NRuns)
		r.runners[idselection].HVStrike=float64(NVenueWins)/float64(NSameVenue)
		r.runners[idselection].HVPlaceStrike=float64(NVenuePlaces)/float64(NSameVenue)
		r.runners[idselection].HDStrike=float64(NDistanceWins)/float64(NSameDistance)
		r.runners[idselection].HDPlaceStrike=float64(NDistancePlaces)/float64(NSameDistance)
		r.runners[idselection].HTStrike=float64(NTrackWins)/float64(NSameTrack)
		r.runners[idselection].HTPlaceStrike=float64(NTrackPlaces)/float64(NSameTrack)
		r.runners[idselection].HJStrike=float64(NJockeyWins)/float64(NSameJockey)
		r.runners[idselection].HJPlaceStrike=float64(NJockeyPlaces)/float64(NSameJockey)
		r.runners[idselection].HNRuns=NRuns
		r.runners[idselection].VNRuns=NSameVenue
		r.runners[idselection].DNRuns=NSameDistance
		r.runners[idselection].TNRuns=NSameTrack
		r.runners[idselection].JNRuns=NSameJockey
		r.runners[idselection].TimeLastRun=DaysSinceLastRace
		r.runners[idselection].TimeLastWin=DaysSinceLastWin
		r.runners[idselection].TimeLastPlace=DaysSinceLastPlace
		r.runners[idselection].RunsLastWin=RunsSinceLastWin
		r.runners[idselection].RunsLastPlace=RunsSinceLastPlace
		r.runners[idselection].Count=NRuns
	}
	
	
	// now we have processed all the historic runs, make up some values for those horses on their first run
	// based on standard times
	distancef:=float64(r.Distance)				// distance of of the race
	furlongs:=int(math.Round(distancef/220))	// rounded to nearest furlongs
	stdtime:=r.StdTime							// 
	wintime:=stdtime*distancef/220
	for n:=0;n<MAXRUNNERS;n++	{
		if r.Runners[n].IdSelection!=0	{
			// for each horse in the race (with a valid idselection), grab the strike info for the jockey and trainer
			jp=r.CalcPerformance(r.Runners[n].IdJockey,n,"IdJockey")
			tp=r.CalcPerformance(r.Runners[n].IdTrainer,n,"IdTrainer")
			r.Runners[n].JStrike=jp.Strike
			r.Runners[n].JVStrike=jp.VStrike
			r.Runners[n].JDStrike=jp.DStrike
			r.Runners[n].JTStrike=jp.TStrike
			r.Runners[n].JOStrike=jp.OStrike
			r.Runners[n].JPStrike=jp.PStrike
			r.Runners[n].JVPStrike=jp.VPStrike
			r.Runners[n].JDPStrike=jp.DPStrike
			r.Runners[n].JTPStrike=jp.TPStrike
			r.Runners[n].JOPStrike=jp.OPStrike
			r.Runners[n].JDNF=jp.DNF
			r.Runners[n].TStrike=tp.Strike
			r.Runners[n].TVStrike=tp.VStrike
			r.Runners[n].TDStrike=tp.DStrike
			r.Runners[n].TTStrike=tp.TStrike
			r.Runners[n].TOStrike=tp.OStrike
			r.Runners[n].TPStrike=tp.PStrike
			r.Runners[n].TVPStrike=tp.VPStrike
			r.Runners[n].TDPStrike=tp.DPStrike
			r.Runners[n].TTPStrike=tp.TPStrike
			r.Runners[n].TOPStrike=tp.OPStrike
			r.Runners[n].TDNF=tp.DNF
			r.Runners[n].JRStrike=jp.RStrike
			r.Runners[n].JRVStrike=jp.RVStrike
			r.Runners[n].JRDStrike=jp.RDStrike
			r.Runners[n].JRTStrike=jp.RTStrike
			r.Runners[n].JROStrike=jp.ROStrike
			r.Runners[n].JRPStrike=jp.RPStrike
			r.Runners[n].JRVPStrike=jp.RVPStrike
			r.Runners[n].JRDPStrike=jp.RDPStrike
			r.Runners[n].JRTPStrike=jp.RTPStrike
			r.Runners[n].JROPStrike=jp.ROPStrike
			r.Runners[n].JRDNF=jp.RDNF
			r.Runners[n].TRStrike=tp.RStrike
			r.Runners[n].TRVStrike=tp.RVStrike
			r.Runners[n].TRDStrike=tp.RDStrike
			r.Runners[n].TRTStrike=tp.RTStrike
			r.Runners[n].TROStrike=tp.ROStrike
			r.Runners[n].TRPStrike=tp.RPStrike
			r.Runners[n].TRVPStrike=tp.RVPStrike
			r.Runners[n].TRDPStrike=tp.RDPStrike
			r.Runners[n].TRTPStrike=tp.RTPStrike
			r.Runners[n].TROPStrike=tp.ROPStrike
			r.Runners[n].TRDNF=tp.RDNF
			r.Runners[n].JRRStrike=jp.RRStrike
			r.Runners[n].JRRVStrike=jp.RRVStrike
			r.Runners[n].JRRDStrike=jp.RRDStrike
			r.Runners[n].JRRTStrike=jp.RRTStrike
			r.Runners[n].JRROStrike=jp.RROStrike
			r.Runners[n].JRRPStrike=jp.RRPStrike
			r.Runners[n].JRRVPStrike=jp.RRVPStrike
			r.Runners[n].JRRDPStrike=jp.RRDPStrike
			r.Runners[n].JRRTPStrike=jp.RRTPStrike
			r.Runners[n].JRROPStrike=jp.RROPStrike
			r.Runners[n].JRRDNF=jp.RRDNF
			r.Runners[n].TRRStrike=tp.RRStrike
			r.Runners[n].TRRVStrike=tp.RRVStrike
			r.Runners[n].TRRDStrike=tp.RRDStrike
			r.Runners[n].TRRTStrike=tp.RRTStrike
			r.Runners[n].TRROStrike=tp.RROStrike
			r.Runners[n].TRRPStrike=tp.RRPStrike
			r.Runners[n].TRRVPStrike=tp.RRVPStrike
			r.Runners[n].TRRDPStrike=tp.RRDPStrike
			r.Runners[n].TRRTPStrike=tp.RRTPStrike
			r.Runners[n].TRROPStrike=tp.RROPStrike
			r.Runners[n].TRRDNF=tp.RRDNF
			
			r.Runners[n].JNum=jp.Num
			r.Runners[n].JVNum=jp.VNum
			r.Runners[n].JDNum=jp.DNum
			r.Runners[n].JTNum=jp.TNum
			r.Runners[n].JONum=jp.ONum
			r.Runners[n].JRNum=jp.RNum
			r.Runners[n].JRVNum=jp.RVNum
			r.Runners[n].JRDNum=jp.RDNum
			r.Runners[n].JRTNum=jp.RTNum
			r.Runners[n].JRONum=jp.RONum
			r.Runners[n].JRRNum=jp.RRNum
			r.Runners[n].JRRVNum=jp.RRVNum
			r.Runners[n].JRRDNum=jp.RRDNum
			r.Runners[n].JRRTNum=jp.RRTNum
			r.Runners[n].JRRONum=jp.RRONum
			r.Runners[n].TNum=tp.Num
			r.Runners[n].TVNum=tp.VNum
			r.Runners[n].TDNum=tp.DNum
			r.Runners[n].TTNum=tp.TNum
			r.Runners[n].TONum=tp.ONum
			r.Runners[n].TRNum=tp.RNum
			r.Runners[n].TRVNum=tp.RVNum
			r.Runners[n].TRDNum=tp.RDNum
			r.Runners[n].TRTNum=tp.RTNum
			r.Runners[n].TRONum=tp.RONum
			r.Runners[n].TRRNum=tp.RRNum
			r.Runners[n].TRRVNum=tp.RRVNum
			r.Runners[n].TRRDNum=tp.RRDNum
			r.Runners[n].TRRTNum=tp.RRTNum
			r.Runners[n].TRRONum=tp.RRONum
			
			if !r.Runners[n].HasHistory 	&& stdtime!=0	{
				// have a horse with no history and the race has a standard time
				lengthsbehind:=float64(r.Starters)*0.5			// behind by 1/2L per number of runners
				r.Runners[n].AvgReal=0							// only 1 result and it's made up so not real
				r.Runners[n].AvgPos=float64(r.Starters)/2		// say finishes mid pack
				r.Runners[n].MedPos=float64(r.Starters)/2
				r.Runners[n].SDPos=0
				r.Runners[n].MinPos=float64(r.Starters)/2
				r.Runners[n].MaxPos=float64(r.Starters)/2
				r.Runners[n].AvgLB=lengthsbehind		
				r.Runners[n].MedLB=lengthsbehind
				r.Runners[n].SDLB=0
				r.Runners[n].MinLB=lengthsbehind
				r.Runners[n].MaxLB=lengthsbehind
				r.Runners[n].AvgOdds=r.Runners[n].Odds			// odds are the current odds
				r.Runners[n].MedOdds=r.Runners[n].Odds
				r.Runners[n].SDOdds=0
				r.Runners[n].MinOdds=r.Runners[n].Odds
				r.Runners[n].MaxOdds=r.Runners[n].Odds
				dist:=distancef-(lengthsbehind*LENGTH2YARDS)
				avgfurtime:=wintime*220/dist
				finish:=wintime+avgfurtime*((lengthsbehind*LENGTH2YARDS)/220)
				f4:=finish*FurlongAdjustments[furlongs][1]+avgfurtime
				f3:=finish*FurlongAdjustments[furlongs][2]+avgfurtime
				f2:=finish*FurlongAdjustments[furlongs][3]+avgfurtime
				f1:=finish*FurlongAdjustments[furlongs][4]+avgfurtime
				start:=(finish-(f1+f2+f3+f4))/((distancef/220)-4)
				// now base values relative to standard time
				finish=wintime/finish
				f4=stdtime/f4
				f3=stdtime/f3
				f2=stdtime/f2
				f1=stdtime/f1
				start=stdtime/start
				r.Runners[n].AvgF=finish
				r.Runners[n].MedF=finish
				r.Runners[n].SDF=0
				r.Runners[n].MinF=finish
				r.Runners[n].MaxF=finish
				r.Runners[n].AvgStart=start
				r.Runners[n].MedStart=start
				r.Runners[n].SDStart=0
				r.Runners[n].MinStart=start
				r.Runners[n].MaxStart=start
				r.Runners[n].AvgF4=f4
				r.Runners[n].MedF4=f4
				r.Runners[n].SDF4=0
				r.Runners[n].MinF4=f4
				r.Runners[n].MaxF4=f4
				r.Runners[n].AvgF3=f3
				r.Runners[n].MedF3=f3
				r.Runners[n].SDF3=0
				r.Runners[n].MinF3=f3
				r.Runners[n].MaxF3=f3
				r.Runners[n].AvgF2=f2
				r.Runners[n].MedF2=f2
				r.Runners[n].SDF2=0
				r.Runners[n].MinF2=f2
				r.Runners[n].MaxF2=f2
				r.Runners[n].AvgF1=f1
				r.Runners[n].MedF1=f1
				r.Runners[n].SDF1=0
				r.Runners[n].MinF1=f1
				r.Runners[n].MaxF1=f1
			}
		}	
	}
	return true
}

func	(race *Record)ExpandRace()		{
	var 	allcolumns	[]string
	allcolumns=append(allcolumns,Expand(race.IdTrack+1,NumRaceTracks,1)...)
	allcolumns=append(allcolumns,Expand(race.IdVenue,NUMVENUES-1,1)...)
	allcolumns=append(allcolumns,Expand(StarterCategory(race.Starters),NUMSTARTERS,1)...)
	allcolumns=append(allcolumns,fmt.Sprintf("%.3f",float64(race.Distance)/(MaxDistance*220)))
	allcolumns=append(allcolumns,Expand(race.IdRunning,NUMRUNNING,1)...)
	allcolumns=append(allcolumns,Expand(race.IdCond,NUMCOND,1)...)
	allcolumns=append(allcolumns,Expand(race.IdAge,NUMAGE,1)...)
	allcolumns=append(allcolumns,Expand(race.IdRType,NUMRTYPE,1)...)
	allcolumns=append(allcolumns,Expand(race.IdGround,NUMGROUND,1)...)
	allcolumns=append(allcolumns,Expand(race.IdClass,NUMCLASS,1)...)
	allcolumns=append(allcolumns,race.raceTypes...)
	allcolumns=append(allcolumns,Expand(race.WindQuarter,NUMWINDDIR,race.WindSpeed/MAXWIND)...)
	allcolumns=append(allcolumns,Expand(race.WindQuarter,NUMWINDDIR,race.WindGust/MAXWIND)...)
	allcolumns=append(allcolumns,fmt.Sprintf("%.2f",race.StdTime))
	for r:=0;r<MAXRUNNERS;r++	{
		included:=0
		if race.Runners[r].IdSelection!=0	{
			included=1
		}
		scratched:=0
		if race.Runners[r].Scratched	{
			scratched=1
		}
		firstrun:=0
		if !race.Runners[r].HasHistory	{
			firstrun=1
		}
		allcolumns=append(allcolumns,fmt.Sprintf("%d",included),
									fmt.Sprintf("%d",scratched))
		allcolumns=append(allcolumns,Expand(Limit(race.Runners[r].Draw+1,NUMDRAW),NUMDRAW,1)...)  // draws run from 0 to 20 expand ignores first value. 
		allcolumns=append(allcolumns,fmt.Sprintf("%f",float64(race.Runners[r].Weight)/MAXWEIGHT))
		allcolumns=append(allcolumns,Expand(Limit(race.Runners[r].Age-1,NUMHORSEAGE),NUMHORSEAGE,1)...) // age starts at position 1=>2
		allcolumns=append(allcolumns,fmt.Sprintf("%f",float64(race.Runners[r].Rating)/MAXRATING))
		allcolumns=append(allcolumns,fmt.Sprintf("%f",race.Runners[r].Odds))
		allcolumns=append(allcolumns,fmt.Sprintf("%d",firstrun),
								fmt.Sprintf("%f",race.Runners[r].Real),
								fmt.Sprintf("%f",race.Runners[r].AvgReal),
								fmt.Sprintf("%f",race.Runners[r].AvgPos),
								fmt.Sprintf("%f",race.Runners[r].MedPos),
								fmt.Sprintf("%f",race.Runners[r].SDPos),
								fmt.Sprintf("%f",race.Runners[r].MinPos),
								fmt.Sprintf("%f",race.Runners[r].MaxPos),
								fmt.Sprintf("%f",race.Runners[r].AvgLB),
								fmt.Sprintf("%f",race.Runners[r].MedLB),
								fmt.Sprintf("%f",race.Runners[r].SDLB),
								fmt.Sprintf("%f",race.Runners[r].MinLB),
								fmt.Sprintf("%f",race.Runners[r].MaxLB),
								fmt.Sprintf("%f",race.Runners[r].AvgOdds),
								fmt.Sprintf("%f",race.Runners[r].MedOdds),
								fmt.Sprintf("%f",race.Runners[r].SDOdds),
								fmt.Sprintf("%f",race.Runners[r].MinOdds),
								fmt.Sprintf("%f",race.Runners[r].MaxOdds),
								fmt.Sprintf("%f",race.Runners[r].AvgStart),
								fmt.Sprintf("%f",race.Runners[r].MedStart),
								fmt.Sprintf("%f",race.Runners[r].SDStart),
								fmt.Sprintf("%f",race.Runners[r].MinStart),
								fmt.Sprintf("%f",race.Runners[r].MaxStart),
								fmt.Sprintf("%f",race.Runners[r].AvgF4),
								fmt.Sprintf("%f",race.Runners[r].MedF4),
								fmt.Sprintf("%f",race.Runners[r].SDF4),
								fmt.Sprintf("%f",race.Runners[r].MinF4),
								fmt.Sprintf("%f",race.Runners[r].MaxF4),
								fmt.Sprintf("%f",race.Runners[r].AvgF3),
								fmt.Sprintf("%f",race.Runners[r].MedF3),
								fmt.Sprintf("%f",race.Runners[r].SDF3),
								fmt.Sprintf("%f",race.Runners[r].MinF3),
								fmt.Sprintf("%f",race.Runners[r].MaxF3),
								fmt.Sprintf("%f",race.Runners[r].AvgF2),
								fmt.Sprintf("%f",race.Runners[r].MedF2),
								fmt.Sprintf("%f",race.Runners[r].SDF2),
								fmt.Sprintf("%f",race.Runners[r].MinF2),
								fmt.Sprintf("%f",race.Runners[r].MaxF2),
								fmt.Sprintf("%f",race.Runners[r].AvgF1),
								fmt.Sprintf("%f",race.Runners[r].MedF1),
								fmt.Sprintf("%f",race.Runners[r].SDF1),
								fmt.Sprintf("%f",race.Runners[r].MinF1),
								fmt.Sprintf("%f",race.Runners[r].MaxF1),
								fmt.Sprintf("%f",race.Runners[r].AvgF),
								fmt.Sprintf("%f",race.Runners[r].MedF),
								fmt.Sprintf("%f",race.Runners[r].SDF),
								fmt.Sprintf("%f",race.Runners[r].MinF),
								fmt.Sprintf("%f",race.Runners[r].MaxF))
		
		allcolumns=append(allcolumns,
								fmt.Sprintf("%f",race.Runners[r].HStrike),
								fmt.Sprintf("%f",race.Runners[r].HVStrike),
								fmt.Sprintf("%f",race.Runners[r].HDStrike),
								fmt.Sprintf("%f",race.Runners[r].HTStrike),
								fmt.Sprintf("%f",race.Runners[r].HJStrike),
								fmt.Sprintf("%f",race.Runners[r].HPlaceStrike),
								fmt.Sprintf("%f",race.Runners[r].HVPlaceStrike),
								fmt.Sprintf("%f",race.Runners[r].HDPlaceStrike),
								fmt.Sprintf("%f",race.Runners[r].HTPlaceStrike),
								fmt.Sprintf("%f",race.Runners[r].HJPlaceStrike),
								fmt.Sprintf("%f",race.Runners[r].HDNFStrike))
								
		allcolumns=append(allcolumns,
								fmt.Sprintf("%f",race.Runners[r].JStrike),
								fmt.Sprintf("%f",race.Runners[r].JVStrike),
								fmt.Sprintf("%f",race.Runners[r].JDStrike),
								fmt.Sprintf("%f",race.Runners[r].JTStrike),
								fmt.Sprintf("%f",race.Runners[r].JOStrike),
								fmt.Sprintf("%f",race.Runners[r].JPStrike),
								fmt.Sprintf("%f",race.Runners[r].JVPStrike),
								fmt.Sprintf("%f",race.Runners[r].JDPStrike),
								fmt.Sprintf("%f",race.Runners[r].JTPStrike),
								fmt.Sprintf("%f",race.Runners[r].JOPStrike),
								fmt.Sprintf("%f",race.Runners[r].JDNF))
								
		allcolumns=append(allcolumns,
								fmt.Sprintf("%f",race.Runners[r].TStrike),
								fmt.Sprintf("%f",race.Runners[r].TVStrike),
								fmt.Sprintf("%f",race.Runners[r].TDStrike),
								fmt.Sprintf("%f",race.Runners[r].TTStrike),
								fmt.Sprintf("%f",race.Runners[r].TOStrike),
								fmt.Sprintf("%f",race.Runners[r].TPStrike),
								fmt.Sprintf("%f",race.Runners[r].TVPStrike),
								fmt.Sprintf("%f",race.Runners[r].TDPStrike),
								fmt.Sprintf("%f",race.Runners[r].TTPStrike),
								fmt.Sprintf("%f",race.Runners[r].TOPStrike),
								fmt.Sprintf("%f",race.Runners[r].TDNF))
		
		allcolumns=append(allcolumns,
								fmt.Sprintf("%f",race.Runners[r].HRStrike),
								fmt.Sprintf("%f",race.Runners[r].HRPlaceStrike),
								fmt.Sprintf("%f",race.Runners[r].HRDNFStrike),
								fmt.Sprintf("%f",race.Runners[r].HRRStrike),
								fmt.Sprintf("%f",race.Runners[r].HRRPlaceStrike),
								fmt.Sprintf("%f",race.Runners[r].HRRDNFStrike))
								
		allcolumns=append(allcolumns,
								fmt.Sprintf("%f",race.Runners[r].JRStrike),
								fmt.Sprintf("%f",race.Runners[r].JRVStrike),
								fmt.Sprintf("%f",race.Runners[r].JRDStrike),
								fmt.Sprintf("%f",race.Runners[r].JRTStrike),
								fmt.Sprintf("%f",race.Runners[r].JROStrike),
								fmt.Sprintf("%f",race.Runners[r].JRPStrike),
								fmt.Sprintf("%f",race.Runners[r].JRVPStrike),
								fmt.Sprintf("%f",race.Runners[r].JRDPStrike),
								fmt.Sprintf("%f",race.Runners[r].JRTPStrike),
								fmt.Sprintf("%f",race.Runners[r].JROPStrike),
								fmt.Sprintf("%f",race.Runners[r].JRRDNF),
								fmt.Sprintf("%f",race.Runners[r].JRRStrike),
								fmt.Sprintf("%f",race.Runners[r].JRRVStrike),
								fmt.Sprintf("%f",race.Runners[r].JRRDStrike),
								fmt.Sprintf("%f",race.Runners[r].JRRTStrike),
								fmt.Sprintf("%f",race.Runners[r].JRROStrike),
								fmt.Sprintf("%f",race.Runners[r].JRRPStrike),
								fmt.Sprintf("%f",race.Runners[r].JRRVPStrike),
								fmt.Sprintf("%f",race.Runners[r].JRRDPStrike),
								fmt.Sprintf("%f",race.Runners[r].JRRTPStrike),
								fmt.Sprintf("%f",race.Runners[r].JRROPStrike),
								fmt.Sprintf("%f",race.Runners[r].JRRDNF))
								
		allcolumns=append(allcolumns,
								fmt.Sprintf("%f",race.Runners[r].TRStrike),
								fmt.Sprintf("%f",race.Runners[r].TRPStrike),
								fmt.Sprintf("%f",race.Runners[r].TRRStrike),
								fmt.Sprintf("%f",race.Runners[r].TRRPStrike))
								
		allcolumns=append(allcolumns,Expand(DaysCategory(race.Runners[r].TimeLastRun),NUMDAYCATS,1)...)
		allcolumns=append(allcolumns,Expand(DaysCategory(race.Runners[r].TimeLastWin),NUMDAYCATS,1)...)
		allcolumns=append(allcolumns,Expand(DaysCategory(race.Runners[r].TimeLastPlace),NUMDAYCATS,1)...)
		allcolumns=append(allcolumns,Expand(RunsCategory(race.Runners[r].RunsLastWin),NUMRUNSLAST,1)...)
		allcolumns=append(allcolumns,Expand(RunsCategory(race.Runners[r].RunsLastPlace),NUMRUNSLAST,1)...)
											
		allcolumns=append(allcolumns,
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].HNRuns),MAXRUNS)),
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].VNRuns),MAXRUNS)),
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].DNRuns),MAXRUNS)),
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].TNRuns),MAXRUNS)),
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].JNRuns),MAXRUNS)))
								
		allcolumns=append(allcolumns,
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].HRNRuns),RECENTDAYS)),
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].VRNRuns),RECENTDAYS)),
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].DRNRuns),RECENTDAYS)),
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].TRNRuns),RECENTDAYS)),
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].JRNRuns),RECENTDAYS)))
								
		allcolumns=append(allcolumns,
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].HRRNRuns),RECENTRUNS)),
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].VRRNRuns),RECENTRUNS)),
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].DRRNRuns),RECENTRUNS)),
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].TRRNRuns),RECENTRUNS)),
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].JRRNRuns),RECENTRUNS)))
								
		allcolumns=append(allcolumns,
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].JNum),5000)),
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].JVNum),800)),
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].JDNum),1000)),
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].JTNum),200)),
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].JONum),2000)))
								
		allcolumns=append(allcolumns,
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].JRNum),150)),
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].JRVNum),50)),
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].JRDNum),50)),
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].JRTNum),20)),
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].JRONum),100)))
								
		allcolumns=append(allcolumns,
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].JRRNum),RECENTRUNS)),
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].JRRVNum),RECENTRUNS)),
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].JRRDNum),RECENTRUNS)),
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].JRRTNum),RECENTRUNS)),
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].JRRONum),RECENTRUNS)))
								
		allcolumns=append(allcolumns,
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].TNum),5000)),
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].TVNum),800)),
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].TDNum),1500)),
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].TTNum),200)),
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].TONum),2000)))
								
		allcolumns=append(allcolumns,
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].TRNum),250)),
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].TRVNum),80)),
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].TRDNum),100)),
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].TRTNum),30)),
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].TRONum),110)))
								
		allcolumns=append(allcolumns,
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].TRRNum),RECENTRUNS)),
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].TRRVNum),RECENTRUNS)),
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].TRRDNum),RECENTRUNS)),
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].TRRTNum),RECENTRUNS)),
								fmt.Sprintf("%f",ScaledNumber(float64(race.Runners[r].TRRONum),RECENTRUNS)))
								
/*		if race.Runners[r].IdSelection!=0	{
			fmt.Printf("RunnerId: %d TimeLastRun:%d TimeLastWin:%d TimeLastPlace: %d RunsLastWin: %d RunsLastPlace: %d\n -> %v\n",race.Runners[r].IdRunner,
						race.Runners[r].TimeLastRun,
						race.Runners[r].TimeLastWin,
						race.Runners[r].TimeLastPlace,
						race.Runners[r].RunsLastWin,
						race.Runners[r].RunsLastPlace,
						race.Runners[r])
		}					*/
									
	}
	for r:=0;r<MAXRUNNERS;r++	{
		allcolumns=append(allcolumns,race.Runners[r].FinishCategory()...)
		allcolumns=append(allcolumns,
							fmt.Sprintf("%f",race.Runners[r].LB),
							fmt.Sprintf("%f",race.Runners[r].Start),
							fmt.Sprintf("%f",race.Runners[r].Finish))
	}
		
//	fmt.Println(len(allcolumns)," Category columns")
	for c:=0;c<len(allcolumns);c++	{
		CSVfile.WriteString(allcolumns[c]+",")
	}
	CSVfile.WriteString("\n")
		
}


func	(r *Record)WriteRace()		{
	if FirstSave	{
		CSVfile.WriteString("IdRace,IdVenue,IdTrack,DaysSince,Starters,Distance,Furlongs,RaceTypes,IdRunning,IdCond,IdAge,IdRType,IdGround,IdClass,"+
							"WindSpeed,WindGust,WindDir,WindQuarter,WinTime,StdTime,")
		for runner:=1;runner<=MAXRUNNERS;runner++	{
			CSVfile.WriteString(fmt.Sprintf("IdRunner%d,IdRace%d,IdSelection%d,IdJockey%d,IdTrainer%d,DaysSince%d,Number%d,Scratched%d,Draw%d,Position%d,Weight%d,Length%ds,"+
							"LB%d,Age%d,Rating%d,Odds%d,Start%d,F4-%d,F3-%d,F2-%d,F1-%d,Real%d,Finish%d,FirstRun%d,",runner,runner,runner,runner,runner,runner,runner,runner,runner,
							runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner))
			CSVfile.WriteString(fmt.Sprintf("AvgReal%d,AvgPos%d,MedPos%d,SDPos%d,MinPos%d,MaxPos%d,AvgLB%d,MedLB%d,SDLB%d,MinLB%d,MaxLB%d,AvgOdds%d,MedOdds%d,SDOdds%d,MinOdds%d,MaxOdds%d,"+
							"AvgStart%d,MedStart%d,SDStart%d,MinStart%d,MaxStart%d,AvgF4-%d,MedF4-%d,SDF4-%d,MinF4-%d,MaxF4-%d,"+
							"AvgF3-%d,MedF3-%d,SDF3-%d,MinF3-%d,MaxF3-%d,AvgF2-%d,MedF2-%d,SDF2-%d,MinF2-%d,MaxF2-%d,AvgF1-%d,MedF1-%d,SDF1-%d,MinF1-%d,MaxF1-%d,"+
							"AvgF%d,MedF%d,SDF%d,MinF%d,MaxF%d,",runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,
							runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,
							runner,runner,runner,runner,runner,runner,runner,runner,runner,runner))
			CSVfile.WriteString(fmt.Sprintf("HStrike%d,HVStrike%d,HDStrike%d,HTStrike%d,HJStrike%d,"+
							"HPStrike%d,HVPStrike%d,HDPStrike%d,HTPStrike%d,HJPStrike%d,HDNFStrike%d,"+
							"JStrike%d,JVStrike%d,JDStrike%d,JTStrike%d,JOStrike%d,JPStrike%d,JVPStrike%d,JDPStrike%d,JTPStrike%d,JOPStrike%d,JDNF%d,"+
							"TStrike%d,TVStrike%d,TDStrike%d,TTStrike%d,TOStrike%d,TPStrike%d,TVPStrike%d,TDPStrike%d,TTPStrike%d,TOPStrike%d,TDNF%d,"+
							"HRStrike%d,HRPStrike%d,HRDNFStrike%d,HRRStrike%d,HRRPStrike%d,HRRDNFStrike%d,"+
							"JRStrike%d,JRVStrike%d,JRDStrike%d,JRTStrike%d,JROStrike%d,JRPStrike%d,JRVPStrike%d,JRDPStrike%d,JRTPStrike%d,JROPStrike%d,JRDNF%d,"+
							"JRRStrike%d,JRRVStrike%d,JRRDStrike%d,JRRTStrike%d,JRROStrike%d,JRRPStrike%d,JRRVPStrike%d,JRRDPStrike%d,JRRTPStrike%d,JRROPStrike%d,JRRDNF%d,"+
							"TRStrike%d,TRPStrike%d,TRRStrike%d,TRRPStrike%d,"+
							"TLastRun%d,TLastWin%d,TLastPlace%d,RLastWin%d,RLastPlace%d,CDWinner%d,",
							runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,
							runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,
							runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,
							runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,
							runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner))
			CSVfile.WriteString(fmt.Sprintf("HNRuns%d,VNRuns%d,DNRuns%d,TNRuns%d,JNRuns%d,HRNRuns%d,VRNRuns%d,DRNRuns%d,TRNRuns%d,JRNRuns%d,"+
							"HRRNRuns%d,VRRNRuns%d,DRRNRuns%d,TRRNRuns%d,JRRNRuns%d,",
							runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner))
			CSVfile.WriteString(fmt.Sprintf("JNum%d,JVNum%d,JDNum%d,JTNum%d,JONum%d,JRNum%d,JRVNum%d,JRDNum%d,JRTNum%d,JRONum%d,"+
							"JRRNum%d,JRRVNum%d,JRRDNum%d,JRRTNum%d,JRRONum%d,",
							runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner))
			CSVfile.WriteString(fmt.Sprintf("TNum%d,TVNum%d,TDNum%d,TTNum%d,TONum%d,TRNum%d,TRVNum%d,TRDNum%d,TRTNum%d,TRONum%d,"+
							"TRRNum%d,TRRVNum%d,TRRDNum%d,TRRTNum%d,TRRONum%d,",
							runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner,runner))
							
		}
		FirstSave=false
		CSVfile.WriteString("\n")
	}
	CSVfile.WriteString(fmt.Sprintf("%d,%d,%d,%d,%d,%d,%.2f,%s,%d,%d,%d,%d,%d,%d,%.2f,%.2f,%d,%d,%.4f,%.4f,",
						r.IdRace,r.IdVenue,r.IdTrack,r.DaysSince,r.Starters,r.Distance,r.Furlongs,"RaceTypes",r.IdRunning,r.IdCond,
						r.IdAge,r.IdRType,r.IdGround,r.IdClass,r.WindSpeed,r.WindGust,r.WindDir,r.WindQuarter,r.WinTime,r.StdTime))
	for runner:=0;runner<MAXRUNNERS;runner++	{
		CSVfile.WriteString(fmt.Sprintf("%d,%d,%d,%d,%d,%d,%d,%v,%d,%d,%d,%.2f,%.2f,%d,%d,%.5f,%.4f,%.4f,%.4f,%.4f,%.4f,%.0f,%.4f,%v,",
						r.Runners[runner].IdRunner,
						r.Runners[runner].IdRace,
						r.Runners[runner].IdSelection,
						r.Runners[runner].IdJockey,
						r.Runners[runner].IdTrainer,
						r.Runners[runner].DaysSince,
						r.Runners[runner].Number,
						r.Runners[runner].Scratched,
						r.Runners[runner].Draw,
						r.Runners[runner].Position,
						r.Runners[runner].Weight,
						r.Runners[runner].Lengths,
						r.Runners[runner].LB,
						r.Runners[runner].Age,
						r.Runners[runner].Rating,
						r.Runners[runner].Odds,
						r.Runners[runner].Start,
						r.Runners[runner].F4,
						r.Runners[runner].F3,
						r.Runners[runner].F2,
						r.Runners[runner].F1,
						r.Runners[runner].Real,
						r.Runners[runner].Finish,
						(!r.Runners[runner].HasHistory && r.Runners[runner].IdSelection!=0)))
		CSVfile.WriteString(fmt.Sprintf("%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,"+
							"%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,",
							r.Runners[runner].AvgReal,
							r.Runners[runner].AvgPos,
							r.Runners[runner].MedPos,
							r.Runners[runner].SDPos,
							r.Runners[runner].MinPos,
							r.Runners[runner].MaxPos,
							r.Runners[runner].AvgLB,
							r.Runners[runner].MedLB,
							r.Runners[runner].SDLB,
							r.Runners[runner].MinLB,
							r.Runners[runner].MaxLB,
							r.Runners[runner].AvgOdds,
							r.Runners[runner].MedOdds,
							r.Runners[runner].SDOdds,
							r.Runners[runner].MinOdds,
							r.Runners[runner].MaxOdds,
							r.Runners[runner].AvgStart,
							r.Runners[runner].MedStart,
							r.Runners[runner].SDStart,
							r.Runners[runner].MinStart,
							r.Runners[runner].MaxStart,
							r.Runners[runner].AvgF4,
							r.Runners[runner].MedF4,
							r.Runners[runner].SDF4,
							r.Runners[runner].MinF4,
							r.Runners[runner].MaxF4,
							r.Runners[runner].AvgF3,
							r.Runners[runner].MedF3,
							r.Runners[runner].SDF3,
							r.Runners[runner].MinF3,
							r.Runners[runner].MaxF3,
							r.Runners[runner].AvgF2,
							r.Runners[runner].MedF2,
							r.Runners[runner].SDF2,
							r.Runners[runner].MinF2,
							r.Runners[runner].MaxF2,
							r.Runners[runner].AvgF1,
							r.Runners[runner].MedF1,
							r.Runners[runner].SDF1,
							r.Runners[runner].MinF1,
							r.Runners[runner].MaxF1,
							r.Runners[runner].AvgF,
							r.Runners[runner].MedF,
							r.Runners[runner].SDF,
							r.Runners[runner].MinF,
							r.Runners[runner].MaxF))
		CSVfile.WriteString(fmt.Sprintf("%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,"+
										"%f,%f,%f,%f,"+
										"%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%d,%d,%d,%d,%d,%v,",
							r.Runners[runner].HStrike,
							r.Runners[runner].HVStrike,
							r.Runners[runner].HDStrike,
							r.Runners[runner].HTStrike,
							r.Runners[runner].HJStrike,
							r.Runners[runner].HPlaceStrike,
							r.Runners[runner].HVPlaceStrike,
							r.Runners[runner].HDPlaceStrike,
							r.Runners[runner].HTPlaceStrike,
							r.Runners[runner].HJPlaceStrike,
							r.Runners[runner].HDNFStrike,
							r.Runners[runner].JStrike,
							r.Runners[runner].JVStrike,
							r.Runners[runner].JDStrike,
							r.Runners[runner].JTStrike,
							r.Runners[runner].JOStrike,
							r.Runners[runner].JPStrike,
							r.Runners[runner].JVPStrike,
							r.Runners[runner].JDPStrike,
							r.Runners[runner].JTPStrike,
							r.Runners[runner].JOPStrike,
							r.Runners[runner].JDNF,
							r.Runners[runner].TStrike,
							r.Runners[runner].TVStrike,
							r.Runners[runner].TDStrike,
							r.Runners[runner].TTStrike,
							r.Runners[runner].TOStrike,
							r.Runners[runner].TPStrike,
							r.Runners[runner].TVPStrike,
							r.Runners[runner].TDPStrike,
							r.Runners[runner].TTPStrike,
							r.Runners[runner].TOPStrike,
							r.Runners[runner].TDNF,
							r.Runners[runner].HRStrike,
							r.Runners[runner].HRPlaceStrike,
							r.Runners[runner].HRDNFStrike,
							r.Runners[runner].HRRStrike,
							r.Runners[runner].HRRPlaceStrike,
							r.Runners[runner].HRRDNFStrike,
							r.Runners[runner].JRStrike,
							r.Runners[runner].JRVStrike,
							r.Runners[runner].JRDStrike,
							r.Runners[runner].JRTStrike,
							r.Runners[runner].JROStrike,
							r.Runners[runner].JRPStrike,
							r.Runners[runner].JRVPStrike,
							r.Runners[runner].JRDPStrike,
							r.Runners[runner].JRTPStrike,
							r.Runners[runner].JROPStrike,
							r.Runners[runner].JRDNF,
							r.Runners[runner].JRRStrike,
							r.Runners[runner].JRRVStrike,
							r.Runners[runner].JRRDStrike,
							r.Runners[runner].JRRTStrike,
							r.Runners[runner].JRROStrike,
							r.Runners[runner].JRRPStrike,
							r.Runners[runner].JRRVPStrike,
							r.Runners[runner].JRRDPStrike,
							r.Runners[runner].JRRTPStrike,
							r.Runners[runner].JRROPStrike,
							r.Runners[runner].JRRDNF,
							r.Runners[runner].TRStrike,
							r.Runners[runner].TRPStrike,
							r.Runners[runner].TRRStrike,
							r.Runners[runner].TRRPStrike,
							r.Runners[runner].TimeLastRun,
							r.Runners[runner].TimeLastWin,
							r.Runners[runner].TimeLastPlace,
							r.Runners[runner].RunsLastWin,
							r.Runners[runner].RunsLastPlace,
							r.Runners[runner].CnDWinner))
		CSVfile.WriteString(fmt.Sprintf("%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,",
							r.Runners[runner].HNRuns,
							r.Runners[runner].VNRuns,
							r.Runners[runner].DNRuns,
							r.Runners[runner].TNRuns,
							r.Runners[runner].JNRuns,
							r.Runners[runner].HRNRuns,
							r.Runners[runner].VRNRuns,
							r.Runners[runner].DRNRuns,
							r.Runners[runner].TRNRuns,
							r.Runners[runner].JRNRuns,
							r.Runners[runner].HRRNRuns,
							r.Runners[runner].VRRNRuns,
							r.Runners[runner].DRRNRuns,
							r.Runners[runner].TRRNRuns,
							r.Runners[runner].JRRNRuns))
		CSVfile.WriteString(fmt.Sprintf("%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,",
							r.Runners[runner].JNum,
							r.Runners[runner].JVNum,
							r.Runners[runner].JDNum,
							r.Runners[runner].JTNum,
							r.Runners[runner].JONum,
							r.Runners[runner].JRNum,
							r.Runners[runner].JRVNum,
							r.Runners[runner].JRDNum,
							r.Runners[runner].JRTNum,
							r.Runners[runner].JRONum,
							r.Runners[runner].JRRNum,
							r.Runners[runner].JRRVNum,
							r.Runners[runner].JRRDNum,
							r.Runners[runner].JRRTNum,
							r.Runners[runner].JRRONum))
		CSVfile.WriteString(fmt.Sprintf("%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,",
							r.Runners[runner].TNum,
							r.Runners[runner].TVNum,
							r.Runners[runner].TDNum,
							r.Runners[runner].TTNum,
							r.Runners[runner].TONum,
							r.Runners[runner].TRNum,
							r.Runners[runner].TRVNum,
							r.Runners[runner].TRDNum,
							r.Runners[runner].TRTNum,
							r.Runners[runner].TRONum,
							r.Runners[runner].TRRNum,
							r.Runners[runner].TRRVNum,
							r.Runners[runner].TRRDNum,
							r.Runners[runner].TRRTNum,
							r.Runners[runner].TRRONum))
							
	}					
	CSVfile.WriteString("\n")
}

type Performance	struct	{
	Strike 			float64			// strike rate
	VStrike			float64			// strike rate at venue
	DStrike			float64			// strike rate at Distance
	TStrike			float64			// strike rate at track (C & D)
	OStrike			float64			// jockey/trainer strike rate
	PStrike			float64			// place strike rate 
	VPStrike		float64			// place strike rate at venue
	DPStrike		float64			// place strike rate at Distance
	TPStrike		float64			// place strike rate at track (C & D)
	OPStrike		float64			// jockey/trainer place strike rate
	DNF				float64			// dnf strike rate
	RStrike 		float64			// strike rate in recent time
	RVStrike		float64			// strike rate at venue in recent time
	RDStrike		float64			// strike rate at distance in recent time
	RTStrike		float64			// strike rate at track in recent time
	ROStrike		float64			// jockey/trainer strike rate in recent time
	RPStrike		float64			// place strike rate in recent time
	RVPStrike		float64			// place strike rate at venue in recent time
	RDPStrike		float64			// place strike rate at distance in recent time
	RTPStrike		float64			// place strike rate at track in recent time
	ROPStrike		float64			// jockey/trainer place strike rate in recent time
	RDNF			float64			// dnf strike rate in recent times
	RRStrike 		float64			// strike rate in recent runs
	RRVStrike		float64			// strike rate at venue in recent runs
	RRDStrike		float64			// strike rate at distance in recent runs
	RRTStrike		float64			// strike rate at track in recent runs
	RROStrike		float64			// jockey/trainer strike rate in recent runs
	RRPStrike		float64			// strike rate in recent runs
	RRVPStrike		float64			// strike rate at venue in recent runs
	RRDPStrike		float64			// strike rate at distance in recent runs
	RRTPStrike		float64			// strike rate at track in recent runs
	RROPStrike		float64			// jockey/trainer place strike rate in recent runs
	RRDNF			float64			// dnf strike rate in recent runs
	Num				int				// number of runs
	VNum			int				// number runs on this venue
	DNum			int				// number of runs at this distance
	TNum			int				// number of runs at this track (C & D)
	ONum			int 			// number of jockey/trainer runs 
	RNum			int				// number of runs IN recent time
	RVNum			int				// number runs on this venue IN recent time
	RDNum			int				// number of runs at this distance IN recent time
	RTNum			int				// number of runs at this track (C & D) IN recent time
	RONum			int 			// number of jockey/trainer runs in recent times
	RRNum			int				// number of runs IN recent runs
	RRVNum			int				// number runs on this venue IN recent runs
	RRDNum			int				// number of runs at this distance IN recent runs
	RRTNum			int				// number of runs at this track (C & D) IN recent runs
	RRONum			int 			// number of jockey/trainer runs in recent runs
}							

func 	(r 	*Record)CalcPerformance(id ,runnernum int, jort string)	(p Performance)	{
	if jort!="IdJockey" && jort!="IdTrainer"	{
		log.Fatal("CalcPerformance: unknown type: ",jort)
	}
	
	runs:=RunnersDF.FilterAggregation(
				dataframe.And, 
				dataframe.F{Colname: jort, Comparator: series.Eq,Comparando:  id},
				dataframe.F{Colname:"Scratched",Comparator: series.Eq,Comparando: false},
				dataframe.F{Colname:"DaysSince",Comparator: series.Less,Comparando: r.DaysSince},
	).Arrange(dataframe.Sort("DaysSince"))
	groupmap:=runs.GroupBy(jort).GetGroups()
	for _,historicruns:=range groupmap	{
//		thisid,_:=strconv.Atoi(idstr)
//		fmt.Printf("%s: %d:\n",jort,thisid)
		NDNF:=0
		NRuns:=0
		NWins:=0
		NTop3:=0
		NSameVenue:=0
		NSameDistance:=0
		NSameTrack:=0
		NSameJockey:=0
		NSameTrainer:=0
		NVenueWins:=0
		NDistanceWins:=0
		NTrackWins:=0
		NJockeyWins:=0
		NTrainerWins:=0
		NVenuePlaces:=0
		NDistancePlaces:=0
		NTrackPlaces:=0 
		NJockeyPlaces:=0
		NTrainerPlaces:=0
		historicmaps:=historicruns.Maps()
		maxruns:=len(historicmaps)
		for rnum:=maxruns;rnum>0;rnum--		{
			run:=historicmaps[rnum-1]
			NRuns++
			pos:=run["Position"].(int)
			idvenue:=run["IdVenue"].(int)
			idtrack:=run["IdTrack"].(int)
			idjockey:=run["IdJockey"].(int)
			idtrainer:=run["IdTrainer"].(int)
			dayssince:=r.DaysSince-run["DaysSince"].(int)
			distance:=run["Distance"].(int)
			if idvenue==r.IdVenue 	{
				NSameVenue++
			}
			if idtrack==r.IdTrack	{
				NSameTrack++
			}
			if idjockey==r.Runners[runnernum].IdJockey	{
				NSameJockey++
			}
			if idtrainer==r.Runners[runnernum].IdTrainer	{
				NSameTrainer++
			}
			if math.Round(float64(distance)/220)==math.Round(float64(r.Distance)/220)	{
				NSameDistance++
			}
			if pos==0	{
				NDNF++
			}
			if pos==1 	{
				NWins++
				if idvenue==r.IdVenue 	{
					NVenueWins++
				}
				if idtrack==r.IdTrack	{
					NTrackWins++
				}
				if idjockey==r.Runners[runnernum].IdJockey	{
					NJockeyWins++
				}
				if idtrainer==r.Runners[runnernum].IdTrainer	{
					NTrainerWins++
				}
				if math.Round(float64(distance)/220)==math.Round(float64(r.Distance)/220)	{
					NDistanceWins++
				}				
			}
			
			if pos>1 && pos<=3 	{
				NTop3++
				if idvenue==r.IdVenue 	{
					NVenuePlaces++
				}
				if idtrack==r.IdTrack	{
					NTrackPlaces++
				}
				if idjockey==r.Runners[runnernum].IdJockey	{
					NJockeyPlaces++
				}
				if idtrainer==r.Runners[runnernum].IdTrainer	{
					NTrainerPlaces++
				}
				if math.Round(float64(distance)/220)==math.Round(float64(r.Distance)/220)	{
					NDistancePlaces++
				}
			}
			if dayssince<=RecentDays	{
				p.RStrike=float64(NWins)/float64(NRuns)
				p.RVStrike=float64(NVenueWins)/float64(NSameVenue)
				p.RDStrike=float64(NDistanceWins)/float64(NSameDistance)
				p.RTStrike=float64(NTrackWins)/float64(NSameTrack)
				if jort=="IdJockey"	{
					p.ROStrike=float64(NTrainerWins)/float64(NSameTrainer)
					p.ROPStrike=float64(NTrainerPlaces)/float64(NSameTrainer)
					p.RONum=NSameTrainer
				}	else 	{
					p.ROStrike=float64(NJockeyWins)/float64(NSameJockey)
					p.ROPStrike=float64(NJockeyPlaces)/float64(NSameJockey)
					p.RONum=NSameJockey
				}	
				p.RPStrike=float64(NTop3)/float64(NRuns)
				p.RVPStrike=float64(NVenuePlaces)/float64(NSameVenue)
				p.RDPStrike=float64(NDistancePlaces)/float64(NSameDistance)
				p.RTPStrike=float64(NTrackPlaces)/float64(NSameTrack)
				p.RDNF=float64(NDNF)/float64(NRuns)
				p.RNum=NRuns
				p.RVNum=NSameVenue
				p.RDNum=NSameDistance
				p.RTNum=NSameTrack
			}
			if NRuns==RecentRuns	{
				p.RRStrike=float64(NWins)/float64(NRuns)
				p.RRVStrike=float64(NVenueWins)/float64(NSameVenue)
				p.RRDStrike=float64(NDistanceWins)/float64(NSameDistance)
				p.RRDStrike=float64(NTrackWins)/float64(NSameTrack)
				p.RRPStrike=float64(NTop3)/float64(NRuns)
				p.RRVPStrike=float64(NVenuePlaces)/float64(NSameVenue)
				p.RRDPStrike=float64(NDistancePlaces)/float64(NSameDistance)
				p.RRDPStrike=float64(NTrackPlaces)/float64(NSameTrack)
				if jort=="IdJockey"	{
					p.RROStrike=float64(NTrainerWins)/float64(NSameTrainer)
					p.RROPStrike=float64(NTrainerPlaces)/float64(NSameTrainer)
					p.RRONum=NSameTrainer
				}	else 	{
					p.RROStrike=float64(NJockeyWins)/float64(NSameJockey)
					p.RROPStrike=float64(NJockeyPlaces)/float64(NSameJockey)
					p.RRONum=NSameJockey
				}	
				p.RRDNF=float64(NDNF)/float64(NRuns)
				p.RRNum=NRuns
				p.RRVNum=NSameVenue
				p.RRDNum=NSameDistance
				p.RRTNum=NSameTrack
			}
		}
		p.Strike=float64(NWins)/float64(NRuns)
		p.PStrike=float64(NTop3)/float64(NRuns)
		p.DNF=float64(NDNF)/float64(NRuns)
		p.VStrike=float64(NVenueWins)/float64(NSameVenue)
		p.VPStrike=float64(NVenuePlaces)/float64(NSameVenue)
		p.DStrike=float64(NDistanceWins)/float64(NSameDistance)
		p.DPStrike=float64(NDistancePlaces)/float64(NSameDistance)
		p.TStrike=float64(NTrackWins)/float64(NSameTrack)
		p.TPStrike=float64(NTrackPlaces)/float64(NSameTrack)
		if jort=="IdJockey"	{
			p.OStrike=float64(NTrainerWins)/float64(NSameTrainer)
			p.OPStrike=float64(NTrainerPlaces)/float64(NSameTrainer)
			p.ONum=NSameTrainer
		}	else 	{
			p.OStrike=float64(NJockeyWins)/float64(NSameJockey)
			p.OPStrike=float64(NJockeyPlaces)/float64(NSameJockey)
			p.ONum=NSameJockey
		}	
		p.Num=NRuns
		p.VNum=NSameVenue
		p.DNum=NSameDistance
		p.TNum=NSameTrack		
//		fmt.Println("P: ",p)
	}
	return p
}


	