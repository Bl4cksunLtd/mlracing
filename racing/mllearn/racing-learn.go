package main

import (
	"github.com/aunum/gold/pkg/v1/common/num"
	"github.com/aunum/gold/pkg/v1/common/require"
	"github.com/aunum/gold/pkg/v1/dense"
	"github.com/aunum/goro/pkg/v1/layer"
	m "github.com/aunum/goro/pkg/v1/model"
	"github.com/aunum/log"

	g "gorgonia.org/gorgonia"
	"gorgonia.org/tensor"

	"github.com/go-gota/gota/dataframe"
	"gonum.org/v1/gonum/mat"

	
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"
	"strings"
	"encoding/gob"

)

/* 	Racing-Learn 		-	builds and trains a simple model from supplied data
		Parameters are:
		-fn 		Input file 																	(default input.csv)
		-sf			Save the generated model as this file 										(default model.gob)
		-t1 		The name of the column that should be used as the target for the model 		(default Target)
		-d1 		Ignore this column as an input source	 									(default Target)
		-d2 		Ignore this column as an input source
		-d3 		Ignore this column as an input source
		-nt 		Number of target columns present in the input file 							(default 2)
		-it 		Number of iterations to perform during training 							(default 10000)
		-lr 		Learning rate 																(default 0.001)
		-p 			filename to save predictions of the test dataset 							(default don't save)
		-lp 		Learning percentage, what percentage (0-1) of the data is used for training	(default 0.8)
		
		Input file must have column headers and any targets must be the last columns. The data can have multiple targets,
		but only one is used to train the model. Any targets must be excluded as an input using the -dn parameters, so the 
		file can have between 1 and 3 targets.
		
*/


const 	(
	VERMAJ		=	1
	VERMIN		=	0
	VERPATCH	=	0
	MAXHORSES	=	20
)
var 	(
	FileName 			string
	SaveFile			string
	Drop,HDrop 			[]string
	MaxIter 			int
	TargetNum			int
	OutRows				int
	NumTargets			int
	Targets,HTargets	[]string
	repeat 				bool
	Predict 			string
	LearnRate 			float64
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
	fmt.Printf("Racing-Learn v%d.%d.%d\n",VERMAJ,VERMIN,VERPATCH)
	fn:=flag.String("fn","input.csv","Filename of the dataset")
	sT:=flag.String("targets","","Comma delimited list of Target column names that are not horse based")
	sHT:=flag.String("htargets","","Comma delimited list of target column names that are horse based, will be postfixed with number")
	iI:=flag.Int("it",10000,"Max interations (default 10000)")
	fLR:=flag.Float64("lr",0.001,"Learn Rate (default 0.001)")
	sP:=flag.String("p","","Filename to save predictions to, default do not save")
	fLP:=flag.Float64("lp",0.8,"Percentage dataset to use for learning")
	sS:=flag.String("sf","model.gob","Save the model to filename")
	sD:=flag.String("drop","","Comma delimited list of columns to drop from inputs that are not horse based")
	sHD:=flag.String("hdrop","","Comma delimited list of columns to drop from inputs that are horse based and will be postfixed with horse number")
	bLinear:=flag.Bool("linear",true,"Output layer should be linear regression (true) or categories (false)")
	iOutRows:=flag.Int("orows",1,"Number of output rows")
	flag.Parse()
	FileName=*fn
	MaxIter=*iI
	Targets=strings.Split(*sT,",")
	HTargets=strings.Split(*sHT,",")
	Drop=strings.Split(*sD,",")
	HDrop=strings.Split(*sHD,",")
	LearnRate=*fLR
	Predict=*sP
	SaveFile=*sS
	OutRows=*iOutRows
//	LearnPercent:=*fLP
	prestr:=""
	if Predict!="" {
		prestr=fmt.Sprintf(", save predictions to file %s",Predict)
	}
	fmt.Printf("Checking file %s, targetting %v from race fields, %v from each set of horse fields.\n",FileName,Targets,HTargets)
	fmt.Printf("Ignoring as inputs: %v from race fields and %v from horse fields\nTest/Train %.2f\n",Drop,HDrop,*fLP)
	fmt.Printf("%d Iterations %f Learning Rate %s\n",MaxIter,LearnRate,prestr)
	fmt.Printf("%d output rows\n",OutRows)
	fmt.Printf("Model type Linear: %v\n",*bLinear)
	
	xtrain, ytrain,xtest,ytest := getXYMat(*fn,*fLP)
	x := tensor.FromMat64(mat.DenseCopyOf(xtrain),tensor.As(g.Float32))
	y := tensor.FromMat64(mat.DenseCopyOf(ytrain),tensor.As(g.Float32))
	log.Infov("x example shape", x.Shape())
	log.Infov("x Dtype: ",x.Dtype())
	log.Infov("y example shape", y.Shape())
	log.Infov("y Dtype: ",y.Dtype())


	exampleSize := x.Shape()[0]
	log.Infov("exampleSize", exampleSize)

	testX:=tensor.FromMat64(mat.DenseCopyOf(xtest),tensor.As(g.Float32))
	testY:= tensor.FromMat64(mat.DenseCopyOf(ytest),tensor.As(g.Float32))

	log.Infov("testX example shape", testX.Shape())
	log.Infov("testX Dtype: ",testX.Dtype())
	log.Infov("testY example shape", testY.Shape())
	log.Infov("testY Dtype: ",testY.Dtype())

	batchSize := 100
	log.Infov("batchsize", batchSize)

	batches := exampleSize / batchSize
	log.Infov("num batches", batches)

	numcols:=x.Shape()[1]
	xi := m.NewInput("x", []int{1,x.Shape()[1]},m.AsType(tensor.Float32))
	log.Infov("xi input shape", xi.Shape())
	log.Infov("numcols: ",numcols)
	
	numoutcols:=y.Shape()[1]/OutRows
	yi := m.NewInput("y", []int{OutRows,numoutcols},m.AsType(tensor.Float32))
	log.Infov("yi input shape", yi.Shape())
	log.Infov("numoutcols:",numoutcols)
	
	model, err := m.NewSequential("sp")
	require.NoError(err)

	outlayer:=layer.FC{Input: 100, Output: numoutcols*OutRows ,Name: "OSM0", NoBias: true, Activation:layer.NewSoftmax()}
	lossfunc:=m.WithLoss(m.CrossEntropy)
	if *bLinear	{
		outlayer=layer.FC{Input: 100, Output: numoutcols*OutRows ,Name: "OL0", NoBias: true, Activation:layer.NewLinear()}
		lossfunc=m.WithLoss(m.MSE)
	}	

	model.AddLayers(
		layer.FC{Input: numcols, Output: numcols*2 ,Name: "L0", NoBias: true},
		layer.Dropout{},
		layer.FC{Input: numcols*2, Output: numcols/4 ,Name: "L1", NoBias: true},
		layer.FC{Input: numcols/4, Output: 100 ,Name: "L1", NoBias: true},
		outlayer,
//		layer.FC{Input: 100, Output: numoutcols ,Name: "O0", NoBias: true, Activation:layer.NewLinear()},
		
	)
	
	
	optimizer := g.NewRMSPropSolver(g.WithBatchSize(float64(batchSize)),g.WithLearnRate(LearnRate),g.WithL1Reg(0.2))
		
	err = model.Compile(xi, yi,
		m.WithOptimizer(optimizer),
		lossfunc,
//		m.WithLoss(m.MSE),
//		m.WithLoss(m.NewPseudoHuberLoss(0.2)),
		m.WithBatchSize(batchSize),
	)
	
	require.NoError(err)
	
	log.Infov("Learnabled: ",model.Learnables())

	epochs := *iI
	starttime:=time.Now()
	log.Infof("Starting %d epochs at %v\n", epochs,starttime)
	for epoch := 0; epoch < epochs; epoch++ {
		for batch := 0; batch < batches; batch++ {
			start := batch * batchSize
			end := start + batchSize
			if start >= exampleSize {
				break
			}
			if end > exampleSize {
				end = exampleSize
			}
			xi, err := x.Slice(dense.MakeRangedSlice(start, end))
			require.NoError(err)
//			xi.Reshape(batchSize, 1, 28, 28)
//			log.Infov("xi slice Dtype ",xi.Dtype())

			yi, err := y.Slice(dense.MakeRangedSlice(start, end))
			require.NoError(err)
			yi.Reshape(batchSize, numoutcols)
//			log.Infov("yi slice Dtype ",yi.Dtype())
			err = model.FitBatch(xi, yi)
			require.NoError(err)
			model.Tracker.LogStep(epoch, batch)
		}
	//	accuracy, loss, err := evaluate(testX.(*tensor.Dense), testY.(*tensor.Dense), model, batchSize)
//		if epoch%5==0  || epoch==epochs-1	{
			accuracy, loss, err := evaluate(testX, testY, model, batchSize)
			require.NoError(err)
			log.Infof("%v completed train epoch %v with accuracy %v and loss %v",time.Since(starttime), epoch, accuracy, loss)
/*		}	else 	{
			fmt.Printf(".")
		} */
	}
	err = save(model.Learnables())
	if err != nil {
			log.Fatal(err)
	}
	err = model.Tracker.Clear()
	require.NoError(err)
}

func evaluate(x, y *tensor.Dense, model *m.Sequential, batchSize int) (acc float64, loss float64, err error) {
	exampleSize := x.Shape()[0]
	batches := exampleSize / batchSize

	accuracies := []float32{}
	for batch := 0; batch < batches; batch++ {
		start := batch * batchSize
		end := start + batchSize
		if start >= exampleSize {
			break
		}
		if end > exampleSize {
			end = exampleSize
		}

		xi, err := x.Slice(dense.MakeRangedSlice(start, end))
		require.NoError(err)
	//	xi.Reshape(batchSize, 1, 28, 28)

		yi, err := y.Slice(dense.MakeRangedSlice(start, end))
		require.NoError(err)
	//	yi.Reshape(batchSize, 10)

		yHat, err := model.PredictBatch(xi)
		require.NoError(err)

		acc, err := accuracy(yHat.(*tensor.Dense), yi.(*tensor.Dense), model)
		require.NoError(err)
		accuracies = append(accuracies, float32(acc))
	}
	lossVal, err := model.Tracker.GetValue("sp_train_batch_loss")
	require.NoError(err)
	loss = float64(lossVal.Scalar())
	acc = float64(num.Mean(accuracies))
	return
}


// Accuracy doesn't work yet
func accuracy(yHat, y *tensor.Dense, model m.Model) (float64, error) {
/*	diff,err:=tensor.Sub(yHat,y)
	require.NoError(err)
	adiff,err:=tensor.Abs(diff)
	require.NoError(err)
	eq,err:=tensor.Lte(adiff,AC)
	require.NoError(err)
*/	

	yMax, err := y.Argmax(1)
	require.NoError(err)

	yHatMax, err := yHat.Argmax(1)
	require.NoError(err)

	eq, err := tensor.ElEq(yMax, yHatMax, tensor.AsSameType())
	require.NoError(err) 
	eqd := eq.(*tensor.Dense)
	len := eqd.Len()
	
/*	numTrue:=0
	for i:=0;i<len;i++	{
		t,err:=eqd.At(i)
		require.NoError(err)
		if t.(bool)	{
			numTrue++
		}
	}
*/
	numTrue, err := eqd.Sum()
	if err != nil {
		return 0, err
	}
	return float64(numTrue.Data().(int)) / float64(len), nil
//	return float32(numTrue) / float32(len), nil
}

// takes a filename and train percentage (0-1) returns xtrain,ytrain,xtest,ytest
func getXYMat(filename string,trainpercent float64) (*matrix, *matrix, *matrix,*matrix) {
	if trainpercent<0 || trainpercent>1	{
		log.Fatal("Train Percentage must be between 0 and 1")
	}
	
	var 	trainindexes,testindexes 	[]int

	hdroplen:=len(HDrop)
	if HDrop[0]==""	{
		hdroplen=0
	}
	htargetlen:=len(HTargets)
	if HTargets[0]==""	{
		htargetlen=0
	}
	droplen:=len(Drop)
	if Drop[0]==""	{
		droplen=0
	}
	targetlen:=len(Targets)
	if Targets[0]==""	{
		targetlen=0
	}
	fmt.Println("Creating column indexes lengths ",hdroplen,htargetlen,droplen,targetlen,MAXHORSES)
	targethorseindexes:=make([]string,(MAXHORSES)*htargetlen+targetlen)
	fmt.Println("THI cap:",cap(targethorseindexes))
	drophorseindexes:=make([]string,(MAXHORSES)*hdroplen+droplen)
	fmt.Println("DHI cap:",cap(drophorseindexes))
	for h:=0;h<MAXHORSES;h++	{
		fmt.Printf("\nHorse %d: ",h)
		for c:=0;c<hdroplen;c++	{
			fmt.Printf("Hd%d: %s%d (%d),",c,strings.TrimSpace(HDrop[c]),h+1,h*hdroplen+c)
			drophorseindexes[h*hdroplen+c]=fmt.Sprintf("%s%d",strings.TrimSpace(HDrop[c]),h+1)
		}
		for c:=0;c<htargetlen;c++	{
			fmt.Printf("Ht%d: %s%d (%d),",c,strings.TrimSpace(HTargets[c]),h+1,h*htargetlen+c)
			targethorseindexes[c+h*htargetlen]=fmt.Sprintf("%s%d",strings.TrimSpace(HTargets[c]),h+1)
		}
	}
	for c:=0;c<droplen;c++	{
		fmt.Printf("D%d: %s (%d),",c,strings.TrimSpace(Drop[c]),MAXHORSES*hdroplen+c)
		drophorseindexes[MAXHORSES*hdroplen+c]=fmt.Sprintf("%s",strings.TrimSpace(Drop[c]))
	}
	for c:=0;c<targetlen;c++	{
		fmt.Printf("T%d: %s (%d),",c,strings.TrimSpace(Targets[c]),MAXHORSES*htargetlen+c)
		targethorseindexes[c+MAXHORSES*htargetlen]=fmt.Sprintf("%s",strings.TrimSpace(Targets[c]))
	}
	
/*	for c:=0;c<droplen;c++	{
		drophorseindexes=append(drophorseindexes,fmt.Sprintf("%s",HDrop[c]))
	}
	for c:=0;c<len(Targets);c++	{
		targethorseindexes=append(targethorseindexes,fmt.Sprintf("%s",Targets[c]))
	}
*/	fmt.Println("\nColumns to drop from Input: ",drophorseindexes)
	fmt.Println("Columns to target: ",targethorseindexes)
	


	f, err := os.Open(filename)
	if err != nil {
		log.Fatal("Failed to open input file: ",err)
	}
	defer f.Close()
	fmt.Printf("Reading input file %s...",filename)
	df := dataframe.ReadCSV(f)
	numRows, numCols := df.Dims()
	fmt.Printf("Read %d rows, %d columns... generating train and test datasets...\n",numRows,numCols)
	rand.Seed(time.Now().UnixNano())
	for r:=0;r<numRows;r++	{
		if rand.Float64()<=trainpercent	{
			trainindexes=append(trainindexes,r)
		}	else 	{
			testindexes=append(testindexes,r)
		}
	}

	fmt.Println("Creating an input subset for training")
	trainDF:=df.Subset(trainindexes)
	x,y:=trainDF.Dims()
	fmt.Println("TrainDF size ",x,y)
	fmt.Println("Creating an input subset for testing")
	testDF:=df.Subset(testindexes)
	x,y=testDF.Dims()
	fmt.Println("TestDF size ",x,y)
	fmt.Println("Dropping unused input columns from training set")
	fmt.Printf("Original dropindex: ")
	for c:=0;c<len(drophorseindexes);c++	{
		fmt.Printf("[%d:%s] ",c,drophorseindexes[c])
	}
	fmt.Printf("\n")
//	drophorseindexes=[]string{"Odds1","Odds2","Odds3","Odds4","Odds5","Odds6","Odds7","Odds8","Odds9","Odds10",
//							"Odds11","Odds12","Odds13","Odds14","Odds15","Odds16","Odds17","Odds18","Odds19","Odds20",}
	fmt.Println("Dropping columns: ")
	for c:=0;c<len(drophorseindexes);c++	{
		fmt.Printf("[%d:%s] ",c,drophorseindexes[c])
	}
	xTrainDF:=trainDF.Drop(drophorseindexes)
//	xTrainDF:=trainDF.Drop([]string{"Odds1","Odds2","Odds3","Odds4","Odds5","Odds6","Odds7","Odds8","Odds9","Odds10",
//							"Odds11","Odds12","Odds13","Odds14","Odds15","Odds16","Odds17","Odds18","Odds19","Odds20",})
	x,y=xTrainDF.Dims()
	fmt.Println("\nxTrainDF size ",x,y)
	fmt.Println("Dropping unused input columns from testing set")
	fmt.Println("Dropping columns: ",drophorseindexes)
	xTestDF:=testDF.Drop(drophorseindexes)
	x,y=xTestDF.Dims()
	fmt.Println("xTestDF size ",x,y)
	fmt.Println("Selecting output columns for training")
	yTrainDF := trainDF.Select(targethorseindexes)
	x,y=yTrainDF.Dims()
	fmt.Println("yTrainDF size ",x,y)
	fmt.Println("Selecting output columns for testing")
	yTestDF := testDF.Select(targethorseindexes)
	x,y=yTestDF.Dims()
	fmt.Println("yTestDF size ",x,y)
	
	
/*	fmt.Println("Creating an input subset for training")
	trainDF:=df.Subset(trainindexes)
	fmt.Println("Dropping unused input columns from training set")
	xTrainDF:=trainDF.Drop(drophorseindexes)
	fmt.Println("Drop of unused input columns from training set complete")
	
	fmt.Println("Selecting output columns for training")
	yTrainDF := trainDF.Select(targethorseindexes)
	fmt.Println("Training output columns complete, freeing training subset")
//	trainDF=nil
	
	
	fmt.Println("Creating an input subset for testing")
	testDF:=df.Subset(testindexes)
	fmt.Println("Dropping unused input columns from testing set")
	xTestDF:=testDF.Drop(drophorseindexes)
	fmt.Println("Drop of unused input columns from testing set complete")
	
	fmt.Println("Selecting output columns for training")
	yTestDF := testDF.Select(targethorseindexes)
	fmt.Println("Testing output columns complete, freeing testing subset")
//	testDF=nil */


	numTrainRows, numTrainCols := xTrainDF.Dims()
	numTrainRowsY,numTrainColsY:= yTrainDF.Dims()
	numTestRows, numTestCols := xTestDF.Dims()
	numTestRowsY,numTestColsY:= yTestDF.Dims()
	fmt.Printf("Train input size    :(%d,%d)   Target size (%d,%d)\nTest input size     :(%d,%d)   Target size (%d,%d)\n",
				numTrainCols,numTrainRows,numTrainColsY,numTrainRowsY,
				numTestCols,numTestRows,numTestColsY,numTestRowsY)
//	fmt.Println("TrainX: ",xTrainDF)
//	fmt.Println("TrainY: ",yTrainDF)
//	fmt.Println("TestX:	",xTestDF)
//	fmt.Println("TestY: ",yTestDF)
	return &matrix{xTrainDF}, &matrix{yTrainDF},&matrix{xTestDF}, &matrix{yTestDF}
}

func save(nodes []*g.Node) error {
        f, err := os.Create(SaveFile)
        if err != nil {
                return err
        }
        defer f.Close()
        enc := gob.NewEncoder(f)
        for _, node := range nodes {
                err := enc.Encode(node.Value())
                if err != nil {
                        return err
                }
        }
        return nil
}