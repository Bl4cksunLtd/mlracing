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
	"encoding/gob"

)

const 	(
	VERMAJ		=	0
	VERMIN		=	1
	VERPATCH	=	1
)
var 	(
	FileName 	string
	SaveFile	string
	D1,D2,D3 	string
	MaxIter 	int
	TargetNum	int
	NumTargets	int
	Target 		string
	repeat 		bool
	Predict 	string
	LearnRate 	float64
	AC 			float64 // abs(predicted-measured)<=AC 
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
	fmt.Printf("Learn v%d.%d.%d\n",VERMAJ,VERMIN,VERPATCH)
	fn:=flag.String("fn","input.csv","Filename of the dataset")
	sT:=flag.String("t1","Target","Target column name")
	iNT:=flag.Int("nt",2,"Number of targets in csv file default 2 (must be last columns)")
	iI:=flag.Int("it",10000,"Max interations (default 10000)")
	fLR:=flag.Float64("lr",0.001,"Learn Rate (default 0.001)")
	sP:=flag.String("p","","Filename to save predictions to, default do not save")
	fAccuracy:=flag.Float64("ac",0.5,"Accuracy")
	fLP:=flag.Float64("lp",0.8,"Percentage dataset to use for learning")
	sS:=flag.String("sf","model.gob","Save the model to filename")
	sD1:=flag.String("d1","Target","Exclude column from the input data")
	sD2:=flag.String("d2","","Exclude column from the input data (default is to ignore this setting)")
	sD3:=flag.String("d3","","Exclude column from the input data (default is to ignore this setting)")
	flag.Parse()
	FileName=*fn
	MaxIter=*iI
	Target=*sT
	D1=*sD1
	D2=*sD2
	D3=*sD3
	NumTargets=*iNT
	LearnRate=*fLR
	AC=*fAccuracy
	Predict=*sP
	SaveFile=*sS
//	LearnPercent:=*fLP
	prestr:=""
	if Predict!="" {
		prestr=fmt.Sprintf(", save predictions to file %s",Predict)
	}
	fmt.Printf("Checking %s ignoring, targetting %s, file contains %d targets\n",FileName,Target,NumTargets)
	fmt.Printf("Ignoring as inputs: [%s %s %s] Test/Train %.2f\n",D1,D2,D3,*fLP)
	fmt.Printf("%d Iterations %f Learning Rate %f Accuracy %s\n",MaxIter,LearnRate,AC,prestr)
	// load the train set
//	x, y, err := mnist.Load("train", "./testdata", g.Float32)
//	require.NoError(err)

	xtrain, ytrain,xtest,ytest := getXYMat(*fn,*fLP)
	x := tensor.FromMat64(mat.DenseCopyOf(xtrain),tensor.As(g.Float32))
	y := tensor.FromMat64(mat.DenseCopyOf(ytrain),tensor.As(g.Float32))
	log.Infov("x example shape", x.Shape())
	log.Infov("x Dtype: ",x.Dtype())
	log.Infov("y example shape", y.Shape())
	log.Infov("y Dtype: ",y.Dtype())


	exampleSize := x.Shape()[0]
	log.Infov("exampleSize", exampleSize)

	// load our test set
//	testX, testY, err := mnist.Load("test", "./testdata", g.Float32)
//	require.NoError(err)
	testX:=tensor.FromMat64(mat.DenseCopyOf(xtest),tensor.As(g.Float32))
	testY:= tensor.FromMat64(mat.DenseCopyOf(ytest),tensor.As(g.Float32))

	log.Infov("testX example shape", x.Shape())
	log.Infov("testX Dtype: ",testX.Dtype())
	log.Infov("y example shape", y.Shape())
	log.Infov("testY Dtype: ",testY.Dtype())

	batchSize := 100
	log.Infov("batchsize", batchSize)

	batches := exampleSize / batchSize
	log.Infov("num batches", batches)

	numcols:=x.Shape()[1]
	xi := m.NewInput("x", []int{1,x.Shape()[1]},m.AsType(tensor.Float32))
	log.Infov("xi input shape", xi.Shape())
	
	yi := m.NewInput("y", []int{1,1},m.AsType(tensor.Float32))
	log.Infov("yi input shape", yi.Shape())
	
	model, err := m.NewSequential("sp")
	require.NoError(err)


	model.AddLayers(
		layer.FC{Input: numcols, Output: numcols*2 ,Name: "L0", NoBias: true},
//		layer.Dropout{},
		layer.FC{Input: numcols*2, Output: 20 ,Name: "L1", NoBias: true},
		layer.FC{Input: 20, Output: 1 ,Name: "O0", NoBias: true, Activation:layer.NewLinear()},
		
	)
	
	
	optimizer := g.NewRMSPropSolver(g.WithBatchSize(float64(batchSize)),g.WithLearnRate(LearnRate),g.WithL1Reg(0.2))
		
	err = model.Compile(xi, yi,
		m.WithOptimizer(optimizer),
		m.WithLoss(m.MSE),
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
//			yi.Reshape(batchSize, 10)
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
	f, err := os.Open(filename)
	if err != nil {
		log.Fatal("Failed to open input file: ",err)
	}
	defer f.Close()
	fmt.Printf("Reading input file %s...",filename)
	df := dataframe.ReadCSV(f)
	numRows, _ := df.Dims()
	fmt.Printf("Read %d rows... generating train and test datasets...\n",numRows)
	rand.Seed(time.Now().UnixNano())
	for r:=0;r<numRows;r++	{
		if rand.Float64()<=trainpercent	{
			trainindexes=append(trainindexes,r)
		}	else 	{
			testindexes=append(testindexes,r)
		}
	}
	
	trainDF:=df.Subset(trainindexes)
	testDF:=df.Subset(testindexes)
	xTrainDF:=trainDF.Drop(D1)
	xTestDF:=testDF.Drop(D1)
	if D2!=""	{
		xTrainDF=xTrainDF.Drop(D2)
		xTestDF=xTestDF.Drop(D2)
	}
	if D3!="" 	{
		xTrainDF=xTrainDF.Drop(D3)
		xTestDF=xTestDF.Drop(D3)
	}
//	yDF := df.Select(Target).Capply(toValue)
	yTrainDF := trainDF.Select(Target)
	yTestDF := testDF.Select(Target)
	numTrainRows, numTrainCols := xTrainDF.Dims()
	numTrainRowsY,numTrainColsY:= yTrainDF.Dims()
	numTestRows, numTestCols := xTestDF.Dims()
	numTestRowsY,numTestColsY:= yTestDF.Dims()
	fmt.Printf("Train input size    :(%d,%d)   Target size (%d,%d)\nTest input size     :(%d,%d)   Target size (%d,%d)\n",
				numTrainCols,numTrainRows,numTrainColsY,numTrainRowsY,
				numTestCols,numTestRows,numTestColsY,numTestRowsY)
	
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