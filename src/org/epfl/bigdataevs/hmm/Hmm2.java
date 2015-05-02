package org.epfl.bigdataevs.hmm;

import org.apache.commons.collections.list.TreeList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class Hmm2 implements Serializable {
  private static final long serialVersionUID = 1L;
  
  private final int N;
  private final int M;

  private double[] pi;
  private double[][] a;
  private double[][] b;
  
  /**
   * Raw constructor for an HMM.
   * Only use this constructor if using raw train/decode/generate sequence functions
   * @param n Number of hidden states.
   * @param m Number of observable states.
   * @param pi Initial probability distribution
   * @param a Hidden states transition probability matrix
   * @param b Observed state probability matrix.
   */
  public Hmm2(int n, int m, double[] pi, double[][] a, double[][] b) {
    this.N = n;
    this.M = m;
    this.pi = pi;
    this.a = a;
    this.b = b;
  }

  /**
   * Perform training on a spark Rdd observation sequence.
   * @param sc Spark context to use
   * @param observedSequenceRdd Rdd containing the sequence (seq index, word index)
   * @param piThreshold Threshold on pi
   * @param aaThreshold  Threshold on a
   * @param maxIterations Max number of iterations
   */
  public void rawSparkTrain(
          JavaSparkContext sc,
          JavaRDD<Tuple2<Integer, Integer>> observedSequenceRdd,
          double piThreshold,
          double aaThreshold,
          int maxIterations,
          int[] observedSequence) {
    
    final int T = (int) observedSequenceRdd.count();
    /*
    //Test with old method to compute alphaHats and betaHats
    double prevLogLikelihood = Double.NEGATIVE_INFINITY;
    // Temporary variables used in every iteration
    double[] alphasScales = new double[ T ]; //c_t
    double[] alphasHat = new double[N * T];
    double[] betasHat = new double[N * T];
    double[] alphasBar = new double[N * T];
    double[] betas = new double[N * T];
    double[] gammas = new double[N];
    double[] gammasSums = new double[N];
    double[][] TAInitTilde = new double[T][N*N];
    double[][] TADirectTilde = new double[T][N*N];
    double[][] TBInitTilde = new double[T][N*N];
    double[][] TBDirectTilde = new double[T][N*N];
    // Iterate until convergence of the transition probabilities
    int maxSteps = 100;
    //for ( int iterationStep = 0; iterationStep < maxSteps; iterationStep++ ) {
      //System.out.println("Iteration " + iterationStep);
      //1. initialise the TA t-1->t
      double[] auxTABar0 = new double[N * N];
      for (int i = 0; i < N; i++) {
        auxTABar0[i * N + i] = pi[i] * b[i][observedSequence[0]];
      }
      double norm0 = Utils.normOne(auxTABar0);
      if(norm0==0) norm0 =1;
      alphasScales[0]=1/norm0;
      for (int i = 0; i < N; i++) {
        for (int j = 0; j < N; j++) {
          TAInitTilde[0][i * N + j] = auxTABar0[i * N + j] / norm0;
        }
      }
      for (int t = 1; t < T; t++) {
        double[] auxTABar = new double[N * N];
        for (int i = 0; i < N; i++) {
          for (int j = 0; j < N; j++) {
            auxTABar[i * N + j] = a[j][i] * b[i][observedSequence[t]];
          }
        }
        double norm = Utils.normOne(auxTABar);
        //System.out.println("norm 1 pos 1 : "+norm);
        if(norm==0) norm =1;
        for (int i = 0; i < N; i++) {
          for (int j = 0; j < N; j++) {
            TAInitTilde[t][i * N + j] = auxTABar[i * N + j] / norm;
          }
        }
      }
      System.out.println();
      //2. compute the TA 0->t
      //initialize TADirectTilde[0]
      for(int i =0;i<N;i++){
        for(int j=0;j<N;j++){
          TADirectTilde[0][i * N + j] = TAInitTilde[0][i* N + j];
        }
      }
      double norm1 = Utils.normOne(TADirectTilde[0]);
      //System.out.println("norm 1 pos 2 : "+norm1);
      for (int t = 1; t < T; t++) {
        double[] auxTATilde = new double[N * N];
        for (int i = 0; i < N; i++) {
          for (int j = 0; j < N; j++) {
            for (int h = 0; h < N; h++) {
              auxTATilde[i * N + j] += TAInitTilde[t][i* N + h]*TADirectTilde[t-1][h * N + j] ;// need to check index t (confusion t->t+1)?
            }
          }
        }
        double norm = Utils.normOne(auxTATilde);
        //System.out.println("norm 1 pos 2 : "+norm);
        if(norm==0) norm =1;
        for (int i = 0; i < N; i++) {
          for (int j = 0; j < N; j++) {
            TADirectTilde[t][i * N + j] = auxTATilde[i * N + j] / norm;
          }
        }
      }
      //3. compute alphaHat(t)
      //then use TATilde to compute each vector alpha;
      for (int t = 0; t < T; t++) {
        for (int i = 0; i < N; i++) {
          double aux = 0.0;
          for (int h = 0; h < N; h++) {
            aux += TADirectTilde[t][i * N + h];
          }
          alphasHat[t * N + i] = aux;
        }
      }
      //4. compute alphaBar(t)
      /*
 for (int t = 1; t < sequenceLength; t++) {
 for(int i = 0;i<n;i++){
 double res =0.0;
 for(int h =0;h<n;h++){
 double aux =0.0;
 for (int l = 0; l < n; l++) {
 aux += TADirectTilde[t - 1][h * n + l] * alphasHat[(0) * n + l];
 }
 res += aux * b[i][observedSequence[t]]* a[h][i];
 }
 alphasBar[t*n+i] = res;
 }
 }
       
      for (int t = 1; t < T; t++) {
        for (int i = 0; i < N; i++) {
          double res = 0.0;
          for (int h = 0; h < N; h++) {
            res += b[i][observedSequence[t]] * a[h][i] * alphasHat[(t - 1) * N + h];
          }
          alphasBar[t * N + i] = res;
        }
      }
      //5. compute c_t i.e. alphasScales
      for (int t = 1; t < T; t++) {
        double sumHat = 0.0;
        double sumBar = 0.0;
        for (int i = 0; i < N; i++) {
          sumHat += alphasHat[t * N + i];
          sumBar += alphasBar[t * N + i];
        }
        alphasScales[t] = sumHat / sumBar;
        //System.out.println("alphaScales "+alphasScales[t]);
        //System.out.println("sumBar "+t+" = "+sumBar);
      }
      /*
       * Generate all the betas coefficients
       *//*
    for (int t = 0; t < T; t++) {
      for (int i = 0; i < N; i++) {
        for (int j = 0; j < N; j++) {
          TBInitTilde[t][i * N + j] = 0.0;
          TBDirectTilde[t][i * N + j] = 0.0;
        }
      }
    }
      //1. initialise the TB t+1->t
      for (int i = 0; i < N; i++) {
        TBInitTilde[T - 1][i * N + i] = 1.0 * alphasScales[T - 1];//to be modified remove 1 *
      }
      for (int t = T - 2; t >= 0; t--) {
        for (int i = 0; i < N; i++) {
          for (int j = 0; j < N; j++) {
            TBInitTilde[t][i * N + j] = a[i][j] * b[j][observedSequence[t+1]] * alphasScales[t];
          }
        }
      }
      //2. compute the TB sL-1->t
      //initialize TADirectTilde[0]
      for (int i = 0; i < N; i++) {
        for (int j = 0; j < N; j++) {
          TBDirectTilde[T - 1][i * N + j] = TBInitTilde[T - 1][i * N + j];
        }
      }
      for (int t = T - 2; t >= 0; t--) {
        for (int i = 0; i < N; i++) {
          for (int j = 0; j < N; j++) {
            for (int h = 0; h < N; h++) {
              TBDirectTilde[t][i * N + j] += TBInitTilde[t][i * N + h] * TBDirectTilde[t + 1][h * N + j];
            }
          }
        }
      }
      //3. compute betasHat(t)
      //then use TATilde to compute each vector alpha;
      for (int t = T - 1; t >= 0; t--) {
        for (int i = 0; i < N; i++) {
          double aux = 0.0;
          for (int h = 0; h < N; h++) {
            aux += TBDirectTilde[t][i * N + h];
          }
          //System.out.println(aux);
          betasHat[t * N + i] = aux;
        }
      }

    
*/
    
    final int blockSize = 1024 * 128;
    //final int T = (int) observedSequenceRdd.count();
    
    final int numBlocks = (T + (blockSize - 1)) / blockSize;
    double piDiff = Double.POSITIVE_INFINITY;
    double aaDiff = Double.POSITIVE_INFINITY;
    
    JavaRDD<Tuple3<Integer, Integer, Integer>> observedSequenceWithBlockIdRdd =
            observedSequenceRdd.flatMap( new FlatMapFunction<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Integer>>() {
              private static final long serialVersionUID = 8L;

              @Override
              public Iterable<Tuple3<Integer, Integer, Integer>> call(Tuple2<Integer, Integer> arg0)
                      throws Exception {
                List<Tuple3<Integer, Integer, Integer>> returnList =
                        new ArrayList<Tuple3<Integer, Integer, Integer>>(2);
                
                int blockId = arg0._1 / blockSize;
                returnList.add( new Tuple3<Integer, Integer, Integer>(blockId, arg0._1, arg0._2));
                
                if ( blockId > 0 && arg0._1 % blockSize == 0 ) {
                  returnList.add( new Tuple3<Integer, Integer, Integer>(blockId - 1, arg0._1, arg0._2));
                }
                return returnList;
              }
              
            });
    
    long observedSequenceWithBlockIdSize = observedSequenceWithBlockIdRdd.count();
    if ( observedSequenceWithBlockIdSize != (T + numBlocks - 1) )
    {
      System.out.println("Flat mapped observed sequence size doesn't match: "
              + observedSequenceWithBlockIdSize );
    }
    
    JavaPairRDD<Integer, Iterable<Tuple3<Integer, Integer, Integer>>> groupedObservedBlocksRdd =
            observedSequenceWithBlockIdRdd.groupBy( new Function<Tuple3<Integer, Integer, Integer>, Integer>() {

              @Override
              public Integer call(Tuple3<Integer, Integer, Integer> arg0) throws Exception {
                return arg0._1();
              }
              
            });
    
    
    long observedBlocksSize = groupedObservedBlocksRdd.count();
    if ( observedBlocksSize != numBlocks )
    {
      System.out.println("Grouped block sequence size doesn't match: "
              + observedBlocksSize );
    }
    
    JavaPairRDD<Integer, int[]> observedBlocksRdd =
            groupedObservedBlocksRdd.mapValues(new Function<Iterable<Tuple3<Integer, Integer, Integer>>, int[]>() {
              private static final long serialVersionUID = 7L;

              @Override
              public int[] call(Iterable<Tuple3<Integer, Integer, Integer>> arg0) throws Exception {
                int blockId = arg0.iterator().next()._1();
                int blockStart = blockId * blockSize;
                int blockEnd = Math.min( (blockId + 1) * blockSize, T);
                int trueBlockSize = (blockEnd - blockStart);
                
                int realBlockSize = 0;
                for ( Tuple3<Integer, Integer, Integer> tuple : arg0 ) {
                  realBlockSize++;
                }
                
                int[] observedBlock;
                if ( blockId == (numBlocks - 1) ) {
                  observedBlock = new int[blockEnd - blockStart];
                  if ( realBlockSize != (blockEnd - blockStart) )
                  {
                    System.out.println("Block sizes don't match: real " + realBlockSize + " assumed " + trueBlockSize);
                  }
                } else {
                  observedBlock = new int[blockSize + 1];
                  if ( realBlockSize != blockSize + 1 )
                  {
                    System.out.println("Block sizes don't match: real " + realBlockSize + " assumed " + (blockSize + 1));
                  }
                }
                
                for ( Tuple3<Integer, Integer, Integer> tuple : arg0 ) {
                  observedBlock[ tuple._2() - blockStart] = tuple._3();
                }
                
                //System.out.println("Block " + blockId + " seq " + Arrays.toString(observedBlock));
                
                return observedBlock;
              }
            });
    
    // iterate until convergence
    for ( int step = 0; step < maxIterations; step++ ) {
      System.out.println("Spark iter " + step);
      
      JavaPairRDD<Integer, SquareMatrix[]> initializedTaRdd =
              observedBlocksRdd.mapToPair( new PairFunction<Tuple2<Integer, int[]>, Integer, SquareMatrix[]>() {
                private static final long serialVersionUID = 2L;

                @Override
                public Tuple2<Integer, SquareMatrix[]> call(Tuple2<Integer, int[]> arg0)
                        throws Exception {
                  int blockId = arg0._1;
                  int blockStart = blockId * blockSize;
                  int blockEnd = Math.min( (blockId + 1) * blockSize, T);
                  int trueBlockSize = (blockEnd - blockStart);
                  
                  int[] observedBlock = arg0._2;

                  //System.out.println("(TAInit) Block " + blockId + "seq " + Arrays.toString(observedBlock));
                  
                  SquareMatrix[] ta = new SquareMatrix[trueBlockSize];
                  
                  for ( int bi = 0; bi < trueBlockSize; bi++ ) {
                    int t = blockStart + bi;
                    
                    ta[bi] = new SquareMatrix(N);
                    
                    if ( t == 0 ) {
                      for ( int i = 0; i < N; i++ ) {
                        ta[bi].elements[i * N + i] = pi[i] * b[i][observedBlock[0]];
                      }
                      
                    } else {
                      for (int i = 0; i < N; i++) {
                        for (int j = 0; j < N; j++) {
                          ta[bi].elements[i * N + j] = a[j][i] * b[i][observedBlock[bi]];
                        }
                      }
                    }
                    
                    double norm = ta[bi].rawNorm1();
                    
                    if( norm <= 0.0 ) {
                      System.out.println("Block " + blockId + " matr " + bi + " norm " + norm + " mat " + ta[bi]);
                    }
                    ta[bi].scalarDivide(norm);
                  }
                  
                  return new Tuple2<Integer, SquareMatrix[]>(blockId, ta);
                }
                
              });
      
      /*{ // debug TA after initialization
        List<Tuple2<Integer, SquareMatrix[]>> initializedTa = initializedTaRdd.collect();
        for ( Tuple2<Integer, SquareMatrix[]> tuple : initializedTa ) {
          System.out.println("Block " + tuple._1 + " matrices :");
          System.out.println(Arrays.toString(tuple._2));
        }
      }
      */
      JavaPairRDD<Integer, SquareMatrix[]> partiallyScannedTaRdd =
              initializedTaRdd.mapValues(new Function<SquareMatrix[], SquareMatrix[]>() {
                private static final long serialVersionUID = 6L;

                @Override
                public SquareMatrix[] call(SquareMatrix[] arg0) throws Exception {
                  int trueBlockSize = arg0.length;
                  
                  SquareMatrix[] outResults = new SquareMatrix[trueBlockSize];
                  outResults[0] = arg0[0].publicClone();
                  
                  for ( int bi = 1; bi < trueBlockSize; bi++ ) {
                    outResults[bi] = arg0[bi].multiplyOut(outResults[bi - 1], new SquareMatrix(N));
                    outResults[bi].scalarDivide(outResults[bi].rawNorm1());
                  }
                  return outResults;
                }
                
              });
      
      JavaPairRDD<Integer, SquareMatrix> lastPartiallyScannedTaRdd =
              partiallyScannedTaRdd.mapValues(new Function<SquareMatrix[], SquareMatrix>() {

                @Override
                public SquareMatrix call(SquareMatrix[] arg0) throws Exception {
                  return arg0[ arg0.length - 1].publicClone();
                }
                
              });
      
      List<Tuple2<Integer, SquareMatrix>> lastPartiallyScannedTa = lastPartiallyScannedTaRdd.collect();
      
      // sort last partially scanned TA matrices
      Collections.sort(lastPartiallyScannedTa, new Comparator<Tuple2<Integer, SquareMatrix>>() {
        @Override
        public int compare(Tuple2<Integer, SquareMatrix> index1,
                Tuple2<Integer, SquareMatrix> index2) {
          return index1._1.compareTo(index2._1);
        }
      });
      
      if ( lastPartiallyScannedTa.size() != numBlocks ) {
        System.out.println("Last TA size doesn't match: " + lastPartiallyScannedTa.size());
      }
      
      List<SquareMatrix> fullyScannedTa = new ArrayList<SquareMatrix>(numBlocks);
      fullyScannedTa.add(lastPartiallyScannedTa.get(0)._2);
      
      // reduce on the master the last TAs
      for ( int i = 1; i < numBlocks; i++ ) {
        SquareMatrix left = fullyScannedTa.get(i - 1);
        SquareMatrix right = lastPartiallyScannedTa.get(i)._2;
        
        SquareMatrix out = right.multiplyOut(left, new SquareMatrix(N));
        
        double norm = out.rawNorm1();
        
        if ( norm <= 0.0 )
        {
          System.out.println("Norm of fully scanned TA " + i + " = " + norm);
        }
        out.scalarDivide(norm);
        fullyScannedTa.add(out);
      }
      
      class FinalTaMapper implements PairFunction<Tuple2<Integer, SquareMatrix[]>, Integer, SquareMatrix[]>{
        private static final long serialVersionUID = 5L;
        
        List<SquareMatrix> fullyScannedLastTa;
        
        public FinalTaMapper(List<SquareMatrix> fullyScannedLastTa ) {
          this.fullyScannedLastTa = fullyScannedLastTa;
        }
        
        @Override
        public Tuple2<Integer, SquareMatrix[]> call(Tuple2<Integer, SquareMatrix[]> arg0) throws Exception {
          int blockId = arg0._1;
          SquareMatrix[] inTAs = arg0._2;
          
          SquareMatrix[] outResults = new SquareMatrix[inTAs.length];
          
          if ( blockId > 0 ) {
            SquareMatrix prevBlockMatrix = fullyScannedLastTa.get(blockId - 1);
            
            for ( int bi = 0; bi < inTAs.length; bi++ ) {
              outResults[bi] = inTAs[bi].multiplyOut(prevBlockMatrix, new SquareMatrix(N));
              
              double norm = outResults[bi].rawNorm1();
              if ( norm <= 0.0 ) {
                System.out.println("(FinalTAMapper) Block " + blockId + " mat " + bi + " norm " + norm);
              }
              
              outResults[bi].scalarDivide(norm);
            }
          } else {
            for ( int bi = 0; bi < inTAs.length; bi++ ) {
              outResults[bi] = inTAs[bi].publicClone();
            }
          }
          return new Tuple2<Integer, SquareMatrix[]>(blockId, outResults);
        }
        
      }
      
      JavaPairRDD<Integer, SquareMatrix[]> finalTaRdd =
              partiallyScannedTaRdd.mapToPair( new FinalTaMapper(fullyScannedTa));
      
      JavaPairRDD<Integer, double[]> alphaHatsRdd = finalTaRdd.mapValues(new Function<SquareMatrix[], double[]>(){
        private static final long serialVersionUID = 3L;

        @Override
        public double[] call(SquareMatrix[] arg0) throws Exception {
          int trueBlockSize = arg0.length;
          double[] alphas = new double[trueBlockSize * N];
          
          for ( int bi = 0; bi < trueBlockSize; bi++ ) {
            SquareMatrix curMatrix = arg0[bi];
            double totSum = 0.0;
            for (int i = 0; i < N; i++ ) {
              double sum = 0.0;
              for ( int j = 0; j < N; j++ ) {
                sum += curMatrix.elements[ i * N + j ];
              }
              alphas[ bi * N + i ] = sum;
              totSum += sum;
            }
            
            if ( totSum <= 0.0 ) {
              System.out.println("Alpha " + bi + " is null");
            }
          }
          
          return alphas;
        }
        
      });
      /*
      System.out.println("alphas sequential "+Arrays.toString(alphasHat));
      List<Tuple2<Integer,double[]>> alphasSpark = alphaHatsRdd.collect();
      for(Tuple2<Integer,double[]> tuple : alphasSpark){
        int blockId = tuple._1;
        int blockStart = blockId * blockSize;
        int blockEnd = Math.min( (blockId + 1) * blockSize, T);
        int trueBlockSize = (blockEnd - blockStart);
        System.out.println("alphas sequential    "+Arrays.toString(Arrays.copyOfRange(alphasHat, blockStart * N, blockEnd * N)));
        System.out.println("alphasSpark blockId "+tuple._1+", "+Arrays.toString(tuple._2));
      }
      */
      
      JavaPairRDD<Integer, double[]> lastAlphaHatsRdd = alphaHatsRdd.mapValues( new Function<double[], double[]>(){
        private static final long serialVersionUID = 4L;

        @Override
        public double[] call(double[] arg0) throws Exception {
          int trueBlockSize = arg0.length / N;
          double[] lastAlpha = new double[N];
          
          for ( int i = 0; i < N; i++ ) {
            lastAlpha[i] = arg0[ (trueBlockSize - 1) * N + i];
          }
          return lastAlpha;
        }
        
      });
      
      List<Tuple2<Integer, double[]>> lastAlphaHats = lastAlphaHatsRdd.collect();
      // sort last alpha hats
      Collections.sort(lastAlphaHats, new Comparator<Tuple2<Integer, double[]>>() {
        @Override
        public int compare(Tuple2<Integer, double[]> index1,
                Tuple2<Integer, double[]> index2) {
          return index1._1.compareTo(index2._1);
        }
      });
      
      if ( lastAlphaHats.size() != numBlocks ) {
        System.out.println("Last AlphaHats size doesn't match: " + lastAlphaHats.size());
      }
      
      JavaPairRDD<Integer, Tuple2<int[], double[]>> observationsWithAlphaHats = observedBlocksRdd.join(alphaHatsRdd);
      
      class CtMapper implements PairFunction<Tuple2<Integer, Tuple2<int[], double[]>>, Integer, double[]>{
        private static final long serialVersionUID = 9L;
        
        List<Tuple2<Integer, double[]>> lastAlphaHats;
        
        public CtMapper(List<Tuple2<Integer, double[]>> lastAlphaHats ) {
          this.lastAlphaHats = lastAlphaHats;
        }
        
        @Override
        public Tuple2<Integer, double[]> call(Tuple2<Integer, Tuple2<int[], double[]>> arg0) throws Exception {
          int blockId = arg0._1;
          int blockStart = blockId * blockSize;
          int blockEnd = Math.min( (blockId + 1) * blockSize, T);
          int trueBlockSize = (blockEnd - blockStart);
          
          Tuple2<int[], double[]> tuple = arg0._2;
          
          int[] observedBlock = tuple._1;
          double[] alphaHats = tuple._2;
          
          double[] cts = new double[trueBlockSize];
          
          { // bi = 0
            double den = 0.0;
            if ( blockStart == 0 ) {
              for (int i = 0; i < N; i++) {
                den += pi[i] * b[i][observedBlock[0]];
              }
            } else {
              double[] prevAlpha = lastAlphaHats.get(blockId - 1)._2;
              for ( int i = 0; i < N; i++ ) {
                double sum = 0.0;
                for ( int j = 0; j < N; j++ ) {
                  sum += a[j][i] * b[i][observedBlock[0]] * prevAlpha[j];
                }
                den += sum;
              }
            }
            
            if ( den <= 0.0 ) {
              System.out.println("den[0]=0.0");
            }
            cts[0] = 1.0 / den;
          }
          
          { // bi > 0
            for ( int bi = 1; bi < trueBlockSize; bi++ ) {
              double den = 0.0;
              for ( int i = 0; i < N; i++ ) {
                double sum = 0.0;
                for ( int j = 0; j < N; j++ ) {
                  sum += a[j][i] * b[i][observedBlock[bi]] * alphaHats[(bi - 1) * N + j];
                }
                den += sum;
              }
              
              if ( den <= 0.0 ) {
                System.out.println("den[" + bi + "]=0.0");
              }
              cts[bi] = 1.0 / den;
            }
          }
          return new Tuple2<Integer, double[]>(blockId, cts);
        }
        
      }
      
      JavaPairRDD<Integer, double[]> ctsRdd = observationsWithAlphaHats.mapToPair( new CtMapper(lastAlphaHats));
      
      
      /*
      List<Tuple2<Integer,double[]>> ctsSpark = ctsRdd.collect();
      for(Tuple2<Integer,double[]> tuple : ctsSpark){
        int blockId = tuple._1;
        int blockStart = blockId * blockSize;
        int blockEnd = Math.min( (blockId + 1) * blockSize, T);
        int trueBlockSize = (blockEnd - blockStart);
        System.out.println("cts sequential    "+Arrays.toString(Arrays.copyOfRange(alphasScales, blockStart, blockEnd)));
        System.out.println("ctsSpark blockId "+tuple._1+", "+Arrays.toString(tuple._2));
      }
      */
      
      
      
      
      
      
      
      JavaPairRDD<Integer, Tuple2<int[], double[]>> observedBlocksWithCtsRdd = observedBlocksRdd.join(ctsRdd);
      
      JavaPairRDD<Integer, SquareMatrix[]> initializedTbRdd =
              observedBlocksWithCtsRdd.mapToPair( new PairFunction<Tuple2<Integer, Tuple2<int[],double[]>>, Integer,SquareMatrix[]>(){
                private static final long serialVersionUID = 10L;

                @Override
                public Tuple2<Integer, SquareMatrix[]> call(
                        Tuple2<Integer, Tuple2<int[], double[]>> arg0) throws Exception {
                  int blockId = arg0._1;
                  int blockStart = blockId * blockSize;
                  int blockEnd = Math.min( (blockId + 1) * blockSize, T);
                  int trueBlockSize = (blockEnd - blockStart);
                  
                  Tuple2<int[], double[]> tuple = arg0._2;
                  int[] observedBlock = tuple._1;
                  double[] cts = tuple._2;
                  
                  SquareMatrix[] tb = new SquareMatrix[trueBlockSize];
                  
                  for ( int bi = trueBlockSize - 1; bi >= 0; bi-- ) {
                    int t = blockStart + bi;
                    
                    tb[bi] = new SquareMatrix(N);
                    if ( t == T - 1 ) {
                      for ( int i = 0; i < N; i++ ) {
                        tb[bi].elements[ i * N + i ] = cts[bi];
                      }
                    } else {
                      for ( int i = 0; i < N; i++ ) {
                        for ( int j = 0; j < N; j++ ) {
                          tb[bi].elements[ i * N + j ] = a[i][j] * b[j][observedBlock[bi + 1]] * cts[bi];
                        }
                      }
                    }
                    
                    double norm = tb[bi].rawNorm1();
                    if ( norm <= 0.0 ) {
                      System.out.println("(TBInit) Block " + blockId + " mat " + bi + " norm " + norm);
                    }
                  }
                  return new Tuple2<Integer, SquareMatrix[]>(blockId, tb);
                }
              });
      
      JavaPairRDD<Integer, SquareMatrix[]> partiallyScannedTbRdd =
              initializedTbRdd.mapValues(new Function<SquareMatrix[], SquareMatrix[]>() {
                private static final long serialVersionUID = 11L;

                @Override
                public SquareMatrix[] call(SquareMatrix[] arg0) throws Exception {
                  int trueBlockSize = arg0.length;
                  
                  SquareMatrix[] outResults = new SquareMatrix[trueBlockSize];
                  outResults[trueBlockSize - 1] = arg0[trueBlockSize - 1].publicClone();
                  
                  for ( int bi = trueBlockSize - 2; bi >= 0; bi-- ) {
                    outResults[bi] = arg0[bi].multiplyOut(outResults[bi + 1], new SquareMatrix(N));
                  }
                  return outResults;
                }
                
              });
      
      JavaPairRDD<Integer, SquareMatrix> firstPartiallyScannedTbRdd =
              partiallyScannedTbRdd.mapValues(new Function<SquareMatrix[], SquareMatrix>() {
                private static final long serialVersionUID = 13L;

                @Override
                public SquareMatrix call(SquareMatrix[] arg0) throws Exception {
                  return arg0[0].publicClone();
                }
                
              });
      
      List<Tuple2<Integer, SquareMatrix>> firstPartiallyScannedTb = firstPartiallyScannedTbRdd.collect();
      
      // sort first partially scanned TB matrices
      Collections.sort(firstPartiallyScannedTb, new Comparator<Tuple2<Integer, SquareMatrix>>() {
        @Override
        public int compare(Tuple2<Integer, SquareMatrix> index1,
                Tuple2<Integer, SquareMatrix> index2) {
          return index1._1.compareTo(index2._1);
        }
      });
      
      if ( firstPartiallyScannedTb.size() != numBlocks ) {
        System.out.println("First TB size doesn't match: " + firstPartiallyScannedTb.size());
      }
      
      List<SquareMatrix> fullyScannedTb = new ArrayList<SquareMatrix>(numBlocks);
      fullyScannedTb.add(firstPartiallyScannedTb.get(numBlocks - 1)._2);
      
      // reduce on the master the first TBs
      for ( int i = numBlocks - 2; i >= 0; i-- ) {
        SquareMatrix left = firstPartiallyScannedTb.get(i)._2;
        SquareMatrix right = fullyScannedTb.get(0);
        
        SquareMatrix out = left.multiplyOut(right, new SquareMatrix(N));
        
        double norm = out.rawNorm1();
        if ( norm <= 0.0 )
        {
          System.out.println("Norm of fully scanned TB " + i + " = " + norm);
        }
        fullyScannedTb.add(0, out);
      }
      
      class FinalTbMapper implements PairFunction<Tuple2<Integer, SquareMatrix[]>, Integer, SquareMatrix[]>{
        private static final long serialVersionUID = 12L;
        
        List<SquareMatrix> fullyScannedFirstTb;
        
        public FinalTbMapper(List<SquareMatrix> fullyScannedFirstTb ) {
          this.fullyScannedFirstTb = fullyScannedFirstTb;
        }
        
        @Override
        public Tuple2<Integer, SquareMatrix[]> call(Tuple2<Integer, SquareMatrix[]> arg0) throws Exception {
          int blockId = arg0._1;
          SquareMatrix[] inTBs = arg0._2;
          
          SquareMatrix[] outResults = new SquareMatrix[inTBs.length];
          
          if ( blockId < numBlocks - 1 ) {
            SquareMatrix nextBlockMatrix = fullyScannedFirstTb.get(blockId + 1);
            
            for ( int bi = 0; bi < inTBs.length; bi++ ) {
              outResults[bi] = inTBs[bi].multiplyOut(nextBlockMatrix, new SquareMatrix(N));
              
              double norm = outResults[bi].rawNorm1();
              if ( norm <= 0.0 ) {
                System.out.println("(FinalTBMapper) Block " + blockId + " mat " + bi + " norm " + norm);
              }
            }
          } else {
            for ( int bi = 0; bi < inTBs.length; bi++ ) {
              outResults[bi] = inTBs[bi].publicClone();
            }
          }
          return new Tuple2<Integer, SquareMatrix[]>(blockId, outResults);
        }
        
      }
      
      JavaPairRDD<Integer, SquareMatrix[]> finalTbRdd =
              partiallyScannedTbRdd.mapToPair( new FinalTbMapper(fullyScannedTb));
      
      JavaPairRDD<Integer, double[]> betaHatsRdd = finalTbRdd.mapValues(new Function<SquareMatrix[], double[]>(){
        private static final long serialVersionUID = 20L;

        @Override
        public double[] call(SquareMatrix[] arg0) throws Exception {
          int trueBlockSize = arg0.length;
          double[] betas = new double[trueBlockSize * N];
          
          for ( int bi = 0; bi < trueBlockSize; bi++ ) {
            SquareMatrix curMatrix = arg0[bi];
            double totSum = 0.0;
            for (int i = 0; i < N; i++ ) {
              double sum = 0.0;
              for ( int j = 0; j < N; j++ ) {
                sum += curMatrix.elements[ i * N + j ];
              }
              betas[ bi * N + i ] = sum;
              totSum += sum;
            }
            
            if ( totSum <= 0.0 ) {
              System.out.println("Beta " + bi + " is null");
            }
          }
          
          return betas;
        }
        
      });
      
      /*
      List<Tuple2<Integer,double[]>> betasSpark = betaHatsRdd.collect();
      for(Tuple2<Integer,double[]> tuple : betasSpark){
        int blockId = tuple._1;
        int blockStart = blockId * blockSize;
        int blockEnd = Math.min( (blockId + 1) * blockSize, T);
        int trueBlockSize = (blockEnd - blockStart);
        System.out.println("betas sequential    "+Arrays.toString(Arrays.copyOfRange(betasHat, blockStart * N, blockEnd * N)));
        System.out.println("betasSpark blockId "+tuple._1+", "+Arrays.toString(tuple._2));
      }
      */
      
      
      
      
      JavaPairRDD<Integer, double[]> firstBetaHatsRdd = betaHatsRdd.mapValues(new Function<double[], double[]>(){

        @Override
        public double[] call(double[] arg0) throws Exception {
          double[] firstBeta = new double[N];
          
          for ( int i = 0; i < N; i++ ) {
            firstBeta[i] = arg0[i];
          }
          return firstBeta;
        }
        
      });
      
      List<Tuple2<Integer, double[]>> firstBetaHats = firstBetaHatsRdd.collect();
      // sort first beta hats
      Collections.sort(firstBetaHats, new Comparator<Tuple2<Integer, double[]>>() {
        @Override
        public int compare(Tuple2<Integer, double[]> index1,
                Tuple2<Integer, double[]> index2) {
          return index1._1.compareTo(index2._1);
        }
      });
      
      
      
      /*
      for(Tuple2<Integer,double[]> tuple : firstBetaHats){
        int blockId = tuple._1;
        int blockStart = blockId * blockSize;
        int blockEnd = Math.min( (blockId + 1) * blockSize, T);
        int trueBlockSize = (blockEnd - blockStart);
        System.out.println("first betas sequential     "+Arrays.toString(Arrays.copyOfRange(betasHat, blockStart * N, (blockStart+1)*N)));
        System.out.println("first betasSpark blockId "+tuple._1+", "+Arrays.toString(tuple._2));
      }
      */
      
      
      
      
      
      
      
      
      
      if ( firstBetaHats.size() != numBlocks ) {
        System.out.println("First BetaHats size doesn't match: " + firstBetaHats.size());
      }
      
      JavaPairRDD<Integer, Tuple2<Tuple2<int[], double[]>, double[]>> observationsWithAlphaHatsWithBetaHats =
              observationsWithAlphaHats.join(betaHatsRdd);
      
      class BlockSumKhiMapper implements PairFunction<Tuple2<Integer, Tuple2<Tuple2<int[], double[]>, double[]>>, Integer, double[]> {
        private static final long serialVersionUID = 21L;
        
        List<Tuple2<Integer, double[]>> firstBetaHats;
        
        public BlockSumKhiMapper( List<Tuple2<Integer, double[]>> firstBetaHats ) {
          this.firstBetaHats = firstBetaHats;
        }
        
        @Override
        public Tuple2<Integer, double[]> call(
                Tuple2<Integer, Tuple2<Tuple2<int[], double[]>, double[]>> arg0)
                throws Exception {
          int blockId = arg0._1;
          int blockStart = blockId * blockSize;
          int blockEnd = Math.min( (blockId + 1) * blockSize, T);
          int trueBlockSize = (blockEnd - blockStart);
          
          Tuple2<Tuple2<int[], double[]>, double[]> tuple0 = arg0._2;
          Tuple2<int[], double[]> tuple1 = tuple0._1;
          
          int[] observedBlock = tuple1._1;
          double[] alphas = tuple1._2;
          double[] betas = tuple0._2;
          
          //System.out.println("Inside KhisMapper blockId "+blockId+" alphas :"+Arrays.toString(alphas));
          //System.out.println("Inside KhisMapper blockId "+blockId+" betas :"+Arrays.toString(betas));
          //System.out.println("Inside KhisMapper blockId "+blockId+" seq :"+Arrays.toString(observedBlock));
          //System.out.println("Inside KhisMapper blockId "+blockId+" a[0] :"+Arrays.toString(a[0]));
          
          double[] khis = new double[N * N];

          //double lval = ((alphas[0] * a[0][0] )* (betas[N] * b[0][observedBlock[1]]));
          //System.out.println("khi0 :" + lval + "( alpha " + alphas[0] + ", a " + a[0][0] + ", beta " + betas[N] + ", b " + b[0][observedBlock[1]]);
          
          // process all but the last element of the block
          for ( int bi = 0; bi < trueBlockSize - 1; bi++ ) {
            for ( int i = 0; i < N; i++ ) {
              for ( int j = 0; j < N; j++ ) {
                double val = ((alphas[bi * N + i ] * a[i][j] )* (betas[ (bi + 1) * N + j ] * b[j][observedBlock[bi + 1]]));
                khis[ i * N + j ] += val;
                //System.out.println("val when blockId "+blockId+", bi "+bi+", i "+i+", j "+j+" : "+val);
              }
            }
          }
          
          // last element must get beta from firstBetaHats
          // we must not append the last value from the last block
          if ( blockId < numBlocks - 1 ) {
            double[] nextBeta = firstBetaHats.get( blockId + 1 )._2;
            int bi = trueBlockSize - 1;
            for ( int i = 0; i < N; i++ ) {
              for ( int j = 0; j < N; j++ ) {
                khis[ i * N + j ] += (alphas[bi * N + i ] * a[i][j] * nextBeta[j] * b[j][observedBlock[bi + 1]]);
              }
            }
          }
          
          return new Tuple2<Integer, double[]>( blockId, khis );
        } 
      }
      
      JavaPairRDD<Integer, double[]> blockKhisRdd =
              observationsWithAlphaHatsWithBetaHats.mapToPair(new BlockSumKhiMapper(firstBetaHats) );
      
      List<Tuple2<Integer, double[]>> blockKhis = blockKhisRdd.collect();
      
      if ( blockKhis.size() != numBlocks ) {
        System.out.println("Not enough khis: " + blockKhis.size());
      }
      /*
      { // print khis
        for ( Tuple2<Integer, double[]> tuple : blockKhis ) {
          System.out.println("Khi " + tuple._1 + " " + Arrays.toString(tuple._2));
        }
      }
      */
      /*
      List<Tuple2<Integer, double[]>> khisSpark = blockKhisRdd.collect();
      for (Tuple2<Integer, double[]> tuple : khisSpark) {
        int blockId = tuple._1;
        int blockStart = blockId * blockSize;
        int blockEnd = Math.min((blockId + 1) * blockSize, T);
        int trueBlockSize = (blockEnd - blockStart);
        double[] khis = new double[N * N];
        for (int t = blockStart; t < blockEnd; t++) {
          if (t != T - 1) {
            for (int i = 0; i < N; i++) {
              for (int j = 0; j < N; j++) {
                double val = (alphasHat[t * N + i] * a[i][j] * betasHat[(t + 1) * N + j])
                        * b[j][observedSequence[t + 1]];
                khis[i * N + j] += val;
                //System.out.println("val sequential when blockId "+blockId+", bi "+(t-blockStart)+", i "+i+", j "+j+" : "+val);
              }
            }
          }
        }
        System.out.println("khis sequential     "
                + Arrays.toString(khis));
        System.out.println("khisSpark blockId " + tuple._1 + ", " + Arrays.toString(tuple._2));
      }
      */
      
      
      
      
      
      
      
      
      
      
      
      
      JavaRDD<double[]> piRdd = observationsWithAlphaHatsWithBetaHats.flatMap(new FlatMapFunction<Tuple2<Integer,Tuple2<Tuple2<int[],double[]>,double[]>>,double[]>(){
        private static final long serialVersionUID = 42L;

        @Override
        public Iterable<double[]> call(
                Tuple2<Integer, Tuple2<Tuple2<int[], double[]>, double[]>> arg0) throws Exception {
          int blockId = arg0._1;
          
          if ( blockId == 0 ) {
            Tuple2<Tuple2<int[], double[]>, double[]> tuple0 = arg0._2;
            Tuple2<int[], double[]> tuple1 = tuple0._1;
            
            double[] alphas = tuple1._2;
            double[] betas = tuple0._2;
            
            double[] pi = new double[N];
            
            double sum = 0.0;
            for ( int i = 0; i < N; i++ ) {
              double val = alphas[i] * betas[i];
              sum += val;
              pi[i] = val;
            }
            
            if ( sum <= 0.0 ) {
              System.out.println("Pi is zero");
            }
            
            for (int i = 0; i < N; i++ ) {
              pi[i] /= sum;
            }
            
            List<double[]> piList = new ArrayList<double[]>(1);
            piList.add(pi);
            
            return piList;
          } else {
            return new ArrayList<double[]>();
          }
        }
        
      });
      
      List<double[]> piList = piRdd.collect();
      
      if ( piList.size() != 1 ) {
        System.out.println("Pi size doesn't match " + piList.size());
      }
      
      
      // temporary variables
      double[][] aaStar = new double[N][N];
      
      // copy new a, renormalize it
      for ( int i = 0; i < N; i++ ) {
        double jsum = 0.0;
        for ( int j = 0; j < N; j++ ) {
          for ( int bi = 0; bi < numBlocks; bi++ ) {
            double[] khis = blockKhis.get(bi)._2;
            aaStar[i][j] += khis[ i * N + j ];
            jsum += khis[ i * N + j ];
          }
        }
        
        for ( int j = 0; j < N; j++ ) {
          aaStar[i][j] /= jsum;
        }
      }
      
      // copy piStar
      double[] piStar = piList.get(0);
      
      // check convergence
      piDiff = 0.0;
      for ( int i = 0; i < N; i++ ) {
        piDiff += Math.abs( pi[i] - piStar[i] );
      }
      
      aaDiff = 0.0;
      for (int i = 0; i < N; i++ ) {
        for (int j = 0; j < N; j++ ) {
          aaDiff += Math.abs( aaStar[i][j] - a[i][j] );
        }
      }
      
      // commit changes
      a = aaStar;
      pi = piStar;
      //break;
      //*
      if ( piDiff < piThreshold && aaDiff < aaThreshold ) {
        break;
      }
      //*/
    }
  }
  
  public int getN() {
    return N;
  }
  
  public int getM() {
    return M;
  }
  
  public double[] getPi() {
    return pi;
  }
  
  public double[][] getA() {
    return a;
  }
  
  public double[][] getB() {
    return b;
  }
}
