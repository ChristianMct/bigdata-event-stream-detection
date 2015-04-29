package org.epfl.bigdataevs.hmm;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.commons.collections.list.TreeList;
import org.apache.commons.math3.fraction.Fraction;

import scala.Array;
import scala.Tuple2;
import scala.Tuple3;

/**
 * A Hidden Markov Model (HMM) built to perform analysis of theme life cycles (paper section 4). An
 * HMM can be trained using train() or used to decode a text stream using decode() ; all the other
 * steps of theme life cycles analysis should not be done in this class.
 * 
 * @author team Damien-Laurent-Sami
 *
 */
public class Hmm implements Serializable{

  private final int k;
  private final int n;
  private final int m;

  private TreeList outputAlphabet;

  private double[] pi;
  private double[][] a;
  private double[][] b;

  /**
   * The basic hmm constructor.
   * 
   * 
   * @param k
   *          the number of transcollection-themes considered, the HMM will have k+1 states
   * @param wordsLexicon
   *          a rdd containing all the words in the vocabulary set (only once)
   * @param models
   *          the k+1 models : background model (index 0) and the k trans-collection theme
   *          probabilities
   */
  public Hmm(int k, JavaRDD<String> wordsLexicon,
          JavaPairRDD<Integer, JavaPairRDD<String, Double>> models) {
    this.k = k;
    n = k + 1;
    outputAlphabet = new TreeList(wordsLexicon.collect());

    m = outputAlphabet.size();
    pi = new double[n];
    a = new double[n][n];
    b = new double[n][m];
    // TODO initialize b using the models provided as arguments
    for (Entry<Integer, JavaPairRDD<String, Double>> entry : models.collectAsMap().entrySet()) {
      int i = entry.getKey();
      for (Entry<String, Double> pair : entry.getValue().collectAsMap().entrySet()) {
        b[i][outputAlphabet.indexOf(pair.getKey())] = pair.getValue();
      }
    }

  }

  /**
   * Dummy constructor for test purposes.
   * 
   */
  public Hmm(List<String> outputAlphabet, double[] pi, double[][] a, double[][] b) {
    n = a.length;
    k = n - 1;
    m = outputAlphabet.size();
    this.outputAlphabet = new TreeList(outputAlphabet);
    this.pi = pi;
    this.a = a;
    this.b = b;

  }
  
  /**
   * Raw constructor for an HMM.
   * Only use this constructor if using raw train/decode/generate sequence functions
   * @param n Number of hidden states.
   * @param m Number of observable states.
   * @param pi Initial probability distribution
   * @param a Hidden states transition probability matrix
   * @param b Observed state probability matrix.
   */
  public Hmm(int n, int m, double[] pi, double[][] a, double[][] b) {
    this.n = n;
    this.k = n - 1;
    this.m = m;
    this.pi = pi;
    this.a = a;
    this.b = b;
  }

  /**
   * This method trains the HMM by performing the Baum-Welch algorithm.
   * 
   * @param fullArticleStream
   *          the full text of the concatenated articles
   */
  public void train(JavaRDD<String> fullArticleStream) {
    /*
     * First convert the fullAtircleStream into a
     * an array of indices as used in the observation probability
     * matrix
     */
    List<String> wordSequence = fullArticleStream.collect();
    int sequenceLength = wordSequence.size();
    
    int[] observedSequence = new int[sequenceLength];
    
    for ( int wordIndex = 0; wordIndex < sequenceLength; wordIndex++ ) {
      observedSequence[wordIndex] = outputAlphabet.indexOf(wordSequence.get(wordIndex));
    }
    
    // And then do the training on this raw sequence.
    rawTrain(observedSequence, sequenceLength);
  }
  
  /**
   * This method trains the HMM by performing the Baum-Welch algorithm.
   * 
   * @param observedSequence
   *          the list of observed output states indexes.
   */
  public void rawTrain(int[] observedSequence, int sequenceLength) {

    // Variables in which we store the next iteration results
    double[] piStar = new double[n];
    double[][] aaStar = new double[n][n];
    
    // variable tracking convergence
    double prevLogLikelihood = Double.NEGATIVE_INFINITY;
    // threshold for convergence
    // TODO propose this parameter as an argument
    double likelihoodThreshold = 1.0;
    
    // Temporary variables used in every iteration
    double[] alphasScales = new double[ sequenceLength ];
    double[] alphas = new double[n * sequenceLength];
    double[] betas = new double[n * sequenceLength];
    double[] gammas = new double[n];
    double[] gammasSums = new double[n];
    
    // Iterate until convergence of the transition probabilities
    int maxSteps = 100;
    for ( int iterationStep = 0; iterationStep < maxSteps; iterationStep++ ) {
      System.out.println("Iteration " + iterationStep);
      
      /*
       * Generate all the alphas
       */
      // initialize the first alphas
      {
        double sum = 0.0;
        for ( int i = 0; i < n; i++ ) {
          double value = pi[i] * b[i][observedSequence[0]];
          alphas[0 * n + i] = value;
          sum += value;
        }
        
        // rescale
        double scale = 1.0 / sum;
        alphasScales[0] = scale;
        
        for ( int i = 0; i < n; i++) {
          alphas[0 * n + i] *= scale; 
        }
      }
      
      // compute the other alphas
      for ( int t = 1; t < sequenceLength; t++ ) {
        double sum = 0.0;
        for (int i = 0; i < n; i++) {
          double res = 0.0;
          for (int j = 0; j < n; j++) {
            res += (alphas[(t - 1) * n + j] * a[j][i]);
          }
          double value = (res * b[i][observedSequence[t]]);
          alphas[t * n + i] = value;
          sum += value;
        }
        
        // rescale
        double scale = 1.0 / sum;
        alphasScales[t] = scale;

        for ( int i = 0; i < n; i++) {
          alphas[t * n + i] *= scale; 
        }
      }
      
      /*
       * Generate all the betas coefficients
       */
      for (int stateIndex = 0; stateIndex < n; stateIndex++) {
        betas[(sequenceLength - 1) * n + stateIndex] = 1.0d;
      }

      for (int t = sequenceLength - 1; t >= 1; t--) {
        for (int i = 0; i < n; i++) {
          double res = 0.0;
          for (int j = 0; j < n; j++) {
            res += (betas[t * n + j] * a[i][j]
                   * b[j][observedSequence[t]] * alphasScales[t - 1]);
          }

          betas[(t - 1) * n + i] = res;
        }
      }
      
      // reset temporary variables
      Arrays.fill(gammasSums, 0.0d);
      for ( int stateIndex = 0; stateIndex < n; stateIndex++ ) {
        Arrays.fill(aaStar[stateIndex], 0.0);
      }
      
      // as we don't need to update b, we can stop at
      // sequenceLength-1
      for ( int t = 0; t < sequenceLength - 1; t++ ) {
        
        // compute the terms alpha(i,t)*beta(i,t) and incrementally the sum of them
        for (int i = 0; i < n; i++) {
          double tempVal = alphas[t * n + i] * betas[t * n + i];
          gammas[i] = tempVal;
        }

        // compute gamma(i,t), and incrementally gamma_sums(i)
        for (int i = 0; i < n; i++) {
          double tempVal = gammas[i] / alphasScales[t];
          gammas[i] = tempVal;
          gammasSums[i] += tempVal;
        }

        // we have now gamma(i,t) in gammas[], and sum( k, alpha(k, t)*beta(k, t) ) in denGamma */
        /* compute khi(i,j) incrementally, put it in aaStar */
        if (t != sequenceLength - 1) {
          for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
              double khi = (alphas[t * n + i] * a[i][j] * betas[(t + 1) * n + j])
                      * b[j][observedSequence[t + 1]];
              aaStar[i][j] += khi;
            }
          }
        }
        /* copy in Pi_star if that's the moment */
        if (t == 0) {
          System.arraycopy(gammas, 0, piStar, 0, n);
        }
      }
      
      // Renormalize aaStar
      for (int i = 0; i < n; i++) {
        double sum = 0.0;
        for (int j = 0; j < n; j++) {
          sum += aaStar[i][j];
        }
        if ( sum > 0.0 ) {
          for (int j = 0; j < n; j++) {
            aaStar[i][j] /= sum;
          }
        }
      }
      
      // Renormalize piStar
      double sum = 0.0;
      for (int i = 0; i < n; i++ ) {
        sum += piStar[i];
      }
      if ( sum > 0.0 ) {
        for ( int i = 0; i < n; i++ ) {
          piStar[i] /= sum;
        }
      }
      
      // Check convergence here
      double logLikelihood = 0.0;
      for ( int t = 0; t < sequenceLength; t++ ) {
        logLikelihood -= Math.log(alphasScales[t]);
      }
      
      // Copy back piStar and aaStar
      double[] temp1 = pi;
      pi = piStar;
      piStar = temp1;
      
      double[][] temp2 = a;
      a = aaStar;
      aaStar = temp2;
      
      // break when both criterion have been  met
      if ( Math.abs(logLikelihood - prevLogLikelihood) < likelihoodThreshold ) {
        //break;
      }
      
      prevLogLikelihood = logLikelihood;
    }
  }
  
  public final class BaumWelchBlock implements Serializable{
    public int blockSize;
    public int blockId;
    public int blockStart;// the start is inclusive
    public int blockEnd;// the end is non inclusive
    
    int N;
    int M;
    int T;
    double[] pi;
    double[][] a;
    double[][] b;
    int[] observedBlock;
    
    double[] ta;
    double[] prevAlphaHat;
    double[] alphasHat;
    double[] alphasScales;
    
    double[] tb;
    double[] nextBetaHat;
    double[] betasHat;
    
    double[] khis;
    
    public BaumWelchBlock(
            int blockSize,
            int blockId,
            int blockStart,
            int blockEnd,
            int N,
            int M,
            int T,
            int[] observedBlock){
      this.blockSize = blockSize;
      this.blockId = blockId;
      this.blockStart = blockStart;
      this.blockEnd = blockEnd;
      
      this.N = N;
      this.M = M;
      this.T = T;
      // the observedBlock is of size blockSize + 1 !
      this.observedBlock = observedBlock;
    }
    
    @Override
    public int hashCode() {
      return blockId;
    }
    
   
    
    @Override
    public String toString() {
      return "BaumWelchBlock [blockSize=" + blockSize + ", blockId=" + blockId + ", blockStart="
              + blockStart + ", blockEnd=" + blockEnd + ", N=" + N + ", M=" + M + ", T=" + T
              + ",\n     pi=" + Arrays.toString(pi) + ",\n     a=" + Arrays.toString(a) + ",\n     b="
              + Arrays.toString(b) + ",\n     observedBlock=" + Arrays.toString(observedBlock) + ",\n     ta="
              + Arrays.toString(ta) + ",\n     prevAlphaHat=" + Arrays.toString(prevAlphaHat)
              + ",\n     alphasHat=" + Arrays.toString(alphasHat) + ",\n     alphasScales="
              + Arrays.toString(alphasScales) + ",\n     tb=" + Arrays.toString(tb) + ",\n     nextBetaHat="
              + Arrays.toString(nextBetaHat) + ",\n     betasHat=" + Arrays.toString(betasHat)
              + ",\n     khis=" + Arrays.toString(khis) + "]";
    }

    /**
     * Function in which we really initialize the block
     * @param pi Current starting distribution
     * @param a Current transition probability matrix
     * @param b Observation matrix.
     */
    public SquareMatrix initialize(
            double[] pi,
            double[][] a,
            double[][] b) {  
      this.pi = new double[N];
      for ( int i = 0; i < N; i++ ) {
        this.pi[i] = pi[i];
      }
      
      this.a = new double[N][N];
      for ( int i = 0; i < N; i++ ) {
        for ( int j = 0; j < N; j++ ) {
          this.a[i][j] = a[i][j];
        }
      }
      
      this.b = new double[N][M];
      for ( int i = 0; i < N; i++ ) {
        for ( int j = 0; j < M; j++ ) {
          this.b[i][j] = b[i][j];
        }
      }
      
      this.ta = new double[ blockSize * N * N];
      
      this.prevAlphaHat = new double[N];
      this.alphasHat = new double[ blockSize * N];
      
      this.alphasScales = new double[ blockSize ];
      
      this.tb = new double[ blockSize * N * N];
      this.nextBetaHat = new double[N];
      this.betasHat = new double[blockSize * N * N];
      
      this.khis = new double[ N * N];
      
      for ( int bi = 0; bi < blockSize; bi++ ) {
        int index = bi + blockStart;

        double sum = 0.0;
        if ( index == 0 ) {
          for ( int i = 0; i < N; i++ ) {
            for ( int j = 0; j < N; j++ ) {
              ta[ i * N + j * N] = 0.0;
            }
            double val = pi[i] * b[i][observedBlock[0]];
            ta[ i * N + i ] = val;
            sum += val;
          }
        } else {
          for ( int i = 0; i < N; i++ ) {
            for ( int j = 0; j < N; j++ ) {
              double val = a[j][i]
                      * b[i][observedBlock[bi]];
              ta[ bi * N * N + i * N + j ] = val;
              sum += val;
            }
          }
        }
        
        // normalize
        for ( int i = 0; i < N; i++ ) {
          for ( int j = 0; j < N; j++ ) {
            ta[ i * N + j ] /= sum;
          }
        }
      }
      
      // perform the initial reduction.
      double[] aux = new double[N * N];
      for ( int bi = 1; bi < blockSize; bi++ ) {
        double sum = 0.0;
        for ( int i = 0; i < N; i++ ) {
          for (int j = 0; j < N; j++ ) {
            double val = 0.0;
            for ( int k = 0; k < N; k++ ) {
              val += ta[ bi * N * N + i * N + k ]
                      * ta[ (bi - 1) * N * N + k * N + j];
            }
            aux[ i * N + j ] = val;
            sum += val;
          }
        }
        
        // renormalize and copy
        for ( int i = 0; i < N; i++ ) {
          for (int j = 0; j < N; j++ ) {
            aux[ i * N + j ] /= sum;
            ta[ bi * N * N + i * N + j ] = aux[ i * N + j ];
          }
        }
      }
      
      // return the last matrix
      SquareMatrix ret = new SquareMatrix(N);
      for (int i = 0; i < N; i++ ) {
        for ( int j = 0; j < N; j++ ) {
          ret.set(i, j, aux[ i * N + j]);
        }
      }
      System.out.println("Baum-Welch block in initialize : "+this);
      return ret;
    }
    
    /**
     * Perform the last scan stage, and compute the alpha vectors.
     * @param prevMatrix Previous block matrix to apply, if any.
     * @return Return the last alpha
     */
    public double[] computeAlphas( SquareMatrix prevMatrix ) {
      // perform scan if necessary
      if ( prevMatrix != null ) {
        double[] aux = new double[N * N];
        for ( int bi = 0; bi < blockSize; bi++ ) {
          double sum = 0.0;
          for ( int i = 0; i < N; i++ ) {
            for ( int j = 0; j < N; j++ ) {
              double val = 0.0;
              for ( int k = 0; k < N; k++ ) {
                val += ta[ bi * N * N + i * N + k ] * prevMatrix.elements[k * N + j];
              }
              sum += val;
              aux[i * N + j ] = val;
            }
          }
          
          // renormalize it
          for ( int i  = 0; i < N; i++ ) {
            for ( int j = 0; j < N; j++ ) {
              ta[ bi * N * N + i * N + j ] = aux[ i * N + j ] / sum;
            }
          }
        }
      }
      
      // compute the alpha vectors
      for (int bi = 0; bi < blockSize; bi++ ) {
        for ( int i = 0; i < N; i++ ) {
          double val = 0.0;
          for ( int j = 0; j < N; j++ ) {
            val += ta[ bi * N * N + i * N + j];
          }
          alphasHat[ bi * N + i ] = val;
        }
      }
      
      // return the last alpha of the block
      double[] lastAlphas = new double[N];
      for ( int i = 0; i < N; i++ ) {
        lastAlphas[i] = alphasHat[ (blockSize - 1) * N + i ];
      }
      return lastAlphas;
    }
    
    /**
     * Compute the Ct coefficients, start reducing the TB matrices.
     * @param prevAlpha Previous block last alpha if any
     * @return The first TB matrix of the block
     */
    public SquareMatrix computeCt( double[] prevAlpha ) {
      // compute the Ct coefficients
      if ( blockId == 0 ) {
        double den = 0.0;
        for ( int i = 0; i < N; i++ ) {
          den += pi[i] * b[i][observedBlock[0]];
        }
        alphasScales[0] = 1.0 / den;
      } else {
        { // bi = 0
          double den = 0.0;
          for ( int i = 0; i < N; i++ ) {
            for (int j = 0; j < N; j++ ) {
              double val = a[j][i]
                      * b[i][observedBlock[0]] * prevAlpha[j];
              den += val;
            }
          }
          alphasScales[0] = 1.0 / den;
        }
      }
      for ( int bi = 1; bi < blockSize; bi++ ) {
        double den = 0.0;
        for ( int i = 0; i < N; i++ ) {
          for (int j = 0; j < N; j++ ) {
            double val = a[j][i]
                    * b[i][observedBlock[bi]] * alphasHat[(bi - 1) * N + j];
            den += val;
          }
        }
        alphasScales[bi] = 1.0 / den;
      }
      
      // set the TB matrices, and then reduce them
      for ( int bi = 0; bi < blockSize; bi++ ) {
        int index = blockStart + bi;
        if ( index == T - 1 ) {
          for ( int i = 0; i < N; i++ ) {
            for ( int j = 0; j < N; j++ ) {
              tb[ bi * N * N + i * N + j ] = 0.0;
            }
            tb[ bi * N * N + i * N + i ] = alphasScales[bi];
          }
        } else {
          for ( int i = 0; i < N; i++ ) {
            for ( int j = 0; j < N; j++ ) {
              int auxTest = observedBlock[bi + 1];
              tb[ bi * N * N + i * N + j ] = alphasScales[bi]
                      * a[i][j] * 
                      b[j][auxTest];
                      //b[observedBlock[bi + 1]][j];
            }
          }
        }
      }
      
      //  reduce the matrices
      double[] aux = new double[N * N];
      
      for ( int bi = blockSize - 2; bi >= 0; bi-- ) {
        for ( int i = 0; i < N; i++ ) {
          for ( int j = 0; j < N; j++ ) {
            double val = 0.0;
            for (int k = 0; k < N; k++ ) {
              val += tb[ bi * N * N + i * N + k] * tb[ (bi + 1) * N * N + k * N + j ];
            }
            aux[ i * N + j ] = val;
          }
        }
        
        for ( int i = 0; i < N; i++ ) {
          for ( int j = 0; j < N; j++ ) {
            tb[ bi * N * N + i * N + j ] = aux[ i * N + j ];
          }
        }
      }
      
      // return the first matrix of the block as the return value
      SquareMatrix ret = new SquareMatrix(N);
      for ( int i = 0; i < N; i++ ) {
        for ( int j = 0; j < N; j++ ) {
          ret.elements[ i * N + j ] = aux[ i * N + j ];
        }
      }
      return ret;
    }
    
    /**
     * Finish the reduction of TB matrices, and compute the khis
     * @param nextTb Next Tb matrix, if any
     * @return The matrix of khis as a square matrix.
     */
    public SquareMatrix computeKhis( SquareMatrix nextTb ) {
      // TODO finish this!
      if ( nextTb != null ) {
        double[] aux = new double[N * N];
        for ( int bi = blockSize - 1; bi >= 0; bi-- ) {
          for ( int i = 0; i < N; i++ ) {
            for ( int j = 0; j < N; j++ ) {
              double val = 0.0;
              for (int k = 0; k < N; k++ ) {
                val += tb[ bi * N * N + i * N + k] * nextTb.elements[ k * N + j ];
              }
              aux[ i * N + j ] = val;
            }
          }
          
          for ( int i = 0; i < N; i++ ) {
            for ( int j = 0; j < N; j++ ) {
              tb[ bi * N * N + i * N + j ] = aux[ i * N + j ];
            }
          }
        }
      }
      
      for ( int bi = 0; bi < blockSize; bi++ ) {
        int index = blockStart + bi;
        if ( bi < blockSize - 1 ) {
          for ( int i = 0; i < N; i++ ) {
            for ( int j = 0; j < N; j++ ) {
              this.khis[ i * N + j ] += alphasHat[ bi * N + i ] * a[i][j]
                      * betasHat[ (bi + 1) * N + j] * b[j][observedBlock[bi + 1]];
                      //* betasHat[ (bi + 1) * N + j] * b[observedBlock[bi + 1]][j]
                      
            }
          }
        } else if ( index != T - 1 ) {
          double[] beta = new double[N];
          for ( int i = 0; i < N; i++ ) {
            double val = 0.0;
            for ( int j = 0; j < N; j++ ) {
              val += nextTb.elements[ i * N + j ];
            }
            beta[i] = val;
          }
          for ( int i = 0; i < N; i++ ) {
            for ( int j = 0; j < N; j++ ) {
              this.khis[ i * N + j ] += alphasHat[ bi * N + i ] * a[i][j]
                      * beta[j] * b[j][observedBlock[bi + 1]];
                      //* beta[j] * b[observedBlock[bi + 1]][j];
            }
          }
        }
      }
      
      SquareMatrix ret = new  SquareMatrix(N);
      for ( int i = 0; i < N; i++ ) {
        for ( int j = 0; j < N; j++ ) {
          ret.elements[ i * N + j ] = khis[ i * N + j ];
        }
      }
      return ret;
    }
  }
  
  /**
   * Perform training on a spark Rdd observation sequence.
   * @param sc Spark context to use
   * @param observedSequence Rdd containing the sequence (seq index, word index)
   * @param piThreshold Threshold on pi
   * @param aaThreshold  Threshold on a
   * @param maxIterations Max number of iterations
   */
  public void rawSparkTrain(
          JavaSparkContext sc,
          JavaRDD<Tuple2<Integer, Integer>> observedSequence,
          double piThreshold,
          double aaThreshold,
          long maxIterations ) {
    //final int blockSize = 1024 * 1024;
    final int blockSize = 32;
    final int N = n;
    final int M = m;
    final int T = (int) observedSequence.count();
    
    final int numBlocks = (T + (blockSize - 1)) / blockSize;
    double piDiff = Double.POSITIVE_INFINITY;
    double aaDiff = Double.POSITIVE_INFINITY;
    
    
    JavaRDD<Tuple3<Integer, Integer, Integer>> observedSequenceWithBlockIds = observedSequence
            .flatMap(new FlatMapFunction<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Integer>>() {
              public Iterable<Tuple3<Integer, Integer, Integer>> call(Tuple2<Integer, Integer> tuple) {
                List<Tuple3<Integer, Integer, Integer>> result = new LinkedList<Tuple3<Integer, Integer, Integer>>();
                if (tuple._1 % blockSize == 0 && tuple._1 != 0) {
                  result.add(new Tuple3<Integer, Integer, Integer>(tuple._1 / blockSize - 1,
                          tuple._1, tuple._2));
                }
                result.add(new Tuple3<Integer, Integer, Integer>(tuple._1 / blockSize, tuple._1,
                        tuple._2));
                return result;
              }
            });
    
    System.out.println("Before seq group");
    JavaPairRDD<Integer, Iterable<Tuple3<Integer, Integer, Integer>>> observerdGroupedByBlock = observedSequenceWithBlockIds
            .groupBy(new Function<Tuple3<Integer, Integer, Integer>, Integer>() {
              public Integer call(Tuple3<Integer, Integer, Integer> tuple) {
                return tuple._1();
              }
            });
    
    
    System.out.println("Before block creation");
    JavaRDD<BaumWelchBlock> blocksRdd = observerdGroupedByBlock
            .map(new Function<Tuple2<Integer, Iterable<Tuple3<Integer, Integer, Integer>>>, BaumWelchBlock>() {
              public BaumWelchBlock call(
                      Tuple2<Integer, Iterable<Tuple3<Integer, Integer, Integer>>> tuple) {

                List<Tuple2<Integer, Integer>> filteredObservations = new ArrayList<Tuple2<Integer, Integer>>(
                        blockSize + 1);
                for (Tuple3<Integer, Integer, Integer> element : tuple._2()) {
                  filteredObservations.add(new Tuple2<Integer, Integer>(element._2(), element._3()));
                }

                // sort filtered observations
                Collections.sort(filteredObservations, new Comparator<Tuple2<Integer, Integer>>() {
                  @Override
                  public int compare(Tuple2<Integer, Integer> index1,
                          Tuple2<Integer, Integer> index2) {
                    return index1._1.compareTo(index2._1);
                  }
                });

                if (filteredObservations.size() > blockSize + 1) {
                  System.out.println("Filtered observation size :" + filteredObservations.size());
                }
                
                int[] observedBlock = new int[blockSize + 1];
                for (Tuple2<Integer, Integer> element : filteredObservations) {
                  observedBlock[element._1() % blockSize] = element._2;
                }
                
                int blockId = tuple._1;
                int blockStart = blockId * blockSize;
                int blockEnd = Math.min((blockId + 1) * blockSize, T);
                BaumWelchBlock block = new BaumWelchBlock(
                        blockEnd - blockStart,
                        tuple._1(),
                        blockStart,
                        blockEnd,
                        N,
                        M,
                        T,
                        observedBlock);
                System.out.println("Baum-Welch block in constructor : "+block);
                return block;
              }
            });
    
    // iterate until convergence
    for ( int step = 0; step < maxIterations; step++ ) {
      // initialize the TA matrices.
      
      class TaBlockInitializer implements
          Function<BaumWelchBlock, Tuple2<Integer,SquareMatrix>>, Serializable {
        private static final long serialVersionUID = 1L;
        
        @Override
        public Tuple2<Integer,SquareMatrix> call(BaumWelchBlock arg0) throws Exception {
          
          return new Tuple2<Integer, SquareMatrix>(
                  arg0.blockId,
                  arg0.initialize(pi, a, b) );
        }
      }
      
      System.out.println("Before TA init");
      JavaRDD<Tuple2<Integer,SquareMatrix>> partialTaScansRdd =
              blocksRdd.map( new TaBlockInitializer() );
      blocksRdd.persist(StorageLevel.MEMORY_ONLY());
      
      // we have initialized and partially scanned the TA matrices.
      List<Tuple2<Integer, SquareMatrix>> partialTaScans = partialTaScansRdd.collect();
      
      // sort them by block id.
      Collections.sort(partialTaScans, new Comparator<Tuple2<Integer, SquareMatrix>>(){
        @Override
        public int compare(
                Tuple2<Integer, SquareMatrix> index1,
                Tuple2<Integer, SquareMatrix> index2) {
            return index1._1.compareTo(index2._1);
        }
      });
      
      // reduce the matrices on the master
      int partialTaScansSize = partialTaScans.size();
      if ( partialTaScansSize != numBlocks ) {
        System.out.println("Incorrect number of partial TA scans!");
      }
      
      for ( int i = 1; i < partialTaScansSize; i++ ) {
        SquareMatrix out = new SquareMatrix(this.n);
        
        Tuple2<Integer, SquareMatrix> left = partialTaScans.get(i - 1);
        Tuple2<Integer, SquareMatrix> right = partialTaScans.get(i);
        
        out = right._2.multiplyOut(left._2, out);
        partialTaScans.set(i, new Tuple2<Integer, SquareMatrix>(right._1, out));
      }
      
      class ComputeAlphasMapper implements Function<BaumWelchBlock, Tuple2<Integer, double[]>>, Serializable {

        private static final long serialVersionUID = 1L;
        List<Tuple2<Integer, SquareMatrix>> partialScans;
        
        public ComputeAlphasMapper(List<Tuple2<Integer, SquareMatrix>> partialScans) {
          this.partialScans = partialScans;
        }
        
        @Override
        public Tuple2<Integer, double[]> call(BaumWelchBlock arg0) throws Exception {
          SquareMatrix prev = null;
          if ( arg0.blockId > 0 ) {
            prev = partialScans.get(arg0.blockId - 1)._2;
          }
          System.out.println("Baum-Welch block in ComputeAlphasMapper : "+arg0);
          double[] lastAlpha = arg0.computeAlphas(prev);
          return new Tuple2<Integer, double[]>(arg0.blockId, lastAlpha);
        }
        
      }
      
      System.out.println("Before alpha map");
      // Finally reduce the TA, compute the alphaHat vectors,
      // get the last alphaHat vector of every block
      JavaRDD<Tuple2<Integer, double[]>> lastAlphasRdd =
              blocksRdd.map(new ComputeAlphasMapper(partialTaScans));
      blocksRdd.persist(StorageLevel.MEMORY_ONLY());
      System.out.println("after alpha map");
      List<Tuple2<Integer, double[]>> lastAlphas = lastAlphasRdd.collect();
      
      // class to compute the Ct, and start reduction of Tb
      class TbBlockInitializer implements
          Function<BaumWelchBlock, Tuple2<Integer,SquareMatrix>> , Serializable{
        private static final long serialVersionUID = 1L;
        
        List<Tuple2<Integer, double[]>> lastAlphas;
        
        public TbBlockInitializer(List<Tuple2<Integer, double[]>> lastAlphas) {
          this.lastAlphas = lastAlphas;
        }
        
        @Override
        public Tuple2<Integer,SquareMatrix> call(BaumWelchBlock arg0) throws Exception {
          System.out.println("Baum-Welch block in tbBlockInitializer "+arg0);
          double[] prev = null;
          if ( arg0.blockId > 0 ) {
            prev = lastAlphas.get(arg0.blockId - 1)._2;
          }
          SquareMatrix firstTb = arg0.computeCt(prev);
          return new Tuple2<Integer, SquareMatrix>(arg0.blockId, firstTb);
        }
        
      }
      
      System.out.println("Before TB init");
      // propagate the last alphas, compute the Ct, start computing the TB.
      JavaRDD<Tuple2<Integer,SquareMatrix>> partialTbScansRdd =
              blocksRdd.map( new TbBlockInitializer(lastAlphas) );
      blocksRdd.persist(StorageLevel.MEMORY_ONLY());
      
      // finish scanning the TB
      // we have initialized and partially scanned the TA matrices.
      List<Tuple2<Integer, SquareMatrix>> partialTbScans = partialTbScansRdd.collect();
      
      // sort them by block id.
      Collections.sort(partialTbScans, new Comparator<Tuple2<Integer, SquareMatrix>>(){
        @Override
        public int compare(
                Tuple2<Integer, SquareMatrix> index1,
                Tuple2<Integer, SquareMatrix> index2) {
            return index1._1.compareTo(index2._1);
        }
      });
      
      // reduce the matrices on the master
      int partialTbScansSize = partialTbScans.size();
      if ( partialTbScansSize != numBlocks ) {
        System.out.println("Incorrect number of partial scans!");
      }
      
      for ( int i = partialTbScansSize - 2; i >= 0; i-- ) {
        SquareMatrix out = new SquareMatrix(this.n);
        
        Tuple2<Integer, SquareMatrix> left = partialTaScans.get(i);
        Tuple2<Integer, SquareMatrix> right = partialTaScans.get(i + 1);
        
        out = left._2.multiplyOut(right._2, out);
        partialTbScans.set(i, new Tuple2<Integer, SquareMatrix>(left._1, out));
      }
      
      // finish scan and compute the Khis
      class KhisMapper implements Function<BaumWelchBlock, SquareMatrix>, Serializable {

        private static final long serialVersionUID = 1L;
        List<Tuple2<Integer,SquareMatrix>> partialScans;
        
        public KhisMapper(List<Tuple2<Integer,SquareMatrix>> partialScans) {
          this.partialScans = partialScans;
        }
        
        @Override
        public SquareMatrix call(BaumWelchBlock arg0) throws Exception {
          SquareMatrix next = null;
          if ( arg0.blockId < numBlocks - 1 ) {
            next = partialScans.get(arg0.blockId + 1)._2;
          }
          
          System.out.println("Baum-Welch block in KhisMapper " + arg0);
          
          SquareMatrix khis = arg0.computeKhis(next);
          return khis;
        }
        
      }
      
      System.out.println("Before khi map");
      JavaRDD<SquareMatrix> khisRdd = blocksRdd.map(new KhisMapper(partialTbScans));
      
      List<SquareMatrix> khis = khisRdd.collect();
     System.out.println("first khi matrix : " + khis.get(0));
      
      // compute and renormalize a
      double[][] aaStar = new double[n][n];
      for ( int i = 0; i < n; i++ ) {
        for ( int bi = 0; bi < khis.size(); bi++ ) {
          SquareMatrix khi = khis.get(bi);
          double sum = 0.0;
          for ( int j = 0; j < n; j++ ) {
            double val = khi.elements[ i * n + j];
            aaStar[i][j] = val;
            sum += val;
          }
          
          for (int j = 0; j < n; j++ ) {
            aaStar[i][j] /= sum;
          }
        }
      }
      
      // get the block 0 to find pi
      class PiFlatMapper implements FlatMapFunction<BaumWelchBlock, double[]>, Serializable {

        @Override
        public Iterable<double[]> call(BaumWelchBlock arg0) throws Exception {
          if ( arg0.blockId != 0 ) {
            return new ArrayList<double[]>();
            //return null;
          } else {
            double[] piStar = new double[arg0.N];
            for ( int i = 0; i < arg0.N; i++ ) {
              piStar[i] = arg0.alphasHat[i] * arg0.betasHat[i];
            }
            
            ArrayList<double[]> lpiStar = new ArrayList<double[]>();
            lpiStar.add(piStar);
            System.out.println("Baum-Welch block in PiFlatMapper " + arg0);
            return lpiStar;
          }
        }
        
      }
      
      System.out.println("Before pi flatmap");
      
      JavaRDD<double[]> piRdd = blocksRdd.flatMap(new PiFlatMapper());
      List<double[]> piList = piRdd.collect();
      
      if ( piList.size() > 1 ) {
        System.out.println("The pi list is too big!");
      }
      
      double[] piStar = piList.get(0);
      
      // renormalize pi
      double sum = 0.0;
      for ( int i = 0; i < n; i++ ) {
        sum += piStar[i];
      }
      for (int i = 0; i < n; i++ ) {
        piStar[i] /= sum;
      }
      
      // check convergence
      piDiff = 0.0;
      for ( int i = 0; i < n; i++ ) {
        piDiff += Math.abs( pi[i] - piStar[i] );
      }
      
      aaDiff = 0.0;
      for (int i = 0; i < n; i++ ) {
        for (int j = 0; j < n; j++ ) {
          aaDiff += Math.abs( aaStar[i][j] - a[i][j] );
        }
      }
      
      // commit changes
      a = aaStar;
      pi = piStar;
      
      if ( piDiff < piThreshold && aaDiff < aaThreshold ) {
        break;
      }
    }
  }

  /**
   * This method associates a state of the HMM to each word of the stream using Viterbi algorithm.
   * 
   * @param fullArticleStream
   *          the full text of the concatenated articles
   * @return the sequence of HMM states associated with the stream : each state is represented by an
   *         integer between 0 and k (0 for the background model)
   */
  public JavaRDD<Integer> decode(JavaRDD<String> fullArticleStream) {
    // TODO implement decode

    return null;

  }

  /**
   * Single process version of decode (without spark).
   * 
   * @param fullArticleStream
   *          the full text of the concatenated articles
   * @return the array representing the sequence of states
   */
  public int[] decode(List<String> fullArticleStream) {
    int T = fullArticleStream.size();
    int[] rawObservedSequence = new int[T];
    for ( int t = 0; t < T; t++ ) {
      rawObservedSequence[t] = outputAlphabet.indexOf(fullArticleStream.get(t));
    }
    return rawDecode(rawObservedSequence);
  }

  /**
   * Single process version of decode (without spark).
   * 
   * @param rawObservedSequence
   *          the observed states sequence
   * @return the array representing the sequence of states
   */
  public int[] rawDecode(int[] rawObservedSequence) {
    int T = rawObservedSequence.length;
    double[][] dynamicValue = new double[n][T];
    int[][] dynamicState = new int[n][T];
    
    int index = rawObservedSequence[0];
    for (int i = 0; i < n; i++) { // initialization
      dynamicValue[i][0] = Math.log(pi[i] * b[i][index]);
    }

    for ( int t = 1; t < T;t++) {
      int observedState = rawObservedSequence[t];
      
      for (int i = 0; i < n; i++) {
        double max = dynamicValue[0][t - 1] + Math.log(a[0][i] * b[i][observedState]);
        int argmax = 0;
        for (int j = 1; j < n; j++) {
          double current = dynamicValue[j][t - 1] + Math.log(a[j][i] * b[i][observedState]);
          if (current > max) {
            max = current;
            argmax = j;
          }
        }
        dynamicValue[i][t] = max;
        dynamicState[i][t] = argmax;
      }
    }

    int[] states = new int[T];
    double max = dynamicValue[0][T - 1];
    int argmax = 0;
    double current = 0D;
    for (int j = 1; j < n; j++) {
      current = dynamicValue[j][T - 1];
      if (current > max) {
        max = current;
        argmax = j;
      }
    }
    states[T - 1] = argmax;
    for (int w = T - 2; w > 0; w--) {
      states[w - 1] = dynamicState[states[w]][w];
    }

    return states;

  }
  
  /**
   * Generates an observation sequence of length "length" given a fully known HMM.
   * 
   * @param length
   *          the length of the observation sequence to be generated
   * @return the observation sequence i.e. a list of outputs
   */
  public List<String> generateObservationSequence(int length) {
    List<String> sequence = new ArrayList<String>(length);
    int[] rawSequence = generateRawObservationSequence(length);
    
    for ( int t = 0; t < length; t++ ) {
      sequence.add((String) outputAlphabet.get(rawSequence[t]));
    }
    return sequence;
  }
  
  /**
   * Generates a raw observation sequence of length "length" given a fully known HMM.
   * 
   * @param length
   *          the length of the observation sequence to be generated
   * @return the raw observation sequence i.e. a list of observation states indexes.
   */
  public int[] generateRawObservationSequence(int length) {
    int[] sequence = new int[length];
    double initRandom = Math.random();
    int initialState = -1;
    while (initRandom > 0.0 && initialState < (n - 1)) {
      initialState++;
      initRandom -= pi[initialState];
    }
    int currentState = initialState;
    for (int t = 0; t < length; t++) {
      // System.out.print(currentState+"     ");
      double randOutput = Math.random();
      double randTransition = Math.random();
      int outIndex = -1;
      while (randOutput > 0.0 && outIndex < (m - 1)) {
        outIndex++;
        randOutput -= b[currentState][outIndex];
      }
      //System.out.print(outIndex+" "+b[currentState][outIndex]+";");
      sequence[t] = outIndex;
      int nextState = -1;
      while (randTransition > 0.0 && nextState < (n - 1)) {
        nextState++;
        randTransition -= a[currentState][nextState];
      }
      currentState = nextState;
      
    }
    
    System.out.println();
    System.out.println("done generating sequence");

    return sequence;
  }

  public int getK() {
    return k;
  }

  public List<String> getOutputAlphabet() {
    return outputAlphabet;
  }

  public int getN() {
    return n;
  }
  
  public int getM() {
    return m;
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
