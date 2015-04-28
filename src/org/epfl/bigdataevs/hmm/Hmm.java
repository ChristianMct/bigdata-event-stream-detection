package org.epfl.bigdataevs.hmm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.commons.collections.list.TreeList;

import scala.Array;
import scala.Tuple2;

/**
 * A Hidden Markov Model (HMM) built to perform analysis of theme life cycles (paper section 4). An
 * HMM can be trained using train() or used to decode a text stream using decode() ; all the other
 * steps of theme life cycles analysis should not be done in this class.
 * 
 * @author team Damien-Laurent-Sami
 *
 */
public class Hmm {

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
  
  public final class BaumWelchBlock {
    public int blockSize;
    public int blockId;
    public int blockStart;// the start is inclusive
    public int blockEnd;// the end is non inclusive
    
    int N;
    int M;
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
            int M){
      this.blockSize = blockSize;
      this.blockId = blockId;
      this.blockStart = blockStart;
      this.blockEnd = blockEnd;
      
      this.N = N;
      this.M = M;
    }
    
    /**
     * Function in which we really initialize the block
     * @param pi Current starting distribution
     * @param a Current transition probability matrix
     * @param b Observation matrix.
     * @return The last (reduced) matrix of the block
     */
    public SquareMatrix initialize(
            double[] pi,
            double[][] a,
            double[][] b,
            int[] observedBlock) {
      // the observedBlock is of size blockSize + 1 !
      this.observedBlock = observedBlock;
      
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
      
      this.khis = new double[ blockSize * N * N];
      
      for ( int bi = 0; bi < blockSize; bi++ ) {
        int index = bi + blockStart;
        
        if ( index == 0 ) {
          double sum = 0.0;
          for ( int i = 0; i < N; i++ ) {
            for ( int j = 0; j < N; j++ ) {
              ta[ i * N + j * N] = 0.0;
            }
            double val = pi[i] * b[i][observedBlock[0]];
            ta[ i * N + i ] = val;
            sum += val;
          }
          
          // normalize
          for ( int i = 0; i < N; i++ ) {
            ta[ i * N + i ] /= sum;
          }
          
        } else {
          double sum = 0.0;
          for ( int i = 0; i < N; i++ ) {
            for ( int j = 0; j < N; j++ ) {
              double val = a[j][i]
                      * b[i][observedBlock[bi]];
              ta[ bi * N * N + i * N + j ] = val;
              sum += val;
            }
          }
          
          // normalize
          for ( int i = 0; i < N; i++ ) {
            for ( int j = 0; j < N; j++ ) {
              ta[ bi * N * N + i * N + j ] /= sum;
            }
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
            val += ta[ bi * N * N * i * N + j];
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
     * Compute the Ct coefficients, start reducing the TB matrices
     * @param prevAlpha
     */
    public void computeCt( double[] prevAlpha ) {
      if ( blockId == 0 ) {
        
      } else {
        
      }
    }
  }
  
  /**
   * Perform training on a spark Rdd observation sequence.
   * @param sc Spark context to use
   * @param observedSequence Rdd containing the sequence
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
    int blockSize = 1024 * 1024;
    int sequenceSize = (int) observedSequence.count();
    
    int numBlocks = (sequenceSize + (blockSize - 1)) / blockSize;
    
    JavaRDD<BaumWelchBlock> blocksRdd;
    
    { // generate the blocks
      ArrayList<BaumWelchBlock> blocks = new ArrayList<BaumWelchBlock>();
      
      for ( int i = 0; i < numBlocks; i++ ) {
        int blockStart = i * blockSize;
        int blockEnd = Math.min((i + 1) * blockSize, sequenceSize);
        
        blocks.add(new BaumWelchBlock(blockSize, i, blockStart, blockEnd, this.n, this.m));
      }
      blocksRdd = sc.parallelize(blocks);
    }
    
    // block id + array of observations
    JavaRDD<Tuple2<Integer, int[]>> observationBlocksRdd;
    
    { // generate the observation blocks
      final class InBlockObservationFilter implements Function<Tuple2<Integer, Integer>, Boolean> {
        private static final long serialVersionUID = 1L;
        BaumWelchBlock block;
        
        public InBlockObservationFilter( BaumWelchBlock block ) {
          this.block = block;
        }
        
        @Override
        public Boolean call(Tuple2<Integer, Integer> arg0) throws Exception {
          // include the observation of index blockEnd
          return (arg0._1 >= block.blockStart) && (arg0._1 <= block.blockEnd);
        }
        
      }
      
      final class ObservationFilter implements Function<BaumWelchBlock, Tuple2<Integer, int[]>> {

        private static final long serialVersionUID = 1L;
        
        JavaRDD<Tuple2<Integer, Integer>> observedSequence;
        
        public ObservationFilter( JavaRDD<Tuple2<Integer, Integer>> observedSequence ) {
          this.observedSequence = observedSequence;
        }

        @Override
        public Tuple2<Integer, int[]> call(BaumWelchBlock arg0) throws Exception {
          JavaRDD<Tuple2<Integer, Integer>> filteredObservationsRdd =
                  observedSequence.filter(new InBlockObservationFilter(arg0));
          
          List<Tuple2<Integer, Integer>> filteredObservations = filteredObservationsRdd.collect();
          
          // sort filtered observations
          Collections.sort(filteredObservations, new Comparator<Tuple2<Integer, Integer>>(){
            @Override
            public int compare(Tuple2<Integer, Integer> index1, Tuple2<Integer, Integer> index2) {
                return index1._1.compareTo(index2._1);
            }
          });
          
          // generate observation block
          Integer blockId = new Integer(arg0.blockId);
          int[] observedBlock = new int[arg0.blockSize];
          for ( int i = 0; i < arg0.blockSize; i++ ) {
            observedBlock[i] = filteredObservations.get(i)._2;
          }
          Tuple2<Integer, int[]> ret = new Tuple2<Integer, int[]>(blockId, observedBlock);
          return ret;
        }
        
      }
      
      observationBlocksRdd = blocksRdd.map(new ObservationFilter(observedSequence));
    }
    
    // iterate until convergence
    for ( int step = 0; step < maxIterations; step++ ) {
      
      class TupleKeyFilter<T> implements Function<Tuple2<Integer, T>, Boolean> {
        private static final long serialVersionUID = 1L;
        Integer comparisonKey;
        
        public TupleKeyFilter( Integer comparisonKey ) {
          this.comparisonKey = comparisonKey;
        }
        
        @Override
        public Boolean call(Tuple2<Integer, T> arg0) throws Exception {
          // include the observation of index blockEnd
          return (arg0._1.intValue() == comparisonKey.intValue());
        } 
      }
      
      // initialize the TA matrices.
      
      class TaBlockInitializer implements
          Function<BaumWelchBlock, Tuple2<Integer,SquareMatrix>> {
        private static final long serialVersionUID = 1L;
        
        JavaRDD<Tuple2<Integer, int[]>> observedBlocksRdd;
        
        public TaBlockInitializer(JavaRDD<Tuple2<Integer, int[]>> observedBlocksRdd) {
          this.observedBlocksRdd = observedBlocksRdd;
        }
        
        @Override
        public Tuple2<Integer,SquareMatrix> call(BaumWelchBlock arg0) throws Exception {
          
          JavaRDD<Tuple2<Integer, int[]>> observedBlockRdd =
                  observedBlocksRdd.filter(new TupleKeyFilter<int[]>(new Integer(arg0.blockId)));
          
          if ( observedBlockRdd.count() > 1 ) {
            System.out.println("Got more than one observedBlock");
          }
          
          Tuple2<Integer, int[]> observedBlock = observedBlockRdd.collect().get(0);
          
          return new Tuple2<Integer, SquareMatrix>(
                  arg0.blockId,
                  arg0.initialize(pi, a, b, observedBlock._2) );
        }
        
      }
      
      JavaRDD<Tuple2<Integer,SquareMatrix>> partialScansRdd =
              blocksRdd.map( new TaBlockInitializer(observationBlocksRdd) );
      
      // we have initialized and partially scanned the TA matrices.
      List<Tuple2<Integer, SquareMatrix>> partialScans = partialScansRdd.collect();
      
      // sort them by block id.
      Collections.sort(partialScans, new Comparator<Tuple2<Integer, SquareMatrix>>(){
        @Override
        public int compare(
                Tuple2<Integer, SquareMatrix> index1,
                Tuple2<Integer, SquareMatrix> index2) {
            return index1._1.compareTo(index2._1);
        }
      });
      
      // reduce the matrices on the master
      int partialScansSize = partialScans.size();
      if ( partialScansSize != numBlocks ) {
        System.out.println("Incorrect number of partial scans!");
      }
      
      for ( int i = 1; i < partialScansSize; i++ ) {
        SquareMatrix out = new SquareMatrix(this.n);
        
        Tuple2<Integer, SquareMatrix> left = partialScans.get(i - 1);
        Tuple2<Integer, SquareMatrix> right = partialScans.get(i);
        
        out = right._2.multiplyOut(left._2, out);
        partialScans.set(i, new Tuple2<Integer, SquareMatrix>(right._1, out));
      }
      
      // re-parallelize it
      partialScansRdd = sc.parallelize(partialScans);
      
      class ComputeAlphasMapper implements Function<BaumWelchBlock, Tuple2<Integer, double[]>> {

        private static final long serialVersionUID = 1L;
        JavaRDD<Tuple2<Integer,SquareMatrix>> partialScansRdd;
        
        public ComputeAlphasMapper(JavaRDD<Tuple2<Integer,SquareMatrix>> partialScansRdd) {
          this.partialScansRdd = partialScansRdd;
        }
        
        @Override
        public Tuple2<Integer, double[]> call(BaumWelchBlock arg0) throws Exception {
          JavaRDD<Tuple2<Integer, SquareMatrix>> prevMatrixRdd =
                  partialScansRdd.filter(new TupleKeyFilter<SquareMatrix>(arg0.blockId - 1));
          
          List<Tuple2<Integer, SquareMatrix>> prevMatrixList = prevMatrixRdd.collect();
          
          if (prevMatrixList.size() > 1 ) {
            System.out.println("The previous matrix lsit is too big!");
          }
          
          SquareMatrix prev = null;
          if ( prevMatrixList.size() == 1 ) {
            prev = prevMatrixList.get(0)._2;
          }
          
          double[] lastAlpha = arg0.computeAlphas(prev);
          return new Tuple2<Integer, double[]>(arg0.blockId, lastAlpha);
        }
        
      }
      // Finally reduce the TA, compute the alphaHat vectors,
      // get the last alphaHat vector of every block
      JavaRDD<Tuple2<Integer, double[]>> lastAlphas = blocksRdd.map(new ComputeAlphasMapper(partialScansRdd));
      
      // propagate the last alphas, compute the Ct, start computing the TB.
      
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
