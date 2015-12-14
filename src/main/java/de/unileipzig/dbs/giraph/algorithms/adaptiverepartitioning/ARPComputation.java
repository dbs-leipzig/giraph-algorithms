/*
 * This file is part of giraph-algorithms.
 *
 * giraph-algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * giraph-algorithms is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with giraph-algorithms. If not, see <http://www.gnu.org/licenses/>.
 */
package de.unileipzig.dbs.giraph.algorithms.adaptiverepartitioning;

import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.Random;

/**
 * Implementation of the Adaptive Repartitioning (ARP) algorithm as described
 * in:
 * <p/>
 * "Adaptive Partitioning of Large-Scale Dynamic Graphs" (ICDCS14)
 * <p/>
 * To share global knowledge about the partition load and demand, the
 * algorithm exploits aggregators. For each of the k partitions, we use two
 * aggregators:
 * The first aggregator stores the capacity (CA_i) of partition i, the second
 * stores the demand (DA_i) for that partition (= number of vertices that want
 * to migrate in the next superstep).
 * <p/>
 * Superstep 0 Initialization Phase:
 * <p/>
 * If the input graph is an unpartitioned graph, the algorithm will at first
 * initialize each vertex with a partition-id i (hash based). This is skipped
 * if the graph is already partitioned.
 * After that, each vertex will notify CA_i that it is currently a member of the
 * partition and propagates the partition-id to its neighbours.
 * <p/>
 * The main computation is divided in two phases:
 * <p/>
 * Phase 1 Demand Phase (odd numbered superstep):
 * <p/>
 * Based on the information about their neighbours, each vertex calculates its
 * desired partition, which is the most frequent one among the neighbours
 * (label propagation). If the desired partition and the actual partition are
 * not equal the vertex will notify the DA of the desired partition that the
 * vertex want to migrate in.
 * <p/>
 * Phase 2 Migration Phase (even numbered superstep):
 * <p/>
 * Based on the information of the first phase, the algorithm will calculate
 * which vertices are allowed to migrate in their desired partition. If a vertex
 * migrates to another partition it notifies the new and old CA.
 * <p/>
 * The computation will terminate if no vertex wants to migrate, the maximum
 * number of iterations (configurable) is reached or each vertex reaches the
 * maximum number of partition switches (configurable).
 *
 * @author Kevin Gomez (gomez@studserv.uni-leipzig.de)
 * @author Martin Junghanns (junghanns@informatik.uni-leipzig.de)
 * @see <a href="http://www.few.vu.nl/~cma330/papers/ICDCS14.pdf">Adaptive
 * Partitioning of Large-Scale Dynamic Graphs</a>
 */
public class ARPComputation extends
  BasicComputation<LongWritable, ARPVertexValue, NullWritable, LongWritable> {
  /**
   * Number of partitions to create.
   */
  public static final String NUMBER_OF_PARTITIONS =
    "partitioning.num.partitions";
  /**
   * Default number of partitions if no value is given.
   */
  public static final int DEFAULT_NUMBER_OF_PARTITIONS = 4;
  /**
   * Number of iterations after which the calculation is stopped.
   */
  public static final String NUMBER_OF_ITERATIONS = "partitioning.iterations";
  /**
   * Default number of iterations if no value is given.
   */
  public static final int DEFAULT_NUMBER_OF_ITERATIONS = 10000;
  /**
   * Number of superstep's the vertex needs to be stable to halt
   */
  public static final String NUMBER_OF_STABLE_ITERATIONS =
    "partitioning.iteration.threshold";
  /**
   * Default number of superstep's the vertex needs to be stable to halt
   */
  public static final int DEFAULT_NUMBER_OF_STABLE_ITERATIONS = 30;
  /**
   * Threshold to calculate total partition capacity.
   */
  public static final String CAPACITY_THRESHOLD =
    "partitioning.capacity.threshold";
  /**
   * Default capacityThreshold if no value is given.
   */
  public static final float DEFAULT_CAPACITY_THRESHOLD = .2f;
  /**
   * Seed number for random instance.
   */
  public static final String SEED = "partitioning.seed";
  /**
   * Default seed
   */
  public static final long DEFAULT_SEED = 0;
  /**
   * Prefix for capacity aggregator which is used by master and worker compute.
   */
  static final String CAPACITY_AGGREGATOR_PREFIX =
    ARPComputation.class.getName() + ".capacity.aggregator.";
  /**
   * Prefix for demand aggregator which is used by master and worker compute.
   */
  static final String DEMAND_AGGREGATOR_PREFIX =
    ARPComputation.class.getName() + ".demand.aggregator.";
  /**
   * Needed for aggregators.
   */
  private static final LongWritable POSITIVE_ONE = new LongWritable(1);
  /**
   * Needed for aggregators.
   */
  private static final LongWritable NEGATIVE_ONE = new LongWritable(-1);
  /**
   * Total number of available partitions.
   */
  private int k;
  /**
   * Capacity capacityThreshold
   */
  private float capacityThreshold;
  /**
   * Number of vertices a partition can hold at maximum.
   */
  private long totalPartitionCapacity;
  /**
   * Number of stable superstep's
   */
  private int stableThreshold;
  /**
   * Used to decide if the given input is already partitioned or not
   */
  private boolean notPartitioned;
  /**
   * Seed number for random generator
   */
  private long seed;
  /**
   * Instance of Random Class
   */
  private Random random;

  /**
   * Returns the desired partition of the given vertex based on the
   * neighbours and the partitions they are in.
   *
   * @param vertex   current vertex
   * @param messages messages sent to current vertex
   * @return desired partition
   */
  private long getDesiredPartition(
    final Vertex<LongWritable, ARPVertexValue, NullWritable> vertex,
    final Iterable<LongWritable> messages) {
    long currentPartition = vertex.getValue().getCurrentPartition().get();
    long desiredPartition = currentPartition;
    // got messages?
    if (messages.iterator().hasNext()) {
      // partition -> neighbours in partition
      long[] countNeighbours = getPartitionFrequencies(messages);
      // partition -> desire to migrate
      double[] partitionWeights =
        getPartitionWeights(countNeighbours, vertex.getNumEdges());
      double firstMax = Integer.MIN_VALUE;
      double secondMax = Integer.MIN_VALUE;
      long firstK = -1;
      long secondK = -1;
      for (int i = 0; i < k; i++) {
        if (partitionWeights[i] > firstMax) {
          secondMax = firstMax;
          firstMax = partitionWeights[i];
          secondK = firstK;
          firstK = i;
        } else if (partitionWeights[i] > secondMax) {
          secondMax = partitionWeights[i];
          secondK = i;
        }
      }
      if (firstMax == secondMax) {
        if (currentPartition != firstK && currentPartition != secondK) {
          desiredPartition = firstK;
        }
      } else {
        desiredPartition = firstK;
      }
    }
    return desiredPartition;
  }

  /**
   * Calculates the partition frequencies among neighbour vertices.
   * Returns a field where element i represents the number of neighbours in
   * partition i.
   *
   * @param messages messages sent to the vertex
   * @return partition frequency
   */
  private long[] getPartitionFrequencies(final Iterable<LongWritable>
    messages) {
    long[] result = new long[k];
    for (LongWritable message : messages) {

      result[(int) message.get()]++;
    }
    return result;
  }

  /**
   * Calculates a weight for each partition based on the partition frequency
   * and the number of outgoing edges of that vertex.
   *
   * @param partitionFrequencies partition frequencies
   * @param numEdges             number of outgoing edges
   * @return partition weights
   */
  private double[] getPartitionWeights(long[] partitionFrequencies,
    int numEdges) {
    double[] partitionWeights = new double[k];
    for (int i = 0; i < k; i++) {
      long load = getPartitionLoad(i);
      long freq = partitionFrequencies[i];
      double weight = (double) freq / (load * numEdges);
      partitionWeights[i] = weight;
    }
    return partitionWeights;
  }

  /**
   * Decides if a vertex is allowed to migrate to a given desired partition.
   * This is based on the free space in the partition and the current demand
   * for that partition.
   *
   * @param desiredPartition desired partition
   * @return true if the vertex is allowed to migrate, false otherwise
   */
  private boolean isAllowedToMigrate(long desiredPartition) {
    long load = getPartitionLoad(desiredPartition);
    long availability = this.totalPartitionCapacity - load;
    double demand = getPartitionDemand(desiredPartition);
    double threshold = availability / demand;
    double randomRange = random.nextDouble();
    return Double.compare(randomRange, threshold) < 0;
  }

  /**
   * Returns the total number of vertices a partition can store. This depends
   * on the strict capacity and the capacity threshold.
   *
   * @return total capacity of a partition
   */
  private int getTotalCapacity() {
    double strictCapacity = getTotalNumVertices() / (double) k;
    double buffer = strictCapacity * capacityThreshold;
    return (int) Math.ceil(strictCapacity + buffer);
  }

  /**
   * Returns the demand for the given partition.
   *
   * @param partition partition id
   * @return demand for partition
   */
  private long getPartitionDemand(long partition) {
    LongWritable demandWritable =
      getAggregatedValue(DEMAND_AGGREGATOR_PREFIX + partition);
    return demandWritable.get();
  }

  /**
   * Returns the current load of the given partition.
   *
   * @param partition partition id
   * @return load of partition
   */
  private long getPartitionLoad(long partition) {
    LongWritable loadWritable =
      getAggregatedValue(CAPACITY_AGGREGATOR_PREFIX + partition);
    return loadWritable.get();
  }

  /**
   * Moves a vertex from its old to its new partition.
   *
   * @param vertex           vertex
   * @param desiredPartition partition to move vertex to
   */
  private void migrateVertex(
    final Vertex<LongWritable, ARPVertexValue, NullWritable> vertex,
    long desiredPartition) {
    // add current partition to partition history
    vertex.getValue()
      .addToPartitionHistory(vertex.getValue().getCurrentPartition().get());
    // decrease capacity in old partition
    String oldPartition = CAPACITY_AGGREGATOR_PREFIX +
      vertex.getValue().getCurrentPartition().get();
    notifyAggregator(oldPartition, NEGATIVE_ONE);
    // increase capacity in new partition
    String newPartition = CAPACITY_AGGREGATOR_PREFIX + desiredPartition;
    notifyAggregator(newPartition, POSITIVE_ONE);
    vertex.getValue().setCurrentPartition(new LongWritable(desiredPartition));
  }

  /**
   * Initializes the vertex with a partition id. This is calculated using
   * modulo (vertex-id % number of partitions).
   *
   * @param vertex vertex
   */
  private void setVertexStartValue(
    final Vertex<LongWritable, ARPVertexValue, NullWritable> vertex) {
    long startValue = vertex.getId().get() % k;
    vertex.getValue().setCurrentPartition(new LongWritable(startValue));
  }

  /**
   * Sends the given value to the given aggregator.
   *
   * @param aggregator aggregator to send value to
   * @param v          value to send
   */
  private void notifyAggregator(final String aggregator, final LongWritable v) {
    aggregate(aggregator, v);
  }

  @Override
  public void initialize(GraphState graphState,
    WorkerClientRequestProcessor<LongWritable, ARPVertexValue, NullWritable>
      workerClientRequestProcessor,
    GraphTaskManager<LongWritable, ARPVertexValue, NullWritable>
      graphTaskManager,
    WorkerGlobalCommUsage workerGlobalCommUsage, WorkerContext workerContext) {
    super.initialize(graphState, workerClientRequestProcessor, graphTaskManager,
      workerGlobalCommUsage, workerContext);
    this.k =
      getConf().getInt(NUMBER_OF_PARTITIONS, DEFAULT_NUMBER_OF_PARTITIONS);
    this.stableThreshold = getConf()
      .getInt(NUMBER_OF_STABLE_ITERATIONS, DEFAULT_NUMBER_OF_STABLE_ITERATIONS);
    this.capacityThreshold =
      getConf().getFloat(CAPACITY_THRESHOLD, DEFAULT_CAPACITY_THRESHOLD);
    this.totalPartitionCapacity = getTotalCapacity();
    this.notPartitioned = getConf()
      .getBoolean(ARPTextVertexInputFormat.PARTITIONED_INPUT,
        ARPTextVertexInputFormat.DEFAULT_PARTITIONED_INPUT);
    this.seed = getConf().getLong(SEED, DEFAULT_SEED);
    if (seed != 0) {
      random = new Random(seed);
    } else {
      random = new Random();
    }
  }

  /**
   * The actual ADP computation.
   *
   * @param vertex   Vertex
   * @param messages Messages that were sent to this vertex in the previous
   *                 superstep.
   * @throws IOException
   */
  @Override
  public void compute(Vertex<LongWritable, ARPVertexValue, NullWritable> vertex,
    Iterable<LongWritable> messages) throws IOException {
    if (getSuperstep() == 0) {
      setVertexStartValue(vertex);
      String aggregator = CAPACITY_AGGREGATOR_PREFIX +
        vertex.getValue().getCurrentPartition().get();
      notifyAggregator(aggregator, POSITIVE_ONE);
      sendMessageToAllEdges(vertex, vertex.getValue().getCurrentPartition());
    } else {
      // even superstep: migrate phase
      if ((getSuperstep() % 2) == 0) {
        long desiredPartition = vertex.getValue().getDesiredPartition().get();
        long currentPartition = vertex.getValue().getCurrentPartition().get();
        long stableCounter = vertex.getValue().getStableCounter().get();
        if (desiredPartition != currentPartition) {
          boolean migrate = isAllowedToMigrate(desiredPartition);
          if (migrate) {
            migrateVertex(vertex, desiredPartition);
            sendMessageToAllEdges(vertex,
              vertex.getValue().getCurrentPartition());
            vertex.getValue().setStableCounter(new LongWritable(0));
          } else {
            stableCounter++;
            vertex.getValue().setStableCounter(new LongWritable(stableCounter));
          }
        }
        vertex.voteToHalt();
      } else { // odd superstep: demand phase
        if (vertex.getValue().getStableCounter().get() >= stableThreshold) {
          vertex.voteToHalt();
        } else {
          long desiredPartition = getDesiredPartition(vertex, messages);
          vertex.getValue()
            .setDesiredPartition(new LongWritable(desiredPartition));
          long currentValue = vertex.getValue().getCurrentPartition().get();
          boolean changed = currentValue != desiredPartition;
          if (changed) {
            notifyAggregator(DEMAND_AGGREGATOR_PREFIX + desiredPartition,
              POSITIVE_ONE);
          }
        }
      }
    }
  }
}
