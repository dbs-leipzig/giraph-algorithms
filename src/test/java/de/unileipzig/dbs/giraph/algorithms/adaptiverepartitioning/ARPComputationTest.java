package de.unileipzig.dbs.giraph.algorithms.adaptiverepartitioning;

import com.google.common.collect.Maps;
import de.unileipzig.dbs.giraph.algorithms.GiraphTestHelper;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.Test;

import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link ARPComputation}.
 */
public class ARPComputationTest {
  private final int numPartitions = 2;
  private final float capacityThreshold = 0.25f;
  private static final Pattern LINE_TOKEN_SEPARATOR = Pattern.compile(" ");
  private int countVerticesPartitionZero = 0;

  @Test
  public void testSmallConnectedGraph() throws Exception {
    final int numIterations = 120;
    final int stabilizationRounds = 5;
    final long seed = 13374242;
    String[] graph = GiraphTestHelper.getConnectedGraphValues();
    validateConnectedGraphResult(
      computeResults(graph, numPartitions, numIterations, capacityThreshold,
        stabilizationRounds, seed));
  }

  private void validateConnectedGraphResult(
    Map<Integer, Integer> vertexIDwithValue) {
    int n = vertexIDwithValue.size();
    int optimalPartitionSize = n / numPartitions;
    double countedOccupation = (float) countVerticesPartitionZero / n;
    double estimatedOccupation =
      (optimalPartitionSize + (optimalPartitionSize * capacityThreshold)) / n;
    assertTrue(Double.compare(countedOccupation, estimatedOccupation) <= 0);
  }

  private Map<Integer, Integer> computeResults(String[] graph,
    int partitionCount, int maxIterations, float capacityTreshold,
    int maxStabilization, long seed) throws Exception {
    GiraphConfiguration conf = getConfiguration();
    conf.setInt(ARPComputation.NUMBER_OF_PARTITIONS, partitionCount);
    conf.setInt(ARPComputation.NUMBER_OF_ITERATIONS, maxIterations);
    conf.setFloat(ARPComputation.CAPACITY_THRESHOLD, capacityTreshold);
    conf.setInt(ARPComputation.NUMBER_OF_STABLE_ITERATIONS, maxStabilization);
    conf.setLong(ARPComputation.SEED, seed);
    Iterable<String> results = InternalVertexRunner.run(conf, graph);
    return parseResults(results);
  }

  private GiraphConfiguration getConfiguration() {
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(ARPComputation.class);
    conf.setMasterComputeClass(ARPMasterComputation.class);
    conf.setVertexInputFormatClass(ARPTextVertexInputFormat.class);
    conf.setVertexOutputFormatClass(ARPTextVertexOutputFormat.class);
    return conf;
  }

  private Map<Integer, Integer> parseResults(Iterable<String> results) {
    Map<Integer, Integer> parsedResults = Maps.newHashMap();
    String[] lineTokens;
    int value;
    int vertexID;
    for (String line : results) {
      lineTokens = LINE_TOKEN_SEPARATOR.split(line);
      vertexID = Integer.parseInt(lineTokens[0]);
      value = Integer.parseInt(lineTokens[1]);
      if (value == 0) {
        countVerticesPartitionZero++;
      }
      parsedResults.put(vertexID, value);
    }
    return parsedResults;
  }
}
