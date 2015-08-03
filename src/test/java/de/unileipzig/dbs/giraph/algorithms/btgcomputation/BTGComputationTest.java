package de.unileipzig.dbs.giraph.algorithms.btgcomputation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static de.unileipzig.dbs.giraph.algorithms.btgcomputation
  .BTGComputationTestHelper.*;

/**
 * Tests for {@link BTGComputation}.
 */
public class BTGComputationTest {
  private static final Pattern LINE_TOKEN_SEPARATOR = Pattern.compile("[,]");

  private static final Pattern VALUE_TOKEN_SEPARATOR = Pattern.compile("[ ]");

  @Test
  public void testConnectedIIG() throws Exception {
    String[] graph = BTGComputationTestHelper.getConnectedIIG();
    validateConnectedIIGResult(computeResults(graph));
  }

  @Test
  public void testDisconnectedIIG() throws Exception {
    String[] graph = BTGComputationTestHelper.getDisconnectedIIG();
    validateDisconnectedIIGResult(computeResults(graph));
  }

  @Test
  public void testSingleMasterVertex() throws Exception {
    String[] graph = BTGComputationTestHelper.getSingleMasterVertexIIG();
    validateSingleMasterVertexIIGResult(computeResults(graph));
  }

  @Test
  public void testSingleTransactionalVertex() throws Exception {
    String[] graph = BTGComputationTestHelper.getSingleTransactionalVertexIIG();
    validateSingleTransactionalVertexIIGResult(computeResults(graph));
  }

  @Test
  public void testSingleMasterVertexWithBTG() throws Exception {
    String[] graph = BTGComputationTestHelper.getSingleMasterVertexIIGWithBTG();
    validateSingleMasterVertexIIGResult(computeResults(graph));
  }

  @Test
  public void testSingleTransactionalVertexWithBTG() throws Exception {
    String[] graph =
      BTGComputationTestHelper.getSingleTransactionalVertexIIGWithBTG();
    validateSingleTransactionalVertexIIGResult(computeResults(graph));
  }

  private Map<Long, List<Long>> computeResults(String[] graph) throws
    Exception {
    GiraphConfiguration conf = getConfiguration();
    Iterable<String> results = InternalVertexRunner.run(conf, graph);
    return parseResults(results);
  }

  private GiraphConfiguration getConfiguration() {
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(BTGComputation.class);
    conf.setVertexInputFormatClass(BTGTextVertexInputFormat.class);
    conf.setVertexOutputFormatClass(BTGTextVertexOutputFormat.class);
    return conf;
  }

  private Map<Long, List<Long>> parseResults(Iterable<String> results) {
    Map<Long, List<Long>> parsedResults = Maps.newHashMap();

    String[] lineTokens, valueTokens;
    List<Long> values;
    Long vertexID;

    for (String line : results) {
      lineTokens = LINE_TOKEN_SEPARATOR.split(line);
      vertexID = Long.parseLong(lineTokens[0]);
      valueTokens = VALUE_TOKEN_SEPARATOR.split(lineTokens[1]);
      values = Lists.newArrayListWithCapacity(lineTokens.length - 2);
      for (int i = 2; i < valueTokens.length; i++) {
        values.add(Long.parseLong(valueTokens[i]));
      }
      parsedResults.put(vertexID, values);
    }
    return parsedResults;
  }
}
