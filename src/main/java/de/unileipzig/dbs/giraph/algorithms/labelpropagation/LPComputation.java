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
 * along with giraph-algorithms.  If not, see <http://www.gnu.org/licenses/>.
 */

package de.unileipzig.dbs.giraph.algorithms.labelpropagation;

import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Label Propagation is used to detect communities inside a graph.
 * <p/>
 * Each vertex stores a unique id, a value (its label) and its outgoing edges.
 * The label represents the community that the vertex belongs to. In
 * superstep 0, each vertex propagates its initial value to its neighbours.
 * In the remaining supersteps, each vertex will adopt the value of the
 * majority of their neighbors or the smallest one if there is only one
 * neighbor. If a vertex adopts a new value, it will propagate the new value
 * to its neighbours. The computation will terminate if no new values are
 * assigned or the maximum number of iterations is reached.
 *
 * @author Kevin Gomez (k.gomez@freenet.de)
 * @author Martin Junghanns (junghanns@informatik.uni-leipzig.de)
 * @see <a href="https://en.wikipedia
 * .org/wiki/Label_Propagation_Algorithm">Wikipedia (Label Propagation)</a>
 */
public class LPComputation extends
  BasicComputation<LongWritable, LPVertexValue, NullWritable, LongWritable> {
  /**
   * Defines the maximum number of vertex migrations.
   */
  public static final String NUMBER_OF_ITERATIONS =
    "labelpropagation.numberofiterations";
  /**
   * Default number of vertex migrations if no value is given.
   */
  public static final int DEFAULT_NUMBER_OF_ITERATIONS = 50;
  /**
   * Number of iterations a vertex needs to be stable (i.e., did not migrate).
   */
  public static final String STABILISATION_ROUNDS =
    "labelpropagation.stabilizationrounds";
  /**
   * Default number of stabilization rounds.
   */
  public static final long DEFAULT_NUMBER_OF_STABILIZATION_ROUNDS = 20;
  /**
   * Stabilization rounds.
   */
  private long stabilizationRounds;

  /**
   * Returns the current new value. This value is based on all incoming
   * messages. Depending on the number of messages sent to the vertex, the
   * method returns:
   * <p/>
   * 0 messages:   The current value
   * <p/>
   * 1 message:    The minimum of the message and the current vertex value
   * <p/>
   * >1 messages:  The most frequent of all message values
   *
   * @param vertex   The current vertex
   * @param messages All incoming messages
   * @return the new Value the vertex will become
   */
  private long getNewCommunity(
    Vertex<LongWritable, LPVertexValue, NullWritable> vertex,
    Iterable<LongWritable> messages) {
    long newCommunity;
    //TODO: create allMessages more efficient
    //List<LongWritable> allMessages = Lists.newArrayList(messages);
    List<Long> allMessages = new ArrayList<>();
    for (LongWritable message : messages) {
      allMessages.add(message.get());
    }
    if (allMessages.isEmpty()) {
      // 1. if no messages are received
      newCommunity = vertex.getValue().getCurrentCommunity().get();
    } else if (allMessages.size() == 1) {
      // 2. if just one message are received
      newCommunity = Math
        .min(vertex.getValue().getCurrentCommunity().get(), allMessages.get(0));
    } else {
      // 3. if multiple messages are received
      newCommunity = getMostFrequent(vertex, allMessages);
    }
    return newCommunity;
  }

  /**
   * Returns the most frequent value among all received messages.
   *
   * @param vertex      The current vertex
   * @param allMessages All messages the current vertex has received
   * @return the maximal frequent number in all received messages
   */
  private long getMostFrequent(
    Vertex<LongWritable, LPVertexValue, NullWritable> vertex,
    List<Long> allMessages) {
    Collections.sort(allMessages);
    long newValue;
    int currentCounter = 1;
    long currentValue = allMessages.get(0);
    int maxCounter = 1;
    long maxValue = 1;
    for (int i = 1; i < allMessages.size(); i++) {
      if (currentValue == allMessages.get(i)) {
        currentCounter++;
        if (maxCounter < currentCounter) {
          maxCounter = currentCounter;
          maxValue = currentValue;
        }
      } else {
        currentCounter = 1;
        currentValue = allMessages.get(i);
      }
    }
    // if the frequency of all received messages is one
    if (maxCounter == 1) {
      // to avoid an oscillating state we use the smaller value
      newValue = Math
        .min(vertex.getValue().getCurrentCommunity().get(), allMessages.get(0));
    } else {
      newValue = maxValue;
    }
    return newValue;
  }

  @Override
  public void initialize(GraphState graphState,
    WorkerClientRequestProcessor<LongWritable, LPVertexValue, NullWritable>
      workerClientRequestProcessor,
    GraphTaskManager<LongWritable, LPVertexValue, NullWritable>
      graphTaskManager,
    WorkerGlobalCommUsage workerGlobalCommUsage, WorkerContext workerContext) {
    super.initialize(graphState, workerClientRequestProcessor, graphTaskManager,
      workerGlobalCommUsage, workerContext);
    this.stabilizationRounds = getConf()
      .getLong(STABILISATION_ROUNDS, DEFAULT_NUMBER_OF_STABILIZATION_ROUNDS);
  }

  /**
   * The actual LabelPropagation Computation
   *
   * @param vertex   Vertex
   * @param messages Messages that were sent to this vertex in the previous
   *                 superstep.
   * @throws IOException
   */
  @Override
  public void compute(Vertex<LongWritable, LPVertexValue, NullWritable> vertex,
    Iterable<LongWritable> messages) throws IOException {
    if (getSuperstep() == 0) {
      sendMessageToAllEdges(vertex, vertex.getValue().getCurrentCommunity());
    } else {
      long currentCommunity = vertex.getValue().getCurrentCommunity().get();
      long lastCommunity = vertex.getValue().getLastCommunity().get();
      long newCommunity = getNewCommunity(vertex, messages);
      long currentStabilizationRound =
        vertex.getValue().getStabilizationRounds();

      // increment the stabilization count if vertex wants to stay in the
      // same partition
      if (lastCommunity == newCommunity) {
        currentStabilizationRound++;
        vertex.getValue().setStabilizationRounds(currentStabilizationRound);
      }

      boolean isUnstable = currentStabilizationRound <= stabilizationRounds;
      boolean mayChange = currentCommunity != newCommunity;
      if (mayChange && isUnstable) {
        vertex.getValue().setLastCommunity(new LongWritable(currentCommunity));
        vertex.getValue().setCurrentCommunity(new LongWritable(newCommunity));
        // reset stabilization counter
        vertex.getValue().setStabilizationRounds(0);
        sendMessageToAllEdges(vertex, vertex.getValue().getCurrentCommunity());
      }
    }
    vertex.voteToHalt();
  }
}
