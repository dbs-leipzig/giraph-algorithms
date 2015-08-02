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

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Encodes the output of the {@link LPComputation} in the following format:
 * <p/>
 * {@code <vertex-id> <community-id> [<neighbour-id>]*}
 *
 * @author Kevin Gomez (k.gomez@freenet.de)
 * @author Martin Junghanns (junghanns@informatik.uni-leipzig.de)
 */
public class LPTextVertexOutputFormat extends
  TextVertexOutputFormat<LongWritable, LPVertexValue, NullWritable> {
  /**
   * Used for splitting the line into the main tokens (vertex id, vertex value
   */
  private static final String VALUE_TOKEN_SEPARATOR = " ";

  /**
   * {@inheritDoc}
   */
  @Override
  public TextVertexWriter createVertexWriter(TaskAttemptContext context) throws
    IOException, InterruptedException {
    return new LabelPropagationTextVertexLineWriter();
  }

  /**
   * Used to convert a {@link LPVertexValue} to a line in the output file.
   */
  private class LabelPropagationTextVertexLineWriter extends
    TextVertexWriterToEachLine {
    /**
     * {@inheritDoc}
     */
    @Override
    protected Text convertVertexToLine(
      Vertex<LongWritable, LPVertexValue, NullWritable> vertex) throws
      IOException {
      // vertex id
      StringBuilder sb = new StringBuilder(vertex.getId().toString());
      sb.append(VALUE_TOKEN_SEPARATOR);
      // vertex value
      sb.append(vertex.getValue().getCurrentCommunity().get());
      sb.append(VALUE_TOKEN_SEPARATOR);
      // edges
      for (Edge<LongWritable, NullWritable> e : vertex.getEdges()) {
        sb.append(e.getTargetVertexId());
        sb.append(VALUE_TOKEN_SEPARATOR);
      }
      return new Text(sb.toString());
    }
  }
}
