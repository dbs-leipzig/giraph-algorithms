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

package de.unileipzig.dbs.giraph.algorithms.btgcomputation;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Encodes the output of the {@link BTGComputation} in the following format:
 * <p/>
 * vertex-id,vertex-class vertex-value[ btg-id]*
 * <p/>
 * e.g. the following line
 * <p/>
 * 0,0 3.14 23 42
 * <p/>
 * decodes vertex-id 0 with vertex-class 0 (0 = transactional, 1 = master)
 * and the value 3.14. The node is connected to two BTGs (23, 42).
 *
 * @author Martin Junghanns (junghanns@informatik.uni-leipzig.de)
 */
public class BTGTextVertexOutputFormat extends
  TextVertexOutputFormat<LongWritable, BTGVertexValue, NullWritable> {

  /**
   * Used for splitting the line into the main tokens (vertex id, vertex value,
   * edges)
   */
  private static final String LINE_TOKEN_SEPARATOR = ",";

  /**
   * Used for splitting a main token into its values (vertex value = type,
   * value, btg-ids; edge list)
   */
  private static final String VALUE_TOKEN_SEPARATOR = " ";

  /**
   * @param context the information about the task
   * @return the text vertex writer to be used
   * @throws java.io.IOException
   * @throws InterruptedException
   */
  @Override
  public TextVertexWriter createVertexWriter(TaskAttemptContext context) throws
    IOException, InterruptedException {
    return new BTGTextVertexLineWriter();
  }

  /**
   * Used to convert a {@link BTGVertexValue} to a line in the output file.
   */
  private class BTGTextVertexLineWriter extends TextVertexWriterToEachLine {

    /**
     * Writes a line for the given vertex.
     *
     * @param vertex the current vertex for writing
     * @return the text line to be written
     * @throws java.io.IOException exception that can be thrown while writing
     */
    @Override
    protected Text convertVertexToLine(
      Vertex<LongWritable, BTGVertexValue, NullWritable> vertex) throws
      IOException {
      StringBuilder sb = new StringBuilder();
      // vertex-id
      sb.append(vertex.getId());
      sb.append(LINE_TOKEN_SEPARATOR);
      // vertex-value (=vertex-class, vertex value and btg-ids)
      sb.append(vertex.getValue().getVertexType().ordinal());
      sb.append(VALUE_TOKEN_SEPARATOR);
      sb.append(vertex.getValue().getVertexValue());
      for (Long btgID : vertex.getValue().getGraphs()) {
        sb.append(VALUE_TOKEN_SEPARATOR);
        sb.append(btgID);
      }
      return new Text(sb.toString());
    }
  }
}