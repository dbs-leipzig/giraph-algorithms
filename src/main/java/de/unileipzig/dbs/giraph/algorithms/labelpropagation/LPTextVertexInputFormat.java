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

import com.google.common.collect.Lists;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Reads an {@link LPVertexValue} from the following format:
 * <p/>
 * {@code <vertex-id> [<neighbour-id>]*}
 *
 * @author Kevin Gomez (k.gomez@freenet.de)
 * @author Martin Junghanns (junghanns@informatik.uni-leipzig.de)
 */
public class LPTextVertexInputFormat extends
  TextVertexInputFormat<LongWritable, LPVertexValue, NullWritable> {
  /**
   * Separator of the vertex and neighbors
   */
  private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

  /**
   * {@inheritDoc}
   */
  @Override
  public TextVertexReader createVertexReader(InputSplit split,
    TaskAttemptContext context) throws IOException {
    return new VertexReader();
  }

  /**
   * Reads a vertex with two values from an input line.
   */
  public class VertexReader extends
    TextVertexReaderFromEachLineProcessed<String[]> {
    /**
     * Vertex id for the current line.
     */
    private int id;
    /**
     * Initial vertex last community.
     */
    private long lastCommunity = Long.MAX_VALUE;
    /**
     * Initial vertex current community. This will be set to the vertex id.
     */
    private long currentCommunity;
    /**
     * Vertex stabilization round.
     */
    private long stabilizationRound = 0;

    /**
     * {@inheritDoc}
     */
    @Override
    protected String[] preprocessLine(Text line) throws IOException {
      String[] tokens = SEPARATOR.split(line.toString());
      id = Integer.parseInt(tokens[0]);
      currentCommunity = id;
      return tokens;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected LongWritable getId(String[] tokens) throws IOException {
      return new LongWritable(id);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected LPVertexValue getValue(String[] tokens) throws IOException {
      return new LPVertexValue(currentCommunity, lastCommunity,
        stabilizationRound);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Iterable<Edge<LongWritable, NullWritable>> getEdges(
      String[] tokens) throws IOException {
      List<Edge<LongWritable, NullWritable>> edges = Lists.newArrayList();
      for (int n = 2; n < tokens.length; n++) {
        edges
          .add(EdgeFactory.create(new LongWritable(Long.parseLong(tokens[n]))));
      }
      return edges;
    }
  }
}
