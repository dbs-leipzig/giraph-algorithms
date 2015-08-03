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

import com.google.common.collect.Lists;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Reads an {@link ARPVertexValue} from the following format:
 * <p/>
 * {@code <vertex-id> <vertex-partition> [<neighbour-id>]*}
 * <p/>
 * e.g. the following line:
 * <p/>
 * 5 3 4 6 7
 * <p/>
 * decodes vertex-id 5, the actual partition 3. The node is connected to
 * three other nodes (4 6 7).
 * <p/>
 * If the config parameter "partitioning.input.partitioned" is set to false
 * (default), this input format also except a simple adjacency list in the
 * following format:
 * <p/>
 * {@code <vertex-id> [<neighbour-id>]*}
 *
 * @author Kevin Gomez (k.gomez@freenet.de)
 * @author Martin Junghanns (junghanns@informatik.uni-leipzig.de)
 */
public class ARPTextVertexInputFormat extends
  TextVertexInputFormat<IntWritable, ARPVertexValue, NullWritable> {

  /**
   * Used to tell the input format if the input graph is already partitioned.
   */
  public static final String PARTITIONED_INPUT =
    "partitioning.input.partitioned";

  /**
   * Default value for PARTITIONED_INPUT.
   */
  public static final boolean DEFAULT_PARTITIONED_INPUT = false;

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
    return new TwoValueVertexReader();
  }

  /**
   * Reads a vertex with two values from an input line.
   */
  public class TwoValueVertexReader extends
    TextVertexReaderFromEachLineProcessed<String[]> {
    /**
     * Edge offset for partitioned graph inputs.
     */
    private static final int PARTITIONED_EDGE_OFFSET = 2;

    /**
     * Edge offset for un-partitioned graph inputs.
     */
    private static final int UNPARTITIONED_EDGE_OFFSET = 1;

    /**
     * If the graph is partitioned, the input contains values for current and
     * last vertex values. In that case, the offset for edges has to be
     * adapted.
     */
    private int edgeOffset;

    /**
     * If true, the reader assumes that a single line contains
     * <p/>
     * {@code <vertex-id> <vertex-current-partition> [<neighbour-id>]*}
     * <p/>
     * If false, the reader assumes that a single line contains just
     * <p/>
     * {@code <vertex-id> [<neighbour-id>]*}
     */
    private boolean isPartitioned;

    /**
     * Cached vertex id for the current line
     */
    private int id;

    /**
     * Cached vertex current Partition
     */
    private int currentPartition = 0;

    /**
     * Cached vertex desired Partition
     */
    private int desiredPartition = 0;

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(InputSplit inputSplit,
      TaskAttemptContext context) throws IOException, InterruptedException {
      super.initialize(inputSplit, context);
      this.isPartitioned =
        getConf().getBoolean(PARTITIONED_INPUT, DEFAULT_PARTITIONED_INPUT);
      // if the input graph is partitioned (contains two vertex values), the
      // the edges start at offset 2
      if (this.isPartitioned) {
        edgeOffset = PARTITIONED_EDGE_OFFSET;
      } else {
        edgeOffset = UNPARTITIONED_EDGE_OFFSET;
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String[] preprocessLine(Text line) throws IOException {
      String[] tokens = SEPARATOR.split(line.toString());
      id = Integer.parseInt(tokens[0]);
      if (this.isPartitioned) {
        currentPartition = Integer.parseInt(tokens[1]);
      }
      return tokens;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected IntWritable getId(String[] tokens) throws IOException {
      return new IntWritable(id);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected ARPVertexValue getValue(String[] tokens) throws IOException {
      ARPVertexValue vertex = new ARPVertexValue();
      vertex.setCurrentPartition(new IntWritable(currentPartition));
      vertex.setDesiredPartition(new IntWritable(desiredPartition));
      return vertex;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Iterable<Edge<IntWritable, NullWritable>> getEdges(
      String[] tokens) throws IOException {
      List<Edge<IntWritable, NullWritable>> edges =
        Lists.newArrayListWithCapacity(tokens.length - this.edgeOffset);
      for (int n = this.edgeOffset; n < tokens.length; n++) {
        edges.add(
          EdgeFactory.create(new IntWritable(Integer.parseInt(tokens[n]))));
      }
      return edges;
    }
  }
}
