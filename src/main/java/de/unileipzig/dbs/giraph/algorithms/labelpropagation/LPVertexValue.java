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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Custom vertex used by {@link LPComputation}.
 *
 * @author Kevin Gomez (k.gomez@freenet.de)
 * @author Martin Junghanns (junghanns@informatik.uni-leipzig.de)
 */
public class LPVertexValue implements Writable {
  /**
   * The desired partition the vertex want to migrate to.
   */
  private long currentCommunity;
  /**
   * The actual partition.
   */
  private long lastCommunity;
  /**
   * Iterations since last migration.
   */
  private long stabilizationRounds;

  /**
   * Default Constructor
   */
  public LPVertexValue() {
  }

  /**
   * Constructor
   *
   * @param currentCommunity    currentCommunity
   * @param lastCommunity       lastCommunity
   * @param stabilizationRounds stabilizationRounds
   */
  public LPVertexValue(long currentCommunity, long lastCommunity,
    long stabilizationRounds) {
    this.currentCommunity = currentCommunity;
    this.lastCommunity = lastCommunity;
    this.stabilizationRounds = stabilizationRounds;
  }

  /**
   * Method to set the current partition
   *
   * @param lastCommunity current partition
   */
  public void setLastCommunity(LongWritable lastCommunity) {
    this.lastCommunity = lastCommunity.get();
  }

  /**
   * Method to set the lastValue of the vertex
   *
   * @param currentCommunity the desired Partition
   */
  public void setCurrentCommunity(LongWritable currentCommunity) {
    this.currentCommunity = currentCommunity.get();
  }

  /**
   * Method to set the stabilization round counter of the vertex
   *
   * @param stabilizationRounds counter
   */
  public void setStabilizationRounds(long stabilizationRounds) {
    this.stabilizationRounds = stabilizationRounds;
  }

  /**
   * Get method to get the desired partition
   *
   * @return the desired Partition
   */
  public LongWritable getCurrentCommunity() {
    return new LongWritable(this.currentCommunity);
  }

  /**
   * Get the current partition
   *
   * @return the current partition
   */
  public LongWritable getLastCommunity() {
    return new LongWritable(this.lastCommunity);
  }

  /**
   * Method to get the stabilization round counter
   *
   * @return the actual counter
   */
  public long getStabilizationRounds() {
    return stabilizationRounds;
  }

  /**
   * Serializes the content of the vertex object.
   *
   * @param dataOutput data to be serialized
   * @throws IOException
   */
  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeLong(this.currentCommunity);
    dataOutput.writeLong(this.lastCommunity);
  }

  /**
   * Deserializes the content of the vertex object.
   *
   * @param dataInput data to be deserialized
   * @throws IOException
   */
  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.currentCommunity = dataInput.readLong();
    this.lastCommunity = dataInput.readLong();
  }
}
