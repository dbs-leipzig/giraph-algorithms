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

import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;

/**
 * Master computation for {@link ARPComputation}.
 *
 * @author Kevin Gomez (gomez@studserv.uni-leipzig.de)
 * @author Martin Junghanns (junghanns@informatik.uni-leipzig.de)
 */
public class ARPMasterComputation extends DefaultMasterCompute {
  /**
   * Creates capacity and demand aggregators for each partition.
   *
   * @throws IllegalAccessException
   * @throws InstantiationException
   */
  @Override
  public void initialize() throws IllegalAccessException,
    InstantiationException {
    int k = getConf().getInt(ARPComputation.NUMBER_OF_PARTITIONS,
      ARPComputation.DEFAULT_NUMBER_OF_PARTITIONS);
    for (int i = 0; i < k; i++) {
      registerAggregator(ARPComputation.DEMAND_AGGREGATOR_PREFIX + i, LongSumAggregator
        .class);
      registerPersistentAggregator(ARPComputation.CAPACITY_AGGREGATOR_PREFIX + i, LongSumAggregator.class);
    }
  }

  /**
   * Stops computation if maximum number of iterations is reached.
   */
  @Override
  public void compute() {
    int iterations = getConf().getInt(ARPComputation.NUMBER_OF_ITERATIONS,
      ARPComputation.DEFAULT_NUMBER_OF_ITERATIONS);
    if (getSuperstep() == iterations) {
      haltComputation();
    }
  }
}
