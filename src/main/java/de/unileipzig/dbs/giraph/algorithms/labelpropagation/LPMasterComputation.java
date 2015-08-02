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

import org.apache.giraph.master.DefaultMasterCompute;

/**
 * Master Computation for {@link LPComputation}.
 *
 * Halts the computation after a given number if iterations.
 *
 * @author Kevin Gomez (k.gomez@freenet.de)
 * @author Martin Junghanns (junghanns@informatik.uni-leipzig.de)
 */
public class LPMasterComputation extends DefaultMasterCompute {
  /**
   * {@inheritDoc}
   */
  @Override
  public void compute() {
    int iterations = getConf().getInt(LPComputation.NUMBER_OF_ITERATIONS,
      LPComputation.DEFAULT_NUMBER_OF_ITERATIONS);
    if (getSuperstep() == iterations) {
      haltComputation();
    }
  }
}
