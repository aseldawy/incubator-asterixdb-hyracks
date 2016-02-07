/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.dataflow.std.sjoin;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;

/**
 * An interface for spatial partitioning of records according to some spatial
 * partitioning function.
 * 
 * @author Ahmed Eldawy
 */
public interface ISpatialPartitioner {

    /**
     * Find all matching cells for a given tuple. The given array is filled in with the IDs
     * of all matching cells. If the number of matching cells exceeds the capacity of the given
     * array, the correct number of matching cells is returned and the value of the given
     * array is indeterministic.
     * 
     * @param tuples
     *            Accessor for tuples to be matched
     * @param index
     *            The index of the tuple to be matched
     * @param matchingCells
     *            Outputs the list of cell IDs matched by the tuple.
     * @return
     */
    public int getMatchingCells(IFrameTupleAccessor tuples, int index, int[] matchingCells);

    /**
     * Returns the boundaries of a cell given its ID.
     * 
     * @param cellID
     * @param boundaries
     */
    public void getCellBoundary(int cellID, int[] boundaries);
}
