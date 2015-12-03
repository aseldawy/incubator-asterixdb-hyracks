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
package org.apache.hyracks.algebricks.core.algebra.operators.physical;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator.JoinKind;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.dataflow.std.sjoin.PlaneSweepJoinOperatorDescriptor;

/**
 * The physical operator of the spatial join operator. This operator applies
 * the plane-sweep algorithm implemented in the {@link PlaneSweepJoinOperatorDescriptor}
 * This operator assumes that data has been already partitioned using a uniform
 * grid partitioner and just applies the plane-sweep algorithm in each grid cell (partition).
 * 
 * @author Ahmed Eldawy
 */
public class PlaneSweepPOperator extends AbstractJoinPOperator {

    private LogicalVariable leftGeomCol;
    private LogicalVariable rightGeomCol;

    /**
     * @param leftGeomCol
     *            The geometry column on the left dataset
     * @param rightGeomCol
     *            The geometry column on the right dataset
     */
    public PlaneSweepPOperator(LogicalVariable leftGeomCol, LogicalVariable rightGeomCol) {
        // For now, we only support inner join.
        // PBSM is a pairwise join where each grid cell from the first dataset
        // is joined with the corresponding
        super(JoinKind.INNER, JoinPartitioningType.PAIRWISE);
        this.leftGeomCol = leftGeomCol;
        this.rightGeomCol = rightGeomCol;
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.PBSM_SPATIAL_JOIN;
    }

    @Override
    public boolean isMicroOperator() {
        return false;
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {
        // TODO Auto-generated method stub

    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
                    throws AlgebricksException {
        // TODO Auto-generated method stub
        IVariableTypeEnvironment env = context.getTypeEnvironment(op);
        Object leftGeomType = env.getVarType(leftGeomCol);
        Object rightGeomType = env.getVarType(rightGeomCol);

    }

}
