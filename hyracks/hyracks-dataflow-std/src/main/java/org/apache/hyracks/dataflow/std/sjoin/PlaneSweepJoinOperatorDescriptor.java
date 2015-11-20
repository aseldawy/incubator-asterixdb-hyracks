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

import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;

/**
 * This Hyracks operator performs a spatial join between two inputs using the
 * plane-sweep spatial join algorithm as described in:
 * Edwin H. Jacox and Hanan Samet. 2007. Spatial join techniques.
 * ACM Trans. Database Syst. 32, 1, Article 7 (March 2007).
 * DOI=http://dx.doi.org/10.1145/1206049.1206056
 * 
 * @author Ahmed Eldawy
 */
public class PlaneSweepJoinOperatorDescriptor extends AbstractOperatorDescriptor {
	private static final int JOIN_ACTIVITY_ID = 0;

	private static final long serialVersionUID = 7908488449729834977L;

	public PlaneSweepJoinOperatorDescriptor(IOperatorDescriptorRegistry spec) {
		super(spec, 2, 1);
	}

	@Override
	public void contributeActivities(IActivityGraphBuilder builder) {
		ActivityId joinId = new ActivityId(getOperatorId(), JOIN_ACTIVITY_ID);
		PlaneSweepJoinActivityNode joinActivity = new PlaneSweepJoinActivityNode(joinId);
		
		builder.addActivity(this, joinActivity);
        builder.addSourceEdge(0, joinActivity, 0);
        builder.addSourceEdge(1, joinActivity, 1);
        builder.addTargetEdge(0, joinActivity, 0);
	}

}
