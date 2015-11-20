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

import java.io.Closeable;
import java.io.IOException;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorNodePushable;

public class PlaneSweepJoinActivityNode extends AbstractActivityNode implements Notifiable {
	private static final long serialVersionUID = 1433873095538085055L;
	
	/**The writer which is used to write output records*/
	protected IFrameWriter outputWriter;
	/**Descriptor of output records*/
	protected RecordDescriptor outputRecordDescriptor;
	
	/**A cached version of the two datasets*/
	protected CacheFrameWriter[] inputDatasets;
	
	/**Number of inputs that have been completely read (0, 1 or 2)*/
	protected int numInputsComplete;
	
	/**Hyracks context of the underlying job*/
	protected IHyracksTaskContext ctx;

	public PlaneSweepJoinActivityNode(ActivityId id) {
		super(id);
		inputDatasets = new CacheFrameWriter[2];
	}

	@Override
	public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
			IRecordDescriptorProvider recordDescProvider, int partition,
			int numPartitions)
					throws HyracksDataException {
		this.ctx = ctx;
		for (int i = 0; i < inputDatasets.length; i++)
			inputDatasets[i] = new CacheFrameWriter(ctx, this);
		
		IOperatorNodePushable op = new AbstractOperatorNodePushable() {
			@Override
			public void initialize() throws HyracksDataException {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void deinitialize() throws HyracksDataException {
				// TODO Auto-generated method stub
				
			}

			@Override
			public int getInputArity() {
				return 2;
			}

			@Override
			public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc)
					throws HyracksDataException {
				outputWriter = writer;
				// TODO Should I define the output record descriptor here or use
				// it as an input?
				outputRecordDescriptor = recordDesc;
			}

			@Override
			public IFrameWriter getInputFrameWriter(int index) {
				return inputDatasets[index];
			}
		};
		
		return op;
	}

	@Override
	public void notify(Object notified) throws HyracksDataException {
		// A notification that one of the inputs has been completely read
		numInputsComplete++;
		if (numInputsComplete == 2) {
			// The two inputs have been completely read. Do the join
			System.out.println("Join");
			outputWriter.open();
			outputWriter.close();
		}
	}
	
}
