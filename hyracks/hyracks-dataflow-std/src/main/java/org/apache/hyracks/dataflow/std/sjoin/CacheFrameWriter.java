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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;

/**
 * A frame writer that caches all frames in memory and makes them available
 * for later use.
 * @author Ahmed Eldawy
 *
 */
public class CacheFrameWriter implements IFrameWriter {
	/**All cached frames*/
	protected List<ByteBuffer> cachedFrames;
	/**Hyracks context of the running job*/
	private IHyracksTaskContext ctx;
	/**The CacheFrameWriter notifies this when the it is closed*/
	private Notifiable notifiable;
	
	public CacheFrameWriter(IHyracksTaskContext ctx, Notifiable notifiable) {
		this.ctx = ctx;
		this.notifiable = notifiable;
	}

	@Override
	public void open() throws HyracksDataException {
		// Initialize the in-memory store that will be used to store frames
		cachedFrames = new ArrayList<ByteBuffer>();
	}

	@Override
	public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
		// TODO Auto-generated method stub
		// Store this buffer in memory for later use
		ByteBuffer copyBuffer = ctx.allocateFrame(buffer.capacity());
        FrameUtils.copyAndFlip(buffer, copyBuffer);
        cachedFrames.add(copyBuffer);
	}

	@Override
	public void fail() throws HyracksDataException {
		cachedFrames = null; // To prevent further insertions
	}

	@Override
	public void close() throws HyracksDataException {
		// Notify its creator that it has been closed
		notifiable.notify(this);
	}

}
