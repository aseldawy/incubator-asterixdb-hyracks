/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.storage.am.common.test;

import java.util.Arrays;

public class TestOperationSelector {

    public static enum TestOperation {
        INSERT,
        DELETE,
        UPDATE,
        POINT_SEARCH,
        RANGE_SEARCH,
        ORDERED_SCAN,
        DISKORDER_SCAN,
        FLUSH,
        MERGE        
    }
    
    private final TestOperation[] ops;
    private final int[] opRanges;    
    
    public TestOperationSelector(TestOperation[] ops, float[] opProbs) {
        sanityCheck(ops, opProbs);
        this.ops = ops;
        this.opRanges = getOpRanges(opProbs);
    }
    
    private void sanityCheck(TestOperation[] ops, float[] opProbs) {
        if (ops.length == 0) {
            throw new RuntimeException("Empty op array.");
        }
        if (opProbs.length == 0) {
            throw new RuntimeException("Empty op probabilities.");
        }
        if (ops.length != opProbs.length) {
            throw new RuntimeException("Ops and op probabilities have unequal length.");
        }
        float sum = 0.0f;
        for (int i = 0; i < opProbs.length; i++) {
            sum += opProbs[i];
        }
        if (sum != 1.0f) {
            throw new RuntimeException("Op probabilities don't add up to 1.");
        }
    }
    
    private int[] getOpRanges(float[] opProbabilities) {
        int[] opRanges = new int[opProbabilities.length];
        if (opRanges.length > 1) {
            opRanges[0] = (int) Math.floor(Integer.MAX_VALUE * opProbabilities[0]);
            for (int i = 1; i < opRanges.length - 1; i++) {
                opRanges[i] = opRanges[i - 1] + (int) Math.floor(Integer.MAX_VALUE * opProbabilities[i]);
            }
            opRanges[opRanges.length - 1] = Integer.MAX_VALUE;
        } else {
            opRanges[0] = Integer.MAX_VALUE;
        }
        return opRanges;
    }
    
    public TestOperation getOp(int randomInt) {
        int ix = Arrays.binarySearch(opRanges, randomInt);
        if (ix < 0) {
            ix = -ix - 1;
        }
        return ops[ix];
    }
}