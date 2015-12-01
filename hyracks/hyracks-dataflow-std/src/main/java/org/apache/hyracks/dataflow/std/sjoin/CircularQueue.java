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

/**
 * A fixed size circular queue used to store objects with a maximum allowed capacity
 * 
 * @author Ahmed Eldawy
 */
public class CircularQueue<E> {

    /**
     * Points to the head of the queue in the array. If the queue is empty,
     * this value is set to -1
     */
    private int head;

    /**
     * Points to the position of the last element in the queue.
     */
    private int tail;

    /** Data elements in the queue */
    private Object[] elements;

    public CircularQueue(int capacity) {
        this.elements = new Object[capacity];
        head = -1; // Initially empty
    }

    public int size() {
        if (head == -1)
            return 0;
        return tail >= head ? (tail - head + 1) : (tail + elements.length - head + 1);
    }

    public boolean isEmpty() {
        return head == -1;
    }

    public void add(E e) {
        if (isFull())
            throw new ArrayIndexOutOfBoundsException(elements.length);
        if (head == -1) {
            // First element
            elements[head = tail = 0] = e;
        } else {
            tail = (tail + 1) % elements.length;
            elements[tail] = e;
        }
    }

    public void removeFirstN(int n) {
        if (size() <= n)
            head = -1;
        else
            head = (head + n) % elements.length;
    }

    /**
     * Returns the ith element in the queue in the queue order.
     * 
     * @param i
     * @return
     */
    public E get(int i) {
        if (i >= size())
            throw new ArrayIndexOutOfBoundsException(i);
        return (E) elements[(head + i) % elements.length];
    }

    public boolean isFull() {
        return size() == elements.length;
    }

    @Override
    public String toString() {
        String str = "[";
        for (int i = 0; i < size(); i++) {
            if (i != 0)
                str += ", ";
            str += get(i);
        }
        str += "]";
        return str;
    }

    public void clear() {
        for (int i = 0; i < elements.length; i++)
            elements[i] = null; // For the GC to collect these objects
        this.head = -1;
    }
}
