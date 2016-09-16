/*
 * Copyright 2012-2016 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.benchmarks;

import com.aerospike.client.*;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.large.LargeList;
import com.aerospike.client.large.LargeStack;

import java.util.HashMap;
import java.util.Map;

import static com.aerospike.benchmarks.MapBenchStuff.emptyValue;
import static com.aerospike.benchmarks.MapBenchStuff.mapPolicy;


public final class InsertTaskSync extends InsertTask {

	private final AerospikeClient client;


	public InsertTaskSync(AerospikeClient client, Arguments args, CounterStore counters, long keyStart, long keyCount) {
		super(args, counters, keyStart, keyCount);
		this.client = client;
	}

	protected void put(Key key, Bin[] bins) throws AerospikeException {
		if (counters.write.latency != null) {
			long begin = System.nanoTime();
			Map<Value, Value> items = new HashMap<Value, Value>();
			for (Bin bin: bins) {
				items.put(bin.value, emptyValue);
			}
            client.operate(args.writePolicy, key, MapOperation.putItems(mapPolicy, bins[0].name, items));
			//client.put(args.writePolicy, key, bins);
			long elapsed = System.nanoTime() - begin;
			counters.write.count.getAndIncrement();
			counters.write.latency.add(elapsed);
		}
		else {
			Map<Value, Value> items = new HashMap<Value, Value>();
			for (Bin bin: bins) {
				items.put(bin.value, emptyValue);
			}
            client.operate(args.writePolicy, key, MapOperation.putItems(mapPolicy, bins[0].name, items));
			//client.put(args.writePolicy, key, bins);
			counters.write.count.getAndIncrement();
		}
	}

	protected void largeListAdd(Key key, Value value) throws AerospikeException {
		long begin = System.nanoTime();
		if (counters.write.latency != null) {
			largeListAdd(key, value, begin);
			long elapsed = System.nanoTime() - begin;
			counters.write.count.getAndIncrement();
			counters.write.latency.add(elapsed);
		}
		else {
			largeListAdd(key, value, begin);
			counters.write.count.getAndIncrement();
		}
	}

	private void largeListAdd(Key key, Value value, long timestamp) throws AerospikeException {
		// Create entry
		Map<String,Value> entry = new HashMap<String,Value>();
		entry.put("key", Value.get(timestamp));
		entry.put("log", value);

		// Add entry
		LargeList list = client.getLargeList(args.writePolicy, key, "listltracker");
		list.add(Value.get(entry));
	}

	protected void largeStackPush(Key key, Value value) throws AerospikeException {
		long begin = System.nanoTime();
		if (counters.write.latency != null) {
			largeStackPush(key, value, begin);
			long elapsed = System.nanoTime() - begin;
			counters.write.count.getAndIncrement();
			counters.write.latency.add(elapsed);
		}
		else {
			largeStackPush(key, value, begin);
			counters.write.count.getAndIncrement();
		}
	}

	private void largeStackPush(Key key, Value value, long timestamp) throws AerospikeException {
		// Create entry
		Map<String,Value> entry = new HashMap<String,Value>();
		entry.put("key", Value.get(timestamp));
		entry.put("log", value);

		// Push entry
		LargeStack lstack = client.getLargeStack(args.writePolicy, key, "stackltracker", null);
		lstack.push(Value.get(entry));
	}
}
