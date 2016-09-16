package com.aerospike.benchmarks;

import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapWriteMode;

public class MapBenchStuff {

    public static final MapPolicy mapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE);
    public static final Value emptyValue = new Value.NullValue();

    public static String getKey(long key) {
        return String.format("%032d", key);
    }
}
