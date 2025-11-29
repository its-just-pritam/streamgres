package com.streamlite.broker.partition.builder;

import java.util.ArrayList;
import java.util.HashMap;

public class NullPartitionBuilder extends BasePartitionBuilder {

    public NullPartitionBuilder() {
        builderQueries = new HashMap<>();
        rollbackQueries = new HashMap<>();
    }

}
