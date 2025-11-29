package com.streamlite.broker.consumer;

import com.streamlite.broker.consumer.dto.ConsumerState;
import com.streamlite.broker.consumer.dto.session.LivePartition;
import com.streamlite.broker.consumer.dto.session.LiveUser;

import java.util.concurrent.ConcurrentHashMap;

public class ConsumerConnections {

    // Store session → assigned partition info
    public static ConcurrentHashMap<String, LivePartition> activeSessions = new ConcurrentHashMap<>();
    // Store userId → live user
    public static ConcurrentHashMap<String, LiveUser> activeUsers = new ConcurrentHashMap<>();
    // Store user;topic;consumer → consumer → state
    public static ConcurrentHashMap<String, ConsumerState> consumerStateMap = new ConcurrentHashMap<>();

}
