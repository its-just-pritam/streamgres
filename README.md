# **Streamgres**

A **centralized event-streaming broker** built on **TimescaleDB**, offering a lightweight and developer-friendly alternative to Kafka.

Streamgres provides:

* ⚡ **REST-based message ingestion**
* 🔄 **GRPC-based message streaming**
* 📦 **Topic management**
* 👥 **Consumer group management**
* 📨 **High-throughput event streaming**

> Built for simplicity — no client libraries, no heavy config. Just plug and stream.

---

# 🚀 **API Documentation**

## 🧩 **Base URL**

```
http://localhost:8080/user/{user}
```

Replace `{user}` with your user identifier.

---

## 📚 **Endpoints Overview**

| Category      | Endpoint                                      | Method | Description          |
| ------------- | --------------------------------------------- | ------ | -------------------- |
| **Topics**    | `/topic/{topicName}`                          | POST   | Create a topic       |
| Topics        | `/topic/{topicName}`                          | GET    | Get topic details    |
| Topics        | `/topic/list`                                 | GET    | List all topics      |
| Topics        | `/topic/{topicName}`                          | DELETE | Delete a topic       |
| **Consumers** | `/topic/{topicName}/consumer/{consumerGroup}` | POST   | Create a consumer    |
| Consumers     | `/topic/{topicName}/consumer/{consumerGroup}` | GET    | Get consumer details |
| Consumers     | `/topic/{topicName}/consumer/list`            | GET    | List consumer groups |

---

# 🗃️ **Topic APIs**

---

## ➕ **Create Topic**

**POST** `/topic/{topicName}`

### **Request Body**

```json
{
  "partitions": 4,
  "schemaDefinition": "{}",
  "cleanupPolicy": "delete",
  "type": "classic"
}
```

### **Example**

```
POST http://localhost:8080/user/{user}/topic/topic2
```

---

## 🔍 **Get Topic**

**GET** `/topic/{topicName}`

### **Example**

```
GET http://localhost:8080/user/{user}/topic/first_topic
```

---

## 📄 **List Topics**

**GET** `/topic/list`

### **Example**

```
GET http://localhost:8080/user/{user}/topic/list
```

---

## ❌ **Delete Topic**

**DELETE** `/topic/{topicName}`

### **Optional Query Parameters**

| Name   | Type    | Description           |
| ------ | ------- | --------------------- |
| `hard` | boolean | Hard delete the topic |

### **Example**

```
DELETE http://localhost:8080/user/{user}/topic/topic2
```

---

# 👥 **Consumer APIs**

---

## ➕ **Create Consumer**

**POST** `/topic/{topicName}/consumer/{consumerGroup}`

### **Example**

```
POST http://localhost:8080/user/{user}/topic/topic2/consumer/default
```

---

## 🔍 **Get Consumer**

**GET** `/topic/{topicName}/consumer/{consumerGroup}`

### **Example**

```
GET http://localhost:8080/user/{user}/topic/topic3/consumer/default
```

---

## 📄 **List Consumers**

**GET** `/topic/{topicName}/consumer/list`

### **Example**

```
GET http://localhost:8080/user/{user}/topic/topic1/consumer/list
```

---

# 🏃‍♂️ **Run Streamgres Locally**

> **Important Notes**
>
> * Use this setup only for **local development**
> * Ensure **Docker Desktop** is installed
> * Stop any locally running **PostgreSQL** instance
> * Ensure ports **8080**, **8081**, **9090** and **5432** are free

---

## 🔧 **Setup Instructions**

### 1. Clone the Repository

```bash
git clone https://github.com/its-just-pritam/streamgres.git
```

### 2. Navigate to Project Directory

```bash
cd streamgres
```

### 3. Build & Start the Stack

```bash
docker compose up --build -d
```

Wait until TimescaleDB and Streamgres broker become healthy.

🎉 **Streamgres is now LIVE!**

---

## 🗄️ Database UI

Open TimescaleDB Visualizer:

```
http://localhost:8081/
```
![Data Visualizer](https://github.com/its-just-pritam/streamgres/blob/main/data_viewer.png)

---

## 🛑 Stop & Clean All Data

```bash
docker compose down -v
```

Got it — here is a clean, polished **“Testing”** section you can drop into your README.

---

# 🧪 Testing

This section explains how to verify that Streamgres is running correctly and how to test the functionality. You can download the **Postman Collection** available in the code repo for:

- Creating Topic
- Creating Consumer
- Sending messages to the topic
- Joining a Consumer group
- Consuming messages

---

## ✅ 1. Verify Services Are Running

After starting Streamgres with Docker:

```bash
docker ps
```

Ensure the following containers are **healthy**:

| Service       | Port       | Purpose                   |
| ------------- |------------| ------------------------- |
| Streamgres    | 8080, 9090 | REST API + GRPC streaming |
| TimescaleDB   | 5432       | Topic storage + offsets   |
| DB Visualizer | 8081       | Database UI (optional)    |


---

## 🧪 2. Create Resources

### Create Topic (demo-topic)

```bash
curl -X POST \
  http://localhost:8080/user/ADMIN/topic/demo-topic \
  -H "Content-Type: application/json" \
  -d '{
        "partitions": 3,
        "schemaDefinition": "{}",
        "cleanupPolicy": "delete",
        "type": "classic"
      }'
```

Expected HTTP 200/201 response.

### Verify Topic Exists

```bash
curl http://localhost:8080/user/ADMIN/topic/demo-topic
```

Should return topic metadata.


### Create a Consumer Group

```bash
curl -X POST \
  http://localhost:8080/user/ADMIN/topic/demo-topic/consumer/default
```


### List Consumers

```bash
curl http://localhost:8080/user/ADMIN/topic/demo-topic/consumer/list
```

---

## 📤 3. Publish Messages

You can import the **Postman Collection** to simulate bulk production of messages
```bash
curl -X POST \
  http://localhost:8080/user/ADMIN/ingest/demo-topic \
  -H "Content-Type: application/json" \
  -d '[
        {
          "messageKey": "Hello there",
          "message": "Hello there",
          "timestamp": 1764686720
        }
      ]'
```

---

## 📥 4. Subscribe and Consume

Create a new **Postman Collection** with GRPC template.
Create a new request.
Import the following `.proto` file into the request.

```bash
syntax = "proto3";

import "google/protobuf/timestamp.proto";

option java_multiple_files = true;
option java_package = "com.streamlite.broker.grpc";
option java_outer_classname = "ConsumerStreamingProto";

service ConsumerStreamingService {

  rpc join(RpcConsumerMetadata) returns (stream PartitionAssignmentMetadata);

  rpc refresh(RpcConsumerMetadata) returns (stream PartitionAssignmentMetadata);

  rpc listen(stream Heartbeat) returns (stream StreamgresMessage);

  rpc ack(stream Acknowledgement) returns (stream HealthStatus);

  rpc pulse(Heartbeat) returns (HealthStatus);

  rpc leave(RpcConsumerMetadata) returns (HealthStatus);

}

message RpcConsumerMetadata {
  string user = 1;
  string topic = 2;
  string consumer_group = 3;
  string instance_id = 4;
  int64 timeout = 5;
}

message PartitionAssignmentMetadata {
  string topic = 1;
  string consumer_group = 2;
  string consumer_id = 3;
  int32 partition = 4;
  string session = 5;
  string consumer_status = 6;
}

message Acknowledgement {
  string session = 1;
  int32 partition = 2;
  int64 offset = 3;
}

message Heartbeat {
  string session = 1;
}

message HealthStatus {
  bool connected = 1;
}

message StreamgresMessage {
  string key = 1;
  bytes message = 2;
  int32 partition = 3;
  int64 offset = 4;
  google.protobuf.Timestamp timestamp = 5;
}

message StreamingError {
  int32 code = 1;
  string message = 2;
  string details = 3;
}
```
You will see the following APIs in your request.

- Make a connection by hitting the `join` endpoint, and copy the `session` id from the response.
- You can pass the `session` id in the `pulse` endpoint to check connectivity status.
- Hit the `listen` endpoint and start consuming messages.
- To update the message offset, use the `ack` endpoint.
- If you forget your `session` id, hit the `refresh` endpoint to get the existing sessions.
- To leave the consumer group, use the `leave` endpoint.

![Screenshot 2025-12-02 202357.png](https://github.com/its-just-pritam/streamgres/blob/main/grpc_request.png)

---

## 🧹 8. Cleanup After Testing

```bash
docker compose down -v
```

This removes containers **and all stored data**.

---

If you want, I can also add:

* A “Postman Collection Import Guide”
* A “Smoke Test Checklist”
* Scenario-based tests (load test, partition test, consumer rebalance test)

Just say the word!


---

# 🏗️ **Backend Architecture**

![Design](https://github.com/its-just-pritam/streamgres/blob/main/streamgres.drawio.png)

---

# 👨‍💻 Developers

* **Pritam Mallick**
  🔗 [https://www.linkedin.com/in/pritammallick20/](https://www.linkedin.com/in/pritammallick20/)
