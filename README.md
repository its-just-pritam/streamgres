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

---

# 🏗️ **Backend Architecture**

![Design](https://github.com/its-just-pritam/streamgres/blob/main/streamgres.drawio.png)

---

# 👨‍💻 Developers

* **Pritam Mallick**
  🔗 [https://www.linkedin.com/in/pritammallick20/](https://www.linkedin.com/in/pritammallick20/)
