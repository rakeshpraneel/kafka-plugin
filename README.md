# kafka-plugin

## Description
This package helps developers to integrate *GSSAPI* and *keytab* supported kafka consumer to their existing system just like plugin. It just needs kafka configurations that are required to establish connection which can be loaded through a yaml manifest.

It also supports loading config through dictionary. Create a manifest with required configs and get started. Use sample manifest as reference for creating one.

This uses *Confluent-kafka* library in backend and designed for reliable, at-least-once delivery and works well with FastAPI, background workers, and microservices. 
## Features

- Easy plugin use.
- Uses in-memory local queue to store the message.
- Runs consumer instance as separate thread.
- Uses manual commit to ensure data is not lost.
- Option to override default values.
- Applies backpressure (pause/resumes) when the app is slow.
- Robust and Reliable config loading options.
- Encapsulated method to read data from queue.
- Safe JSON decoding.
- Works with synchronous or async apps.
- Kerberos (GSSAPI) authentication support

## Installation
Install using `pip`:
```bash
pip install kafka_plugin
```

# Usage
### Sample Manifest
```yaml
app_constants:
  kafka_topics: ["SAMPLE.TOPIC"]
  kafka_principal: "sample_principal@REALM"
  kafka_servicename: "sample_servicename"
  kafka_keytab: "/path/to/samplefile.keytab"
  kafka_sslca: "/path/to/sample.pem"
  kafka_groupid: "SAMPLE_GROUPID"
  kafka_servers: "broker1:9092 broker2:9092"
  queue_size: 10000               #optional (default: 10000)
  client_id: "sample_client"      #optional (default: "default")
  cache_path: "./path/tmp/sample" #optional
  poll_timeout: 10.0              #optional (default: 5.0)
  session_timeout: 6000           #optional (default: 6000)
  auto_commit: False              #optional (default: False)
```

### Sample Dictionary
```python
app_config = {
    "app_constants": {
        "kafka_topics": ["my-topic"],
        "kafka_principal": "user@REALM",
        "kafka_servicename": "sample_servicename",
        "kafka_keytab": "/path/samplefile.keytab",
        "kafka_sslca": "/path/to/ca.pem",
        "kafka_groupid": "my-group",
        "kafka_servers": "broker1:9093,broker2:9093",

        "client_id": "my-client",       #optional (default: "default")
        "cache_path": "/tmp/krb5cc_",   #optional
        "queue_size": 10000,            #optional (default: 10000)
        "poll_timeout": 5.0,            #optional (default: 5.0)
        "session_timeout": 6000,        #optional (default: 6000)
        "auto_commit": False            #optional (default: False)
    }
}
```
## Initialization
### Load Config File
```python
from kafka_plugin import load_manifest

path = "/root/config/kafka_manifest.yaml"
app_conf = load_manifest.Config(path)

config_data = app_conf.get_config()
```

### Start Kafka
Load config through manifest
```python
from kafka_plugin import kafka_consumer, load_manifest

path = "/root/config/kafka_manifest.yaml"
app_conf = load_manifest.Config(path)

config_data = app_conf.get_config()

# This auto initiates the consumer instance
kafka_local_agent = kafka_consumer.KafkaConsumer(config_data)
kafka_local_agent.start_read_msg()

while True:
    msg = kafka_local_agent.get_message()
    if msg:
        print("Received:", msg)
```

This package uses
```python
enable.auto.commit = False
```
commits offsets manually after enqueue.
So it provides
**At-least-once delivery**
Messages are not committed until safely stored in the local queue.

If your app crashes, Kafka may resend messages.
No message is silently dropped under load.

**kafka_plugin** starts consumer instance by default when it is initiated. This behaviour can be overidden by setting it to False
```python
from kafka_plugin import kafka_consumer

kafka_local_agent = kafka_consumer.KafkaConsumer(config_data,auto_start=False)
kafka_local_agent.start_consumer()
if kafka_local_agent.consumer:
    kafka_local_agent.start_read_msg()
else:
    print("Consumer hasn't spinned up")
```

### Async Wrapper
```python
import asyncio

async def async_worker():
    while True:
        msg = consumer.get_message()
        if msg:
            print(msg)
        await asyncio.sleep(0.01)
```

### Threading Model

- Kafka runs in a background daemon thread.
- Application runs in the main thread.
- Communication happens via a thread-safe queue.Queue

### Process

> KafkaConsumer(app_config)

Creates a new consumer instance.

---

> start_read_msg()

Starts the Kafka polling loop in a background thread.

---

> get_message()

Returns the next message from the local queue.

## Key Points

- Designed for Linux environments with Kerberos auth.
- Messages must be valid JSON.
- Queue size controls memory usage.

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.