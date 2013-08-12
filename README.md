pika-twisted-example
====================

Example code handling multiple connections using [pika](https://pika.readthedocs.org/en/latest/index.html) 
and [Twisted](http://twistedmatrix.com/trac/)

How to work
-----------

```
+-------+                    +---------+                    +-----------+
|send.py| -> [prev_queue] -> |worker.py| -> [next_queue] -> |consumer.py|
+-------+                    +---------+                    +-----------+
```

- send.py: publish message using Blocking connection.
- worker.py: consume, trasform (upper) and publish message using two `TwistedProtocolConnection`s. (Main example code)
- consumer.py: consume message using Blocking connection.

How to run
----------

1. Install [RabbitMQ](http://www.rabbitmq.com/); for Mac OS X:
    ```
    brew install rabbitmq
    ```

2. Setup libraries.
    ```
    pip install -r requiements.txt
    ```

3. Start workers (RabbitMQ, worker.py and consumer.py).
    ```
    honcho start
    ```

4. Send message from the other console.
    ```
    python send.py hello
    ```

5. You will see:
    ```
    15:51:01 worker.1   | INFO:__main__:Received: hello
    15:51:01 worker.1   | INFO:__main__:Publishing: HELLO
    15:51:01 consumer.1 | INFO:__main__:Received: HELLO
    ```
