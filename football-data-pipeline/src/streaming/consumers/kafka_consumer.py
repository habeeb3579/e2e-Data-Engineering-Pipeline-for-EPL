# src/streaming/consumers/kafka_consumer.py

import json
import logging
import threading
from typing import Dict, Any, List, Optional, Callable
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

from config.settings import config
from src.utils.logging_utils import get_logger


class KafkaConsumer:
    """Consumer for receiving data from Kafka/Redpanda topics"""
    
    def __init__(
        self, 
        group_id: str = "football-data-consumer",
        auto_offset_reset: str = "earliest"
    ):
        """
        Initialize Kafka consumer
        
        Args:
            group_id: Consumer group ID
            auto_offset_reset: Offset reset policy ('earliest' or 'latest')
        """
        self.logger = get_logger(self.__class__.__name__)
        self.broker_urls = config.streaming.broker_urls
        self.topic_prefix = config.streaming.topic_prefix
        self.schema_registry_url = config.streaming.schema_registry_url
        
        # Configure Kafka consumer
        consumer_config = {
            'bootstrap.servers': ','.join(self.broker_urls),
            'group.id': group_id,
            'auto.offset.reset': auto_offset_reset,
            'enable.auto.commit': False,
        }
        
        self.consumer = Consumer(consumer_config)
        
        # Set up schema registry if available
        self.schema_registry = None
        self.deserializers = {}
        
        if self.schema_registry_url:
            self._setup_schema_registry()
        
        # Flag to control consume loop
        self.running = False
        self.consume_thread = None
    
    def _setup_schema_registry(self):
        """Set up connection to schema registry"""
        try:
            self.schema_registry = SchemaRegistryClient({
                'url': self.schema_registry_url
            })
            self.logger.info(f"Connected to schema registry at {self.schema_registry_url}")
        except Exception as e:
            self.logger.warning(f"Failed to connect to schema registry: {e}")
            self.schema_registry = None
    
    def _get_topic_name(self, data_type: str) -> str:
        """
        Get the full topic name based on data type
        
        Args:
            data_type: Type of data (e.g., "matches", "players")
            
        Returns:
            Full topic name
        """
        return f"{self.topic_prefix}_{data_type}"
    
    def _get_deserializer(self, data_type: str) -> Optional[AvroDeserializer]:
        """
        Get or create Avro deserializer for the data type
        
        Args:
            data_type: Type of data (e.g., "matches", "players")
            
        Returns:
            Avro deserializer if schema registry is available, None otherwise
        """
        if not self.schema_registry:
            return None
        
        if data_type not in self.deserializers:
            try:
                # Load schema from file
                schema_path = f"src/streaming/schemas/{data_type}_schema.avsc"
                with open(schema_path, 'r') as f:
                    schema_str = f.read()
                
                # Create deserializer
                self.deserializers[data_type] = AvroDeserializer(
                    schema_registry_client=self.schema_registry,
                    schema_str=schema_str
                )
                
                self.logger.info(f"Created Avro deserializer for {data_type}")
            except Exception as e:
                self.logger.error(f"Failed to create Avro deserializer for {data_type}: {e}")
                return None
        
        return self.deserializers.get(data_type)
    
    def subscribe(self, data_types: List[str]):
        """
        Subscribe to topics for specified data types
        
        Args:
            data_types: List of data types to subscribe to
        """
        topics = [self._get_topic_name(data_type) for data_type in data_types]
        self.consumer.subscribe(topics)
        self.logger.info(f"Subscribed to topics: {topics}")
    
    def consume_batch(self, timeout: float = 1.0, batch_size: int = 100) -> List[Dict[str, Any]]:
        """
        Consume a batch of messages from subscribed topics
        
        Args:
            timeout: Maximum time to wait for messages (seconds)
            batch_size: Maximum number of messages to consume
            
        Returns:
            List of consumed messages
        """
        messages = []
        
        try:
            # Consume up to batch_size messages or until timeout
            for _ in range(batch_size):
                msg = self.consumer.poll(timeout)
                
                if msg is None:
                    break
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition, no more messages
                        continue
                    else:
                        self.logger.error(f"Consumer error: {msg.error()}")
                        break
                
                # Process message
                topic = msg.topic()
                data_type = topic.replace(f"{self.topic_prefix}_", "")
                
                # Deserialize message
                deserializer = self._get_deserializer(data_type)
                
                if deserializer:
                    # Use Avro deserializer if available
                    data = deserializer(
                        msg.value(), 
                        SerializationContext(topic, MessageField.VALUE)
                    )
                else:
                    # Fall back to JSON deserialization
                    data = json.loads(msg.value().decode('utf-8'))
                
                # Add message metadata
                data['_topic'] = topic
                data['_partition'] = msg.partition()
                data['_offset'] = msg.offset()
                data['_timestamp'] = msg.timestamp()[1]
                
                messages.append(data)
            
            # Commit offsets if messages were consumed
            if messages:
                self.consumer.commit()
                
        except KafkaException as e:
            self.logger.error(f"Kafka exception: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error in consume_batch: {e}")
        
        return messages
    
    def consume_with_callback(self, callback: Callable[[Dict[str, Any]], None], stop_after: int = None):
        """
        Consume messages and process them with a callback function
        
        Args:
            callback: Function to call for each message
            stop_after: Number of messages to consume before stopping (optional)
        """
        message_count = 0
        self.running = True
        
        try:
            while self.running:
                if stop_after is not None and message_count >= stop_after:
                    self.logger.info(f"Reached message limit: {stop_after}")
                    break
                
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        self.logger.error(f"Consumer error: {msg.error()}")
                        break
                
                # Process message
                topic = msg.topic()
                data_type = topic.replace(f"{self.topic_prefix}_", "")
                
                # Deserialize message
                deserializer = self._get_deserializer(data_type)
                
                if deserializer:
                    # Use Avro deserializer if available
                    data = deserializer(
                        msg.value(), 
                        SerializationContext(topic, MessageField.VALUE)
                    )
                else:
                    # Fall back to JSON deserialization
                    data = json.loads(msg.value().decode('utf-8'))
                
                # Add message metadata
                data['_topic'] = topic
                data['_partition'] = msg.partition()
                data['_offset'] = msg.offset()
                data['_timestamp'] = msg.timestamp()[1]
                
                # Call the callback function
                try:
                    callback(data)
                    message_count += 1
                except Exception as e:
                    self.logger.error(f"Error in callback function: {e}")
                
                # Commit offset after processing
                self.consumer.commit(msg)
                
        except KeyboardInterrupt:
            self.logger.info("Received keyboard interrupt, stopping consumer")
        except Exception as e:
            self.logger.error(f"Unexpected error in consume_with_callback: {e}")
        finally:
            self.running = False
    
    def start_background_consumption(self, callback: Callable[[Dict[str, Any]], None]):
        """
        Start consuming messages in a background thread
        
        Args:
            callback: Function to call for each message
        """
        if self.consume_thread and self.consume_thread.is_alive():
            self.logger.warning("Consumer thread is already running")
            return
        
        self.running = True
        self.consume_thread = threading.Thread(
            target=self.consume_with_callback, 
            args=(callback,)
        )
        self.consume_thread.daemon = True
        self.consume_thread.start()
        self.logger.info("Started background consumption thread")
    
    def stop_background_consumption(self):
        """Stop background consumption thread"""
        self.running = False
        if self.consume_thread:
            self.consume_thread.join(timeout=5.0)
            self.logger.info("Stopped background consumption thread")
    
    def close(self):
        """Close consumer and clean up resources"""
        if self.running:
            self.stop_background_consumption()
        
        self.consumer.close()
        self.logger.info("Closed Kafka consumer")