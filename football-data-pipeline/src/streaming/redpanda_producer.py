# src/streaming/redpanda_producer.py
import json
import logging
from typing import Dict, List, Optional, Union

from confluent_kafka import Producer
from pydantic import BaseModel

from src.utils.config import Config

logger = logging.getLogger(__name__)


class RedpandaConfig(BaseModel):
    """Configuration for Redpanda/Kafka producer."""
    bootstrap_servers: str
    client_id: str
    acks: str = "all"
    retries: int = 3
    batch_size: int = 16384
    linger_ms: int = 0
    buffer_memory: int = 33554432


class RedpandaProducer:
    """Producer class for Redpanda/Kafka streaming."""

    def __init__(self, config: Union[Dict, RedpandaConfig]):
        """Initialize the Redpanda producer.

        Args:
            config: Configuration for the producer as dict or RedpandaConfig object
        """
        if isinstance(config, dict):
            self.config = RedpandaConfig(**config)
        else:
            self.config = config

        self.producer_config = {
            'bootstrap.servers': self.config.bootstrap_servers,
            'client.id': self.config.client_id,
            'acks': self.config.acks,
            'retries': self.config.retries,
            'batch.size': self.config.batch_size,
            'linger.ms': self.config.linger_ms,
            'buffer.memory': self.config.buffer_memory,
        }
        
        self.producer = Producer(self.producer_config)
        logger.info(f"Initialized Redpanda producer with bootstrap servers: {self.config.bootstrap_servers}")

    def delivery_report(self, err, msg):
        """Callback for message delivery reports."""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def produce(self, topic: str, value: Dict, key: Optional[str] = None) -> None:
        """Produce a message to a topic.

        Args:
            topic: Topic name
            value: Message value as dict
            key: Optional message key
        """
        try:
            self.producer.produce(
                topic=topic,
                key=key.encode('utf-8') if key else None,
                value=json.dumps(value).encode('utf-8'),
                callback=self.delivery_report
            )
            # Trigger any callbacks for messages that have been delivered
            self.producer.poll(0)
        except Exception as e:
            logger.error(f"Error producing message to {topic}: {e}")
            raise

    def produce_batch(self, topic: str, messages: List[Dict], key_field: Optional[str] = None) -> None:
        """Produce a batch of messages to a topic.

        Args:
            topic: Topic name
            messages: List of message values as dicts
            key_field: Optional field to use as message key
        """
        try:
            for message in messages:
                key = str(message.get(key_field)) if key_field and key_field in message else None
                self.produce(topic, message, key)
            
            # Wait for all messages to be delivered
            self.producer.flush()
        except Exception as e:
            logger.error(f"Error producing batch messages to {topic}: {e}")
            raise

    def close(self) -> None:
        """Close the producer."""
        self.producer.flush()
        logger.info("Redpanda producer closed")


def create_producer_from_config(config_path: str = "config/config.yaml") -> RedpandaProducer:
    """Create a RedpandaProducer from configuration file.

    Args:
        config_path: Path to config file

    Returns:
        RedpandaProducer instance
    """
    config = Config(config_path)
    redpanda_config = config.get("redpanda", {})
    return RedpandaProducer(redpanda_config)