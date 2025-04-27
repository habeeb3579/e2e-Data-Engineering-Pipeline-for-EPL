# src/streaming/redpanda_consumer.py
import json
import logging
from typing import Callable, Dict, List, Optional, Union

from confluent_kafka import Consumer, KafkaError, KafkaException
from pydantic import BaseModel

from src.utils.config import Config

logger = logging.getLogger(__name__)


class RedpandaConsumerConfig(BaseModel):
    """Configuration for Redpanda/Kafka consumer."""
    bootstrap_servers: str
    group_id: str
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = True
    auto_commit_interval_ms: int = 5000
    session_timeout_ms: int = 30000
    max_poll_interval_ms: int = 300000


class RedpandaConsumer:
    """Consumer class for Redpanda/Kafka streaming."""

    def __init__(self, config: Union[Dict, RedpandaConsumerConfig], topics: List[str]):
        """Initialize the Redpanda consumer.

        Args:
            config: Configuration for the consumer as dict or RedpandaConsumerConfig object
            topics: List of topics to subscribe to
        """
        if isinstance(config, dict):
            self.config = RedpandaConsumerConfig(**config)
        else:
            self.config = config

        self.topics = topics
        
        self.consumer_config = {
            'bootstrap.servers': self.config.bootstrap_servers,
            'group.id': self.config.group_id,
            'auto.offset.reset': self.config.auto_offset_reset,
            'enable.auto.commit': self.config.enable_auto_commit,
            'auto.commit.interval.ms': self.config.auto_commit_interval_ms,
            'session.timeout.ms': self.config.session_timeout_ms,
            'max.poll.interval.ms': self.config.max_poll_interval_ms,
        }
        
        self.consumer = Consumer(self.consumer_config)
        self.consumer.subscribe(self.topics)
        
        logger.info(f"Initialized Redpanda consumer with bootstrap servers: {self.config.bootstrap_servers}")
        logger.info(f"Subscribed to topics: {', '.join(self.topics)}")

    def consume(self, processor: Callable[[Dict], None], timeout: float = 1.0, max_messages: Optional[int] = None) -> None:
        """Consume messages and process them with the given processor function.
        
        Args:
            processor: Function to process each message
            timeout: Timeout for polling in seconds
            max_messages: Maximum number of messages to consume (None for infinite)
        """
        try:
            message_count = 0
            running = True
            
            while running:
                msg = self.consumer.poll(timeout=timeout)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        logger.debug(f"Reached end of partition {msg.topic()}/{msg.partition()}")
                    else:
                        logger.error(f"Error while consuming message: {msg.error()}")
                else:
                    # Parse and process the message
                    try:
                        value = json.loads(msg.value().decode('utf-8'))
                        processor(value)
                        message_count += 1
                        
                        if max_messages and message_count >= max_messages:
                            running = False
                            
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to parse message: {e}")
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
                        
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        finally:
            self.close()

    def close(self) -> None:
        """Close the consumer."""
        self.consumer.close()
        logger.info("Redpanda consumer closed")


def create_consumer_from_config(topics: List[str], config_path: str = "config/config.yaml") -> RedpandaConsumer:
    """Create a RedpandaConsumer from configuration file.
    
    Args:
        topics: List of topics to subscribe to
        config_path: Path to config file
        
    Returns:
        RedpandaConsumer instance
    """
    config = Config(config_path)
    redpanda_config = config.get("redpanda", {})
    return RedpandaConsumer(redpanda_config, topics)