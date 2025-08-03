# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an AWS IoT Core Message Queuing test suite that demonstrates and validates the new message queuing functionality for MQTT shared subscriptions.

## Development Commands

### Setup
```bash
# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip3 install -r src/requirements.txt

# Setup environment configuration
cp src/.env.example src/.env
# Edit .env with actual AWS IoT endpoint and settings
```

### Running Individual Components
```bash
# From project root directory
python -m src.publisher          # Message publisher only
python -m src.subscriber         # Shared subscribers only
```

### Manual Testing
```bash
# Run subscriber in one terminal
python -m src.subscriber

# Run publisher in another terminal  
python -m src.publisher
```

### Prerequisites
- AWS IoT Core Thing configured with certificates
- Certificates placed in `src/certs/` directory:
  - `AmazonRootCA1.pem`
  - `device.pem.crt` 
  - `private.pem.key`
- Python 3.7+ (required for AWS IoT Device SDK v2)

## Architecture

### Core Components
- **src/config.py**: AWS IoT configuration management with environment variables and certificate path resolution
- **src/publisher.py**: QoS1 message publisher using AWS IoT Device SDK v2 that sends structured JSON messages to shared subscription topics  
- **src/subscriber.py**: Multiple shared subscribers using AWS IoT Device SDK v2 with simulated disconnection/reconnection cycles for manual testing

### Key Design Patterns
- **Shared Subscriptions**: Uses `$share/{group}/{topic}` pattern for load distribution across multiple subscribers
- **Persistent Sessions**: Clean Session=false to enable message queuing during disconnections
- **QoS1 Messaging**: Ensures message delivery guarantees required for queuing functionality
- **Connection Resilience**: Simulates network interruptions to validate queuing behavior

### Message Flow Architecture
1. Publisher sends QoS1 messages to `test/shared/messages`
2. Multiple subscribers connect to `$share/message-queuing-group/test/shared/messages`
3. Random disconnections simulate network issues
4. AWS IoT Core queues messages during subscriber disconnections
5. Queued messages are delivered upon reconnection with persistent sessions

### Configuration System
- Environment-based configuration with `.env` file support
- Certificate path resolution (relative to src/certs/ or absolute paths)
- Configurable MQTT settings (client ID prefixes, topic prefixes, shared group names)
- Test parameters (intervals, message counts, subscriber counts)

## Manual Testing Strategy

The manual testing approach validates Message Queuing functionality through hands-on verification:

1. **Basic Connectivity**: Run subscriber to establish MQTT connections and shared subscription registration
2. **Message Flow**: Run publisher to send messages and observe distribution across subscribers  
3. **Message Queuing**: Manually disconnect/reconnect subscribers to verify message queuing during disconnections

This hands-on approach provides direct visibility into the Message Queuing behavior.

## Important Technical Notes

- Uses AWS IoT Device SDK v2 for improved performance and reliability over paho-mqtt
- All functions use type annotations as per Python best practices
- Clean Session must be false for persistent sessions and queuing functionality
- QoS1 is required for message queuing (QoS0 messages are not queued)
- Shared subscription group names are configurable via environment variables
- Certificate files must be properly configured with AWS IoT Core Thing certificates
- The test runner uses subprocess management for concurrent publisher/subscriber execution
- Future-based async operations for connection and publish operations