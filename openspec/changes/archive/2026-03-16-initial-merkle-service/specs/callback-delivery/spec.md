## ADDED Requirements

### Requirement: Consume STUMP and status messages from Kafka
The system SHALL consume messages from the Kafka `stumps` topic using a consumer group. Messages include both STUMP payloads (MINED notifications) and status notifications (SEEN_ON_NETWORK, SEEN_MULTIPLE_NODES).

> Traces to: requirements.md — "callback: subscribes to stumps kafka topic"

#### Scenario: Consume STUMP message
- **WHEN** a STUMP message is available on the Kafka `stumps` topic
- **THEN** the system consumes the message and extracts the callback URL, STUMP data, and message type

#### Scenario: Consume status notification
- **WHEN** a SEEN_ON_NETWORK or SEEN_MULTIPLE_NODES message is available on the Kafka `stumps` topic
- **THEN** the system consumes the message and extracts the callback URL, txid, and status type

### Requirement: Deliver callbacks via HTTP
The system SHALL perform HTTP POST requests to the registered callback URLs with the appropriate payload (STUMP or status notification).

> Traces to: requirements.md — "callback: for each stump perform callback to appropriate business"

#### Scenario: Successful STUMP callback delivery
- **WHEN** the system delivers a STUMP payload to a callback URL
- **THEN** the system sends an HTTP POST with the STUMP data in BRC-0074 format, block hash, and a MINED status indicator
- **THEN** the callback URL responds with HTTP 2xx
- **THEN** the system marks the delivery as successful and commits the Kafka offset

#### Scenario: Successful status callback delivery
- **WHEN** the system delivers a SEEN_ON_NETWORK notification to a callback URL
- **THEN** the system sends an HTTP POST with the txid and SEEN_ON_NETWORK status
- **THEN** the callback URL responds with HTTP 2xx

### Requirement: Retry failed callbacks via Kafka re-enqueue
The system SHALL re-enqueue failed callback deliveries back to the Kafka `stumps` topic for retry. Retry SHALL use linear backoff. After a configurable maximum number of retries, the message SHALL be sent to a dead letter topic.

> Traces to: requirements.md — "callback: if callback fails, put it back on kafka"

#### Scenario: Callback delivery fails with retries remaining
- **WHEN** a callback delivery fails (non-2xx response or connection error)
- **THEN** the system increments the retry counter in the message
- **THEN** the system re-enqueues the message to the Kafka `stumps` topic with a linear backoff delay

#### Scenario: Callback delivery exhausts retries
- **WHEN** a callback delivery fails and the retry counter has reached the configured maximum
- **THEN** the system publishes the message to the dead letter topic
- **THEN** the system logs the permanent failure with the callback URL and message details

#### Scenario: Callback delivery fails with timeout
- **WHEN** a callback delivery times out (configurable timeout per request)
- **THEN** the system treats it as a failure and follows the retry logic
