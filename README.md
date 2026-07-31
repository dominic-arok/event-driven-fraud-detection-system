# event-driven-fraud-detection-system

# Overview

This is an event-driven backend system that models how modern payment platforms detect suspicious transactions as they happen.

Instead of checking transactions in a batch later, the system consumes transaction events in real time, applies fraud detection rules, and emits alerts immediately when suspicious behavior is found.

This project demonstrates how fraud monitoring systems can be built using streaming events, in-memory state, and decoupled services.

The system detects fraud using three rule types:

- large transaction amount
- multiple rapid transactions
- unusual location change

---

# System Architecture

Client → Go Transaction Service → Kafka Transactions Topic → Go Fraud Detection Service → Kafka Fraud Alerts Topic → Go Alert Service

Supporting Store:

Go Fraud Detection Service ↔ Redis

---

# How To Run

1. Open Docker Desktop app

2. Build and run container from terminal with "docker compose up -d"

3. _For First Time Only:_ Open http://localhost:8080 (Kafka UI) and create two topics:

   Topic - Partitions - Replication Factor - Retention

   transactions - 10 - 1 - 7 days  
   fraud-alerts - 5 - 1 - 7 days

4. Run each service in a separate terminal

   "go run ./cmd/transaction-service"
   "go run ./cmd/fraud-detection-service"
   "go run ./cmd/alert-service"

5. Open a fourth terminal and send a transaction with curl (alternatively you can do this with Postman)

   "curl -X POST http://localhost:3000/api/v1/transactions \
    -H "Content-Type: application/json" \
    -d '{"user_id":"dominic","amount":15000,"currency":"USD","location":"USA"}'"

   To trigger each rule specifically:

   Large amount: any amount over 10000
   Rapid transactions: same user_id, 5+ requests within 30 seconds
   Location change: same user_id, second request with a different location than the first

---

# How To Shutdwon

1.  Close Go services with Ctrl+C in each Go service terminal

2.  Stop container from terminal with "docker compose down"
