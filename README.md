# Apache Kafka — পরিচিতি (Introduction)

> **Apache Kafka** হলো একটি **distributed event streaming platform**, যা একাধিক application বা microservice-এর মধ্যে real-time-এ data collect, store, process ও transmit করে।

**সহজ ভাষায়:** Kafka হলো এমন একটি messaging system, যার মাধ্যমে একাধিক application একে অপরের সঙ্গে **সরাসরি যোগাযোগ না করেও** data আদান-প্রদান করতে পারে।

---

## 📌 সংক্ষেপে (TL;DR)

```
Producer ──► Kafka Broker (Topic) ──► Consumer
 (পাঠায়)      (জমা রাখে)             (গ্রহণ করে)
```

| শব্দ | এক লাইনে |
|---|---|
| Producer | Message পাঠায় |
| Broker | Message সংরক্ষণ ও পরিচালনা করে |
| Topic | Message-এর category / channel |
| Consumer | Message গ্রহণ করে |
| Consumer Group | একাধিক consumer-এর দল |

---

## 📑 সূচিপত্র

1. [Kafka কেন দরকার?](#1-kafka-কেন-দরকার)
2. [Kafka কীভাবে কাজ করে?](#2-kafka-কীভাবে-কাজ-করে)
3. [Kafka Workflow](#3-kafka-workflow)
4. [গুরুত্বপূর্ণ Concepts](#4-গুরুত্বপূর্ণ-concepts)
5. [Kafka vs RabbitMQ](#5-kafka-vs-rabbitmq)
6. [Kafka with NestJS](#6-kafka-with-nestjs)
7. [কখন Kafka ব্যবহার করবে?](#7-কখন-kafka-ব্যবহার-করবে)
8. [শেখার পরামর্শ](#8-শেখার-পরামর্শ)

---

## 1. Kafka কেন দরকার?

ধরো, তুমি একটি **E-commerce application** বানিয়েছো, যেখানে কয়েকটি microservice আছে:

- User Service
- Order Service
- Payment Service
- Notification Service
- Inventory Service

Customer **order place** করলে অনেকগুলো কাজ করতে হয়:

1. **Order Service** → নতুন order তৈরি করবে
2. **Payment Service** → payment process করবে
3. **Inventory Service** → product stock update করবে
4. **Notification Service** → customer-কে email পাঠাবে

### ❓ সমস্যা

Order Service কীভাবে অন্য service-গুলোকে জানাবে যে নতুন order তৈরি হয়েছে?

প্রতিটি service-কে সরাসরি **HTTP request** পাঠানো যায়, কিন্তু এতে service-গুলোর মধ্যে **tight coupling** তৈরি হয়।

```
❌ HTTP দিয়ে সরাসরি (Tight Coupling)

Order Service ──► Payment Service
              ──► Inventory Service
              ──► Notification Service
```

### ✅ সমাধান: Kafka

```
✅ Kafka দিয়ে (Loose Coupling)

Order Service ──► Kafka ──► Payment Service
                       ──► Inventory Service
                       ──► Notification Service
```

Order Service শুধু Kafka-তে event **publish** করে। বাকি service-গুলো নিজেদের মতো **consume** করে।

---

## 2. Kafka কীভাবে কাজ করে?

Kafka-র ৫টি গুরুত্বপূর্ণ component:

### ১) Producer
যে application Kafka-তে message / event **পাঠায়**।
> উদাহরণ: Order Service একটি `order.created` event পাঠায়।

### ২) Broker
যে **server**-এ Kafka messages সংরক্ষিত ও পরিচালিত হয়।

### ৩) Topic
Message রাখার একটি **logical category বা channel**।
> উদাহরণ: `orders`, `payments`, `notifications`

### ৪) Consumer
যে application Kafka থেকে message **গ্রহণ ও process** করে।
> উদাহরণ: Payment Service `orders` topic থেকে event গ্রহণ করে।

### ৫) Consumer Group
একাধিক consumer মিলে একটি group তৈরি করে এবং topic-এর **partitions থেকে কাজ ভাগ করে নেয়**।

---

## 3. Kafka Workflow

```
┌─────────────────────┐
│   Order Service     │
│   (Producer)        │
└──────────┬──────────┘
           │  publish: order.created
           ▼
┌─────────────────────┐
│   Kafka Broker      │
│   Topic: orders     │
└──────────┬──────────┘
           │
   ┌───────┼────────────┐
   ▼       ▼            ▼
┌───────┐ ┌───────────┐ ┌──────────────┐
│Payment│ │ Inventory │ │ Notification │
│(Cons.)│ │  (Cons.)  │ │   (Cons.)    │
└───────┘ └───────────┘ └──────────────┘
```

Order Service শুধু event **publish** করে। অন্য service-গুলো প্রয়োজন অনুযায়ী event **consume** করে।

> 🏷️ এটিকেই বলা হয় **Event-Driven Architecture (EDA)**।

---

## 4. গুরুত্বপূর্ণ Concepts

| Concept | মানে |
|---|---|
| **Producer** | Message পাঠায় |
| **Consumer** | Message গ্রহণ করে |
| **Broker** | Message সংরক্ষণ ও পরিচালনা করে |
| **Topic** | Message-এর category |
| **Partition** | Topic-এর data ভাগ করে |
| **Offset** | Message-এর position |
| **Replication** | Data-র অতিরিক্ত copy রাখা |
| **Consumer Group** | একাধিক consumer-এর group |

---

## 5. Kafka vs RabbitMQ

দুটোই messaging technology, তবে ব্যবহারের ক্ষেত্রে পার্থক্য আছে।

| Feature | Kafka | RabbitMQ |
|---|---|---|
| Architecture | Distributed event log | Message broker |
| Message storage | Persistent log | Queue-based |
| Message replay | ✅ Supported | সাধারণত মূল design নয় |
| Throughput | Very high | Use case অনুযায়ী |
| Best for | **Event streaming** | **Task / message queue** |

---

## 6. Kafka with NestJS

NestJS-এর `@nestjs/microservices` package **Kafka transport** support করে।

### Installation

```bash
npm install @nestjs/microservices@^11 kafkajs
```

### Kafka Server Configuration (`main.ts`)

```ts
import { NestFactory } from '@nestjs/core';
import { Transport } from '@nestjs/microservices';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.createMicroservice(
    AppModule,
    {
      transport: Transport.KAFKA,
      options: {
        client: {
          clientId: 'user-service',
          brokers: ['localhost:9092'],
        },
        consumer: {
          groupId: 'user-consumer',
        },
      },
    },
  );

  await app.listen();
}

bootstrap();
```

| Code | মানে |
|---|---|
| `Transport.KAFKA` | Kafka transport ব্যবহার করবে |
| `clientId` | Kafka client-এর পরিচয় |
| `brokers` | Kafka broker-এর address |
| `groupId` | Consumer group-এর পরিচয় |

### 📚 Official Documentation

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [NestJS Kafka Documentation](https://docs.nestjs.com/microservices/kafka)

---

## 7. কখন Kafka ব্যবহার করবে?

- ✅ Microservices communication
- ✅ Real-time data streaming
- ✅ Log aggregation
- ✅ Event-driven architecture
- ✅ Payment ও order processing
- ✅ Data pipeline
- ✅ Real-time analytics

---

## 8. শেখার পরামর্শ

**ধাপ ১:** আগে এই concepts ভালোভাবে বোঝো:

```
Producer → Consumer → Topic → Partition → Consumer Group
```

**ধাপ ২:** তারপর NestJS দিয়ে একটি **Order Management Microservices project** বানাও।

এতে Kafka-র পাশাপাশি **বাস্তব Microservices Architecture** সম্পর্কেও ভালো ধারণা তৈরি হবে। 🚀
