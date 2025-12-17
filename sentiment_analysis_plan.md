Detailed Social Media Sentiment Analysis Execution Plan

Version: 2.1 (Text-Based Visuals)
Timeline: January – June (6 Months)
Primary Goal: Prototype Launch by March (FB, YT, Discord) -> Full Scale by June.

1. System Architecture & Tech Stack

1.1 Infrastructure Flow

This infrastructure relies on a decoupled Medallion Architecture. Data flows from left to right, orchestrated by Airflow.

+-----------------+       +-----------------+       +---------------------------+
|  ORCHESTRATION  | ----> |   DATA SOURCES  | ----> |  DATA LAKEHOUSE (Storage) |
|  Apache Airflow |       |  FB, YT, Discord|       |      Apache Iceberg       |
+-----------------+       +-----------------+       +---------------------------+
         |                                                       |
         | Triggers                                              | Raw Data
         v                                                       v
+-----------------+                                     +------------------+
|    COMPUTE      | <---------------------------------> |   BRONZE TABLE   |
| PySpark Cluster |             Reads/Writes            |    (Raw JSON)    |
+-----------------+                                     +------------------+
         |                                                       |
         | Sends Text Batches                                    | Cleansed
         v                                                       v
+-----------------+                                     +------------------+
| HYBRID AI ENGINE|                                     |   SILVER TABLE   |
| 1. Local HF     | ----------------------------------> | (Sentiment Data) |
| 2. OpenAI API   |           Returns Labels            +------------------+
+-----------------+                                              |
                                                                 | Aggregated
                                                                 v
+-----------------+                                     +------------------+
|   PRESENTATION  | <---------------------------------- |    GOLD TABLE    |
| Looker / PowerBI|             Queries                 |  (Business KPIs) |
+-----------------+                                     +------------------+


1.2 Technology Decisions

Component

Technology

Rationale

Orchestration

Apache Airflow

Handles complex dependencies and retries for flaky APIs.

Ingestion

Python (requests, pandas)

Lightweight, easy to handle pagination and rate limits.

Processing

PySpark

Necessary for distributed processing when data scales to millions of comments.

Storage

Apache Iceberg on S3/GCS

Supports schema evolution (social APIs change often) and time-travel debugging.

AI Model A

XLM-RoBERTa (Local)

Zero cost for high-volume, short text. Multilingual support (Thai/Indo/Tagalog).

AI Model B

OpenAI (API)

High accuracy for slang, sarcasm, and complex "Taglish".

2. Data Source Specifications (Technical)

This table defines exactly what the engineering team needs to build for the March prototype.

Platform

Endpoint / Method

Auth Method

Rate Limits (Est.)

Key Fields to Extract

Facebook

v22.0/{page-id}/feed (Posts)



v22.0/{post-id}/comments

Page Access Token

200 calls/hr/user

message, created_time, comments.summary(true), likes.summary(true), shares

YouTube

commentThreads?part=snippet

API Key (GCP)

10,000 units/day

textDisplay, authorDisplayName, likeCount, publishedAt, replies

Discord

on_message (Gateway)



history() (REST)

Bot Token

50 requests/sec

content, author.id, timestamp, channel.name, reactions

3. Data Schema Design (Iceberg Tables)

3.1 Silver Table Schema (Standardized)

This is the core table where sentiment analysis happens.

Column Name

Type

Description

platform

String

'Facebook', 'YouTube', 'Discord'

source_id

String

Unique ID from platform (e.g., Comment ID)

parent_id

String

ID of the post/video the comment belongs to

content_text

String

The raw text (cleaned of HTML tags)

author_id

String

Hashed User ID (Privacy Compliance)

created_at

Timestamp

Standardized ISO UTC format

engagement_score

Integer

Likes + Replies + Shares

sentiment_label

String

'Positive', 'Negative', 'Neutral' (AI Output)

sentiment_score

Float

Confidence score 0.0 - 1.0 (AI Output)

ai_model_used

String

'XLM-RoBERTa' or 'GPT-4o-mini'

4. Hybrid AI Implementation Plan

To balance cost and accuracy, we implement a Routing Logic.

The Logic Flow:

[Incoming Comment]
       |
       v
< IS TEXT COMPLEX? >
(Length > 200 chars OR High Slang Density?)
       |
       |--- NO ---> [Send to Local Model]
       |            (Free, Fast, XLM-RoBERTa)
       |                   |
       |                   v
       |            [FINAL SENTIMENT OUTPUT]
       |
       |--- YES --> [Send to Cloud API]
                    (Paid, Accurate, OpenAI)
                           |
                           v
                    [FINAL SENTIMENT OUTPUT]


Cost Control: By routing ~80% of short comments to the local model, we save ~80% of API costs.

Accuracy: Long, complex rants (often the most critical feedback) are handled by the LLM which understands context better.

5. Detailed Execution Roadmap (Sprints)

We assume 2-week Sprints.

Phase 1: Foundation & Prototype Ingestion (January)

Focus: Getting the data flowing for the 3 priority sources.

Sprint 1 (Jan 1-15): Infrastructure Setup

[DevOps] Set up Airflow instance and S3/GCS bucket structure.

[DevOps] Configure Iceberg catalog and PySpark environment.

[Eng] Create common_utils library (logging, API error handling, secrets management).

Sprint 2 (Jan 16-31): Connector Development (Bronze)

[Eng] Build YouTube Collector (REST API). Handle quota management.

[Eng] Build Discord Bot. Set up listener for target channels.

[Eng] Build Facebook Collector. Implement pagination handling.

Milestone: Raw JSON files landing in Bronze storage daily.

Phase 2: Processing & Hybrid AI (February)

Focus: Turning raw JSON into sentiment-enriched data.

Sprint 3 (Feb 1-14): Standardization (Silver)

[Data Eng] Write PySpark jobs to parse raw JSON into the Silver Schema.

[Data Eng] Implement PII Hashing (anonymize usernames) for GDPR compliance.

[Data Eng] Handle deduplication (upsert logic in Iceberg).

Sprint 4 (Feb 15-28): AI Engine Integration

[DS] Deploy cardiffnlp/twitter-xlm-roberta-base-sentiment as a UDF (User Defined Function) in Spark.

[DS] Build the Router Logic (Length check).

[Eng] Create the OpenAI API wrapper with error handling/backoff.

Milestone: Silver table populated with Sentiment Labels.

Phase 3: Prototype Launch (March)

Focus: Visualization and Stakeholder Demo.

Sprint 5 (Mar 1-15): Aggregation (Gold) & BI

[Data Eng] Create Gold views (Daily Sentiment Trend, Platform Comparison).

[Analyst] Connect Looker Studio/Power BI to Gold Tables.

[Analyst] Build "Executive Dashboard" (High level) and "Community Manager View" (Drill down).

Sprint 6 (Mar 16-31): QA & Demo

[QA] Validate sentiment accuracy (manual spot check of 100 rows).

[Team] DEMO DAY: Present working prototype with FB, YT, and Discord data.

Phase 4: Expansion (April)

Focus: Adding difficult sources.

Sprint 7 (Apr 1-15): TikTok Integration

[Eng] Implement TikTok Marketing API.

[Eng] Handle TikTok's specific constraints (video-based ID structure).

Sprint 8 (Apr 16-30): Reddit Integration

[Eng] Implement PRAW collector for Reddit.

[DS] Tune AI model prompts for Reddit's heavy sarcasm.

Phase 5: Completion (May)

Focus: Niche sources and Optimization.

Sprint 9 (May 1-15): Telegram & LINE

[Eng] Build Telegram Collector (Telethon).

[Eng] Set up LINE Webhook receiver (Requires HTTP endpoint).

Sprint 6 (May 16-31): Optimization

[DS] Analyze "Model Confidence" scores. Retrain local model if needed.

[DevOps] Optimize Airflow scheduling and Spark cluster sizing for cost.

Phase 6: Handoff (June)

Sprint 11-12: Documentation, User Training, and Final Handover.

6. Resource Requirements

Team Roles

Data Engineer (1): Focus on Airflow, Spark, and API Connectors.

AI/Data Scientist (1): Focus on Hugging Face deployment, Prompt Engineering, and Routing logic.

Data Analyst (Part-time): Focus on SQL, Looker Studio/Power BI, and insights.

Estimated Infrastructure Costs (Monthly)

Storage (S3/GCS): Low (<$50) for text data.

Compute (Spark/Airflow): Medium ($200 - $500) depending on cloud provider.

OpenAI API: Variable based on volume.

Estimate: 10k "long" comments/month = ~$10-$20 USD.

Short comments: Free (Local CPU).

7. Risk Management

Risk

Impact

Mitigation Strategy

API Rate Limits

Pipeline Failure

Implement "Exponential Backoff" retries. Spread ingestion over 24 hours.

API Breaking Changes

Data Loss

Use Iceberg Schema Evolution. Monitor API changelogs (esp. Meta/TikTok).

AI Cost Overrun

Budget Blowout

Strict logic in Router (only send >200 chars to OpenAI). Set hard budget caps in OpenAI dashboard.

Discord Privacy

Legal/Ban

Only ingest from servers you own/admin. Do not scrape public servers without permission.