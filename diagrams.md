# Distributed systems diagrams

## A streaming lakehouse and its query paths

High-level composition. The Iceberg catalog and object store are distinct services; Rust and Arrow operate inside native execution processes.

Source references: iceberg-flink, comet-jni, df-iceberg.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
flowchart TB

subgraph ingest["Ingestion and processing processes"]
 P["Application producer"]:::io --> K["Kafka broker cluster"]:::store
 K --> F["Flink TaskManagers"]:::worker
 K --> A["Streams / Connect / Spark
alternative consumers"]:::runtime
end
subgraph storage["Persistent table services"]
 F -->|"write files"| O[("Object storage
Parquet and metadata")]:::store
 F -->|"publish snapshot"| C["Iceberg catalog"]:::control
end
subgraph query["Query processes"]
 S["Spark executors
Comet + DataFusion + Arrow"]:::worker
 D["Application process
DataFusion + Iceberg adapter"]:::runtime
 C -. "resolve snapshot" .-> S
 C -. "resolve snapshot" .-> D
 O -->|"read file ranges"| S
 O -->|"read file ranges"| D
end

classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e,stroke-width:1.6px
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d,stroke-width:1.6px
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764,stroke-width:1.6px
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12,stroke-width:1.6px
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12,stroke-width:1.6px
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155,stroke-width:1.6px
classDef fail fill:#fee2e2,stroke:#dc2626,color:#7f1d1d,stroke-width:1.6px
linkStyle default stroke:#64748b,stroke-width:1.6px,stroke-linecap:round

```

## Control, data and recovery planes

Separate paths prevent the common mistake of drawing every record through a coordinator.

Source references: flink-deploy, spark-launch, flink-checkpoint.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
flowchart TB

subgraph control["Control plane"]
 C["Coordinator / scheduler"]:::control
end
subgraph workers["Worker processes"]
 A["Worker A"]:::worker
 B["Worker B"]:::worker
 A -->|"records / shuffle batches"| B
end
subgraph persistence["Recovery plane"]
 S[("Log / checkpoint / snapshot")]:::store
end
 C -. "deploy, heartbeat, trigger" .-> A
 C -. "deploy, heartbeat, trigger" .-> B
 A -->|"state or output"| S
 B -->|"state or output"| S
 A -. "completion evidence" .-> C
 B -. "completion evidence" .-> C

classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e,stroke-width:1.6px
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d,stroke-width:1.6px
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764,stroke-width:1.6px
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12,stroke-width:1.6px
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12,stroke-width:1.6px
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155,stroke-width:1.6px
classDef fail fill:#fee2e2,stroke:#dc2626,color:#7f1d1d,stroke-width:1.6px
linkStyle default stroke:#64748b,stroke-width:1.6px,stroke-linecap:round

```

## Kafka deployment topology

A broker can host leaders, follower replicas and coordinators for different partitions. The controller role may run separately or share a process depending on deployment.

Source references: kafka-api, kafka-controller, kafka-txn-state.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
flowchart TB

subgraph clients["Client processes"]
 P["Producer"]:::io
 U["Consumer group members"]:::io
end
subgraph brokers["Broker processes"]
 L["Broker A
orders P0 leader"]:::worker
 R["Broker B
orders P0 follower"]:::worker
 G["Group / transaction coordinators"]:::control
 L -->|"Fetch response: record batches"| R
 R -. "next Fetch: follower offset" .-> L
end
subgraph durable["Durable Kafka logs"]
 D[("Data partition segments")]:::store
 I[("Offsets and transaction topics")]:::store
end
subgraph metadata["KRaft controller quorum"]
 Q["Active controller + quorum replicas"]:::state
end
 P -->|"Produce"| L
 U -->|"Fetch"| L
 U -->|"group and offset requests"| G
 L --> D
 G --> I
 Q -. "metadata / leader epochs" .-> L
 Q -. "metadata / leader epochs" .-> R

classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e,stroke-width:1.6px
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d,stroke-width:1.6px
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764,stroke-width:1.6px
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12,stroke-width:1.6px
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12,stroke-width:1.6px
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155,stroke-width:1.6px
classDef fail fill:#fee2e2,stroke:#dc2626,color:#7f1d1d,stroke-width:1.6px
linkStyle default stroke:#64748b,stroke-width:1.6px,stroke-linecap:round

```

## Successful produce with replication

Followers pull. Their subsequent fetch offsets let the leader observe replication progress. This diagram models a successful acks=all path, omitting protocol retries.

Source references: kafka-sender, kafka-replicas, kafka-partition, kafka-hw.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
sequenceDiagram

box rgb(255,237,213) Producer process
 participant P as Sender thread
end
box rgb(220,252,231) Broker processes
 participant L as Leader broker
 participant F as Follower broker
end
 P->>L: Produce(partition, batch, acks=all)
 L->>L: Validate ISR, append local log
 F->>L: Fetch(next offset)
 L-->>F: Record batches
 F->>F: Append follower log
 F->>L: Fetch(advanced offset)
 L->>L: Advance HW, satisfy delayed produce
 L-->>P: ProduceResponse(offset or error)
```

## Inside the broker process

Network I/O and append processing are decoupled. A waiting all-ack request need not block a handler thread for the whole replication delay.

Source references: kafka-socket, kafka-api, kafka-replicas.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
flowchart TB

subgraph network["Broker network threads"]
 N["SocketServer processors"]:::io
end
subgraph handlers["Request handler threads"]
 Q["RequestChannel"]:::state --> A["KafkaApis.handleProduceRequest"]:::runtime
 A --> R["ReplicaManager.appendRecords"]:::runtime
end
subgraph storage["Partition and delayed operations"]
 L[("Leader log append")]:::store
 W["Delayed produce waiting state"]:::state
end
 N -->|"decoded request"| Q
 R --> L
 R --> W
 L -. "replication progress" .-> W
 W -. "response callback" .-> N

classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e,stroke-width:1.6px
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d,stroke-width:1.6px
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764,stroke-width:1.6px
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12,stroke-width:1.6px
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12,stroke-width:1.6px
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155,stroke-width:1.6px
classDef fail fill:#fee2e2,stroke:#dc2626,color:#7f1d1d,stroke-width:1.6px
linkStyle default stroke:#64748b,stroke-width:1.6px,stroke-linecap:round

```

## Read-process-write inside Kafka

Simplified protocol chronology. Marker fan-out includes participating output and offset state; version-specific optimizations are omitted.

Source references: streams-commit, kafka-txn, kafka-markers.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
sequenceDiagram

box rgb(255,237,213) Application
 participant P as Transactional producer
end
box rgb(224,242,254) Kafka coordinators
 participant T as Transaction coordinator
 participant G as Offsets leader / group coordinator
end
box rgb(241,245,249) Data brokers
 participant B as Output partition leaders
end
 P->>B: Produce transactional batches
 P->>G: Stage consumed offsets in transaction
 P->>T: EndTxn(COMMIT)
 T->>T: Persist commit decision
 T-->>P: EndTxn success
 T->>B: WriteTxnMarkers(COMMIT)
 T->>G: WriteTxnMarkers(offsets partition)
 G->>G: completeTransaction, expose committed offsets
 B->>B: Advance stable visibility
 Note over P,B: Commit response does not mean a consumer has fetched the output
```

## A timeout is an unknown outcome until reconciled

This is a general distributed write decision tree. Kafka producer retries and transaction state resolve it within Kafka; external sinks need their own protocol.

Source references: kafka-txn, streams-commit.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
flowchart TB

subgraph caller["Caller"]
 S["Send write"]:::io --> Q{"Response received?"}:::state
 Q -->|"yes"| OK["Interpret success or error"]:::runtime
 Q -->|"timeout"| U["Outcome unknown"]:::fail
end
subgraph authority["Durable authority"]
 U --> R["Retry with same identity
or query commit state"]:::control
 R --> C{"Already committed?"}:::state
 C -->|"yes"| D["Return existing result"]:::runtime
 C -->|"definitely absent"| W["Perform authorized retry"]:::runtime
end

classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e,stroke-width:1.6px
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d,stroke-width:1.6px
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764,stroke-width:1.6px
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12,stroke-width:1.6px
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12,stroke-width:1.6px
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155,stroke-width:1.6px
classDef fail fill:#fee2e2,stroke:#dc2626,color:#7f1d1d,stroke-width:1.6px
linkStyle default stroke:#64748b,stroke-width:1.6px,stroke-linecap:round

```

## Two instances and Kafka-backed state

State is local to the owning task, while restoration is backed by Kafka. A repartition edge is a real Kafka write and read.

Source references: streams-state, streams-restore.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
flowchart TB

subgraph broker["Kafka cluster"]
 I[("Input topics")]:::store
 R[("Repartition topics")]:::store
 C[("State changelog topics")]:::store
 O[("Output topics")]:::store
end
subgraph a["Application instance A"]
 T["Stream task A
processor + local store"]:::runtime
end
subgraph b["Application instance B"]
 U["Stream task B
processor + local store"]:::runtime
 S["Standby for task A"]:::state
end
 I --> T
 T --> R
 R --> U
 T -->|"store changes"| C
 C -->|"restore / standby update"| S
 U --> O

classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e,stroke-width:1.6px
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d,stroke-width:1.6px
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764,stroke-width:1.6px
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12,stroke-width:1.6px
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12,stroke-width:1.6px
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155,stroke-width:1.6px
classDef fail fill:#fee2e2,stroke:#dc2626,color:#7f1d1d,stroke-width:1.6px
linkStyle default stroke:#64748b,stroke-width:1.6px,stroke-linecap:round

```

## Successful task commit

The changelog and output share the Kafka transaction. The local store remains an execution cache / state representation whose recovery follows the committed history.

Source references: streams-commit, streams-state.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
sequenceDiagram

box rgb(237,233,254) Streams process
 participant T as Stream task
 participant P as StreamsProducer
end
box rgb(241,245,249) Kafka
 participant K as Kafka transaction participants
end
 T->>T: Process input, update local store
 T->>P: Send output and changelog records
 P->>K: Produce transactional records
 T->>P: Commit processed offsets
 P->>K: sendOffsetsToTransaction
 P->>K: commitTransaction
 K-->>P: Commit decision acknowledged
 Note over T,K: read_committed consumers observe committed Kafka results
```

## Task takeover after instance loss

Standby promotion or changelog replay reconstructs state; fencing protects against the old instance continuing to commit.

Source references: streams-restore, streams-state, streams-commit.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
flowchart TB

subgraph failure["Old instance"]
 X["Instance lost or fenced"]:::fail
end
subgraph recovery["New owning instance"]
 A["Receive task assignment"]:::control --> Q{"Usable state available?"}:::state
 Q -->|"standby / valid local state"| W["Catch up changelog"]:::runtime
 Q -->|"cold start"| R["Restore store from changelog"]:::runtime
 W --> P["Resume active processing"]:::worker
 R --> P
end
 X -. "rebalance" .-> A

classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e,stroke-width:1.6px
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d,stroke-width:1.6px
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764,stroke-width:1.6px
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12,stroke-width:1.6px
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12,stroke-width:1.6px
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155,stroke-width:1.6px
classDef fail fill:#fee2e2,stroke:#dc2626,color:#7f1d1d,stroke-width:1.6px
linkStyle default stroke:#64748b,stroke-width:1.6px,stroke-linecap:round

```

## Management and record paths

Workers coordinate through Kafka membership and backing topics. External records do not flow through the REST endpoint.

Source references: connect-group, connect-sink.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
flowchart TB

subgraph management["Control services"]
 R["Connect REST management"]:::control
 K[("Kafka internal config / offset / status topics")]:::store
 G["Kafka group coordinator"]:::control
end
subgraph workers["Worker processes"]
 H["DistributedHerder"]:::state
 S["Source task"]:::worker
 T["Sink task"]:::worker
 H -. "start / stop assigned tasks" .-> S
 H -. "start / stop assigned tasks" .-> T
end
subgraph external["Record endpoints"]
 DB[("External source")]:::store
 TOP[("Kafka data topics")]:::store
 OUT[("External sink")]:::store
end
 R --> H
 H <--> G
 H <--> K
 DB --> S --> TOP --> T --> OUT

classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e,stroke-width:1.6px
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d,stroke-width:1.6px
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764,stroke-width:1.6px
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12,stroke-width:1.6px
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12,stroke-width:1.6px
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155,stroke-width:1.6px
classDef fail fill:#fee2e2,stroke:#dc2626,color:#7f1d1d,stroke-width:1.6px
linkStyle default stroke:#64748b,stroke-width:1.6px,stroke-linecap:round

```

## Connect exactly-once source commit

The external source is read, not transactionally mutated by this protocol. Source-offset records and Kafka output share the transactional producer.

Source references: connect-source-eos.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
sequenceDiagram

box rgb(220,252,231) Connect worker
 participant S as SourceTask
 participant W as ExactlyOnce worker
end
box rgb(241,245,249) Kafka
 participant K as Data and offset topics
end
 S-->>W: SourceRecords + source offsets
 W->>K: Produce transactional output
 W->>W: offsetWriter.beginFlush
 W->>K: Flush transactional source-offset records
 W->>K: producer.commitTransaction
 K-->>W: Commit result
 W-->>S: Record / task commit callbacks
```

## A sink side effect can precede offset commit

A process crash in the highlighted gap permits replay. A destination idempotency key can make it harmless.

Source references: connect-sink, connect-precommit.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
sequenceDiagram

box rgb(220,252,231) Connect worker
 participant W as WorkerSinkTask
 participant T as SinkTask
end
box rgb(241,245,249) Remote services
 participant D as Destination
 participant K as Kafka coordinator
end
 W->>T: put(records)
 T->>D: Write or buffer output
 W->>T: preCommit(current offsets)
 T->>D: Flush durable writes if needed
 T-->>W: Safe offsets
 Note over W,D: Crash here can cause duplicate delivery
 W->>K: commitAsync / commitSync
 K-->>W: Offset commit acknowledgement
```

## Flink process boundaries

Worker resource allocation and per-job scheduling are separate responsibilities. Deployment mode determines whether jobs share these cluster services.

Source references: flink-deploy, flink-checkpoint, flink-network.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
flowchart TB

subgraph control["Cluster control processes"]
 D["Dispatcher"]:::control
 R["ResourceManager"]:::control
 J["JobMaster
scheduler + checkpoint coordinator"]:::control
 D --> J
 J -->|"resource requirements"| R
end
subgraph workerA["TaskManager process A"]
 A["TaskExecutor RPC endpoint"]:::worker
 T["Task / StreamTask
operator chain"]:::runtime
 A --> T
end
subgraph workerB["TaskManager process B"]
 B["TaskExecutor RPC endpoint"]:::worker
 U["Task / StreamTask
operator chain"]:::runtime
 B --> U
end
 J -. "submitTask / control RPC" .-> A
 J -. "submitTask / control RPC" .-> B
 T -->|"Netty shuffle data"| U
 T -. "state / checkpoint acknowledgement" .-> J
 U -. "state / checkpoint acknowledgement" .-> J

classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e,stroke-width:1.6px
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d,stroke-width:1.6px
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764,stroke-width:1.6px
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12,stroke-width:1.6px
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12,stroke-width:1.6px
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155,stroke-width:1.6px
classDef fail fill:#fee2e2,stroke:#dc2626,color:#7f1d1d,stroke-width:1.6px
linkStyle default stroke:#64748b,stroke-width:1.6px,stroke-linecap:round

```

## Resource allocation before deployment

This is the connected-worker path. Worker provisioning, registration and retry messages are omitted; a requested slot must still be accepted by the job slot pool.

Source references: flink-requirements, flink-slot-request, flink-slot-offer.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
sequenceDiagram

box rgb(224,242,254) Cluster control
 participant J as JobMaster slot pool
 participant R as ResourceManager
end
box rgb(220,252,231) Worker process
 participant T as TaskExecutor
end
 J->>R: declareRequiredResources(profiles, counts)
 R->>T: requestSlot(job, allocation, leader identity)
 T->>T: Validate leader, reserve slot
 T->>J: offerSlots(reserved slots)
 J-->>T: Accepted slot offers
 Note over J,T: Task deployment is a separate submitTask call
```

## A task starts on a worker

The task descriptor crosses RPC; subsequent operator processing runs inside the worker.

Source references: flink-deploy.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
sequenceDiagram

box rgb(224,242,254) JobManager process
 participant J as JobMaster / scheduler
end
box rgb(220,252,231) TaskManager process
 participant E as TaskExecutor
 participant T as Task thread
end
 J->>E: submitTask(descriptor, leader identity)
 E->>E: Validate slot and job leadership
 E->>T: Construct task, start task thread
 T->>T: Set up gates and result partitions
 T->>T: Restore state and initialize operators
 T-->>J: Report task execution state
 T->>T: Invoke StreamTask mailbox loop
```

## Logical intent becomes retryable attempts

Each step adds a different kind of execution information.

Source references: flink-graph, flink-deploy.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
flowchart TB

subgraph compile["Program and graph construction"]
 T["Transformation DAG
API operations"]:::control --> S["StreamGraph
operators and partitioners"]:::control
 S --> J["JobGraph
chained deployable vertices"]:::control
end
subgraph schedule["Scheduler runtime"]
 J --> E["ExecutionGraph
parallel attempts"]:::state
 E --> A["Task attempt on a slot"]:::worker
end

classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e,stroke-width:1.6px
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d,stroke-width:1.6px
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764,stroke-width:1.6px
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12,stroke-width:1.6px
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12,stroke-width:1.6px
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155,stroke-width:1.6px
classDef fail fill:#fee2e2,stroke:#dc2626,color:#7f1d1d,stroke-width:1.6px
linkStyle default stroke:#64748b,stroke-width:1.6px,stroke-linecap:round

```

## One keyBy creates a physical boundary

Source and map may be chained. The downstream keyed operator owns a different key-group range and receives shuffled data.

Source references: flink-graph, flink-keygroup, flink-network.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
flowchart TB

subgraph taskA["Task A / one mailbox thread"]
 S["Source operator"]:::runtime --> M["Map / filter"]:::runtime
end
subgraph exchange["Network boundary"]
 W["RecordWriter
key-group channel selection"]:::io --> R["Result subpartition"]:::io
end
subgraph taskB["Task B / another mailbox thread"]
 G["InputGate + deserializer"]:::io --> O["Keyed operator"]:::runtime
 O --> ST[("State for owned key groups")]:::state
end
 M --> W
 R -->|"local or remote buffers"| G

classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e,stroke-width:1.6px
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d,stroke-width:1.6px
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764,stroke-width:1.6px
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12,stroke-width:1.6px
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12,stroke-width:1.6px
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155,stroke-width:1.6px
classDef fail fill:#fee2e2,stroke:#dc2626,color:#7f1d1d,stroke-width:1.6px
linkStyle default stroke:#64748b,stroke-width:1.6px,stroke-linecap:round

```

## I/O ownership differs from state ownership

The record emitter updates source progress in the task path after emission. Prefetched records can still be replayed.

Source references: flink-mailbox, fk-fetch, fk-emit.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
flowchart TB

subgraph fetcher["Source I/O thread"]
 P["Poll external source"]:::io --> Q["Fetched record queue"]:::io
end
subgraph task["StreamTask mailbox thread"]
 M["MailboxProcessor"]:::runtime --> E["SourceReader.pollNext
record emitter"]:::runtime
 E --> S["Advance mutable split position"]:::state
 E --> O["Operator chain"]:::runtime
end
 Q --> E
 C["Checkpoint / timer mail"]:::control -.-> M

classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e,stroke-width:1.6px
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d,stroke-width:1.6px
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764,stroke-width:1.6px
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12,stroke-width:1.6px
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12,stroke-width:1.6px
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155,stroke-width:1.6px
classDef fail fill:#fee2e2,stroke:#dc2626,color:#7f1d1d,stroke-width:1.6px
linkStyle default stroke:#64748b,stroke-width:1.6px,stroke-linecap:round

```

## Remote shuffle credit cycle

This is the data transport between TaskManagers, separate from JobMaster task-control RPC.

Source references: flink-network, flink-input.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
sequenceDiagram

box rgb(220,252,231) Upstream TaskManager
 participant P as Result subpartition
end
box rgb(255,237,213) Downstream TaskManager
 participant C as Remote input channel
 participant T as Input task
end
 C->>P: Partition request + available credit
 P-->>C: Data buffers within credit
 C->>T: Deserialized input records
 T-->>C: Consume / recycle buffers
 C->>P: Additional credit
 Note over P,T: No free buffers means upstream cannot drain indefinitely
```

## Two active inputs bound event time

For A=100 and B=80, the combined frontier is 80. Correctly marking B idle removes it from the active minimum.

Source references: flink-watermark.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
flowchart TB

subgraph inputs["Input channel progress"]
 A["A: watermark 100"]:::io
 B["B: watermark 80"]:::io
end
subgraph operator["Operator event-time path"]
 V["StatusWatermarkValve
active aligned minimum = 80"]:::state
 T["Advance event-time timers"]:::runtime
 O["Forward watermark 80"]:::runtime
 V --> T --> O
end
 A --> V
 B --> V

classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e,stroke-width:1.6px
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d,stroke-width:1.6px
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764,stroke-width:1.6px
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12,stroke-width:1.6px
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12,stroke-width:1.6px
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155,stroke-width:1.6px
classDef fail fill:#fee2e2,stroke:#dc2626,color:#7f1d1d,stroke-width:1.6px
linkStyle default stroke:#64748b,stroke-width:1.6px,stroke-linecap:round

```

## Trigger acceptance, state acknowledgement and sink commit

The three acknowledgements have different meanings. The durable checkpoint store is represented inside the coordinator lane.

Source references: flink-barrier, flink-checkpoint.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
sequenceDiagram

box rgb(224,242,254) JobManager
 participant J as Checkpoint coordinator
end
box rgb(220,252,231) TaskManager
 participant T as Task / mailbox
 participant A as Async snapshot worker
end
box rgb(241,245,249) Storage
 participant S as Checkpoint storage
end
 J->>T: triggerCheckpoint(CP42)
 T-->>J: Trigger accepted
 T->>T: Pre-barrier sink preparation
 T->>T: Broadcast barrier, snapshot operators
 T->>A: Materialize snapshot futures
 A->>S: Persist state
 S-->>A: Durable state handles
 A-->>J: AcknowledgeCheckpoint(CP42, handles)
 J->>J: All required ACKs, store completed CP42
 J-->>T: notifyCheckpointComplete(CP42)
 T->>T: Invoke checkpoint-aware sink commit
```

## Checkpoint mode chooses where the waiting cost goes

Both modes preserve a recovery cut when used correctly. Unaligned does not remove checkpoint storage bandwidth limits.

Source references: flink-barrier, flink-checkpoint.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
flowchart TB

subgraph arrival["Multi-input task"]
 B["First checkpoint barrier"]:::io --> Q{"Checkpoint mode?"}:::state
end
subgraph aligned["Aligned"]
 A["Block post-barrier channel"]:::runtime --> W["Wait for remaining barriers"]:::runtime
 W --> AS[("Operator state snapshot")]:::store
end
subgraph unaligned["Unaligned"]
 U["Capture in-flight channel data"]:::runtime --> US[("Operator + channel state")]:::store
end
 Q -->|"aligned"| A
 Q -->|"unaligned"| U

classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e,stroke-width:1.6px
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d,stroke-width:1.6px
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764,stroke-width:1.6px
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12,stroke-width:1.6px
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12,stroke-width:1.6px
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155,stroke-width:1.6px
classDef fail fill:#fee2e2,stroke:#dc2626,color:#7f1d1d,stroke-width:1.6px
linkStyle default stroke:#64748b,stroke-width:1.6px,stroke-linecap:round

```

## Task recovery state machine

A checkpointed sink receipt can be retried after a failed attempt. The external authority decides whether it is already committed.

Source references: flink-restore, flink-checkpoint, fk-commit.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
flowchart TB

subgraph scheduler["JobMaster recovery"]
 X["Task failure or lost worker"]:::fail --> C["Select scope; cancel attempts"]:::control
 C --> R["Restore completed checkpoint"]:::state
end
subgraph workers["Replacement attempts"]
 R --> S["Restore source / operator state"]:::worker
 R --> K["Restore sink committables"]:::worker
 S --> P["Replay input after cut"]:::runtime
 K --> D["Retry or reconcile sink commit"]:::runtime
end

classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e,stroke-width:1.6px
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d,stroke-width:1.6px
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764,stroke-width:1.6px
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12,stroke-width:1.6px
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12,stroke-width:1.6px
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155,stroke-width:1.6px
classDef fail fill:#fee2e2,stroke:#dc2626,color:#7f1d1d,stroke-width:1.6px
linkStyle default stroke:#64748b,stroke-width:1.6px,stroke-linecap:round

```

## Record 104 becomes checkpointed next offset 105

The queue is an in-process thread handoff. The Kafka fetch is network I/O; emission and split-state mutation are task-thread work.

Source references: fk-enumerator, fk-fetch, fk-emit, fk-state.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
sequenceDiagram

box rgb(241,245,249) Kafka broker
 participant K as Partition leader
end
box rgb(255,237,213) TaskManager fetcher thread
 participant F as Kafka split reader
end
box rgb(237,233,254) Task mailbox thread
 participant R as KafkaSourceReader / emitter
end
 F->>K: Fetch through consumer.poll
 K-->>F: Record P0 at offset 104
 F-->>R: Enqueue fetched records
 R->>R: Emit record to operator chain
 R->>R: setCurrentOffset(105)
 R->>R: snapshotState(CP42) creates split at 105
```

## CP42 stores both source progress and output transaction recovery

The two post-checkpoint Kafka operations may finish in either order. No arrow implies atomicity between them.

Source references: fk-offset, fk-writer, fk-commit.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
flowchart TB

subgraph checkpoint["Flink durable recovery cut"]
 CP[("Completed CP42
P0 next offset 105
T42 sink receipt")]:::store
end
subgraph sink["Sink task and Kafka producer"]
 C["KafkaCommitter"]:::runtime --> T["commitTransaction T42"]:::io
 T --> M["Kafka transaction markers
read_committed visibility"]:::state
end
subgraph source["Source task and Kafka consumer"]
 S["KafkaSourceReader completion"]:::runtime --> F["Fetcher commitAsync P0=105"]:::io
 F --> O[("__consumer_offsets
external group position")]:::store
end
 CP -. "completion" .-> C
 CP -. "optional bookkeeping" .-> S

classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e,stroke-width:1.6px
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d,stroke-width:1.6px
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764,stroke-width:1.6px
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12,stroke-width:1.6px
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12,stroke-width:1.6px
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155,stroke-width:1.6px
classDef fail fill:#fee2e2,stroke:#dc2626,color:#7f1d1d,stroke-width:1.6px
linkStyle default stroke:#64748b,stroke-width:1.6px,stroke-linecap:round

```

## Crash after checkpoint, before sink commit acknowledgement

A replacement committer resumes from the durable receipt. The exact connector transaction recovery code decides retry / fatal outcomes.

Source references: fk-commit, fk-writer.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
sequenceDiagram

box rgb(224,242,254) Flink
 participant C as Completed checkpoint
 participant W as Committer task
end
box rgb(241,245,249) Kafka
 participant K as Transaction coordinator
end
 C-->>W: CP42 completion with T42 receipt
 W->>K: Commit T42
 Note over W,K: Task fails or response is lost
 C-->>W: Restore receipt into replacement task
 W->>K: Recover and retry commit protocol
 K-->>W: Committed result or actionable error
```

## Driver control and executor data paths

Comet, when enabled, runs inside an executor task. It does not introduce a separate native cluster scheduler.

Source references: spark-launch, spark-executor, spark-shuffle, comet-create.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
flowchart TB

subgraph driver["Driver process"]
 D["DAGScheduler"]:::control --> T["TaskScheduler + backend"]:::control
end
subgraph e1["Executor process A"]
 A["Executor backend"]:::worker --> M["Map task
JVM or Comet native"]:::runtime
 M --> B[("Local shuffle blocks")]:::store
end
subgraph e2["Executor process B"]
 E["Executor backend"]:::worker --> R["Reduce task
JVM or Comet native"]:::runtime
end
 T -. "LaunchTask RPC" .-> A
 T -. "LaunchTask RPC" .-> E
 B -->|"block fetch network"| R
 A -. "StatusUpdate" .-> T
 E -. "StatusUpdate" .-> T

classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e,stroke-width:1.6px
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d,stroke-width:1.6px
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764,stroke-width:1.6px
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12,stroke-width:1.6px
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12,stroke-width:1.6px
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155,stroke-width:1.6px
classDef fail fill:#fee2e2,stroke:#dc2626,color:#7f1d1d,stroke-width:1.6px
linkStyle default stroke:#64748b,stroke-width:1.6px,stroke-linecap:round

```

## A missing shuffle block causes re-execution

This is a simplified classic shuffle failure path. Newer shuffle modes can alter the retry scope.

Source references: spark-retry, spark-shuffle.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
sequenceDiagram

box rgb(220,252,231) Executor processes
 participant R as Reduce task
 participant M as Map-output server
end
box rgb(224,242,254) Driver
 participant D as DAGScheduler
end
 R->>M: Fetch shuffle block
 M-->>R: Missing block / connection failure
 R-->>D: FetchFailed
 D->>D: Invalidate affected map output
 D->>M: Schedule replacement map attempt
 M-->>D: New output location
 D->>R: Resubmit dependent work
```

## Micro-batch 42 with Kafka input

Driver checkpoint logs and destination writes are distinct durable operations.

Source references: spark-offset, spark-end, spark-state.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
sequenceDiagram

box rgb(224,242,254) Driver
 participant D as MicroBatchExecution
end
box rgb(241,245,249) Query storage
 participant C as Checkpoint directory
end
box rgb(220,252,231) Executors
 participant E as Batch tasks
end
box rgb(241,245,249) Destination
 participant S as Sink
end
 D->>C: Write offsets/42
 D->>E: Execute recorded input range
 E->>E: Read source, update state-store version
 E->>S: Write batch output
 E-->>D: Sink execution completes
 D->>C: Write commits/42
 Note over D,S: Crash before commits/42 can replay output
```

## One recorded range can produce twice

Both write attempts can be valid Kafka producer sessions. The missing Spark commit marker is what causes query replay.

Source references: spark-kafka, spark-offset, spark-end.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
flowchart TB

subgraph checkpoint["Durable query checkpoint"]
 O[("offsets/42 exists
commits/42 absent")]:::state
end
subgraph first["First execution"]
 A["Read P0 100..104"]:::runtime --> W["Write output to Kafka"]:::io
 W --> X["Driver dies before commit log"]:::fail
end
subgraph replay["Restart"]
 O --> R["Re-execute batch 42"]:::runtime
 R --> D["Write same logical output again"]:::io
end
 O --> A
 X -. "restart" .-> R

classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e,stroke-width:1.6px
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d,stroke-width:1.6px
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764,stroke-width:1.6px
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12,stroke-width:1.6px
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12,stroke-width:1.6px
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155,stroke-width:1.6px
classDef fail fill:#fee2e2,stroke:#dc2626,color:#7f1d1d,stroke-width:1.6px
linkStyle default stroke:#64748b,stroke-width:1.6px,stroke-linecap:round

```

## From table identity to rows

Metadata arrows are references, not a requirement that all reads occur serially. Planning and file I/O can be parallelized.

Source references: iceberg-scan, iceberg-rest.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
flowchart TB

subgraph catalog["Catalog service or implementation"]
 C["Table identifier
current metadata pointer"]:::control
end
subgraph metadata["Immutable metadata in storage"]
 M[("Table metadata JSON")]:::state
 S["Chosen snapshot"]:::state
 L[("Manifest list")]:::store
 F[("Data / delete manifests")]:::store
 C --> M --> S --> L --> F
end
subgraph data["Data storage"]
 P[("Parquet data files")]:::store
 D[("Delete files when applicable")]:::store
end
 F --> P
 F --> D

classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e,stroke-width:1.6px
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d,stroke-width:1.6px
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764,stroke-width:1.6px
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12,stroke-width:1.6px
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12,stroke-width:1.6px
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155,stroke-width:1.6px
classDef fail fill:#fee2e2,stroke:#dc2626,color:#7f1d1d,stroke-width:1.6px
linkStyle default stroke:#64748b,stroke-width:1.6px,stroke-linecap:round

```

## Write files, then publish one table update

A failed response may be ambiguous. The storage writes alone do not make rows visible to snapshot readers.

Source references: iceberg-snapshot, iceberg-rest.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
sequenceDiagram

box rgb(220,252,231) Engine process
 participant W as Writer / committer
end
box rgb(241,245,249) Durable services
 participant O as File storage
 participant C as REST catalog
end
 W->>C: Load current table metadata
 C-->>W: Base metadata and snapshot
 W->>O: Write data / delete files
 W->>O: Write manifests / manifest list
 W->>C: Commit requirements + metadata updates
 C->>C: Validate and atomically publish
 C-->>W: Updated table metadata
 Note over W,C: Lost reply requires reconciliation, not blind file deletion
```

## A conflict and an unknown outcome need different handling

Retry only when the operation and validation permit it. Retain possibly committed files while the result is unknown.

Source references: iceberg-snapshot, iceberg-rest.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
flowchart TB

subgraph commit["Snapshot publication"]
 direction TB
 A["Attempt metadata commit"]:::control --> Q{"Outcome"}:::state
 Q -->|"success"| S["Snapshot visible"]:::runtime
 Q -->|"known conflict"| R["Refresh base; revalidate; retry"]:::runtime
 R --> A
 Q -->|"unknown"| U["Preserve files; reconcile state"]:::fail
end

classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e,stroke-width:1.6px
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d,stroke-width:1.6px
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764,stroke-width:1.6px
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12,stroke-width:1.6px
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12,stroke-width:1.6px
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155,stroke-width:1.6px
classDef fail fill:#fee2e2,stroke:#dc2626,color:#7f1d1d,stroke-width:1.6px
linkStyle default stroke:#64748b,stroke-width:1.6px,stroke-linecap:round

```

## Recover a checkpoint whose commit notification was lost

CP42 is durable before the engine fails. Recovery finds whether its files are already represented in the table.

Source references: iceberg-flink, iceberg-recover.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
sequenceDiagram

box rgb(224,242,254) Flink
 participant C as Checkpoint storage
 participant W as Iceberg files committer
end
box rgb(241,245,249) Iceberg
 participant T as Table snapshots
end
 W->>C: Store pending CP42 file metadata
 Note over C,W: Checkpoint completes, process fails before notification
 C-->>W: Restore CP42 pending state
 W->>T: Find max committed checkpoint for identity
 T-->>W: Last committed checkpoint
 W->>T: Commit only restored pending work after it
 T-->>W: Snapshot with checkpoint identity
```

## Prune work before decoding rows

The exact filters pushed into each layer depend on engine and adapter support. Residual predicates remain when pushdown is inexact.

Source references: iceberg-scan, parquet-stream, df-iceberg.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
flowchart TB

subgraph planning["Engine planning"]
 Q["Query projection and predicates"]:::control --> M["Prune manifests and files"]:::state
 M --> T["File scan tasks
with delete applicability"]:::state
end
subgraph worker["Execution worker"]
 T --> R["Parquet ranged reads
selected columns / row groups"]:::io
 R --> A["Decode Arrow or engine batches"]:::runtime
 A --> D["Apply deletes and residual predicates"]:::runtime
end

classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e,stroke-width:1.6px
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d,stroke-width:1.6px
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764,stroke-width:1.6px
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12,stroke-width:1.6px
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12,stroke-width:1.6px
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155,stroke-width:1.6px
classDef fail fill:#fee2e2,stroke:#dc2626,color:#7f1d1d,stroke-width:1.6px
linkStyle default stroke:#64748b,stroke-width:1.6px,stroke-linecap:round

```

## One host process using DataFusion

Object-store calls are external I/O. Most operator-to-operator movement is local batch / stream execution.

Source references: df-execute, df-stream.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
flowchart TB

subgraph process["Application process"]
 S["SessionContext
SQL and catalog resolution"]:::control --> L["Logical plan and optimizer"]:::control
 L --> P["Physical ExecutionPlan tree"]:::runtime
 P --> E["execute partition
TaskContext + stream"]:::runtime
 E --> A["Arrow RecordBatch consumer"]:::runtime
end
subgraph remote["External storage"]
 O[("Parquet / object storage")]:::store
 C["Catalog through adapter"]:::control
end
 S <-->|"catalog API"| C
 O -->|"async file reads"| E

classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e,stroke-width:1.6px
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d,stroke-width:1.6px
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764,stroke-width:1.6px
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12,stroke-width:1.6px
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12,stroke-width:1.6px
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155,stroke-width:1.6px
classDef fail fill:#fee2e2,stroke:#dc2626,color:#7f1d1d,stroke-width:1.6px
linkStyle default stroke:#64748b,stroke-width:1.6px,stroke-linecap:round

```

## A memory request chooses the next execution path

Spill is an operator capability. It avoids some OOM conditions at the cost of I/O and longer latency.

Source references: df-memory, df-repartition.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
flowchart TB

subgraph operator["Stateful physical operator"]
 direction TB
 B["Receive input RecordBatch"]:::runtime --> R["Request memory reservation"]:::state
 R --> Q{"Budget available?"}:::state
 Q -->|"yes"| P["Process in memory"]:::runtime
 Q -->|"no; spill supported"| S["Write temporary spill files"]:::io
 S --> F["Release reservations; continue / merge"]:::runtime
 Q -->|"no spill path"| E["Return resource error"]:::fail
end

classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e,stroke-width:1.6px
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d,stroke-width:1.6px
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764,stroke-width:1.6px
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12,stroke-width:1.6px
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12,stroke-width:1.6px
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155,stroke-width:1.6px
classDef fail fill:#fee2e2,stroke:#dc2626,color:#7f1d1d,stroke-width:1.6px
linkStyle default stroke:#64748b,stroke-width:1.6px,stroke-linecap:round

```

## Async stream progress

Pending means the caller should wait for a wakeup, not spin. Actual tasks and executors are managed by the host runtime.

Source references: df-execute, df-stream.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
sequenceDiagram

box rgb(237,233,254) Rust process
 participant C as Batch consumer
 participant P as Execution stream
 participant I as Async I/O task
end
 C->>P: poll_next(context)
 P->>I: Start / await read
 P-->>C: Pending
 I-->>P: Read completes, wake task
 C->>P: poll_next(context)
 P-->>C: Ready(RecordBatch)
 C->>C: Consume batch before next poll
```

## Conceptual distributed host around DataFusion

Dashed control links are responsibilities a host must implement. This diagram is a design decomposition, not a claim that DataFusion core includes these services.

Source references: df-execute, df-repartition.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
flowchart TB

subgraph host["Host-provided distributed services"]
 S["Scheduler and membership"]:::control
 M[("Job / attempt metadata")]:::store
 S --> M
end
subgraph workerA["Worker process A"]
 A["DataFusion partition execution"]:::runtime
end
subgraph workerB["Worker process B"]
 B["DataFusion partition execution"]:::runtime
end
subgraph transport["Host-provided data exchange"]
 X["Remote shuffle / batch transport"]:::io
end
 S -. "assign attempt" .-> A
 S -. "assign attempt" .-> B
 A --> X --> B

classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e,stroke-width:1.6px
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d,stroke-width:1.6px
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764,stroke-width:1.6px
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12,stroke-width:1.6px
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12,stroke-width:1.6px
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155,stroke-width:1.6px
classDef fail fill:#fee2e2,stroke:#dc2626,color:#7f1d1d,stroke-width:1.6px
linkStyle default stroke:#64748b,stroke-width:1.6px,stroke-linecap:round

```

## Comet fits inside Spark execution

Scheduling RPC and remote shuffle remain Spark responsibilities. JNI is an in-process boundary.

Source references: comet-extension, comet-create, comet-jni, comet-next.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
flowchart TB

subgraph driver["Spark driver process"]
 P["Catalyst physical plan"]:::control --> R["Comet extension rules"]:::control
 R --> S["Spark stage and task scheduling"]:::control
end
subgraph executor["Spark executor process"]
 I["CometExecIterator
JVM task thread"]:::worker
 J["JNI createPlan / executePlan"]:::io
 D["Native DataFusion physical plan"]:::runtime
 A["Arrow batches / native memory"]:::state
 I --> J --> D --> A
 A --> I
end
 S -. "LaunchTask RPC" .-> I

classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e,stroke-width:1.6px
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d,stroke-width:1.6px
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764,stroke-width:1.6px
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12,stroke-width:1.6px
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12,stroke-width:1.6px
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155,stroke-width:1.6px
classDef fail fill:#fee2e2,stroke:#dc2626,color:#7f1d1d,stroke-width:1.6px
linkStyle default stroke:#64748b,stroke-width:1.6px,stroke-linecap:round

```

## One executor task enters and exits native execution

No network RPC occurs between these JVM and Rust lanes. External file or shuffle I/O can occur within the native execution path.

Source references: comet-create, comet-jni, comet-next, comet-close.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
sequenceDiagram

box rgb(220,252,231) JVM in executor process
 participant J as CometExecIterator
end
box rgb(237,233,254) Native library in same process
 participant N as JNI execution context
 participant D as DataFusion stream
end
 J->>N: createPlan(serialized plan, task context)
 N-->>J: Native plan handle
 J->>N: executePlan(array/schema addresses)
 N->>D: execute / poll next batch
 D-->>N: Arrow RecordBatch
 N-->>J: Export columnar output
 J->>N: releasePlan on close / task completion
 N->>N: Drop streams and release reservations
```

## Where a native speedup can disappear

These are performance analysis categories, not measured benchmark results.

Source references: comet-extension, comet-next.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
flowchart TB

subgraph execution["One Spark stage"]
 direction TB
 I["Storage / shuffle input"]:::io --> Q{"Supported native plan?"}:::state
 Q -->|"yes"| N["Native vector execution"]:::runtime
 Q -->|"partial / unsupported"| F["Spark fallback or mixed plan"]:::worker
 N --> O["Output conversion / shuffle / commit"]:::io
 F --> O
end

classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e,stroke-width:1.6px
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d,stroke-width:1.6px
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764,stroke-width:1.6px
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12,stroke-width:1.6px
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12,stroke-width:1.6px
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155,stroke-width:1.6px
classDef fail fill:#fee2e2,stroke:#dc2626,color:#7f1d1d,stroke-width:1.6px
linkStyle default stroke:#64748b,stroke-width:1.6px,stroke-linecap:round

```

## Batch views share one backing allocation

The ownership graph is local to a process. Reference sharing is not durable replication.

Source references: arrow-buffer, arrow-slice.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
flowchart TB

subgraph taskA["Consumer task A"]
 A["Array / Buffer view A"]:::runtime
end
subgraph taskB["Consumer task B"]
 B["Sliced Buffer view B"]:::runtime
end
subgraph allocation["Process memory"]
 R["Arc ownership count"]:::state
 D[("Shared Bytes allocation")]:::store
end
 A --> R
 B --> R
 R --> D

classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e,stroke-width:1.6px
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d,stroke-width:1.6px
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764,stroke-width:1.6px
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12,stroke-width:1.6px
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12,stroke-width:1.6px
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155,stroke-width:1.6px
classDef fail fill:#fee2e2,stroke:#dc2626,color:#7f1d1d,stroke-width:1.6px
linkStyle default stroke:#64748b,stroke-width:1.6px,stroke-linecap:round

```

## A Parquet read becomes columnar execution

Parquet is an on-disk format. Arrow is an in-memory layout. Reading compressed Parquet requires decoding even when later Arrow sharing avoids copies.

Source references: parquet-stream, df-execute.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
flowchart TB

subgraph storage["File / object storage"]
 P[("Parquet row groups and pages")]:::store
end
subgraph native["Rust process"]
 R["Async ranged reads"]:::io --> D["Decompress and decode"]:::runtime
 D --> B["Arrow RecordBatch"]:::state
 B --> V["Vector filters / projections / aggregates"]:::runtime
end
 P --> R

classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e,stroke-width:1.6px
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d,stroke-width:1.6px
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764,stroke-width:1.6px
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12,stroke-width:1.6px
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12,stroke-width:1.6px
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155,stroke-width:1.6px
classDef fail fill:#fee2e2,stroke:#dc2626,color:#7f1d1d,stroke-width:1.6px
linkStyle default stroke:#64748b,stroke-width:1.6px,stroke-linecap:round

```

## Foreign-memory ownership must remain explicit

Conceptual Arrow ABI lifecycle. The actual release callback is supplied by the exporting allocation owner.

Source references: arrow-ffi, comet-close.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
sequenceDiagram

box rgb(237,233,254) Exporting runtime
 participant E as Arrow allocation owner
end
box rgb(220,252,231) Importing runtime
 participant I as Foreign array consumer
end
 E->>I: Array / schema + release contract
 I->>I: Import pointer-backed view
 I->>I: Process while owner remains valid
 I-->>E: Invoke release once on final ownership exit
 E->>E: Free allocation when no owner remains
 Note over E,I: Invalid pointers or double release violate the unsafe ABI contract
```

## The record, checkpoint and table commit

The writer and committer are Flink operators, not a standalone Iceberg compute service. Their catalog and file-storage calls are expanded in the Iceberg chapter. A file upload alone is not the table visibility point.

Source references: fk-emit, fk-state, flink-checkpoint, iceberg-flink.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
sequenceDiagram

box rgb(241,245,249) Kafka
 participant K as Orders P0
end
box rgb(220,252,231) Flink
 participant F as Source and keyed task
 participant C as Checkpoint coordinator
end
box rgb(220,252,231) Flink sink task
 participant I as Writer / table committer
end
 K-->>F: Fetch record offset 104
 F->>F: Emit, next offset=105, total=100
 F->>I: Write event / aggregate output
 C->>F: Trigger CP42
 F->>I: Barrier CP42 through data path
 I->>I: Snapshot pending file metadata
 F-->>C: Source and operator state ACK
 I-->>C: Pending sink state ACK
 C->>C: Persist completed CP42
 C-->>I: Checkpoint completion reaches sink
 I->>I: Publish S900 with CP42 identity
```

## Rollback the recovery position, not the Kafka input log

Input remains in Kafka. The replacement task replays it against restored state.

Source references: flink-restore, fk-writer, iceberg-snapshot.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
flowchart TB

subgraph durable["Last completed checkpoint"]
 C[("CP41
next offset=100; total=70")]:::store
end
subgraph failed["Failed attempt"]
 A["Process offsets 100..104"]:::runtime --> X["CP42 incomplete
worker lost"]:::fail
end
subgraph replacement["Replacement task"]
 C --> R["Restore CP41 state"]:::worker
 R --> P["Replay offsets 100..104"]:::runtime
 P --> N["Prepare new valid sink work"]:::io
end
 X -. "failover" .-> R

classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e,stroke-width:1.6px
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d,stroke-width:1.6px
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764,stroke-width:1.6px
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12,stroke-width:1.6px
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12,stroke-width:1.6px
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155,stroke-width:1.6px
classDef fail fill:#fee2e2,stroke:#dc2626,color:#7f1d1d,stroke-width:1.6px
linkStyle default stroke:#64748b,stroke-width:1.6px,stroke-linecap:round

```

## An ambiguous Iceberg commit response

The table state is consulted before treating the pending checkpoint as new work.

Source references: iceberg-recover, iceberg-flink.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
sequenceDiagram

box rgb(220,252,231) Flink sink attempts
 participant A as Attempt 1
 participant B as Replacement attempt
end
box rgb(241,245,249) Durable services
 participant T as Iceberg table
 participant C as Completed CP42
end
 A->>T: Publish files with CP42 identity
 T->>T: Commit S900
 Note over A,T: Success reply is lost, task fails
 C-->>B: Restore pending CP42 information
 B->>T: Read committed checkpoint identity
 T-->>B: CP42 already represented in S900
 B->>B: Avoid duplicate logical publication
```

## Published snapshot to distributed native query

Catalog lookup and object-store reads are network operations. JNI calls are local to each executor.

Source references: iceberg-scan, spark-launch, comet-jni, spark-shuffle.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
flowchart TB

subgraph driver["Spark driver"]
 Q["Resolve S900 and plan files"]:::control --> S["Schedule scan / aggregate stages"]:::control
end
subgraph exA["Executor A"]
 J["Comet iterator -> JNI"]:::worker --> D["DataFusion scan / partial aggregate"]:::runtime
end
subgraph exB["Executor B"]
 R["Shuffle receive + final aggregate"]:::runtime
end
subgraph data["Object storage"]
 P[("S900 Parquet files")]:::store
end
 S -. "launch task" .-> J
 S -. "launch task" .-> R
 P -->|"ranged read"| D
 D -->|"Spark shuffle transport"| R

classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e,stroke-width:1.6px
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d,stroke-width:1.6px
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764,stroke-width:1.6px
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12,stroke-width:1.6px
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12,stroke-width:1.6px
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155,stroke-width:1.6px
classDef fail fill:#fee2e2,stroke:#dc2626,color:#7f1d1d,stroke-width:1.6px
linkStyle default stroke:#64748b,stroke-width:1.6px,stroke-linecap:round

```

## One checkpoint can release two independent commits

The missing edge between external commits is deliberate: neither destination atomically commits the other.

Source references: flink-checkpoint, fk-commit, iceberg-flink.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
flowchart TB

subgraph flink["Completed recovery cut"]
 C[("CP42
source + both sink receipts")]:::store
end
subgraph kafka["Kafka transaction authority"]
 K["Commit T42"]:::control --> V["Kafka output visible"]:::runtime
end
subgraph iceberg["Iceberg catalog authority"]
 I["Publish S900"]:::control --> T["Table snapshot visible"]:::runtime
end
 C -.-> K
 C -.-> I

classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e,stroke-width:1.6px
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d,stroke-width:1.6px
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764,stroke-width:1.6px
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12,stroke-width:1.6px
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12,stroke-width:1.6px
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155,stroke-width:1.6px
classDef fail fill:#fee2e2,stroke:#dc2626,color:#7f1d1d,stroke-width:1.6px
linkStyle default stroke:#64748b,stroke-width:1.6px,stroke-linecap:round

```

## The path to an externally visible result

This is a latency budget, not a mandatory serial call graph. Actual stages overlap and must be measured.

Source references: flink-checkpoint, fk-commit, iceberg-flink, spark-end.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
flowchart TB

subgraph ingress["Transport and compute"]
 I["Producer / fetch delay"]:::io --> Q["Queue wait"]:::state --> P["Operator compute"]:::runtime
end
subgraph publish["Commit-gated visibility"]
 W["Wait for cut / batch boundary"]:::state --> C["Complete durable checkpoint / batch"]:::control
 C --> S["Sink transaction / snapshot publication"]:::io
end
 P --> W

classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e,stroke-width:1.6px
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d,stroke-width:1.6px
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764,stroke-width:1.6px
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12,stroke-width:1.6px
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12,stroke-width:1.6px
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155,stroke-width:1.6px
classDef fail fill:#fee2e2,stroke:#dc2626,color:#7f1d1d,stroke-width:1.6px
linkStyle default stroke:#64748b,stroke-width:1.6px,stroke-linecap:round

```

## Overload policy determines failure behavior

A bounded queue creates a decision point. An unbounded queue defers the decision to memory exhaustion.

Source references: flink-input, df-memory, kafka-producer.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
flowchart TB

subgraph pipeline["Bounded processing stage"]
 direction TB
 A["Incoming records"]:::io --> Q{"Queue has capacity?"}:::state
 Q -->|"yes"| P["Process at sustainable rate"]:::runtime
 Q -->|"no"| B{"Overload policy"}:::state
 B -->|"backpressure"| W["Slow upstream"]:::control
 B -->|"reject / shed"| R["Explicit failure or dropped work"]:::fail
 B -->|"durable buffer"| D[("Retained log / backlog")]:::store
end

classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e,stroke-width:1.6px
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d,stroke-width:1.6px
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764,stroke-width:1.6px
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12,stroke-width:1.6px
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12,stroke-width:1.6px
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155,stroke-width:1.6px
classDef fail fill:#fee2e2,stroke:#dc2626,color:#7f1d1d,stroke-width:1.6px
linkStyle default stroke:#64748b,stroke-width:1.6px,stroke-linecap:round

```

## Detection must be followed by enforced ownership

The coordinator replacing an owner does not by itself stop an old process that is still alive.

Source references: streams-commit, fk-commit, flink-deploy.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
flowchart TB

subgraph control["Control plane"]
 H["Missed heartbeat / lease expiry"]:::state --> N["Assign new owner with epoch E+1"]:::control
end
subgraph owners["Worker processes"]
 O["Old owner, epoch E"]:::fail
 W["New owner, epoch E+1"]:::worker
end
subgraph authority["Commit authority"]
 F["Validate epoch / fencing token"]:::state
 C[("Durable accepted commit")]:::store
end
 N -.-> W
 O -->|"stale commit"| F
 W -->|"current commit"| F
 F -->|"accept only authorized owner"| C

classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e,stroke-width:1.6px
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d,stroke-width:1.6px
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764,stroke-width:1.6px
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12,stroke-width:1.6px
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12,stroke-width:1.6px
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155,stroke-width:1.6px
classDef fail fill:#fee2e2,stroke:#dc2626,color:#7f1d1d,stroke-width:1.6px
linkStyle default stroke:#64748b,stroke-width:1.6px,stroke-linecap:round

```

## Recovery is complete only after catch-up

A RUNNING task can still be far behind real time. Track lag and output freshness after restart.

Source references: flink-restore, streams-restore, spark-retry.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#142337","lineColor":"#718096","actorBkg":"#eaf2fa","actorBorder":"#53728c","actorTextColor":"#142337","signalColor":"#526479","signalTextColor":"#142337","noteBkgColor":"#fff7d6","noteTextColor":"#423511"},"flowchart":{"curve":"basis","nodeSpacing":42,"rankSpacing":65,"htmlLabels":false,"padding":20},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":34,"noteMargin":16,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":175}}}%%
flowchart TB

subgraph recovery["Recovery timeline"]
 direction TB
 D["Detect failure"]:::fail --> A["Allocate replacement"]:::control
 A --> R["Restore durable state"]:::state
 R --> C["Replay backlog while new input arrives"]:::runtime
 C --> Q{"Processing rate exceeds arrival?"}:::state
 Q -->|"yes"| F["Catch up to freshness target"]:::worker
 Q -->|"no"| B["Lag persists or grows"]:::fail
end

classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e,stroke-width:1.6px
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d,stroke-width:1.6px
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764,stroke-width:1.6px
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12,stroke-width:1.6px
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12,stroke-width:1.6px
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155,stroke-width:1.6px
classDef fail fill:#fee2e2,stroke:#dc2626,color:#7f1d1d,stroke-width:1.6px
linkStyle default stroke:#64748b,stroke-width:1.6px,stroke-linecap:round

```

<!-- kafka-rpc-deep-dive -->

# Kafka RPC and Flink runtime deep dive

## A deployment is not one pod per box

Illustrative Kubernetes placement, not an observed cluster. Kafka brokers and Flink TaskManagers are separate processes; connector libraries live in TaskManagers. KRaft controllers are shown as a separate quorum; combined-role deployments also exist.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"flowchart":{"curve":"basis","nodeSpacing":48,"rankSpacing":65,"htmlLabels":false,"padding":22}}}%%
flowchart TB

subgraph KAFKA["Kafka cluster — multiple processes / pods"]
 QC["KRaft controller quorum
metadata log"]
 B1["Broker A
input P0 leader"]
 B2["Broker B
output P0 leader"]
 BR["Other broker replicas
independent volumes"]
end
subgraph FLINK["Flink cluster"]
 JM["JobManager pod
JobMaster + SourceCoordinator"]
 T1["TaskManager pod A
KafkaSource + parse chain"]
 T2["TaskManager pod B
keyed transform + KafkaSink"]
end
subgraph DURABLE["Independent durable storage"]
 CP[("Checkpoint object storage")]
end
B1 -->|"Fetch response batches"| T1
T1 -->|"keyBy: Flink data channel"| T2
T2 -->|"Produce request batches"| B2
BR -->|"replica Fetch"| B1
BR -->|"replica Fetch"| B2
JM -. "deploy / checkpoint RPCs" .-> T1
JM -. "deploy / checkpoint RPCs" .-> T2
T1 --> CP
T2 --> CP
QC -. "metadata replicated to brokers" .-> B1
QC -. "metadata replicated to brokers" .-> B2
class B1,B2,BR,QC,CP store
class JM control
class T1,T2 worker

classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12
linkStyle default stroke:#64748b,stroke-width:1.7px,stroke-linecap:round
```

## Three very different meanings of “connected”

A direct operator call needs neither Kafka nor Flink RPC. Separate tasks use a data exchange even when co-located. Different TaskManagers use Netty data transport, not a per-record JobMaster RPC.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"flowchart":{"curve":"basis","nodeSpacing":48,"rankSpacing":65,"htmlLabels":false,"padding":22}}}%%
flowchart TB

subgraph ONE["Case 1 — one chained task / one mailbox thread"]
 direction TB
 A["Source operator"] --> B["Map operator"]
end
subgraph TWO["Case 2 — separate tasks / same TaskManager JVM"]
 direction TB
 C["Task thread 1
serialize output"] --> D["Local input channel
Task thread 2"]
end
subgraph THREE["Case 3 — separate TaskManager processes"]
 direction TB
 E["TaskManager A
ResultPartition"] -->|"Netty TCP buffers + credits"| F["TaskManager B
RemoteInputChannel"]
end
class A,B runtime
class C,D worker
class E,F io

classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12
linkStyle default stroke:#64748b,stroke-width:1.7px,stroke-linecap:round
```

## Bootstrap finds owners; owners serve requests

Simplified healthy path. Connections are reused; TLS/SASL and ApiVersions happen per connection as needed. The transaction coordinator and group coordinator can be different brokers from both partition leaders.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":35,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":170}}}%%
sequenceDiagram

box rgb(255,237,213) Client process
participant C as Kafka client
end
box rgb(241,245,249) Kafka broker processes
participant S as Bootstrap broker
participant L as Partition leader
participant G as Coordinator broker
end
C->>S: Connect + ApiVersions
S-->>C: Supported API version ranges
C->>S: Metadata for input/output topics
S-->>C: Partition leaders + advertised endpoints
C->>L: Produce or Fetch at selected version
L-->>C: Per-partition data / result / error
C->>S: FindCoordinator keyed by ID/type
S-->>C: Coordinator node endpoint
C->>G: OffsetCommit or EndTxn
G-->>C: Coordinator result
Note over C,G: No load balancer hop per record after metadata routing
```

## Inside one Kafka broker process

These are threads, queues and components within a broker JVM—not separate pods. Replica fetchers in other brokers pull from this leader. Delayed operations release handler threads rather than blocking one thread per waiting request.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"flowchart":{"curve":"basis","nodeSpacing":48,"rankSpacing":65,"htmlLabels":false,"padding":22}}}%%
flowchart TB

subgraph NET["SocketServer network threads"]
 A["Acceptor
accept TCP"] --> P["Processor / Selector
read framed request"]
 R["Processor
write response"]
end
subgraph SERVER["Request handling within same JVM"]
 Q["RequestChannel queue"] --> H["KafkaRequestHandler
KafkaApis authorization + dispatch"]
 H --> M["ReplicaManager / Partition
validate + append"]
 M --> D["Delayed operation
await replication or timeout"]
end
subgraph DISK["Broker-local storage"]
 L[("UnifiedLog → LogSegment
page cache / log files")]
end
P --> Q
M --> L
D -. "completion callback" .-> R
M -. "immediate result when ready" .-> R
class A,P,R io
class Q,H,M worker
class D state
class L store

classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12
linkStyle default stroke:#64748b,stroke-width:1.7px,stroke-linecap:round
```

## Produce durability is partition replication

Example: replication factor 3, acks=all, min.insync.replicas=2. Both followers are initially in ISR. A real request batches many records/partitions; followers repeat Fetch calls and the leader learns their advanced offsets.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":35,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":170}}}%%
sequenceDiagram

box rgb(255,237,213) Producer process
participant P as Sender thread
end
box rgb(241,245,249) Three broker processes
participant L as P0 leader
participant F1 as ISR follower B
participant F2 as ISR follower C
end
P->>L: Produce batch with acks=-1
L->>L: Validate ISR + append local log
F1->>L: Replica Fetch at old LEO
L-->>F1: New batch
F1->>F1: Append replica log
F2->>L: Replica Fetch at old LEO
L-->>F2: New batch
F2->>F2: Append replica log
F1->>L: Next Fetch reports advanced offset
F2->>L: Next Fetch reports advanced offset
L->>L: Advance HW / complete delayed Produce
L-->>P: ProduceResponse with base offset
Note over P,L: A timeout can be ambiguous: append may already exist
```

## One KafkaSource subtask crosses a thread boundary

A SplitFetcher owns consumer API calls. A bounded handoff queue feeds the task mailbox, where deserialization/emission advances checkpointed split state. Consumer internal network threading depends on client configuration/version.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"flowchart":{"curve":"basis","nodeSpacing":48,"rankSpacing":65,"htmlLabels":false,"padding":22}}}%%
flowchart TB

subgraph JM["JobManager process"]
 E["KafkaSourceEnumerator
discover and assign splits"]
end
subgraph IO["TaskManager — SplitFetcher I/O thread"]
 K["Kafka partition split reader
assign / seek / poll"]
end
subgraph MAIL["TaskManager — StreamTask mailbox thread"]
 A["SourceOperator
handle AddSplitEvent"]
 S["SourceReaderBase
pollNext()"] --> EM["KafkaRecordEmitter
deserialize and emit"]
 EM --> MAP["Chained transform
processElement"]
 MAP -->|"return to emitter"| O["Split currentOffset
record.offset + 1"]
end
subgraph SHARED["TaskManager handoff"]
 Q["RecordsWithSplitIds queue"]
end
E -. "AddSplitEvent via Flink control path" .-> A
A -. "split assignment work" .-> K
K --> Q --> S
class E control
class K io
class A,S,EM,MAP runtime
class O state
class Q worker

classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12
linkStyle default stroke:#64748b,stroke-width:1.7px,stroke-linecap:round
```

## The checkpoint ties recovery positions to sink intent

Aligned checkpoint sketch; each downstream task is triggered by barriers on data channels. “RPC accepted” is not “snapshot durable.” The coordinator waits for all required task/coordinator state, not only the source.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":35,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":170}}}%%
sequenceDiagram

box rgb(224,242,254) JobManager
participant J as Checkpoint coordinator
end
box rgb(237,233,254) TaskManager task/mailbox paths
participant S as Source task
participant W as Transform + sink task
end
box rgb(241,245,249) Durable storage
participant D as Checkpoint storage
end
J->>S: triggerCheckpoint RPC
S-->>J: Trigger accepted
S-)W: Barrier through Flink data channel
S->>S: Snapshot next offsets at mailbox cut
W->>W: Align inputs if needed
W->>W: Flush + prepareCommit + snapshot state
S->>D: Async persist source state
W->>D: Async persist state + committables
S-->>J: acknowledgeCheckpoint with state handles
W-->>J: acknowledgeCheckpoint with state handles
J->>D: Persist completed checkpoint metadata
J-->>S: notifyCheckpointComplete
J-->>W: notifyCheckpointComplete
Note over J,W: Completion makes recovery state durable, not every output immediately visible
```

## Completed checkpoint to externally visible output

Healthy KafkaSink commit path. The group-offset commit is independent bookkeeping. EndTxn success confirms the coordinator decision; control markers and the partition LSO govern read_committed visibility.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":35,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":170}}}%%
sequenceDiagram

box rgb(237,233,254) Flink task paths
participant C as Sink committer
participant S as Source fetcher
end
box rgb(241,245,249) Kafka broker roles
participant T as Transaction coordinator
participant L as Output leader
participant G as Group coordinator
end
par Commit output transaction
C->>T: EndTxn COMMIT via producer Sender
T->>T: Replicate decision in __transaction_state
T-->>C: EndTxn success
T->>L: WriteTxnMarkers COMMIT
L->>L: Replicate marker / advance LSO when eligible
L-->>T: Marker result
and Optional independent source bookkeeping
S->>G: Optional OffsetCommit via commitAsync
G-->>S: Offset commit callback result
end
Note over L,G: Output visibility and group offset are not one atomic Flink Kafka transaction
```

## Four stores answer four different questions

A Kafka topic is not a pod. Its partitions and replicas live across broker logs. A Flink keyed-state backend is not Kafka Streams’ changelog topic. The JobManager manages handles/metadata, not all workers’ state bytes.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"flowchart":{"curve":"basis","nodeSpacing":48,"rankSpacing":65,"htmlLabels":false,"padding":22}}}%%
flowchart TB

subgraph KAFKA["Kafka replicated partition logs"]
 DATA[("User topic logs
What records exist?")]
 OFF[("__consumer_offsets
What group position is recorded?")]
 TXN[("__transaction_state
What decision did the coordinator make?")]
 META[("KRaft metadata log
Who owns what?")]
end
subgraph FLINK["Flink state and recovery"]
 LIVE[("Live keyed/operator state
heap / RocksDB / configured backend")]
 CHECK[("Durable checkpoints
source offsets + state + sink intent")]
end
DATA -. "replay retained input" .-> LIVE
LIVE -->|"snapshot"| CHECK
TXN -. "fences and transaction decisions" .-> DATA
META -. "leadership and placement" .-> DATA
class DATA,OFF,TXN,META,CHECK store
class LIVE state

classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12
linkStyle default stroke:#64748b,stroke-width:1.7px,stroke-linecap:round
```

## Failure after checkpoint completion but before commit reply

Illustrative recoverable case: CP42 is durable and T42 has not timed out. Retrying the same transaction is not resending its business records in a new transaction. Unknown/fatal commit outcomes must not be silently ignored.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"sequence":{"useMaxWidth":false,"actorMargin":55,"boxMargin":18,"messageMargin":35,"diagramMarginX":24,"diagramMarginY":24,"wrap":true,"width":170}}}%%
sequenceDiagram

box rgb(224,242,254) Flink control
participant J as JobMaster
end
box rgb(237,233,254) Flink task attempts
participant Old as Old task
participant New as Replacement task
end
box rgb(241,245,249) Durable systems
participant CP as Checkpoint store
participant K as Kafka
end
Old->>CP: CP42 includes nextOffset=105 and T42
J->>J: CP42 completed
Old->>K: EndTxn T42
Note over Old: Task/pod dies before seeing reply
J->>New: Deploy replacement with CP42 handles
New->>CP: Restore state + pending committables
New->>K: Resolve/retry commit of T42
K-->>New: Commit outcome or actionable error
New->>New: Restore split / local assign + seek
New->>K: Fetch input P0 from offset 105
K-->>New: Replay records after checkpoint cut
```

## Remember: owner → thread → message → durable fact → visibility

Ask these five questions for every arrow. An acknowledgement has meaning only when you know which boundary it acknowledges.

```mermaid
%%{init: {"theme":"base","htmlLabels":false,"themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"flowchart":{"curve":"basis","nodeSpacing":48,"rankSpacing":65,"htmlLabels":false,"padding":22}}}%%
flowchart TB

subgraph MODEL["Five questions for the presentation"]
 direction TB
 A["1. OWNER
Which leader or coordinator?"]
 B["2. THREAD
Which process mutates state?"]
 C["3. MESSAGE
Call, queue, data channel or RPC?"]
 D["4. DURABLE FACT
Which log or checkpoint survives?"]
 E["5. VISIBILITY
What can a downstream reader see?"]
 A --> B --> C --> D --> E
end
class A control
class B runtime
class C io
class D store
class E state

classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12
linkStyle default stroke:#64748b,stroke-width:1.7px,stroke-linecap:round
```

<!-- kafka-recovery-diagrams -->

# Kafka leadership and coordinator recovery

## One cluster, several independent owners

Illustrative placement: these are roles inside processes, not one required pod per box. The controller assigns partition leaders; backing-partition leadership determines coordinator ownership.

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"flowchart":{"curve":"basis","htmlLabels":false,"nodeSpacing":44,"rankSpacing":64},"sequence":{"useMaxWidth":false,"actorMargin":65,"messageMargin":42,"wrap":true,"width":170}}}%%
flowchart TB
subgraph C["Controller quorum"]
 C1["Active controller"]
 CM[("Replicated metadata log")]
 C1 --> CM
end
subgraph B["Broker processes — roles can coexist"]
 P["User partition leader"]
 G["Group coordinator shard"]
 T["Transaction coordinator shard"]
 S["Share coordinator shard"]
end
subgraph D["Replicated broker logs"]
 PD[("orders / P0")]
 GD[("__consumer_offsets")]
 TD[("__transaction_state")]
 SD[("__share_group_state")]
end
CM -. metadata .-> P
CM -. metadata .-> G
CM -. metadata .-> T
CM -. metadata .-> S
P --> PD
G --> GD
T --> TD
S --> SD
classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e
classDef group fill:#fef9c3,stroke:#ca8a04,color:#713f12
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d
classDef txn fill:#ede9fe,stroke:#7c3aed,color:#3b0764
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155
classDef danger fill:#fee2e2,stroke:#dc2626,color:#7f1d1d
linkStyle default stroke:#64748b,stroke-width:2px,stroke-linecap:round
class C1 control
class CM,PD,GD,TD,SD store
class P worker
class G,S group
class T txn
```

## KRaft election: votes choose the controller

Three controller voters; C1 fails. The inspected code has a prospective/pre-vote phase before a new election epoch. Exact wire support depends on the negotiated KRaft version.

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"flowchart":{"curve":"basis","htmlLabels":false,"nodeSpacing":44,"rankSpacing":64},"sequence":{"useMaxWidth":false,"actorMargin":65,"messageMargin":42,"wrap":true,"width":170}}}%%
sequenceDiagram
box rgb(224,242,254) Controller quorum
 participant C1 as C1 / old leader
 participant C2 as C2 / voter
 participant C3 as C3 / voter
end
 Note over C1: Fails or becomes unreachable
 C2->>C2: Fetch/election timeout
 C2->>C3: Vote(preVote), log epoch + offset
 C3-->>C2: Grant if eligible and log up-to-date
 C2->>C2: Persist new epoch and own vote
 C2->>C3: Vote(candidate epoch)
 C3-->>C2: Vote granted
 C2->>C2: Majority reached, become leader
 C2->>C3: BeginQuorumEpoch
 C3->>C2: Fetch metadata log
 C2-->>C3: New-epoch record + metadata
 C3->>C2: Fetch at advanced offset
 C2->>C2: Advance committed metadata boundary
```

## Metadata travels as a replicated log, then an image

This is KRaft metadata propagation, not a legacy controller sending LeaderAndIsr for every change. Fetch arrows indicate pull replication; replies carry the records.

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"flowchart":{"curve":"basis","htmlLabels":false,"nodeSpacing":44,"rankSpacing":64},"sequence":{"useMaxWidth":false,"actorMargin":65,"messageMargin":42,"wrap":true,"width":170}}}%%
sequenceDiagram
box rgb(241,245,249) Requester
 participant A as Broker control / forwarding
end
box rgb(224,242,254) Control plane
 participant C as Active controller
 participant V as Controller voter
end
box rgb(220,252,231) Data plane
 participant B as Broker / observer
end
 A->>C: Forward admin RPC or send broker control RPC
 C->>C: Ordered event produces metadata records
 V->>C: Fetch metadata
 C-->>V: Records
 V->>C: Report advanced fetch position
 C->>C: Quorum commit, complete pending operation
 C-->>A: Response under API completion rules
 B->>C: Fetch metadata as non-voting observer
 C-->>B: Records + committed boundary
 B->>B: MetadataLoader builds delta/image
 B->>B: Publisher updates replicas + coordinators
```

## A broker failure fans out into independent recoveries

Example: B1 led a user partition and one group shard. Other transaction/share shards may live elsewhere and do not automatically move.

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"flowchart":{"curve":"basis","htmlLabels":false,"nodeSpacing":44,"rankSpacing":64},"sequence":{"useMaxWidth":false,"actorMargin":65,"messageMargin":42,"wrap":true,"width":170}}}%%
sequenceDiagram
box rgb(224,242,254) Control plane
 participant C as Active controller
end
box rgb(220,252,231) Broker replicas
 participant O as B1 / failed
 participant N as B2 / eligible replica
end
box rgb(241,245,249) Client
 participant U as Consumer / producer
end
 Note over O: Heartbeats stop
 C->>C: Detect expired broker session
 C->>C: Commit fence + partition changes via quorum
 N->>C: Fetch committed metadata
 C-->>N: New leader + partition epochs
 N->>N: Apply replica-role change
 N->>N: Load coordinator if internal partition moved
 U->>N: Refresh metadata / FindCoordinator then retry
 N-->>U: Loading error until shard ready
 N->>N: Finish replay, activate shard
 U->>N: Retry valid request
 N-->>U: Serve from reconstructed state
```

## Group coordinator = a recoverable shard

No separate election per group. The controller elects the backing partition leader; that broker loads all groups mapped to the shard.

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"flowchart":{"curve":"basis","htmlLabels":false,"nodeSpacing":44,"rankSpacing":64},"sequence":{"useMaxWidth":false,"actorMargin":65,"messageMargin":42,"wrap":true,"width":170}}}%%
flowchart TB
subgraph CONTROL["Committed cluster metadata"]
 A["__consumer_offsets / P7
leader changes B1 → B2"]
end
subgraph RUNTIME["B2 coordinator runtime"]
 B["onElection(P7, leader epoch)"]
 C["LOADING: replay log and markers"]
 D{"Load succeeds?"}
 E["ACTIVE: restore protocol timers"]
 F["FAILED: reject service"]
end
subgraph CLIENT["Client recovery"]
 G["FindCoordinator + retry/backoff"]
 H["Resume heartbeat / offset operations"]
end
A --> B --> C --> D
D -- yes --> E
D -- no --> F
G -- requests see loading --> C
E -. retry succeeds .-> H
classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e
classDef group fill:#fef9c3,stroke:#ca8a04,color:#713f12
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d
classDef txn fill:#ede9fe,stroke:#7c3aed,color:#3b0764
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155
classDef danger fill:#fee2e2,stroke:#dc2626,color:#7f1d1d
linkStyle default stroke:#64748b,stroke-width:2px,stroke-linecap:round
class A control
class B,C,D,E group
class F group,danger
class G,H store
```

## Share groups have three owners

Membership, record delivery and durable delivery state are deliberately separate. B1, B2 and B3 can be the same process or different brokers.

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"flowchart":{"curve":"basis","htmlLabels":false,"nodeSpacing":44,"rankSpacing":64},"sequence":{"useMaxWidth":false,"actorMargin":65,"messageMargin":42,"wrap":true,"width":170}}}%%
flowchart TB
subgraph U["Application"]
 C["Share consumer"]
end
subgraph G["B1 — group coordinator"]
 M["Membership + assignment"]
 O[("__consumer_offsets")]
 M --> O
end
subgraph P["B2 — source partition leader"]
 F["SharePartitionManager"]
 L["SharePartition cache
acquisition locks + timers"]
 D[("orders / P0 records")]
 F --> L
 F --> D
end
subgraph S["B3 — share coordinator"]
 X["ShareCoordinatorShard"]
 SS[("__share_group_state")]
 X --> SS
end
C -- ShareGroupHeartbeat --> M
C -- ShareFetch / ShareAcknowledge --> F
L -- Read / WriteShareGroupState --> X
classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e
classDef group fill:#fef9c3,stroke:#ca8a04,color:#713f12
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d
classDef txn fill:#ede9fe,stroke:#7c3aed,color:#3b0764
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155
classDef danger fill:#fee2e2,stroke:#dc2626,color:#7f1d1d
linkStyle default stroke:#64748b,stroke-width:2px,stroke-linecap:round
class C store
class M group
class O,D,SS store
class F,L worker
class X group
```

## An acknowledgement crosses a durability boundary

Ordinary ACCEPT shown. A successful local application side effect is not atomically coupled to this acknowledgement. Share state persistence uses broker replication, not controller-quorum replication.

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"flowchart":{"curve":"basis","htmlLabels":false,"nodeSpacing":44,"rankSpacing":64},"sequence":{"useMaxWidth":false,"actorMargin":65,"messageMargin":42,"wrap":true,"width":170}}}%%
sequenceDiagram
box rgb(241,245,249) Application
 participant U as Share consumer
end
box rgb(220,252,231) Source leader
 participant L as SharePartition
end
box rgb(254,249,195) State shard
 participant S as Share coordinator
 participant R as State-topic replica
end
 U->>L: ShareFetch
 L->>L: Acquire records + lock timer
 L-->>U: Records and acquired ranges
 U->>U: Process record / external side effect
 U->>L: ShareAcknowledge(ACCEPT)
 L->>L: Validate ownership, provisional transition
 L->>S: WriteShareGroupState(epochs, batches)
 S->>S: Append state record
 R->>S: Replica Fetch
 S-->>R: State records
 R->>S: Fetch at advanced offset
 S->>S: Advance high watermark
 S-->>L: Success after durable write boundary
 L->>L: Finalize local transition
 L-->>U: Acknowledgement result
```

## Recover the state owner that actually failed

Do not conflate a crashed consumer, a source-partition leader and the share-state coordinator.

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"flowchart":{"curve":"basis","htmlLabels":false,"nodeSpacing":44,"rankSpacing":64},"sequence":{"useMaxWidth":false,"actorMargin":65,"messageMargin":42,"wrap":true,"width":170}}}%%
flowchart TB
subgraph FAILURE["Which component failed?"]
 F{"Failure domain"}
 C["Consumer process"]
 P["Source partition leader"]
 S["Share state coordinator"]
end
subgraph RECOVERY["Independent recovery paths"]
 CR["Release or acquisition-lock expiry
redelivery remains possible"]
 PR["Elect source leader
reload state via persister
use newer leader epoch"]
 SR["Elect state-topic leader
replay share shard
retry state RPCs"]
end
F --> C --> CR
F --> P --> PR
F --> S --> SR
classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e
classDef group fill:#fef9c3,stroke:#ca8a04,color:#713f12
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d
classDef txn fill:#ede9fe,stroke:#7c3aed,color:#3b0764
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155
classDef danger fill:#fee2e2,stroke:#dc2626,color:#7f1d1d
linkStyle default stroke:#64748b,stroke-width:2px,stroke-linecap:round
class F,C,P,S store
class CR,PR worker
class SR group
```

## A transaction leaves evidence in different logs

A Kafka consume-transform-produce transaction can include __consumer_offsets. Ordinary Flink KafkaSource offset commits are not automatically part of this transaction.

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"flowchart":{"curve":"basis","htmlLabels":false,"nodeSpacing":44,"rankSpacing":64},"sequence":{"useMaxWidth":false,"actorMargin":65,"messageMargin":42,"wrap":true,"width":170}}}%%
flowchart TB
subgraph TC["Transaction coordinator"]
 T["Transaction state machine"]
 D[("__transaction_state
prepare decision → complete")]
 T --> D
end
subgraph P["Output partition leader"]
 W["Validate and append marker"]
 L[("Output log
transactional data + control batch")]
 W --> L
end
subgraph G["Group coordinator — only if enrolled"]
 O["Complete transactional offsets"]
 OL[("__consumer_offsets
pending offsets + control batch")]
 O --> OL
end
T -- WriteTxnMarkers --> W
T -- WriteTxnMarkers --> O
classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e
classDef group fill:#fef9c3,stroke:#ca8a04,color:#713f12
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d
classDef txn fill:#ede9fe,stroke:#7c3aed,color:#3b0764
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155
classDef danger fill:#fee2e2,stroke:#dc2626,color:#7f1d1d
linkStyle default stroke:#64748b,stroke-width:2px,stroke-linecap:round
class T txn
class W worker
class O group
class D,L,OL store
```

## EndTxn decision, marker fan-out, completion

Two participant leaders shown. Each marker must be replicated under its partition policy. Final COMPLETE bookkeeping is not the moment at which every reader simultaneously sees the transaction.

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"flowchart":{"curve":"basis","htmlLabels":false,"nodeSpacing":44,"rankSpacing":64},"sequence":{"useMaxWidth":false,"actorMargin":65,"messageMargin":42,"wrap":true,"width":170}}}%%
sequenceDiagram
box rgb(241,245,249) Producer process
 participant P as Producer
end
box rgb(237,233,254) Transaction shard
 participant T as Coordinator
end
box rgb(220,252,231) Participant leaders
 participant A as Output P0 leader
 participant B as Output P1 leader
end
 P->>T: EndTxn(COMMIT)
 T->>T: Replicate PREPARE_COMMIT + participant set
 T-->>P: EndTxn success
 par Marker for P0
 T->>A: WriteTxnMarkers(COMMIT, identities, epochs)
 A->>A: Append + replicate control batch
 A-->>T: Per-partition result
 and Marker for P1
 T->>B: WriteTxnMarkers(COMMIT, identities, epochs)
 B->>B: Append + replicate control batch
 B-->>T: Per-partition result
 end
 T->>T: Replicate COMPLETE_COMMIT after all finish
 Note over A,B: Each partition advances LSO independently
```

## Crash recovery follows the durable state

This is a decision tree over recovered log state, not every legal TransactionMetadata transition. A prepared decision is irrevocable; completion work is repeatable.

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"flowchart":{"curve":"basis","htmlLabels":false,"nodeSpacing":44,"rankSpacing":64},"sequence":{"useMaxWidth":false,"actorMargin":65,"messageMargin":42,"wrap":true,"width":170}}}%%
flowchart TB
subgraph LOAD["Replacement transaction coordinator"]
 A["Replay __transaction_state shard"]
 Q{"Recovered transaction state"}
end
subgraph WORK["Recovery action"]
 O["ONGOING
client may continue; timeout may abort"]
 C["PREPARE_COMMIT
resume COMMIT markers"]
 B["PREPARE_ABORT
resume ABORT markers"]
 D["COMPLETE state
no unfinished marker fan-out"]
 E["All required markers complete
append COMPLETE state"]
end
A --> Q
Q --> O
Q --> C
Q --> B
Q --> D
C --> E
B --> E
classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e
classDef group fill:#fef9c3,stroke:#ca8a04,color:#713f12
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d
classDef txn fill:#ede9fe,stroke:#7c3aed,color:#3b0764
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155
classDef danger fill:#fee2e2,stroke:#dc2626,color:#7f1d1d
linkStyle default stroke:#64748b,stroke-width:2px,stroke-linecap:round
class A,Q txn
class O,C,B,D,E worker
```

## The presentation mental model: map, journal, receipt

Use the same four questions for every failure: who owned it, which durable journal survives, what fences the old owner, and what proves completion?

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"flowchart":{"curve":"basis","htmlLabels":false,"nodeSpacing":44,"rankSpacing":64},"sequence":{"useMaxWidth":false,"actorMargin":65,"messageMargin":42,"wrap":true,"width":170}}}%%
flowchart LR
subgraph MAP["1 — Map"]
 M["KRaft metadata
who owns each partition?"]
end
subgraph JOURNAL["2 — Journal"]
 J["User and internal logs
what happened durably?"]
end
subgraph FENCE["3 — Fence"]
 F["Epochs + validation
which owner is stale?"]
end
subgraph RECEIPT["4 — Receipt"]
 R["HW / marker / LSO / checkpoint
what is actually complete?"]
end
M --> J --> F --> R
classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e
classDef group fill:#fef9c3,stroke:#ca8a04,color:#713f12
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d
classDef txn fill:#ede9fe,stroke:#7c3aed,color:#3b0764
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155
classDef danger fill:#fee2e2,stroke:#dc2626,color:#7f1d1d
linkStyle default stroke:#64748b,stroke-width:2px,stroke-linecap:round
class M control
class J store
class F group
class R worker
```

<!-- kafka-offset-diagrams -->

# Offset commits: Kafka, Flink and Spark

## Three progress authorities, not three spellings of commit

Solid arrows store recovery progress. The dashed Flink path publishes optional Kafka group offsets. Sink output durability is a separate concern in every row.

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"flowchart":{"curve":"basis","htmlLabels":false,"nodeSpacing":48,"rankSpacing":72},"sequence":{"useMaxWidth":false,"actorMargin":75,"messageMargin":44,"wrap":true,"width":180}}}%%
flowchart TB
subgraph APP["Processing runtimes"]
 K["Plain Kafka consumer"]
 F["Flink KafkaSource"]
 S["Spark Structured Streaming"]
end
subgraph STORE["Durable progress stores"]
 O[("Kafka __consumer_offsets")]
 C[("Flink completed checkpoint")]
 L[("Spark checkpoint logs and state")]
end
K -->|"commitSync or commitAsync"| O
F -->|"snapshot split offsets"| C
F -. "after checkpoint completion" .-> O
S -->|"offsets / commits / state"| L
classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155
classDef danger fill:#fee2e2,stroke:#dc2626,color:#7f1d1d
linkStyle default stroke:#64748b,stroke-width:2px,stroke-linecap:round
class K,F,S runtime
class O,C,L store
```

## Commit the safe frontier, not the furthest finished record

Illustrative contiguous input offsets. 102 is unfinished, so the safe restart offset is 102 even though 103 and 104 finished. Real Kafka offsets can have gaps.

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"flowchart":{"curve":"basis","htmlLabels":false,"nodeSpacing":48,"rankSpacing":72},"sequence":{"useMaxWidth":false,"actorMargin":75,"messageMargin":44,"wrap":true,"width":180}}}%%
flowchart TB
subgraph P["One input partition — fetched offsets 100 through 104"]
 A["100 done"] --> B["101 done"] --> C["102 pending"] --> D["103 done"] --> E["104 done"]
end
subgraph DEC["Application completion tracker"]
 F["Safe next offset = 102"]
 U["Unsafe next offset = 105"]
end
B --> F
E -. "ignores unfinished work" .-> U
subgraph REC["After a crash"]
 R["Resume at 102
Later completed effects may repeat"]
 X["Resume at 105
Record 102 is skipped"]
end
F --> R
U --> X
classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155
classDef danger fill:#fee2e2,stroke:#dc2626,color:#7f1d1d
linkStyle default stroke:#64748b,stroke-width:2px,stroke-linecap:round
class A,B,D,E io
class C,U,X danger
class F,R control
```

## Same server acknowledgement, different caller waiting

Two alternatives on one consumer owner thread. Async submission may reach the network before or after method return; this shows one possible schedule. Return is not a broker acknowledgement.

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"flowchart":{"curve":"basis","htmlLabels":false,"nodeSpacing":48,"rankSpacing":72},"sequence":{"useMaxWidth":false,"actorMargin":75,"messageMargin":44,"wrap":true,"width":180}}}%%
sequenceDiagram
box rgb(237,233,254) Application process
 participant A as Consumer owner thread
 participant C as Kafka client internals
end
box rgb(241,245,249) Kafka broker
 participant G as Group coordinator
end
 A->>C: commitSync(safe offsets, timeout)
 C->>G: OffsetCommit API 8
 G->>G: Validate and replicate offsets
 G-->>C: Per-partition result
 C-->>A: Return success or throw
 Note over A: Caller waited for result
 A->>C: commitAsync(safe offsets, callback)
 C-->>A: Return without broker acknowledgement
 C->>G: OffsetCommit API 8
 G->>G: Same validation and replication
 G-->>C: Per-partition result
 C->>C: Queue callback completion
 A->>C: Later poll / commit / close
 C-->>A: Invoke callback on owner thread
```

## The word async appears at two different layers

Both delegate implementations expose both APIs. Classic does not acquire a dedicated commit thread just because commitAsync is used. In the consumer-protocol delegate, network I/O is separated from user callback execution.

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"flowchart":{"curve":"basis","htmlLabels":false,"nodeSpacing":48,"rankSpacing":72},"sequence":{"useMaxWidth":false,"actorMargin":75,"messageMargin":44,"wrap":true,"width":180}}}%%
flowchart TB
subgraph OLD["Classic / consumer thread"]
 A["commitSync / commitAsync"] --> B["ConsumerCoordinator"] --> C["ConsumerNetworkClient polling"]
 C --> D["Drain completed callbacks"]
end
subgraph APP["New consumer / app thread"]
 E["AsyncKafkaConsumer
commitSync / commitAsync"]
 F["OffsetCommitCallbackInvoker"]
end
subgraph BG["New consumer / network thread"]
 G["ApplicationEventProcessor"] --> H["CommitRequestManager"] --> I["NetworkClientDelegate"]
end
E -. "application event queue" .-> G
I -. "completion queue" .-> F
subgraph SERVER["Remote coordinator broker"]
 J["OffsetCommit handler"]
end
C --> J
I --> J
classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155
classDef danger fill:#fee2e2,stroke:#dc2626,color:#7f1d1d
linkStyle default stroke:#64748b,stroke-width:2px,stroke-linecap:round
class A,B,D,E,F runtime
class C,G,H,I io
class J control
style OLD fill:#f5f3ff,stroke:#c4b5fd
style APP fill:#f5f3ff,stroke:#c4b5fd
style BG fill:#fff7ed,stroke:#fdba74
style SERVER fill:#f0f9ff,stroke:#7dd3fc
```

## The commit goes to the coordinator, not every input leader

Illustrative RF=3 offsets partition. Follower Fetch replication advances the high watermark. Broker role placement is independent of which broker leads the input data partition.

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"flowchart":{"curve":"basis","htmlLabels":false,"nodeSpacing":48,"rankSpacing":72},"sequence":{"useMaxWidth":false,"actorMargin":75,"messageMargin":44,"wrap":true,"width":180}}}%%
sequenceDiagram
box rgb(255,237,213) Client process
 participant C as Consumer
end
box rgb(224,242,254) Kafka brokers
 participant B as Bootstrap broker
 participant G as Offsets leader / coordinator
 participant R as Offsets followers
end
 C->>B: FindCoordinator(group.id)
 B-->>C: Coordinator host and port
 C->>G: OffsetCommit(group, epoch, offsets)
 G->>G: Validate membership / ACL / partitions
 G->>G: Append __consumer_offsets records
 R->>G: Fetch offsets log
 G-->>R: New record batches
 R->>G: Fetch with advanced position
 G->>G: Advance high watermark
 G-->>C: OffsetCommitResponse errors per partition
 Note over G,R: On leader loss, new owner replays committed log
 C->>B: Rediscover after coordinator error
```

## Why retrying an old callback payload is unsafe

This is a new application retry after a newer request—not Kafka reordering the original calls. Assume unchanged valid ownership; both writes are accepted.

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"flowchart":{"curve":"basis","htmlLabels":false,"nodeSpacing":48,"rankSpacing":72},"sequence":{"useMaxWidth":false,"actorMargin":75,"messageMargin":44,"wrap":true,"width":180}}}%%
sequenceDiagram
box rgb(237,233,254) Consumer application
 participant A as Owner thread
end
box rgb(224,242,254) Kafka coordinator
 participant G as Offset log
end
 A->>G: Async request A = next offset 120
 G-->>A: A fails or outcome is uncertain
 A->>G: Async request B = next offset 180
 G-->>A: B succeeds
 Note over G: Stored offset = 180
 A->>G: Delayed application retry of A = 120
 G-->>A: Retry succeeds
 Note over G: Stored offset = 120
 Note over A,G: Crash now can replay already-processed records
 Note over A: Publish current safe frontier instead of stale payload
```

## Flink checkpoint completion fans out to two different commits

TaskManager source mailbox and fetcher are threads in a JVM, not separate pods. The sink committer may be in another task/JVM. This is a logical flow, not a total ordering between source-offset and sink-transaction completion.

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"flowchart":{"curve":"basis","htmlLabels":false,"nodeSpacing":48,"rankSpacing":72},"sequence":{"useMaxWidth":false,"actorMargin":75,"messageMargin":44,"wrap":true,"width":180}}}%%
sequenceDiagram
box rgb(224,242,254) Flink control plane
 participant J as JobManager
end
box rgb(237,233,254) TaskManager source
 participant M as Source mailbox
 participant F as Split fetcher
end
box rgb(220,252,231) Sink task
 participant S as Kafka sink committer
end
box rgb(241,245,249) Durable systems
 participant K as Kafka coordinators
end
 M->>M: Emit record 119, track next offset 120
 J->>M: Trigger checkpoint 42
 M->>M: snapshotState(42) = 120
 M-->>J: Acknowledge persisted source snapshot
 Note over J,S: Required snapshots and prepared sink recovery state are durable
 J->>J: Persist completed checkpoint 42
 par Publish source offsets
 J-->>M: notifyCheckpointComplete(42)
 M-)F: Enqueue saved offsets for checkpoint 42
 F->>K: Consumer commitAsync(120)
 K-->>F: Offset commit result
 F->>F: Update source commit metrics
 and Finalize transactional output
 J-->>S: Checkpoint completion lifecycle
 S->>K: Producer commitTransaction()
 end
 Note over M,K: Source OffsetCommit failure does not invalidate checkpoint 42
```

## Spark Structured Streaming: checkpoint logs drive restart

Default synchronous microbatch path. Async progress tracking, continuous processing and RealTimeTrigger have different timing and are outside this sequence. There is no input OffsetCommit RPC in this flow.

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"flowchart":{"curve":"basis","htmlLabels":false,"nodeSpacing":48,"rankSpacing":72},"sequence":{"useMaxWidth":false,"actorMargin":75,"messageMargin":44,"wrap":true,"width":180}}}%%
sequenceDiagram
box rgb(224,242,254) Spark driver
 participant D as MicroBatchExecution
end
box rgb(241,245,249) Checkpoint storage
 participant L as offsets / commits / state
end
box rgb(220,252,231) Executor JVMs
 participant E as Kafka reader and tasks
end
box rgb(241,245,249) External systems
 participant K as Kafka input leaders
 participant S as Output sink
end
 D->>D: Choose batch 42 range [120, 180)
 D->>L: Persist offsets/42 with end 180
 D->>E: Schedule explicit offset ranges
 E->>K: Assign / seek / Fetch from 120
 K-->>E: Input records
 E->>S: Write batch output
 S-->>E: Output completion
 E-->>D: Tasks and writer completion
 D->>L: Persist commits/42
 Note over D,L: Missing commits/42 means replay batch 42
 D->>D: Kafka source.commit(end) is no-op
```

## Legacy DStreams: commitAsync first means enqueue

Call on the original DirectKafkaInputDStream, after the output action succeeds. Queue coalescing uses max(untilOffset) only for submitted completed work; it cannot detect holes caused by out-of-order batch completion.

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"flowchart":{"curve":"basis","htmlLabels":false,"nodeSpacing":48,"rankSpacing":72},"sequence":{"useMaxWidth":false,"actorMargin":75,"messageMargin":44,"wrap":true,"width":180}}}%%
flowchart TB
subgraph DRIVER["Spark driver — output callback"]
 A["Capture original RDD offset ranges"] --> B["Wait for output action success"] --> C["CanCommitOffsets.commitAsync"] --> Q["Thread-safe queue of OffsetRange"]
end
subgraph COMPUTE["Spark driver — later DStream compute"]
 D["commitAll drains queue"] --> M["Max untilOffset per partition
Most recent callback"] --> K["Driver KafkaConsumer.commitAsync"]
end
subgraph STORE["Remote Kafka coordinator"]
 O[("__consumer_offsets")]
end
Q -. "deferred drain" .-> D
K --> O
classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155
classDef danger fill:#fee2e2,stroke:#dc2626,color:#7f1d1d
linkStyle default stroke:#64748b,stroke-width:2px,stroke-linecap:round
class A,B,C runtime
class Q,D,M,K io
class O store
```
<!-- /kafka-offset-diagrams -->

<!-- spark-deep-diagrams -->

# Spark distributed execution and recovery

## One driver, several executors, three durable systems

Illustrative three-executor placement. Kafka payloads travel directly to executor readers. The cluster manager supplies processes/resources; Spark schedules tasks within them.

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"flowchart":{"curve":"basis","htmlLabels":false,"nodeSpacing":52,"rankSpacing":70},"sequence":{"useMaxWidth":false,"actorMargin":70,"messageMargin":44,"wrap":true,"width":180}}}%%
flowchart TB
subgraph DRIVER["Driver JVM"]
 D["Query execution thread
Batch plan and task schedulers"]
end
subgraph WORKERS["Parallel executor JVMs"]
 direction LR
 T1["E1 task slots
Readers and operators"]
 T2["E2 task slots
Readers and state"]
 T3["E3 task slots
Readers and state"]
end
K[("Kafka partition logs")]
C[("Shared checkpoint storage")]
O[("Output sink")]
D -->|"LaunchTask RPC"| WORKERS
K -->|"Direct record fetch"| WORKERS
D -->|"offset and commit logs"| C
WORKERS -->|"state checkpoints"| C
WORKERS -->|"sink writes"| O
classDef driver fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e
classDef executor fill:#dcfce7,stroke:#16a34a,color:#14532d
classDef kafka fill:#ffedd5,stroke:#f97316,color:#7c2d12
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155
classDef state fill:#ede9fe,stroke:#7c3aed,color:#3b0764
classDef decision fill:#fef9c3,stroke:#ca8a04,color:#713f12
classDef danger fill:#fee2e2,stroke:#dc2626,color:#7f1d1d
linkStyle default stroke:#64748b,stroke-width:2px,stroke-linecap:round
style WORKERS fill:#f0fdf4,stroke:#86efac
class D driver
class T1,T2,T3 executor
class K kafka
class C,O store
```

## A successful microbatch across three executors

Default synchronous microbatch. Each executor may run several task attempts. The checkpoint stores an offset vector for the whole source, not a separate completion cursor for each executor.

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"flowchart":{"curve":"basis","htmlLabels":false,"nodeSpacing":52,"rankSpacing":70},"sequence":{"useMaxWidth":false,"actorMargin":70,"messageMargin":44,"wrap":true,"width":180}}}%%
sequenceDiagram
box rgb(224,242,254) Driver
 participant D as MicroBatchExecution
end
box rgb(241,245,249) Shared checkpoint
 participant C as Logs and state
end
box rgb(220,252,231) Executor JVMs
 participant E1 as E1
 participant E2 as E2
 participant E3 as E3
end
 D->>D: Determine batch 42 offset vector
 D->>C: Persist offsets/42
 par Execute assigned source ranges
 D->>E1: Launch P0 and P1 tasks
 E1->>E1: Fetch and transform records
 and
 D->>E2: Launch P2 task
 E2->>E2: Fetch and transform records
 and
 D->>E3: Launch P3 task
 E3->>E3: Fetch and transform records
 end
 Note over E1,E3: Stateful plans shuffle keys and persist state as needed
 E1-->>D: Successful task results
 E2-->>D: Successful task results
 E3-->>D: Successful task results
 D->>D: Finish driver sink commit after collecting results
 D->>C: Persist commits/42
 D->>D: Advance to batch 43
```

## Kafka partitions map to input tasks, not fixed executors

Worked range placement for one batch. Start inclusive, end exclusive. minPartitions or maxRecordsPerPartition can split one Kafka partition into more than one input task; this picture uses the unsplit case.

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"flowchart":{"curve":"basis","htmlLabels":false,"nodeSpacing":52,"rankSpacing":70},"sequence":{"useMaxWidth":false,"actorMargin":70,"messageMargin":44,"wrap":true,"width":180}}}%%
flowchart TB
subgraph PLAN["Driver plan for batch 42"]
 P0["P0 [120, 180)"]
 P1["P1 [200, 240)"]
 P2["P2 [90, 115)"]
 P3["P3 [50, 70)"]
end
subgraph E1["Executor E1 — two slots"]
 A["Task A / P0"]
 B["Task B / P1"]
end
subgraph E2["Executor E2"]
 C["Task C / P2"]
end
subgraph E3["Executor E3"]
 D["Task D / P3"]
end
P0 --> A
P1 --> B
P2 --> C
P3 --> D
subgraph RETRY["After an executor failure"]
 R["Same range, new task attempt
May run on another executor"]
end
C -. "reschedule" .-> R
classDef driver fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e
classDef executor fill:#dcfce7,stroke:#16a34a,color:#14532d
classDef kafka fill:#ffedd5,stroke:#f97316,color:#7c2d12
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155
classDef state fill:#ede9fe,stroke:#7c3aed,color:#3b0764
classDef decision fill:#fef9c3,stroke:#ca8a04,color:#713f12
classDef danger fill:#fee2e2,stroke:#dc2626,color:#7f1d1d
linkStyle default stroke:#64748b,stroke-width:2px,stroke-linecap:round
class P0,P1,P2,P3 driver
class A,B,C,D executor
class R state
```

## V2 read path: driver planning to executor fetch

Exact class families from the pinned source. V1 KafkaSource/KafkaSourceRDD has an alternate scan path, covered in the notes; checkpoint ownership is the same.

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"flowchart":{"curve":"basis","htmlLabels":false,"nodeSpacing":52,"rankSpacing":70},"sequence":{"useMaxWidth":false,"actorMargin":70,"messageMargin":44,"wrap":true,"width":180}}}%%
flowchart TB
subgraph DRIVER["Driver process"]
 A["StreamExecution
query thread"] --> B["MicroBatchExecution
constructNextBatch / runBatch"]
 B --> C["KafkaMicroBatchStream
latestOffset / planInputPartitions"]
 C --> O["KafkaOffsetReaderAdmin
Admin listOffsets"]
 B --> D["Physical plan and RDD stages"] --> S["DAGScheduler
TaskSchedulerImpl"]
end
subgraph EXEC["Executor process"]
 E["Executor.TaskRunner"] --> F["KafkaBatchPartitionReader"] --> G["KafkaDataConsumer"] --> H["InternalKafkaConsumer
assign / seek / poll"]
end
subgraph KAFKA["Kafka brokers"]
 K["Metadata and partition leaders"]
end
S -->|"LaunchTask RPC"| E
O --> K
H -->|"Kafka binary protocol"| K
classDef driver fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e
classDef executor fill:#dcfce7,stroke:#16a34a,color:#14532d
classDef kafka fill:#ffedd5,stroke:#f97316,color:#7c2d12
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155
classDef state fill:#ede9fe,stroke:#7c3aed,color:#3b0764
classDef decision fill:#fef9c3,stroke:#ca8a04,color:#713f12
classDef danger fill:#fee2e2,stroke:#dc2626,color:#7f1d1d
linkStyle default stroke:#64748b,stroke-width:2px,stroke-linecap:round
class A,B,C,O,D,S driver
class E,F,G,H executor
class K kafka
```

## Three network paths carry different things

Task RPC carries serialized work and task status. Kafka connections carry input records. Shuffle transfers exchange data between executor-side tasks; the driver tracks where blocks can be fetched.

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"flowchart":{"curve":"basis","htmlLabels":false,"nodeSpacing":52,"rankSpacing":70},"sequence":{"useMaxWidth":false,"actorMargin":70,"messageMargin":44,"wrap":true,"width":180}}}%%
sequenceDiagram
box rgb(224,242,254) Driver process
 participant D as Scheduler endpoint
end
box rgb(220,252,231) Executor processes
 participant E1 as E1 / map task
 participant E2 as E2 / reduce task
end
box rgb(255,237,213) Kafka broker
 participant K as Partition leader
end
 D->>E1: LaunchTask(TaskDescription)
 E1->>K: Metadata and Fetch requests
 K-->>E1: Kafka records
 E1->>E1: Execute map and write shuffle blocks
 E1-->>D: StatusUpdate and map result
 D->>E2: Launch downstream task
 E2->>E1: Fetch shuffle blocks
 E1-->>E2: Serialized shuffle rows
 E2->>E2: Execute reduce / state / sink work
 E2-->>D: StatusUpdate and task result
```

## Aggregation changes the partitioning boundary

Example customer-key aggregation. Source tasks have Kafka partition ranges; state tasks have hash-partitioned keys. State partition 0 and Kafka partition 0 are different identities.

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"flowchart":{"curve":"basis","htmlLabels":false,"nodeSpacing":52,"rankSpacing":70},"sequence":{"useMaxWidth":false,"actorMargin":70,"messageMargin":44,"wrap":true,"width":180}}}%%
flowchart TB
subgraph MAP["Source and map tasks"]
 A["E1 / P0 and P1"]
 B["E2 / P2"]
 C["E3 / P3"]
end
subgraph SHUFFLE["Exchange by customer key"]
 X["Materialized shuffle blocks
hash customer into state partition"]
end
subgraph REDUCE["Stateful tasks — example two partitions"]
 S0["E2 / state partition 0
Load state version 42"]
 S1["E3 / state partition 1
Load state version 42"]
end
subgraph FS["Shared durable checkpoint"]
 V0[("Operator / partition 0
Committed next state 43")]
 V1[("Operator / partition 1
Committed next state 43")]
end
A --> X
B --> X
C --> X
X --> S0
X --> S1
S0 --> V0
S1 --> V1
classDef driver fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e
classDef executor fill:#dcfce7,stroke:#16a34a,color:#14532d
classDef kafka fill:#ffedd5,stroke:#f97316,color:#7c2d12
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155
classDef state fill:#ede9fe,stroke:#7c3aed,color:#3b0764
classDef decision fill:#fef9c3,stroke:#ca8a04,color:#713f12
classDef danger fill:#fee2e2,stroke:#dc2626,color:#7f1d1d
linkStyle default stroke:#64748b,stroke-width:2px,stroke-linecap:round
class A,B,C executor
class X store
class S0,S1 state
class V0,V1 store
```

## A checkpoint directory holds several kinds of evidence

Offsets describe input, state files describe operator state, and commit records identify completed batches. Output durability and any sink deduplication log live in the sink’s own storage.

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"flowchart":{"curve":"basis","htmlLabels":false,"nodeSpacing":52,"rankSpacing":70},"sequence":{"useMaxWidth":false,"actorMargin":70,"messageMargin":44,"wrap":true,"width":180}}}%%
flowchart TB
subgraph DRIVER["Driver writes"]
 D["Query and source metadata"]
 P["Planned end offsets"]
 F["Completed batch metadata"]
end
subgraph EXEC["Executor writes"]
 E["Executor task"]
 SP["State-store provider
Delta / snapshot / changelog"]
 W["Sink writer"]
end
subgraph CKPT["Shared checkpoint directory"]
 M[("metadata and sources")]
 O[("offsets / batchId")]
 C[("commits / batchId")]
 S[("state / operator / partition")]
end
subgraph SINK["Sink storage"]
 R[("Durable output
Optional batch ledger / manifest")]
end
D --> M
P --> O
F --> C
E --> SP
E --> W
SP --> S
W --> R
classDef driver fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e
classDef executor fill:#dcfce7,stroke:#16a34a,color:#14532d
classDef kafka fill:#ffedd5,stroke:#f97316,color:#7c2d12
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155
classDef state fill:#ede9fe,stroke:#7c3aed,color:#3b0764
classDef decision fill:#fef9c3,stroke:#ca8a04,color:#713f12
classDef danger fill:#fee2e2,stroke:#dc2626,color:#7f1d1d
linkStyle default stroke:#64748b,stroke-width:2px,stroke-linecap:round
class D,P,F driver
class E,W executor
class SP state
class M,O,C,S,R store
```

## Lost executor: reload the required state version elsewhere

Example stateful batch 42. Task attempts can write version 43 before the global batch succeeds. The retry must reconstruct from the required version 42 and the selected checkpoint IDs when enabled.

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"flowchart":{"curve":"basis","htmlLabels":false,"nodeSpacing":52,"rankSpacing":70},"sequence":{"useMaxWidth":false,"actorMargin":70,"messageMargin":44,"wrap":true,"width":180}}}%%
sequenceDiagram
box rgb(224,242,254) Driver
 participant D as Scheduler and state coordinator
end
box rgb(220,252,231) Executors
 participant E2 as E2 / old task
 participant E3 as E3 / retry
end
box rgb(241,245,249) Shared checkpoint
 participant C as State storage
end
 E2->>C: Load state version 42
 E2->>E2: Apply batch 42 updates
 E2->>C: Persist state version 43
 Note over E2: Executor fails before batch completion
 D->>E3: Retry task for same batch and partition
 E3->>C: Load required version 42
 C-->>E3: Restore snapshot plus delta / changelog
 E3->>E3: Replay batch 42 updates
 E3->>C: Persist retry state version 43
 E3-->>D: Task succeeds
 Note over D,C: State locality RPC is metadata, durable state is in storage
```

## Driver restart: compare offsets and commits

Normal synchronous microbatch recovery. The previous offset entry supplies the start vector; the latest planned entry supplies the end vector. Inconsistent or missing required entries can fail recovery.

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"flowchart":{"curve":"basis","htmlLabels":false,"nodeSpacing":52,"rankSpacing":70},"sequence":{"useMaxWidth":false,"actorMargin":70,"messageMargin":44,"wrap":true,"width":180}}}%%
flowchart TB
subgraph LOAD["New driver reads same checkpoint"]
 O["Latest offsets entry = batch N"] --> Q{"Matching commits/N exists?"}
end
subgraph REPLAY["Incomplete latest batch"]
 A["Recover start vector
from previous batch"] --> B["Recover end vector
from offsets/N"] --> C["Load required prior state"] --> D["Rerun batch N and sink"]
end
subgraph NEXT["Completed latest batch"]
 E["Use end vector as next start"] --> F["Restore committed operator state"] --> G["Construct batch N + 1"]
end
Q -- "no" --> A
Q -- "yes" --> E
classDef driver fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e
classDef executor fill:#dcfce7,stroke:#16a34a,color:#14532d
classDef kafka fill:#ffedd5,stroke:#f97316,color:#7c2d12
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155
classDef state fill:#ede9fe,stroke:#7c3aed,color:#3b0764
classDef decision fill:#fef9c3,stroke:#ca8a04,color:#713f12
classDef danger fill:#fee2e2,stroke:#dc2626,color:#7f1d1d
linkStyle default stroke:#64748b,stroke-width:2px,stroke-linecap:round
class O,A,B,E driver
class Q decision
class C,F state
class D,G executor
```

## Output can succeed before the query commit is durable

This sequence uses a replayable input and a sink that accepts repeats. Recovery preserves input coverage but can duplicate external effects. A sink must provide its own replay-safe behavior for exactly-once results.

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"flowchart":{"curve":"basis","htmlLabels":false,"nodeSpacing":52,"rankSpacing":70},"sequence":{"useMaxWidth":false,"actorMargin":70,"messageMargin":44,"wrap":true,"width":180}}}%%
sequenceDiagram
box rgb(224,242,254) Driver
 participant D as Driver
end
box rgb(241,245,249) Checkpoint
 participant C as Offset and commit logs
end
box rgb(220,252,231) Executor tasks
 participant E as Tasks
end
box rgb(241,245,249) Sink
 participant S as External output
end
 D->>C: Persist offsets/42
 D->>E: Run batch 42
 E->>S: Write output
 S-->>E: Output accepted
 Note over D: Driver fails before commits/42 is durable
 D->>C: Restart and read checkpoint
 C-->>D: offsets/42 exists, commits/42 missing
 D->>E: Replay batch 42
 E->>S: Write same logical output again
 Note over S: Duplicates unless sink recognizes replay identity
 E-->>D: Success
 D->>C: Persist commits/42
```

## Application pattern: output and batch ledger in one transaction

Illustrative foreachBatch sink design, not a built-in Spark transaction. All output changes covered by the ledger must share a supported atomic finalization protocol; ordinary parallel JDBC writes do not automatically share one transaction.

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"flowchart":{"curve":"basis","htmlLabels":false,"nodeSpacing":52,"rankSpacing":70},"sequence":{"useMaxWidth":false,"actorMargin":70,"messageMargin":44,"wrap":true,"width":180}}}%%
sequenceDiagram
box rgb(224,242,254) foreachBatch application
 participant A as Batch callback
end
box rgb(241,245,249) Transactional sink
 participant DB as Output plus batch ledger
end
 A->>DB: Begin concurrency-safe finalization for Q / 42
 A->>DB: Check durable ledger entry
 alt Batch already finalized
 DB-->>A: Ledger entry exists
 A->>DB: End without reapplying output
 else First successful finalization
 A->>DB: Apply all covered output changes
 A->>DB: Insert Q / 42 ledger entry
 A->>DB: Commit output and ledger atomically
 DB-->>A: Durable success
 end
 Note over A,DB: Unique ledger key with conflict-safe finalization
 Note over DB: Conflicts roll back attempt output
 Note over A: Return only after success or recognized completed batch
```

## No new input can still require a stateful batch

constructNextBatch considers new offsets and lastExecution.shouldRunAnotherBatch. Watermark/timeout work can require a batch with unchanged input offsets. This is different from losing source data.

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"flowchart":{"curve":"basis","htmlLabels":false,"nodeSpacing":52,"rankSpacing":70},"sequence":{"useMaxWidth":false,"actorMargin":70,"messageMargin":44,"wrap":true,"width":180}}}%%
flowchart TB
subgraph DRIVER["Driver trigger"]
 A["Fetch latest source offsets"] --> Q{"Offsets advanced?"}
 R{"State cleanup or timeout
requires another batch?"}
end
subgraph RUN["Batch execution"]
 B["Log planned offsets"] --> C["Run required state and sink work"] --> D["Write completion log"]
end
subgraph WAIT["No batch needed"]
 E["Wait for next trigger
No new completion record"]
end
Q -- "yes" --> B
Q -- "no" --> R
R -- "yes" --> B
R -- "no" --> E
classDef driver fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e
classDef executor fill:#dcfce7,stroke:#16a34a,color:#14532d
classDef kafka fill:#ffedd5,stroke:#f97316,color:#7c2d12
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155
classDef state fill:#ede9fe,stroke:#7c3aed,color:#3b0764
classDef decision fill:#fef9c3,stroke:#ca8a04,color:#713f12
classDef danger fill:#fee2e2,stroke:#dc2626,color:#7f1d1d
linkStyle default stroke:#64748b,stroke-width:2px,stroke-linecap:round
class A driver
class Q,R decision
class B,D store
class C executor
class E driver
```

## Input preservation depends on all these conditions

These are necessary design conditions, not a proof that every deployment satisfies them. failOnDataLoss=true stops on detected missing offsets; it cannot regenerate records already removed from Kafka.

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"Ubuntu, Arial, sans-serif","fontSize":"17px","primaryTextColor":"#0f172a","lineColor":"#64748b","actorBkg":"#f1f5f9","actorBorder":"#64748b","actorTextColor":"#0f172a","signalColor":"#475569","signalTextColor":"#0f172a","noteBkgColor":"#fef9c3","noteTextColor":"#422006"},"flowchart":{"curve":"basis","htmlLabels":false,"nodeSpacing":52,"rankSpacing":70},"sequence":{"useMaxWidth":false,"actorMargin":70,"messageMargin":44,"wrap":true,"width":180}}}%%
flowchart TB
subgraph INPUT["Replayable input"]
 K["Kafka records retained
through recovery window"]
end
subgraph PROGRESS["Recoverable progress"]
 C["Durable compatible checkpoint"]
 O["One active owner
for the checkpoint"]
end
subgraph PROCESS["Replayable computation"]
 R["Required operator state available"]
 D["Compatible deterministic replay
for intended results"]
end
subgraph OUTPUT["Replay-safe output"]
 S["Sink success means durable output"]
 I["Deduplication or replay-safe finalization
when exactly-once effects are required"]
end
subgraph RESULT["Resulting behavior"]
 G["Recover and cover planned input"]
 E["Recover without duplicate sink effects"]
end
K --> G
C --> G
O --> G
R --> G
S --> G
G --> E
D --> E
I --> E
classDef driver fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e
classDef executor fill:#dcfce7,stroke:#16a34a,color:#14532d
classDef kafka fill:#ffedd5,stroke:#f97316,color:#7c2d12
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155
classDef state fill:#ede9fe,stroke:#7c3aed,color:#3b0764
classDef decision fill:#fef9c3,stroke:#ca8a04,color:#713f12
classDef danger fill:#fee2e2,stroke:#dc2626,color:#7f1d1d
linkStyle default stroke:#64748b,stroke-width:2px,stroke-linecap:round
class K kafka
class C,O store
class R,D state
class S,I store
class G,E executor
```
<!-- /spark-deep-diagrams -->
