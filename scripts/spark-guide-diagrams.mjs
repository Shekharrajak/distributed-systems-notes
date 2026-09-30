const theme={theme:'base',themeVariables:{fontFamily:'Ubuntu, Arial, sans-serif',fontSize:'17px',primaryTextColor:'#0f172a',lineColor:'#64748b',actorBkg:'#f1f5f9',actorBorder:'#64748b',actorTextColor:'#0f172a',signalColor:'#475569',signalTextColor:'#0f172a',noteBkgColor:'#fef9c3',noteTextColor:'#422006'},flowchart:{curve:'basis',htmlLabels:false,nodeSpacing:52,rankSpacing:70},sequence:{useMaxWidth:false,actorMargin:70,messageMargin:44,wrap:true,width:180}};
const colors=`classDef driver fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e
classDef executor fill:#dcfce7,stroke:#16a34a,color:#14532d
classDef kafka fill:#ffedd5,stroke:#f97316,color:#7c2d12
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155
classDef state fill:#ede9fe,stroke:#7c3aed,color:#3b0764
classDef decision fill:#fef9c3,stroke:#ca8a04,color:#713f12
classDef danger fill:#fee2e2,stroke:#dc2626,color:#7f1d1d
linkStyle default stroke:#64748b,stroke-width:2px,stroke-linecap:round`;
const d=(id,title,caption,body)=>({id:'spark-deep-'+id,title,caption,source:`%%{init: ${JSON.stringify(theme)}}%%\n${body}\n`});
export const diagrams=[
 d('deployment','One driver, several executors, three durable systems','Illustrative three-executor placement. Kafka payloads travel directly to executor readers. The cluster manager supplies processes/resources; Spark schedules tasks within them.',`flowchart TB
subgraph DRIVER["Driver JVM"]
 D["Query execution thread\nBatch plan and task schedulers"]
end
subgraph WORKERS["Parallel executor JVMs"]
 direction LR
 T1["E1 task slots\nReaders and operators"]
 T2["E2 task slots\nReaders and state"]
 T3["E3 task slots\nReaders and state"]
end
K[("Kafka partition logs")]
C[("Shared checkpoint storage")]
O[("Output sink")]
D -->|"LaunchTask RPC"| WORKERS
K -->|"Direct record fetch"| WORKERS
D -->|"offset and commit logs"| C
WORKERS -->|"state checkpoints"| C
WORKERS -->|"sink writes"| O
${colors}
style WORKERS fill:#f0fdf4,stroke:#86efac
class D driver
class T1,T2,T3 executor
class K kafka
class C,O store`),
 d('batch','A successful microbatch across three executors','Default synchronous microbatch. Each executor may run several task attempts. The checkpoint stores an offset vector for the whole source, not a separate completion cursor for each executor.',`sequenceDiagram
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
 D->>D: Advance to batch 43`),
 d('ranges','Kafka partitions map to input tasks, not fixed executors','Worked range placement for one batch. Start inclusive, end exclusive. minPartitions or maxRecordsPerPartition can split one Kafka partition into more than one input task; this picture uses the unsplit case.',`flowchart TB
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
 R["Same range, new task attempt\nMay run on another executor"]
end
C -. "reschedule" .-> R
${colors}
class P0,P1,P2,P3 driver
class A,B,C,D executor
class R state`),
 d('calls','V2 read path: driver planning to executor fetch','Exact class families from the pinned source. V1 KafkaSource/KafkaSourceRDD has an alternate scan path, covered in the notes; checkpoint ownership is the same.',`flowchart TB
subgraph DRIVER["Driver process"]
 A["StreamExecution\nquery thread"] --> B["MicroBatchExecution\nconstructNextBatch / runBatch"]
 B --> C["KafkaMicroBatchStream\nlatestOffset / planInputPartitions"]
 C --> O["KafkaOffsetReaderAdmin\nAdmin listOffsets"]
 B --> D["Physical plan and RDD stages"] --> S["DAGScheduler\nTaskSchedulerImpl"]
end
subgraph EXEC["Executor process"]
 E["Executor.TaskRunner"] --> F["KafkaBatchPartitionReader"] --> G["KafkaDataConsumer"] --> H["InternalKafkaConsumer\nassign / seek / poll"]
end
subgraph KAFKA["Kafka brokers"]
 K["Metadata and partition leaders"]
end
S -->|"LaunchTask RPC"| E
O --> K
H -->|"Kafka binary protocol"| K
${colors}
class A,B,C,O,D,S driver
class E,F,G,H executor
class K kafka`),
 d('network','Three network paths carry different things','Task RPC carries serialized work and task status. Kafka connections carry input records. Shuffle transfers exchange data between executor-side tasks; the driver tracks where blocks can be fetched.',`sequenceDiagram
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
 E2-->>D: StatusUpdate and task result`),
 d('shuffle','Aggregation changes the partitioning boundary','Example customer-key aggregation. Source tasks have Kafka partition ranges; state tasks have hash-partitioned keys. State partition 0 and Kafka partition 0 are different identities.',`flowchart TB
subgraph MAP["Source and map tasks"]
 A["E1 / P0 and P1"]
 B["E2 / P2"]
 C["E3 / P3"]
end
subgraph SHUFFLE["Exchange by customer key"]
 X["Materialized shuffle blocks\nhash customer into state partition"]
end
subgraph REDUCE["Stateful tasks — example two partitions"]
 S0["E2 / state partition 0\nLoad state version 42"]
 S1["E3 / state partition 1\nLoad state version 42"]
end
subgraph FS["Shared durable checkpoint"]
 V0[("Operator / partition 0\nCommitted next state 43")]
 V1[("Operator / partition 1\nCommitted next state 43")]
end
A --> X
B --> X
C --> X
X --> S0
X --> S1
S0 --> V0
S1 --> V1
${colors}
class A,B,C executor
class X store
class S0,S1 state
class V0,V1 store`),
 d('storage','A checkpoint directory holds several kinds of evidence','Offsets describe input, state files describe operator state, and commit records identify completed batches. Output durability and any sink deduplication log live in the sink’s own storage.',`flowchart TB
subgraph DRIVER["Driver writes"]
 D["Query and source metadata"]
 P["Planned end offsets"]
 F["Completed batch metadata"]
end
subgraph EXEC["Executor writes"]
 E["Executor task"]
 SP["State-store provider\nDelta / snapshot / changelog"]
 W["Sink writer"]
end
subgraph CKPT["Shared checkpoint directory"]
 M[("metadata and sources")]
 O[("offsets / batchId")]
 C[("commits / batchId")]
 S[("state / operator / partition")]
end
subgraph SINK["Sink storage"]
 R[("Durable output\nOptional batch ledger / manifest")]
end
D --> M
P --> O
F --> C
E --> SP
E --> W
SP --> S
W --> R
${colors}
class D,P,F driver
class E,W executor
class SP state
class M,O,C,S,R store`),
 d('state-restore','Lost executor: reload the required state version elsewhere','Example stateful batch 42. Task attempts can write version 43 before the global batch succeeds. The retry must reconstruct from the required version 42 and the selected checkpoint IDs when enabled.',`sequenceDiagram
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
 Note over D,C: State locality RPC is metadata, durable state is in storage`),
 d('recovery','Driver restart: compare offsets and commits','Normal synchronous microbatch recovery. The previous offset entry supplies the start vector; the latest planned entry supplies the end vector. Inconsistent or missing required entries can fail recovery.',`flowchart TB
subgraph LOAD["New driver reads same checkpoint"]
 O["Latest offsets entry = batch N"] --> Q{"Matching commits/N exists?"}
end
subgraph REPLAY["Incomplete latest batch"]
 A["Recover start vector\nfrom previous batch"] --> B["Recover end vector\nfrom offsets/N"] --> C["Load required prior state"] --> D["Rerun batch N and sink"]
end
subgraph NEXT["Completed latest batch"]
 E["Use end vector as next start"] --> F["Restore committed operator state"] --> G["Construct batch N + 1"]
end
Q -- "no" --> A
Q -- "yes" --> E
${colors}
class O,A,B,E driver
class Q decision
class C,F state
class D,G executor`),
 d('crash-window','Output can succeed before the query commit is durable','This sequence uses a replayable input and a sink that accepts repeats. Recovery preserves input coverage but can duplicate external effects. A sink must provide its own replay-safe behavior for exactly-once results.',`sequenceDiagram
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
 D->>C: Persist commits/42`),
 d('sink-ledger','Application pattern: output and batch ledger in one transaction','Illustrative foreachBatch sink design, not a built-in Spark transaction. All output changes covered by the ledger must share a supported atomic finalization protocol; ordinary parallel JDBC writes do not automatically share one transaction.',`sequenceDiagram
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
 Note over A: Return only after success or recognized completed batch`),
 d('no-new-data','No new input can still require a stateful batch','constructNextBatch considers new offsets and lastExecution.shouldRunAnotherBatch. Watermark/timeout work can require a batch with unchanged input offsets. This is different from losing source data.',`flowchart TB
subgraph DRIVER["Driver trigger"]
 A["Fetch latest source offsets"] --> Q{"Offsets advanced?"}
 R{"State cleanup or timeout\nrequires another batch?"}
end
subgraph RUN["Batch execution"]
 B["Log planned offsets"] --> C["Run required state and sink work"] --> D["Write completion log"]
end
subgraph WAIT["No batch needed"]
 E["Wait for next trigger\nNo new completion record"]
end
Q -- "yes" --> B
Q -- "no" --> R
R -- "yes" --> B
R -- "no" --> E
${colors}
class A driver
class Q,R decision
class B,D store
class C executor
class E driver`),
 d('loss-conditions','Input preservation depends on all these conditions','These are necessary design conditions, not a proof that every deployment satisfies them. failOnDataLoss=true stops on detected missing offsets; it cannot regenerate records already removed from Kafka.',`flowchart TB
subgraph INPUT["Replayable input"]
 K["Kafka records retained\nthrough recovery window"]
end
subgraph PROGRESS["Recoverable progress"]
 C["Durable compatible checkpoint"]
 O["One active owner\nfor the checkpoint"]
end
subgraph PROCESS["Replayable computation"]
 R["Required operator state available"]
 D["Compatible deterministic replay\nfor intended results"]
end
subgraph OUTPUT["Replay-safe output"]
 S["Sink success means durable output"]
 I["Deduplication or replay-safe finalization\nwhen exactly-once effects are required"]
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
${colors}
class K kafka
class C,O store
class R,D state
class S,I store
class G,E executor`)
];
