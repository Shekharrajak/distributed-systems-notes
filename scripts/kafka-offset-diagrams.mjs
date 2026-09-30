const theme={theme:'base',themeVariables:{fontFamily:'Ubuntu, Arial, sans-serif',fontSize:'17px',primaryTextColor:'#0f172a',lineColor:'#64748b',actorBkg:'#f1f5f9',actorBorder:'#64748b',actorTextColor:'#0f172a',signalColor:'#475569',signalTextColor:'#0f172a',noteBkgColor:'#fef9c3',noteTextColor:'#422006'},flowchart:{curve:'basis',htmlLabels:false,nodeSpacing:48,rankSpacing:72},sequence:{useMaxWidth:false,actorMargin:75,messageMargin:44,wrap:true,width:180}};
const colors=`classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155
classDef danger fill:#fee2e2,stroke:#dc2626,color:#7f1d1d
linkStyle default stroke:#64748b,stroke-width:2px,stroke-linecap:round`;
const d=(id,title,caption,body)=>({id:'offset-'+id,title,caption,source:`%%{init: ${JSON.stringify(theme)}}%%\n${body}\n`});
export const diagrams=[
 d('authority','Three progress authorities, not three spellings of commit','Solid arrows store recovery progress. The dashed Flink path publishes optional Kafka group offsets. Sink output durability is a separate concern in every row.',`flowchart TB
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
${colors}
class K,F,S runtime
class O,C,L store`),
 d('frontier','Commit the safe frontier, not the furthest finished record','Illustrative contiguous input offsets. 102 is unfinished, so the safe restart offset is 102 even though 103 and 104 finished. Real Kafka offsets can have gaps.',`flowchart TB
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
 R["Resume at 102\nLater completed effects may repeat"]
 X["Resume at 105\nRecord 102 is skipped"]
end
F --> R
U --> X
${colors}
class A,B,D,E io
class C,U,X danger
class F,R control`),
 d('sync-async','Same server acknowledgement, different caller waiting','Two alternatives on one consumer owner thread. Async submission may reach the network before or after method return; this shows one possible schedule. Return is not a broker acknowledgement.',`sequenceDiagram
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
 C-->>A: Invoke callback on owner thread`),
 d('threads','The word async appears at two different layers','Both delegate implementations expose both APIs. Classic does not acquire a dedicated commit thread just because commitAsync is used. In the consumer-protocol delegate, network I/O is separated from user callback execution.',`flowchart TB
subgraph OLD["Classic / consumer thread"]
 A["commitSync / commitAsync"] --> B["ConsumerCoordinator"] --> C["ConsumerNetworkClient polling"]
 C --> D["Drain completed callbacks"]
end
subgraph APP["New consumer / app thread"]
 E["AsyncKafkaConsumer\ncommitSync / commitAsync"]
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
${colors}
class A,B,D,E,F runtime
class C,G,H,I io
class J control
style OLD fill:#f5f3ff,stroke:#c4b5fd
style APP fill:#f5f3ff,stroke:#c4b5fd
style BG fill:#fff7ed,stroke:#fdba74
style SERVER fill:#f0f9ff,stroke:#7dd3fc`),
 d('rpc','The commit goes to the coordinator, not every input leader','Illustrative RF=3 offsets partition. Follower Fetch replication advances the high watermark. Broker role placement is independent of which broker leads the input data partition.',`sequenceDiagram
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
 C->>B: Rediscover after coordinator error`),
 d('stale-retry','Why retrying an old callback payload is unsafe','This is a new application retry after a newer request—not Kafka reordering the original calls. Assume unchanged valid ownership; both writes are accepted.',`sequenceDiagram
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
 Note over A: Publish current safe frontier instead of stale payload`),
 d('flink','Flink checkpoint completion fans out to two different commits','TaskManager source mailbox and fetcher are threads in a JVM, not separate pods. The sink committer may be in another task/JVM. This is a logical flow, not a total ordering between source-offset and sink-transaction completion.',`sequenceDiagram
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
 Note over M,K: Source OffsetCommit failure does not invalidate checkpoint 42`),
 d('spark','Spark Structured Streaming: checkpoint logs drive restart','Default synchronous microbatch path. Async progress tracking, continuous processing and RealTimeTrigger have different timing and are outside this sequence. There is no input OffsetCommit RPC in this flow.',`sequenceDiagram
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
 D->>D: Kafka source.commit(end) is no-op`),
 d('dstream','Legacy DStreams: commitAsync first means enqueue','Call on the original DirectKafkaInputDStream, after the output action succeeds. Queue coalescing uses max(untilOffset) only for submitted completed work; it cannot detect holes caused by out-of-order batch completion.',`flowchart TB
subgraph DRIVER["Spark driver — output callback"]
 A["Capture original RDD offset ranges"] --> B["Wait for output action success"] --> C["CanCommitOffsets.commitAsync"] --> Q["Thread-safe queue of OffsetRange"]
end
subgraph COMPUTE["Spark driver — later DStream compute"]
 D["commitAll drains queue"] --> M["Max untilOffset per partition\nMost recent callback"] --> K["Driver KafkaConsumer.commitAsync"]
end
subgraph STORE["Remote Kafka coordinator"]
 O[("__consumer_offsets")]
end
Q -. "deferred drain" .-> D
K --> O
${colors}
class A,B,C runtime
class Q,D,M,K io
class O store`)
];
