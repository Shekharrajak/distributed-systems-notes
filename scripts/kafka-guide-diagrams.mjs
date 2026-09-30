const theme = {theme:'base',htmlLabels:false,themeVariables:{fontFamily:'Ubuntu, Arial, sans-serif',fontSize:'17px',primaryTextColor:'#0f172a',lineColor:'#64748b',actorBkg:'#f1f5f9',actorBorder:'#64748b',actorTextColor:'#0f172a',signalColor:'#475569',signalTextColor:'#0f172a',noteBkgColor:'#fef9c3',noteTextColor:'#422006'}};
const palette = `
classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d
classDef runtime fill:#ede9fe,stroke:#7c3aed,color:#3b0764
classDef io fill:#ffedd5,stroke:#f97316,color:#7c2d12
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155
classDef state fill:#fef9c3,stroke:#ca8a04,color:#713f12
linkStyle default stroke:#64748b,stroke-width:1.7px,stroke-linecap:round`;
const flow = (id,title,caption,body)=>({id,title,caption,source:`%%{init: ${JSON.stringify({...theme,flowchart:{curve:'basis',nodeSpacing:48,rankSpacing:65,htmlLabels:false,padding:22}})}}%%\nflowchart TB\n${body}\n${palette}`});
const seq = (id,title,caption,body)=>({id,title,caption,source:`%%{init: ${JSON.stringify({...theme,sequence:{useMaxWidth:false,actorMargin:55,boxMargin:18,messageMargin:35,diagramMarginX:24,diagramMarginY:24,wrap:true,width:170}})}}%%\nsequenceDiagram\n${body}`});

export const diagrams = [
flow('rpc-placement','A deployment is not one pod per box','Illustrative Kubernetes placement, not an observed cluster. Kafka brokers and Flink TaskManagers are separate processes; connector libraries live in TaskManagers. KRaft controllers are shown as a separate quorum; combined-role deployments also exist.',`
subgraph KAFKA["Kafka cluster — multiple processes / pods"]
 QC["KRaft controller quorum\nmetadata log"]
 B1["Broker A\ninput P0 leader"]
 B2["Broker B\noutput P0 leader"]
 BR["Other broker replicas\nindependent volumes"]
end
subgraph FLINK["Flink cluster"]
 JM["JobManager pod\nJobMaster + SourceCoordinator"]
 T1["TaskManager pod A\nKafkaSource + parse chain"]
 T2["TaskManager pod B\nkeyed transform + KafkaSink"]
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
class T1,T2 worker`),
flow('rpc-locality','Three very different meanings of “connected”','A direct operator call needs neither Kafka nor Flink RPC. Separate tasks use a data exchange even when co-located. Different TaskManagers use Netty data transport, not a per-record JobMaster RPC.',`
subgraph ONE["Case 1 — one chained task / one mailbox thread"]
 direction TB
 A["Source operator"] --> B["Map operator"]
end
subgraph TWO["Case 2 — separate tasks / same TaskManager JVM"]
 direction TB
 C["Task thread 1\nserialize output"] --> D["Local input channel\nTask thread 2"]
end
subgraph THREE["Case 3 — separate TaskManager processes"]
 direction TB
 E["TaskManager A\nResultPartition"] -->|"Netty TCP buffers + credits"| F["TaskManager B\nRemoteInputChannel"]
end
class A,B runtime
class C,D worker
class E,F io`),
seq('rpc-bootstrap','Bootstrap finds owners; owners serve requests','Simplified healthy path. Connections are reused; TLS/SASL and ApiVersions happen per connection as needed. The transaction coordinator and group coordinator can be different brokers from both partition leaders.',`
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
Note over C,G: No load balancer hop per record after metadata routing`),
flow('rpc-broker-threads','Inside one Kafka broker process','These are threads, queues and components within a broker JVM—not separate pods. Replica fetchers in other brokers pull from this leader. Delayed operations release handler threads rather than blocking one thread per waiting request.',`
subgraph NET["SocketServer network threads"]
 A["Acceptor\naccept TCP"] --> P["Processor / Selector\nread framed request"]
 R["Processor\nwrite response"]
end
subgraph SERVER["Request handling within same JVM"]
 Q["RequestChannel queue"] --> H["KafkaRequestHandler\nKafkaApis authorization + dispatch"]
 H --> M["ReplicaManager / Partition\nvalidate + append"]
 M --> D["Delayed operation\nawait replication or timeout"]
end
subgraph DISK["Broker-local storage"]
 L[("UnifiedLog → LogSegment\npage cache / log files")]
end
P --> Q
M --> L
D -. "completion callback" .-> R
M -. "immediate result when ready" .-> R
class A,P,R io
class Q,H,M worker
class D state
class L store`),
seq('rpc-produce-replicate','Produce durability is partition replication','Example: replication factor 3, acks=all, min.insync.replicas=2. Both followers are initially in ISR. A real request batches many records/partitions; followers repeat Fetch calls and the leader learns their advanced offsets.',`
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
Note over P,L: A timeout can be ambiguous: append may already exist`),
flow('rpc-source-threads','One KafkaSource subtask crosses a thread boundary','A SplitFetcher owns consumer API calls. A bounded handoff queue feeds the task mailbox, where deserialization/emission advances checkpointed split state. Consumer internal network threading depends on client configuration/version.',`
subgraph JM["JobManager process"]
 E["KafkaSourceEnumerator\ndiscover and assign splits"]
end
subgraph IO["TaskManager — SplitFetcher I/O thread"]
 K["Kafka partition split reader\nassign / seek / poll"]
end
subgraph MAIL["TaskManager — StreamTask mailbox thread"]
 A["SourceOperator\nhandle AddSplitEvent"]
 S["SourceReaderBase\npollNext()"] --> EM["KafkaRecordEmitter\ndeserialize and emit"]
 EM --> MAP["Chained transform\nprocessElement"]
 MAP -->|"return to emitter"| O["Split currentOffset\nrecord.offset + 1"]
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
class Q worker`),
seq('rpc-checkpoint','The checkpoint ties recovery positions to sink intent','Aligned checkpoint sketch; each downstream task is triggered by barriers on data channels. “RPC accepted” is not “snapshot durable.” The coordinator waits for all required task/coordinator state, not only the source.',`
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
Note over J,W: Completion makes recovery state durable, not every output immediately visible`),
seq('rpc-transaction-visibility','Completed checkpoint to externally visible output','Healthy KafkaSink commit path. The group-offset commit is independent bookkeeping. EndTxn success confirms the coordinator decision; control markers and the partition LSO govern read_committed visibility.',`
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
Note over L,G: Output visibility and group offset are not one atomic Flink Kafka transaction`),
flow('rpc-storage','Four stores answer four different questions','A Kafka topic is not a pod. Its partitions and replicas live across broker logs. A Flink keyed-state backend is not Kafka Streams’ changelog topic. The JobManager manages handles/metadata, not all workers’ state bytes.',`
subgraph KAFKA["Kafka replicated partition logs"]
 DATA[("User topic logs\nWhat records exist?")]
 OFF[("__consumer_offsets\nWhat group position is recorded?")]
 TXN[("__transaction_state\nWhat decision did the coordinator make?")]
 META[("KRaft metadata log\nWho owns what?")]
end
subgraph FLINK["Flink state and recovery"]
 LIVE[("Live keyed/operator state\nheap / RocksDB / configured backend")]
 CHECK[("Durable checkpoints\nsource offsets + state + sink intent")]
end
DATA -. "replay retained input" .-> LIVE
LIVE -->|"snapshot"| CHECK
TXN -. "fences and transaction decisions" .-> DATA
META -. "leadership and placement" .-> DATA
class DATA,OFF,TXN,META,CHECK store
class LIVE state`),
seq('rpc-recovery','Failure after checkpoint completion but before commit reply','Illustrative recoverable case: CP42 is durable and T42 has not timed out. Retrying the same transaction is not resending its business records in a new transaction. Unknown/fatal commit outcomes must not be silently ignored.',`
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
K-->>New: Replay records after checkpoint cut`),
flow('rpc-mental-model','Remember: owner → thread → message → durable fact → visibility','Ask these five questions for every arrow. An acknowledgement has meaning only when you know which boundary it acknowledges.',`
subgraph MODEL["Five questions for the presentation"]
 direction TB
 A["1. OWNER\nWhich leader or coordinator?"]
 B["2. THREAD\nWhich process mutates state?"]
 C["3. MESSAGE\nCall, queue, data channel or RPC?"]
 D["4. DURABLE FACT\nWhich log or checkpoint survives?"]
 E["5. VISIBILITY\nWhat can a downstream reader see?"]
 A --> B --> C --> D --> E
end
class A control
class B runtime
class C io
class D store
class E state`)
];
