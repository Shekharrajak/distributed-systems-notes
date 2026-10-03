const theme={theme:'base',themeVariables:{fontFamily:'Ubuntu, Arial, sans-serif',fontSize:'17px',primaryTextColor:'#0f172a',lineColor:'#64748b',actorBkg:'#f1f5f9',actorBorder:'#64748b',actorTextColor:'#0f172a',signalColor:'#475569',signalTextColor:'#0f172a'},flowchart:{curve:'basis',htmlLabels:false,nodeSpacing:48,rankSpacing:64},sequence:{useMaxWidth:false,actorMargin:60,messageMargin:42,wrap:true,width:160}};
const styles=`classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e
classDef native fill:#ede9fe,stroke:#7c3aed,color:#3b0764
classDef jvm fill:#dcfce7,stroke:#16a34a,color:#14532d
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155
linkStyle default stroke:#64748b,stroke-width:2px,stroke-linecap:round`;
const d=(id,title,caption,body)=>({id:'comet-deep-'+id,title,caption,source:`%%{init: ${JSON.stringify(theme)}}%%\n${body}\n`});
export const diagrams=[
d('deployment','Spark distributes work; Comet executes native fragments','Illustrative cluster deployment, not the local capture. JNI is in-process. DataFusion is a library inside each executor, not a separate cluster coordinator.',`flowchart TB
subgraph DRIVER["Driver JVM / control plane"]
 D["Catalyst + Iceberg planning"] --> P["Comet rewrite + Spark stage scheduling"]
end
subgraph E1["Executor JVM 1"]
 T1["Task threads + Spark shuffle client"] --> N1["JNI / Rust / Arrow / DataFusion"]
end
subgraph E2["Executor JVM 2"]
 T2["Task threads + Spark shuffle client"] --> N2["JNI / Rust / Arrow / DataFusion"]
end
subgraph STORAGE["Storage and data transfer"]
 O[("Iceberg data + delete files")]
 S[("Local shuffle blocks")]
end
P -->|"LaunchTask RPC"| T1
P -->|"LaunchTask RPC"| T2
O --> N1
O --> N2
N1 --> S
S -->|"Block fetch"| T2
${styles}
class D,P control
class T1,T2 jvm
class N1,N2 native
class O,S store`),
d('iceberg','Iceberg pruning is not the same as row filtering','Planning chooses snapshot and file tasks. Native execution reads projected columns, applies supported delete semantics and produces Arrow batches. Retained filters enforce residual SQL predicates.',`flowchart TB
subgraph PLAN["Driver / Java Iceberg"]
 A["Snapshot + schema + manifests"] --> B["Prune files and plan FileScanTasks"] --> C["Serialize task and delete metadata"]
end
subgraph READ["Executor / native Iceberg reader"]
 D["FileIO + projected Parquet reads"] --> E["Delete handling + schema adaptation"] --> F["Arrow RecordBatch"]
end
subgraph COMPUTE["Eligible native fragment"]
 G["Residual Filter + Project"] --> H["Join / Aggregate / Sort"]
end
C --> D
F --> G
${styles}
class A,B,C control
class D,E,F,H,G native`),
d('boundaries','Columnar is a representation contract, not a language','Spark ColumnarBatch may use non-Arrow vectors. Comet needs an Arrow-compatible bridge. Unsupported operators can create interior conversion boundaries; the atlas plans show their actual placement.',`flowchart TB
subgraph NATIVE["Native-eligible path"]
 A["Iceberg native scan"] --> B["Arrow Filter / Join / Aggregate"]
 C["Resume native operators"]
end
subgraph ROWS["Row-only Spark operator or consumer"]
 R["ColumnarToRow"] --> U["UnsafeRow / InternalRow operator"] --> V["CometSparkRowToColumnar"]
end
subgraph OUTPUT["Row-facing result API"]
 Z["Final CometColumnarToRow"]
end
B --> R
V --> C
B --> Z
${styles}
class A,B,C native
class R,U,V,Z jvm`),
d('jvm-shuffle','Ordinary Spark row shuffle and sort-merge join','The shuffle groups records by reduce partition. Separate SortExec operators establish join-key order within each reducer input.',`flowchart TB
subgraph MAP["Map tasks / JVM"]
 L["Left UnsafeRows"] --> HL["Hash partition by key"]
 R["Right UnsafeRows"] --> HR["Hash partition by key"]
end
subgraph DISK["Map output files"]
 FL[("Left data + index")]
 FR[("Right data + index")]
end
subgraph REDUCE["One reducer task / JVM"]
 SL["Fetch left partition + SortExec"]
 SR["Fetch right partition + SortExec"]
 J["SortMergeJoinExec\nMerge keys + evaluate residual"]
end
HL --> FL --> SL --> J
HR --> FR --> SR --> J
${styles}
class L,R,HL,HR,SL,SR,J jvm
class FL,FR store`),
d('native-shuffle','Comet native shuffle: columnar compute, Spark transport','Inspected source path: the native producer subtree can be fused into ShuffleWriter. Spark retains scheduling, map-output metadata and block transport.',`flowchart TB
subgraph MAP["Map executor / native fragment"]
 A["Arrow producer subtree"] --> B["ShuffleWriter\nPartition + encode + compress"]
end
subgraph LOCAL["Map executor / publication"]
 C[("Temporary output / spill files")]
 D["Commit data + index\nReturn MapStatus"]
end
subgraph REDUCE["Reducer executor"]
 E["Spark ShuffleBlockFetcherIterator"] --> F["Native decode / ShuffleScan"] --> G["Arrow join or aggregate"]
end
B --> C --> D --> E
${styles}
class A,B,F,G native
class D,E jvm
class C store`),
d('jvm-columnar-shuffle','Comet JVM columnar shuffle: rows can exist in the middle','Columnar describes the shuffle payload and decoded batches here; it does not promise a row-free producer path. This diagram describes inspected source, not an additional atlas capture.',`flowchart TB
subgraph INPUT["Producer"]
 A["Comet columnar child"] --> B["child.execute: ColumnarToRow"]
 X["Spark row child"]
end
subgraph WRITER["JVM partitioning / native encoding"]
 C["UnsafeRows + partition IDs\nPages / pointer sorter"] --> D["JNI encode Arrow shuffle frames"]
end
subgraph OUTPUT["Fetch and consume"]
 E[("Data / index files")] --> F["Spark block fetch + native decode"] --> G["ColumnarBatch / native consumer"]
end
B --> C
X --> C
D --> E
${styles}
class A,D,G native
class B,X,C,F jvm
class E store`),
d('network','Who sends what during a shuffle','Illustrative ordinary local-disk shuffle. A deployed external shuffle service may serve blocks instead. Result metadata goes to the driver; shuffle payloads travel between workers.',`sequenceDiagram
box rgb(224,242,254) Driver
 participant D as Scheduler / MapOutputTracker
end
box rgb(220,252,231) Map executor
 participant M as Task / writer
 participant B as Block server
end
box rgb(237,233,254) Reduce executor
 participant R as Task / fetcher
end
D->>M: Launch map task
M->>M: Write and publish data / index
M-->>D: Task completion / MapStatus
D->>R: Launch reduce task
R->>D: Resolve map output locations when needed
D-->>R: Addresses and block sizes
R->>B: Fetch reduce-partition blocks
B-->>R: Serialized row or Arrow shuffle bytes
R->>R: Decode + join / aggregate
R-->>D: Task completion`),
d('broadcast','Broadcast is a different topology','Unlike ordinary shuffle, broadcast exchange collects build-side data at the driver. Spark distributes a HashedRelation; Comet distributes serialized Arrow batches for native consumption.',`flowchart TB
subgraph BUILD["Build-side executor tasks"]
 A["Filter / project small relation"]
end
subgraph DRIVER["Driver broadcast exchange"]
 B["Collect build-side output"] --> C["Publish broadcast blocks"]
end
subgraph WORKERS["Probe executor tasks"]
 D["Partition 0 probe + hash lookup"]
 E["Partition 1 probe + hash lookup"]
end
A --> B
C --> D
C --> E
${styles}
class A,D,E jvm
class B,C control`),
d('q3','Q3 with threshold −1: change the partition key twice','Reduced topology from the captured plans, not every Project/Filter node. Four join-input exchanges feed two sort-merge joins. Grouping can reuse the order-key distribution.',`flowchart TB
subgraph FIRST["Join 1 / customer key"]
 C["customer filter\nHash custkey + sort"] --> J1["orders ⋈ customer"]
 O["orders filter\nHash custkey + sort"] --> J1
end
subgraph SECOND["Join 2 / order key"]
 X["Repartition result by orderkey\nSort orderkey"] --> J2["lineitem ⋈ result"]
 L["lineitem filter\nHash orderkey + sort"] --> J2
end
subgraph FINAL["Revenue ranking"]
 A["Partial + final group aggregate"] --> T["TakeOrderedAndProject / top 10"]
end
J1 --> X
J2 --> A
${styles}
class C,O,J1,X,J2,L,A,T native`),
d('aggregate','Q1: local partials reduce the shuffle payload','Illustrative values, captured operator shape. The group-by exchange and final range-ordering exchange do different jobs. AVG requires mergeable sum/count state, not averaging partition averages.',`flowchart TB
subgraph MAP["Parallel scan tasks"]
 A["Scan + filter + partial aggregate\nTask 0"]
 B["Scan + filter + partial aggregate\nTask 1"]
end
subgraph REDUCE["Grouped aggregation"]
 C["Hash exchange by returnflag / linestatus"] --> D["Merge partial aggregate states"]
end
subgraph ORDER["Global ORDER BY"]
 E["Range exchange"] --> F["Local sort within ranges"]
end
A --> C
B --> C
D --> E
${styles}
class A,B,C,D,E,F native`),
d('recovery','Lost shuffle is recomputed; it is not a database commit','Task attempts are retryable work. Missing blocks can invalidate map outputs and cause stage recomputation. The input snapshot and files must remain readable.',`sequenceDiagram
box rgb(224,242,254) Driver
 participant D as DAGScheduler
end
box rgb(220,252,231) Executors
 participant R as Reduce attempt
 participant M as New map attempt
end
box rgb(241,245,249) Durable input
 participant I as Snapshot files
end
R-->>D: FetchFailed for missing map block
D->>D: Invalidate output and reschedule required work
D->>M: Launch replacement map task
M->>I: Read pinned input files
I-->>M: Data and deletes
M->>M: Rebuild shuffle output
M-->>D: New MapStatus
D->>R: Launch reduce reattempt
R->>M: Fetch replacement blocks`),
d('join-choice','Join choice has two layers','Simplified decision model, not an exact ordering of every Spark hint or fallback rule. Spark chooses distribution and join strategy; Comet validates and translates eligible operators.',`flowchart TB
subgraph SPARK["Spark physical planning"]
 A["Equi-join + hints + stats + join type"] --> B{"Broadcast eligible?"}
 B -->|"yes"| C["Broadcast hash join"]
 B -->|"no"| D{"Local hash build suitable?"}
 D -->|"yes"| E["Shuffled hash join"]
 D -->|"otherwise / sortable keys"| F["Sort-merge join"]
end
subgraph COMET["Comet rewrite"]
 G["Validate types / expressions / semantics"] --> H["Native join or Spark fallback"]
end
C --> G
E --> G
F --> G
${styles}
class A,B,C,D,E,F control
class G,H native`)
];
