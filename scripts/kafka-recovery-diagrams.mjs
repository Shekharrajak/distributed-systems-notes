const theme={theme:'base',themeVariables:{fontFamily:'Ubuntu, Arial, sans-serif',fontSize:'17px',primaryTextColor:'#0f172a',lineColor:'#64748b',actorBkg:'#f1f5f9',actorBorder:'#64748b',actorTextColor:'#0f172a',signalColor:'#475569',signalTextColor:'#0f172a',noteBkgColor:'#fef9c3',noteTextColor:'#422006'},flowchart:{curve:'basis',htmlLabels:false,nodeSpacing:44,rankSpacing:64},sequence:{useMaxWidth:false,actorMargin:65,messageMargin:42,wrap:true,width:170}};
const colors=`classDef control fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e
classDef group fill:#fef9c3,stroke:#ca8a04,color:#713f12
classDef worker fill:#dcfce7,stroke:#16a34a,color:#14532d
classDef txn fill:#ede9fe,stroke:#7c3aed,color:#3b0764
classDef store fill:#f1f5f9,stroke:#64748b,color:#334155
classDef danger fill:#fee2e2,stroke:#dc2626,color:#7f1d1d
linkStyle default stroke:#64748b,stroke-width:2px,stroke-linecap:round`;
const d=(id,title,caption,body)=>({id:'recovery-'+id,title,caption,source:`%%{init: ${JSON.stringify(theme)}}%%\n${body}\n`});
export const diagrams=[
 d('roles','One cluster, several independent owners','Illustrative placement: these are roles inside processes, not one required pod per box. The controller assigns partition leaders; backing-partition leadership determines coordinator ownership.',`flowchart TB
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
${colors}
class C1 control
class CM,PD,GD,TD,SD store
class P worker
class G,S group
class T txn`),
 d('election','KRaft election: votes choose the controller','Three controller voters; C1 fails. The inspected code has a prospective/pre-vote phase before a new election epoch. Exact wire support depends on the negotiated KRaft version.',`sequenceDiagram
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
 C2->>C2: Advance committed metadata boundary`),
 d('metadata','Metadata travels as a replicated log, then an image','This is KRaft metadata propagation, not a legacy controller sending LeaderAndIsr for every change. Fetch arrows indicate pull replication; replies carry the records.',`sequenceDiagram
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
 B->>B: Publisher updates replicas + coordinators`),
 d('broker-failure','A broker failure fans out into independent recoveries','Example: B1 led a user partition and one group shard. Other transaction/share shards may live elsewhere and do not automatically move.',`sequenceDiagram
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
 N-->>U: Serve from reconstructed state`),
 d('group-recovery','Group coordinator = a recoverable shard','No separate election per group. The controller elects the backing partition leader; that broker loads all groups mapped to the shard.',`flowchart TB
subgraph CONTROL["Committed cluster metadata"]
 A["__consumer_offsets / P7\nleader changes B1 → B2"]
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
${colors}
class A control
class B,C,D,E group
class F group,danger
class G,H store`),
 d('share-planes','Share groups have three owners','Membership, record delivery and durable delivery state are deliberately separate. B1, B2 and B3 can be the same process or different brokers.',`flowchart TB
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
 L["SharePartition cache\nacquisition locks + timers"]
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
${colors}
class C store
class M group
class O,D,SS store
class F,L worker
class X group`),
 d('share-ack','An acknowledgement crosses a durability boundary','Ordinary ACCEPT shown. A successful local application side effect is not atomically coupled to this acknowledgement. Share state persistence uses broker replication, not controller-quorum replication.',`sequenceDiagram
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
 L-->>U: Acknowledgement result`),
 d('share-failure','Recover the state owner that actually failed','Do not conflate a crashed consumer, a source-partition leader and the share-state coordinator.',`flowchart TB
subgraph FAILURE["Which component failed?"]
 F{"Failure domain"}
 C["Consumer process"]
 P["Source partition leader"]
 S["Share state coordinator"]
end
subgraph RECOVERY["Independent recovery paths"]
 CR["Release or acquisition-lock expiry\nredelivery remains possible"]
 PR["Elect source leader\nreload state via persister\nuse newer leader epoch"]
 SR["Elect state-topic leader\nreplay share shard\nretry state RPCs"]
end
F --> C --> CR
F --> P --> PR
F --> S --> SR
${colors}
class F,C,P,S store
class CR,PR worker
class SR group`),
 d('txn-logs','A transaction leaves evidence in different logs','A Kafka consume-transform-produce transaction can include __consumer_offsets. Ordinary Flink KafkaSource offset commits are not automatically part of this transaction.',`flowchart TB
subgraph TC["Transaction coordinator"]
 T["Transaction state machine"]
 D[("__transaction_state\nprepare decision → complete")]
 T --> D
end
subgraph P["Output partition leader"]
 W["Validate and append marker"]
 L[("Output log\ntransactional data + control batch")]
 W --> L
end
subgraph G["Group coordinator — only if enrolled"]
 O["Complete transactional offsets"]
 OL[("__consumer_offsets\npending offsets + control batch")]
 O --> OL
end
T -- WriteTxnMarkers --> W
T -- WriteTxnMarkers --> O
${colors}
class T txn
class W worker
class O group
class D,L,OL store`),
 d('txn-markers','EndTxn decision, marker fan-out, completion','Two participant leaders shown. Each marker must be replicated under its partition policy. Final COMPLETE bookkeeping is not the moment at which every reader simultaneously sees the transaction.',`sequenceDiagram
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
 Note over A,B: Each partition advances LSO independently`),
 d('txn-crash','Crash recovery follows the durable state','This is a decision tree over recovered log state, not every legal TransactionMetadata transition. A prepared decision is irrevocable; completion work is repeatable.',`flowchart TB
subgraph LOAD["Replacement transaction coordinator"]
 A["Replay __transaction_state shard"]
 Q{"Recovered transaction state"}
end
subgraph WORK["Recovery action"]
 O["ONGOING\nclient may continue; timeout may abort"]
 C["PREPARE_COMMIT\nresume COMMIT markers"]
 B["PREPARE_ABORT\nresume ABORT markers"]
 D["COMPLETE state\nno unfinished marker fan-out"]
 E["All required markers complete\nappend COMPLETE state"]
end
A --> Q
Q --> O
Q --> C
Q --> B
Q --> D
C --> E
B --> E
${colors}
class A,Q txn
class O,C,B,D,E worker`),
 d('mental','The presentation mental model: map, journal, receipt','Use the same four questions for every failure: who owned it, which durable journal survives, what fences the old owner, and what proves completion?',`flowchart LR
subgraph MAP["1 — Map"]
 M["KRaft metadata\nwho owns each partition?"]
end
subgraph JOURNAL["2 — Journal"]
 J["User and internal logs\nwhat happened durably?"]
end
subgraph FENCE["3 — Fence"]
 F["Epochs + validation\nwhich owner is stale?"]
end
subgraph RECEIPT["4 — Receipt"]
 R["HW / marker / LSO / checkpoint\nwhat is actually complete?"]
end
M --> J --> F --> R
${colors}
class M control
class J store
class F group
class R worker`)
];
