export function walkthrough({section:s,table:t,figure:f,refs:r,code:c,details:d}) {
  return [
    s('start','Start here: the answer in one minute',`
      <p>A <strong>Kafka API</strong> is a versioned binary request/response contract. A <strong>Kafka role</strong> is the owner that handles it. A <strong>pod</strong> is a deployment/failure boundary. A <strong>thread</strong> is an execution boundary. These are four different things.</p>
      ${t(['Question','Answer'],[
        ['Does every component get a pod?','No. Kafka group and transaction coordinators are broker components. KafkaSource and KafkaSink are connector libraries instantiated inside Flink TaskManager processes.'],
        ['Are source, transform and sink in the same pod?','Sometimes. Chainable operators can run in one task/mailbox thread. Separate tasks may share a TaskManager JVM, or run on different TaskManagers/pods. Placement depends on the job graph and available slots.'],
        ['Does every record make an RPC?','No. Kafka batches records into Produce/Fetch requests. Chained Flink operators use direct calls; inter-task data uses buffered channels. Checkpoint/deployment RPCs are a separate control path.'],
        ['What survives a pod failure?','Replicated Kafka logs and durable Flink checkpoints, if configured and still available. An in-memory queue or producer buffer does not become durable because it lives in a pod.'],
        ['What provides source-to-sink exactly-once?','A recoverable source position + consistent operator state + recoverable/idempotent sink commit. Reprocessing can happen; externally committed effects must not be duplicated.']
      ])}
      <p>Reading route: placement → a record journey → broker threads → source/transform/sink threads → checkpoint/commit → failure cases → presentation mental model. Keep the <a href="kafka-rpc-atlas.html">complete Kafka RPC atlas</a> open as a lookup tab.</p>
      ${r('k-roles','k-broker','f-source','f-chain','f-reader','f-commit')}`),
    s('placement','1. From cluster to pod to task to operator',`
      ${f('rpc-placement')}
      ${t(['Layer','Lives where','What it owns'],[
        ['Kafka broker','Usually one broker process per Kubernetes pod, or a process on a VM','Many topic-partition replicas, listeners, request handlers, caches and broker coordinator services. A broker can lead P0 and follow P1 at the same time.'],
        ['KRaft controller','Dedicated controller processes, or combined broker/controller roles when configured','Cluster metadata decisions: registrations, topic/partition assignments, leadership and feature levels. A quorum replicates the metadata log; it is not the payload path for each application record.'],
        ['Flink JobManager','A process/container; deployment mode determines job and cluster lifecycle','Dispatcher/ResourceManager and per-job JobMaster/scheduler. SourceCoordinator contains the connector enumerator. User source-enumerator code can run here; per-record transformations normally run in TaskManagers.'],
        ['Flink TaskManager','Worker JVM, usually one per worker pod','TaskExecutor control endpoint, tasks/mailboxes, managed memory, network buffers, state backends and connector client threads.'],
        ['Slot','Scheduling/resource allocation inside a TaskManager','Not a process, CPU core or thread. Slot sharing can place several tasks from the same job into one slot; it does not make those tasks one operator chain.'],
        ['Task / subtask / chain','A deployed parallel instance in a TaskManager','A chain of synchronous operators typically executes in one StreamTask thread. A task can also have fetcher, producer, network and async-snapshot helper threads.'],
        ['Container / pod / Kubernetes node','Infrastructure around the above','A pod can have sidecars and multiple containers; a node can host several pods. “Different pod” is not automatically “different failure domain.”']
      ])}
      <p>Example, not a cluster observation: input topic has four partitions; KafkaSource parallelism is two; parse is chained with each source; keyed aggregate and sink each have parallelism two. Each source reader can own two partitions. Two TaskManagers with two slots each provide a possible placement, not a promise that each operator gets a dedicated slot or pod. Scheduler decisions, chaining, slot-sharing groups, resource profiles and anti-affinity determine the actual result.</p>
      <p>The connector JAR must be available to the user-code classloader where its coordinator and reader/writer code run. Adding a Kafka connector does not launch Kafka Connect workers. Kafka Connect and Kafka Streams are different runtimes: Connect has connector/task worker processes and internal topics; Streams runs inside application instances and uses Kafka for repartition/changelog topics.</p>
      ${r('k-roles','k-broker','f-enum','f-source','f-chain')}`),
    s('journey','2. Follow order-7 from produce to consume to output',`
      <p>Illustrative record: key <code>customer-7</code>, value <code>{orderId: 7, amount: 50}</code>, input partition P0, offset 104. Offsets are scoped to a topic partition, not globally unique record IDs. An output record receives its own offset; it does not inherit 104.</p>
      ${t(['Step','Execution / message','Result'],[
        ['1. Application send','KafkaProducer.send serializes key/value, selects a partition and appends to RecordAccumulator. Sender assembles batches and performs broker I/O.','send returning a Future is not a durable broker acknowledgement. Buffer pressure or metadata lookup can also block the calling thread.'],
        ['2. Locate owners','Bootstrap → ApiVersions / Metadata; use advertised broker addresses for the partition leader.','bootstrap.servers is an entry list, not a proxy traversed for every request.'],
        ['3. Append and replicate','Produce to P0 leader → authorization/validation → append → followers Fetch from leader.','With acks=all, completion waits for the required replicated position subject to ISR/min-ISR policy.'],
        ['4. Source fetch','KafkaSource reader’s consumer Fetches input partitions at their assigned positions.','The response is record batches with offsets/metadata, not calls to the transform operator.'],
        ['5. Emit and transform','Fetcher queue → mailbox → KafkaRecordEmitter → source output → map/keyed processing.','After successful deserialize/emit, split state advances to 105. A prefetched but un-emitted record is not yet part of this source state.'],
        ['6. Write output','KafkaWriter serializes output → KafkaProducer accumulator → Sender → output partition leader.','In exactly-once mode the output belongs to an open transaction, not yet visible to read_committed readers.'],
        ['7. Complete CP42','Flink persists source position 105, relevant operator state and sink recovery/committable state.','CP42 is the recovery boundary. Source-offset bookkeeping in Kafka is separate.'],
        ['8. Publish transaction','Sink committer commits T42 → coordinator decision → markers → eligible LSO advance.','Downstream read_committed consumers can see the committed output. Their own processing/offset commit is another boundary.']
      ])}
      ${f('rpc-bootstrap')}
      <p>DNS, TLS certificate names, SASL credentials, ACLs and network policies must work for <em>all advertised owners</em>, not only the bootstrap endpoint. Kafka’s ordinary protocol is binary over TCP (optionally TLS/SASL), not HTTP/gRPC. A schema registry, if used for payload serialization, is a separate service and protocol.</p>
      ${r('k-producer','k-produce','k-fetch','f-emit','f-writer','k-endtxn')}`),
    s('broker','3. Zoom into Kafka: server, queues, threads and log files',`
      ${f('rpc-broker-threads')}
      ${t(['Component / module','Work and thread boundary','Why it matters'],[
        ['clients: KafkaProducer / Sender / NetworkClient','Application-side buffering; Sender owns producer network progress and completes callbacks.','A producer is not a separate broker. Slow callbacks can stall producer network progress.'],
        ['core: SocketServer.Acceptor / Processor','Acceptor distributes new connections; Processor uses a selector, reads complete frames, queues requests and writes responses.','One network thread multiplexes many connections; not one pod or dedicated thread per client.'],
        ['server/core: RequestChannel / KafkaRequestHandler / KafkaApis','Handler pool dequeues and dispatches by API key. Requests may complete asynchronously through callbacks.','A request waiting for replication need not occupy a handler for the entire wait. Queues can still saturate.'],
        ['core/storage: ReplicaManager / Partition / UnifiedLog / LogSegment','Validate leadership, epochs, record batches, producer sequence and ISR; append to partition logs.','A topic has multiple partition logs across brokers; storage placement is not the same thing as consumer group ownership.'],
        ['Replica fetcher threads','Follower brokers request leader data with Fetch and append their own copies.','Replication is pull-based. Replication factor is not a count of application consumers.'],
        ['Group coordinator / transaction coordinator','Ownership follows partitions of the respective internal topics. Components may coexist with data partition leaders on one broker.','FindCoordinator resolves an owner for a key; moving ownership requires loading durable state and fencing stale incarnations.'],
        ['metadata / raft: QuorumController / KafkaRaftClient','Controller decisions are serialized and replicated through the KRaft metadata quorum.','Metadata quorum majority is a different rule from user-partition ISR acknowledgement.']
      ])}
      ${f('rpc-produce-replicate')}
      <p>Partition storage includes append-only <code>.log</code> segments plus offset/time indexes, transaction indexes and producer/epoch recovery metadata. The OS page cache is part of the hot path. Appending, replicating, flushing and making a transaction visible are separate events. <code>acks=all</code> is not a promise that every replica has performed a physical disk fsync for that batch.</p>
      <p>Example: RF=3 and min.insync.replicas=2. If all three replicas are in the relevant ISR, acks=all is not simply “wait for any two”; it follows the ISR/high-watermark completion rule. If one follower falls out and ISR remains sufficient, writes can continue. If ISR falls below the minimum, safe writes are rejected or fail—even if the leader process is alive. Current eligible-leader/ISR details are version- and feature-dependent.</p>
      ${r('k-socket','k-handler','k-produce','k-fetch','k-isr','k-delayed','k-log','k-lso','t-isr')}`),
    s('source','4. KafkaSource: control plane and data plane are different',`
      ${t(['Path','Concrete components','Calls / state'],[
        ['Coordinator path in JobManager','SourceCoordinator → KafkaSourceEnumerator → AdminClient','Discover topic partitions and resolve initial offsets via async work; serialize state changes/assignments on coordinator thread; deliver AddSplitEvent to readers.'],
        ['Task control path','TaskExecutor operator-event gateway → StreamTask mailbox → SourceOperator','Deserialize assigned splits and call sourceReader.addSplits. Coordinator events are control messages, not Kafka record payloads.'],
        ['Fetcher path in TaskManager','KafkaSourceFetcherManager → SplitFetcher → KafkaPartitionSplitReader','Own KafkaConsumer API calls, explicitly assign partitions, seek start offsets, poll and enqueue RecordsWithSplitIds.'],
        ['Mailbox data path','SourceReaderBase.pollNext → KafkaRecordEmitter.emitRecord','Deserialize/emit records and mutate KafkaPartitionSplitState.currentOffset. This is the state later snapshotted.'],
        ['Snapshot / restore','SourceOperator → KafkaSourceReader.snapshotState → KafkaPartitionSplit','Persist next offsets in Flink state. Restore/seek from checkpointed split offsets; initializer settings are not a replacement for existing restored state.'],
        ['External group offsets','notifyCheckpointComplete → fetcher manager → consumer.commitAsync','Optional Kafka OffsetCommit after checkpoint completion. Failure affects monitoring/interoperability, not the source recovery position stored by Flink.']
      ])}
      ${f('rpc-source-threads')}
      ${d('Does group.id mean Kafka’s group coordinator assigns the Flink source partitions?',`<p>No for this ordinary FLIP-27 KafkaSource. The enumerator assigns Flink splits and the reader calls <code>consumer.assign(...)</code>, not subscription-based group assignment. Do not draw JoinGroup/SyncGroup/Heartbeat as the source’s mandatory assignment loop. A group ID can still be used for initial committed-offset lookup and external offset commits.</p><p>Two independently deployed Flink jobs using the same group ID do not thereby become one coordinated split-assignment job. They can read the same partitions and overwrite the same external group offsets. Give jobs clear identities; let Flink own assignment within each job.</p>`)}
      ${d('Does KafkaConsumer have its own network thread?',`<p>The connector-level invariant is single-threaded consumer API ownership by the split fetcher. In the inspected Kafka client, ConsumerDelegateCreator chooses ClassicKafkaConsumer or AsyncKafkaConsumer according to group.protocol. Classic polling drives much of the network work on the calling path; the asynchronous consumer has a dedicated internal network thread. This implementation choice does not turn the connector into a separate pod or move operator state mutation out of the Flink mailbox.</p>`)}
      <p>Parallelism: with four partitions, at most four ordinary source readers can simultaneously own nonempty distinct partition splits; extra readers can be idle. A reader can multiplex many partitions. Partition discovery, rescaling and checkpoint restore change assignment. Reading more records into a client/queue can improve throughput but does not advance the durable recovery offset until they are emitted and checkpointed.</p>
      ${r('f-enum','f-source','f-assign','f-poll','f-base','f-emit','f-reader','f-offsetcommit','f-fetchcommit','k-consumer','t-offset','t-disabled')}`),
    s('transform','5. Transform and sink: local calls, local channels or network',`
      ${f('rpc-locality')}
      ${c(`Illustrative DataStream topology, not a compiled deployment recipe:
KafkaSource (parallelism 2)
  -> map(parse)                 [can chain with source]
  -> keyBy(customerId)          [hash partitioning; breaks direct chaining]
  -> keyed aggregate           [state belongs to its key groups]
  -> KafkaSink EXACTLY_ONCE     [writer + committing runtime topology]`)}
      <p>For a chain, ChainingOutput invokes the next operator’s record processor. StreamMap.processElement calls the user function. No Kafka serialization or network hop is needed between those operators; Flink may copy objects depending on reuse configuration. A blocking synchronous function stalls the shared mailbox and therefore its chained neighbors.</p>
      <p>At an unchained edge, RecordWriter serializes Flink records into network buffers and result subpartitions. LocalInputChannel consumes a co-located task’s result without remote TCP. RemoteInputChannel requests subpartitions through a Netty connection to the peer TaskManager. Credit-based flow control prevents a fast sender from unlimited buffering at a slow receiver. Same pod does not necessarily mean same chain; same slot does not mean same thread.</p>
      <p>KafkaSink is another client library. KafkaWriter.write runs on the owning task thread, serializes the value and calls producer.send. The producer Sender does socket I/O and invokes callbacks; the writer records async errors and surfaces them back to the mailbox/flush path. Each parallel writer can have producer resources, and exactly-once writers may have multiple in-flight transactional producers. “One sink” is not necessarily one TCP connection or one producer instance.</p>
      <p>Sink V2 expands a committing sink into writer/committer runtime operators. They may be chained/co-located when the topology allows; they are not independent external services by definition. The committer runs in its task mailbox and commitTransaction can wait for Kafka’s producer network thread. Connector backchannels used to recycle producer resources are local implementation mechanisms, not durable recovery storage.</p>
      ${r('f-chain','f-map','f-local','f-remote','f-writer','f-prepare','f-committerop','f-commit')}`),
    s('messages','6. Identify the protocol before explaining an arrow',`
      ${t(['Message boundary','Request / payload','Response / completion'],[
        ['Kafka TCP wire','4-byte length + versioned RequestHeader + API body. Produce carries record batches; Fetch carries offsets/limits/isolation.','Length + ResponseHeader + matching versioned body; correlate by correlation ID. Errors can be per partition. acks=0 Produce has no normal response.'],
        ['Flink submitTask RPC','TaskDeploymentDescriptor, JobMasterId, timeout → TaskExecutorGateway','CompletableFuture<Acknowledge>; successful submission is not proof that processing will never fail.'],
        ['Flink operator event','ExecutionAttemptID, OperatorID, SerializedValue<OperatorEvent>; e.g. AddSplitEvent','Delivery/dispatch acknowledgement and mailbox execution. Splits are control state, not records.'],
        ['Flink triggerCheckpoint RPC','ExecutionAttemptID, checkpointID, timestamp, CheckpointOptions','Acknowledge of trigger acceptance. This is not the durable task-state ACK.'],
        ['Flink CheckpointBarrier','Checkpoint ID, timestamp and options through the same data-channel ordering domain','Downstream alignment/snapshot; not a separate Kafka topic and not a Kafka RPC.'],
        ['Flink acknowledgeCheckpoint RPC','JobID, attempt ID, checkpoint ID, CheckpointMetrics, serialized TaskStateSnapshot (state handles)','Coordinator aggregates required ACKs; completed checkpoint is persisted before notifications. Bulk state bytes normally went to checkpoint storage.'],
        ['Flink confirmCheckpoint RPC','Task attempt, completed checkpoint ID/timestamp, last subsumed checkpoint ID','Task schedules notifyCheckpointComplete callbacks. Sink commits and optional source offset commit are separate consequences.'],
        ['Flink task data transport','Result partition/subpartition requests, serialized buffers/events and credits','Local channel or Netty protocol; not Pekko RPC per row.'],
        ['Object storage / sink service','Backend-specific filesystem/HTTP calls for checkpoint objects or sink data','Storage-specific durability/commit semantics. HTTP 200 on one data object is not necessarily a committed table snapshot.']
      ])}
      <p>Flink control gateways here are typed Java RPC interfaces backed by the runtime’s Pekko RPC service. They are not Kafka’s API keys or a stable cross-version public binary schema. Client REST submission, internal control RPC, inter-task data exchange, and Kafka client traffic require different endpoints and diagnostics.</p>
      <p>Illustrative ports often seen in examples are Kafka 9092 and Flink REST 8081, but do not use those as a firewall specification. Listener configuration, advertised.listeners, TLS/SASL listeners, JobManager/TaskManager RPC and data port settings determine the actual network map. Verify the running configuration.</p>
      ${r('k-headers','f-taskrpc','f-ackrpc','f-barrier','f-remote')}`),
    s('checkpoint','7. Exactly-once is a recovery protocol, not a packet flag',`
      ${f('rpc-checkpoint')}
      <p>At CP42 the mailbox establishes a consistent cut. The task’s pre-barrier hooks flush/prepare sink work, the barrier is broadcast, operator snapshots are initiated, and asynchronous snapshot futures are finalized. For aligned checkpoints, multi-input tasks wait for the matching barriers without letting post-barrier data overtake the cut. Unaligned checkpoints can include in-flight channel state instead; they do not eliminate snapshot I/O or sink commit work.</p>
      <p>Only after required task and coordinator state is available and the completed checkpoint is stored does Flink issue completion notifications. Checkpoint files must be on durable storage reachable by replacement processes, with appropriate HA metadata/recovery configuration. A local temporary directory inside a lost pod is not sufficient for cluster recovery.</p>
      ${c(`Conceptual CP42 contents (not the exact serialized class layout):
source:       orders/P0 -> next offset 105
operator:     customer-7 total -> 250 at the same logical cut
sink:         recoverable transaction/committable identity T42
coordinator:  source split-assignment state
optional:     channel state for an unaligned checkpoint`)}
      ${f('rpc-transaction-visibility')}
      <p>There are two different after-checkpoint actions: the sink commits output T42, and the source may commit offset 105 to __consumer_offsets. Standard KafkaSource → KafkaSink does <strong>not</strong> call sendOffsetsToTransaction to place both actions in one Kafka transaction. Flink’s durable checkpoint plus sink recovery protocol is the cross-system link.</p>
      <p>Kafka’s transaction coordinator persists its decision in __transaction_state and sends WriteTxnMarkers to participant partition leaders. An EndTxn success response does not mean every marker has finished replicating. read_committed Fetch is bounded by the last stable offset; an earlier open transaction can hold visibility back. Aborted transaction batches remain log history but are filtered from read_committed application delivery. read_uncommitted can expose transactional records before their final outcome.</p>
      <p>Version caveat: older transaction sequences explicitly register partitions via AddPartitionsToTxn; newer transaction protocol versions integrate more partition registration/verification into Produce. Neither sequence implies that every application record causes its own AddPartitionsToTxn RPC. Use the catalog’s versioned schemas and the negotiated client/broker feature level.</p>
      ${r('f-barrier','f-async','f-complete','f-sinkop','f-prepare','f-committerop','f-commit','f-offsetcommit','k-endtxn','k-lso')}`),
    s('storage','8. Put every piece of state in the correct store',`
      ${f('rpc-storage')}
      ${t(['State','Primary owner / durable location','Recovery / caveat'],[
        ['Input/output events','Kafka user-topic partition replica logs','Replay retained input after restore. Topic retention/deletion can remove required recovery history.'],
        ['Consumer group offsets/membership','Group coordinator; __consumer_offsets','External progress and group protocol state. Not the authoritative Flink checkpoint source position.'],
        ['Transaction decisions','Transaction coordinator; __transaction_state','Reload coordinator state and finish/retry decisions and markers. Transaction timeout and producer fencing still matter.'],
        ['Commit/abort markers + producer sequence state','Participant partition logs plus local recovery metadata','Detect duplicate producer retries and compute transactional visibility. Idempotent producer identity is not a global business-key deduplication database.'],
        ['Cluster metadata','KRaft metadata log and snapshots','Broker/controller registrations, topic/partition assignments and leadership. Separate from user record logs and Flink checkpoints.'],
        ['Live keyed/operator state','TaskManager configured state backend: e.g. heap or RocksDB working state','Low-latency local access; durable snapshots/changelog mechanisms depend on backend. Losing local files is recoverable only with usable remote state.'],
        ['Flink source splits and sink committables','Checkpoint state + completed checkpoint metadata/handles','Restore to the same logical cut, then recover sink commits. JobManager is not the sole holder of all state bytes.'],
        ['Kafka Streams local stores','Streams application plus changelog/repartition topics when configured','Different runtime from Flink; do not draw an automatic Kafka changelog topic for every Flink keyed state.'],
        ['Kafka Connect metadata','Distributed worker config, offset and status topics','Different runtime from KafkaSource. Source/sink task delivery depends on the connector and mode.'],
        ['Share delivery state','Share coordinator; share state internal storage','Different acknowledgement model from ordinary KafkaSource offsets. This chapter does not apply share-group transactional designs to the normal source.']
      ])}
      ${r('k-log','k-lso','k-endtxn','f-reader','f-prepare')}`),
    s('failures','9. Failure cases: what fails, what survives, what gets retried',`
      ${f('rpc-recovery')}
      ${t(['Failure point','Recovery / distributed response','Boundary or risk'],[
        ['Produce reply lost after append','Client retries; idempotent producer identity/epoch/sequence suppresses applicable duplicate retries.','A timeout is ambiguous. Non-idempotent retries or resending the business event using a new identity can duplicate it.'],
        ['Input partition leader dies','Controller chooses an eligible leader; clients refresh metadata/epochs and reconnect.','Availability depends on surviving replicas and election policy. Unclean election trades availability for possible data loss.'],
        ['ISR drops below minimum','acks=all writes fail/wait according to the request and ISR state; replicas must catch up or policy must change.','Lowering minimum ISR is a durability tradeoff, not a harmless timeout fix.'],
        ['One controller fails','A healthy metadata quorum can elect/continue with a majority.','Losing the quorum blocks metadata decisions. Existing data service may continue temporarily, but failover/control operations are impaired; do not promise indefinite availability.'],
        ['Source reader dies before CP42 completes','Restart affected execution region/job according to strategy; restore last completed checkpoint CP41 and replay retained input.','Fetched/emitted records beyond CP41 can be processed again. Exactly-once does not mean the map function runs once.'],
        ['Transform throws / deserialization fails','Fail task and recover according to restart strategy unless application explicitly routes the error.','A deterministic poison record can fail every retry. Dropping it or using a dead-letter path changes application semantics and must be designed.'],
        ['Sink write fails before snapshot ACK','Async producer failure reaches writer/flush; checkpoint cannot safely certify that sink work.','Do not swallow producer callbacks or mark a failed write as successful.'],
        ['Crash after output write but before checkpoint completion','Restore prior completed cut; abort/fence lingering uncommitted work as supported; replay input.','read_committed hides abandoned transactions. At-least-once nontransactional output may already be visible and can duplicate.'],
        ['Crash after checkpoint completion before commit finishes','Restore pending committables and retry/recover the same transaction.','Requires retained checkpoint and viable transaction state. Fatal/expired/fenced transactions are not magically converted into successful commits.'],
        ['Checkpoint storage unavailable','Checkpoint attempts fail/time out; processing may continue until configured failure tolerance/restart policy intervenes.','Exactly-once sink visibility can lag, state/transactions accumulate and recovery age grows.'],
        ['Transaction times out during long outage','Kafka may abort; recovered committer can encounter fencing/invalid state and fail.','Size transaction timeout for checkpoint duration plus expected recovery window, within broker limits; no finite timeout tolerates an unlimited outage.'],
        ['Optional source OffsetCommit fails after CP42','Log/metric records failure; Flink still restores from CP42 split state.','External lag/group offsets may be stale. Do not rewind checkpoint recovery to that stale external offset.'],
        ['TaskManager pod/node disappears','Heartbeat loss → scheduler failover → replacement slots → restore state.','All tasks in that process fail together; multiple pods on one node can fail together. Spare capacity and state-transfer speed determine recovery latency.'],
        ['JobManager process disappears','With HA, new leader retrieves job/checkpoint metadata, fences old leadership and recovers tasks.','Kubernetes restarting a container alone does not guarantee that durable HA metadata and checkpoints were configured.'],
        ['Input retention expires before restore','Checkpoint points before available log start; initialization/reset policy or explicit failure applies.','Cannot reconstruct deleted history. Silent offset reset can lose business data.'],
        ['Slow downstream sink / hot key','Output buffers fill, credits disappear, mailbox/fetch progress slows, lag grows upstream.','This is backpressure, not necessarily network failure. More source parallelism cannot split one indivisible hot key’s state update.']
      ])}
      ${r('k-isr','k-lso','k-endtxn','f-offsetcommit','f-commit','t-retry','t-recovery')}`),
    s('tradeoffs','10. Throughput, latency, durability and availability tradeoffs',`
      ${t(['Design lever','Benefit','Cost / diagnostic question'],[
        ['Larger producer batches / linger / compression','Fewer requests, better compression and amortized I/O.','More waiting/memory/CPU; distinguish client enqueue latency from broker ACK latency and committed visibility latency.'],
        ['Fetch min bytes / max wait / batch size','Amortize request overhead and move more bytes per fetch.','Low-volume streams may wait longer; large batches increase queue memory and per-turn work.'],
        ['More topic partitions / Flink parallelism','More independent leaders/readers/key groups and greater aggregate capacity.','More metadata, connections, files and state redistribution. Small keys/partitions can skew; too many idle tasks add overhead.'],
        ['Operator chaining','Avoid network serialization, queues and scheduling between compatible operators.','Shared blocking/CPU/failure behavior; fewer independent scaling boundaries.'],
        ['Separate tasks/pods','Independent resource placement and some failure isolation.','Serialization, network buffers, RTT and operational cost. More pods do not automatically create more useful parallel work.'],
        ['Higher replication / stricter ISR','More resilience to replica loss and stronger acknowledgement requirements.','More network/disk work and potentially lower write availability when replicas lag.'],
        ['Short checkpoint interval','Younger recovery point and often earlier transactional output publication.','More state I/O, coordination and transactions. Under load, duration/alignment may dominate the configured interval.'],
        ['Unaligned checkpoints','Can reduce alignment delay under backpressure.','Snapshots include in-flight channel data; more checkpoint/storage/recovery I/O. Does not make a slow sink fast.'],
        ['Larger queues/buffers','Absorb bursts and improve batching.','More memory, tail latency and volatile in-flight work. Buffers delay overload; they do not fix sustained arrival rate above service rate.'],
        ['Local embedded state vs remote checkpoint storage','Local state is fast; remote durable state makes worker replacement possible.','Serialization/upload/download overhead and recovery duration; incremental snapshots reduce some transfer, not all metadata work.'],
        ['Exactly-once transactional sink','Hide aborted/replayed attempts from read_committed readers.','Commit coordination, timeout management and visibility delay. At-least-once + business-key deduplication is a different design with different costs.']
      ])}
      ${c(`Analytical model — estimates, not benchmark results:
visible latency ≈ source wait + compute + queues + sink batching
                + wait for checkpoint completion + transaction visibility delay
recovery time  ≈ failure detection + scheduling + state restore + replay/catch-up
stable system requires sustained arrival rate < bottleneck service rate
replay work depends on retained input after the last completed checkpoint`)}
      <p>Measure each term. Useful views include producer queue/request latency and retries; broker request queue/local/remote time and under-replicated partitions; Flink busy/backpressured/idle time, checkpoint duration/alignment/state size and failed checkpoints; source lag and sink errors. Metric names/labels vary by version and reporter. No numeric throughput or latency guarantee was measured for this chapter.</p>`),
    s('sink-contract','11. What changes if the sink is not Kafka?',`
      ${t(['Sink contract','What must be coordinated','What cannot be assumed'],[
        ['Kafka transactional sink','Checkpointed committable → transaction decision → partition markers → read_committed visibility.','The output transaction does not atomically include Flink source group offsets.'],
        ['Transactional database connector','Checkpoint-linked transaction/precommit handle and recoverable commit, subject to database/connector support.','A JDBC insert acknowledged once is not by itself end-to-end exactly-once.'],
        ['Idempotent upsert/deduplicating sink','Stable business key or event ID with well-defined update/dedup semantics.','Producer idempotence does not deduplicate arbitrary HTTP side effects or repeated application-generated events.'],
        ['Lakehouse table sink (e.g. Iceberg)','Write data/delete files, then publish table metadata/snapshot using the connector’s commit protocol.','Object file upload is not the table commit point; abandoned files and commit retries need handling.'],
        ['Arbitrary HTTP/email side effect in map','External idempotency key/outbox/transactional design if duplicates matter.','Flink checkpoint rollback cannot unsend an email or undo an uncoordinated external call.']
      ])}
      <p>For table commit detail, continue with <a href="iceberg-storage-commits.html">Iceberg storage and commits</a>. For broad guarantee comparisons, see <a href="06-delivery-and-recovery.html">delivery and recovery</a>. This chapter makes exact connector-code claims for KafkaSink; the other rows are design contracts, not validation of an installed database/lakehouse connector.</p>`),
    s('verify-placement','12. How to check the real deployment instead of guessing',`
      ${t(['Question to investigate','Read-only evidence to collect'],[
        ['Which processes are in which pods?','Kubernetes pod/container listing, node placement, command/args, controller owner and image versions. Do not copy environment variables or secrets into notes.'],
        ['Which operators share a task?','Flink Web UI job graph, chained operator labels, subtask-to-TaskManager mapping and actual job plan. Source and sink parallelism alone do not show placement.'],
        ['Which thread is busy?','TaskManager thread dump: task/mailbox threads, SplitFetcher, kafka-producer-network-thread and Netty threads. Names are diagnostic hints, not a stable ABI.'],
        ['Where does each connection go?','Advertised Kafka metadata and FindCoordinator result; Flink RPC/data configuration; network policy and socket telemetry. Bootstrap success is insufficient.'],
        ['What is durable?','Last completed checkpoint and storage location/retention, HA metadata, broker replica/ISR state, persistent volume/failure-domain placement.'],
        ['Why is output not visible?','Checkpoint completion, pending committables, transaction outcome/timeout, output partition LSO and downstream isolation.level. Distinguish “not produced” from “produced but not committed.”']
      ])}
      <p>No Kubernetes cluster, running job, pod layout, network capture or fault-injection experiment was inspected for this addition. Pod diagrams are explicitly illustrative. Implementation behavior is source-backed at the pinned revisions below, not a report of your deployed cluster.</p>`),
    s('evidence','13. Source trail, code excerpts and validation limits',`
      <p>The source and test links below are pinned to the inspected commits. Expand a component to see its class/module, exact file and line-numbered excerpt. These are three separately inspected repository revisions: the connector POM declares a Flink 2.2.1 / kafka-clients 4.2.0 baseline, so the Kafka HEAD schema catalog is not a claim that every API/version is emitted by that connector client or enabled by every cluster.</p>
      <p>Tests were read/located as evidence, not executed against Kafka/Flink for this documentation change. The documentation generator, schema completeness, internal links, diagrams and browser behavior are validated separately. Full API schema data and Apache attribution are included in the <a href="kafka-rpc-atlas.html">RPC atlas</a>.</p>
      <div id="source-evidence"></div>`),
    s('mental-model','14. Presentation mental model: five questions and three planes',`
      ${f('rpc-mental-model')}
      ${t(['Remember','Say this during the presentation'],[
        ['Three planes','Data plane moves records; control plane assigns ownership and initiates checkpoints; durability plane stores logs/checkpoints/decisions.'],
        ['Three Flink boundaries','Same chain: function call. Different task, same worker: local buffered channel. Different workers: network data channel. Control RPC is separate.'],
        ['Three Kafka owners','Partition leader owns a log; group coordinator owns group progress; transaction coordinator owns transaction decisions. They can be different brokers or coexist in one process.'],
        ['Three milestones','Produced is not replicated; replicated is not transactionally visible; visible is not downstream processing completed.'],
        ['Two offset homes','Flink checkpoint offset is the recovery bookmark. Kafka group offset is external group progress; in ordinary KafkaSource its post-checkpoint commit is bookkeeping.'],
        ['One failure question','If this process disappears now, what durable fact tells its replacement what to replay and what not to publish twice?']
      ])}
      ${d('A 90-second closing script',`<p>“Kafka is a set of partitioned replicated logs. Clients discover owners and speak a versioned batched protocol directly to those owners. A broker contains many threads and coordinator components; it is not one pod per RPC.</p><p>Flink embeds Kafka clients inside its TaskManagers. The source fetcher talks to Kafka, the task mailbox emits and transforms records, and the sink’s producer sends output. Chained operators call each other locally; only task boundaries introduce buffered data exchange.</p><p>The checkpoint stores a consistent recovery bookmark: source positions, operator state and sink intent. After completion, the sink resolves its transaction. Kafka’s decision and markers control when readers see it. If a process dies, replay is normal; the protocol prevents replay from becoming duplicate committed effects.</p><p>For every diagram arrow, ask: who owns it, which thread runs it, what message crosses the boundary, what durable fact survives, and when does the result become visible?”</p>`)}
      <p>Fast lookup for audience questions: <a href="kafka-rpc-atlas.html#rpc-0">Produce</a> · <a href="kafka-rpc-atlas.html#rpc-1">Fetch</a> · <a href="kafka-rpc-atlas.html#rpc-10">FindCoordinator</a> · <a href="kafka-rpc-atlas.html#rpc-26">EndTxn</a> · <a href="kafka-rpc-atlas.html#rpc-27">WriteTxnMarkers</a> · <a href="kafka-rpc-atlas.html#rpc-8">OffsetCommit</a>.</p>`)
  ];
}
