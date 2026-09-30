export function chapters({section,table,figure,refs,code,details}){
 return [
 {file:'spark-distributed-execution.html',number:25,title:'Spark: driver, executors and Kafka reads',intro:'Follow a Kafka record through driver planning, parallel executor tasks, shuffle, state and output. Every process and networking boundary is tied to the Spark checkout you supplied.',sections:[
 section('summary','Start with the process map',`
 <p>One driver owns the streaming query’s progress. Several executor JVMs run the work the driver schedules. Kafka brokers retain the input, checkpoint storage retains recovery information, and the sink retains output. These systems communicate through different protocols and commit different kinds of state.</p>
 ${table(['Component','Runs where','Responsibility'],[
 ['StreamExecution / MicroBatchExecution','Driver query execution thread','Choose each batch’s offset vector, plan work, execute the sink and publish batch completion'],
 ['DAGScheduler / TaskSchedulerImpl','Driver scheduling machinery','Turn the physical plan into stages and task attempts; place tasks on available executor resources'],
 ['Executor / TaskRunner','Many executor JVMs and task-pool threads','Read assigned ranges and execute transformations, shuffle, state and sink writes'],
 ['Kafka consumer / producer','Inside executor tasks or their client pools','Fetch input directly from brokers and send output directly to the destination Kafka brokers'],
 ['StateStore provider','Executor-local working state plus shared checkpoint storage','Load a specific state version and persist the next state version for each operator partition'],
 ['Cluster manager / deployment','Kubernetes, YARN or another configured deployment','Provide executor processes/resources and the driver restart policy; this is separate from query checkpointing']])}
 ${figure('spark-deep-deployment')}
 <p>Diagrams use three executors as an example. Executor placement can change between task attempts; a task box is not a permanent Kafka owner or a separate pod. A Kubernetes executor pod commonly contains one executor JVM with multiple task slots and client instances.</p>
 <p>This chapter follows the ordinary synchronous microbatch path with the V2 Kafka reader, which is used by default in the inspected test. A configurable V1 fallback uses <code>KafkaSource</code> and <code>KafkaSourceRDD</code>. Both use Spark checkpoint progress. <code>KafkaRDD</code> belongs to legacy DStreams, not this fallback. Continuous processing, asynchronous progress tracking and <code>RealTimeTrigger</code> have separate timing; see the mode comparison in <a href="spark-checkpoint-failure-recovery.html">the recovery chapter</a>.</p>
 ${refs('spk-query-thread','spk-v2-choice','spk-test-v2-default','spk-dag-tasks','spk-task-thread')}`),
 section('worked-batch','One batch, four Kafka partitions, three executors',`
 <p>Use batch 42 throughout the examples. Batch 41 completed with next offsets <code>{P0:120, P1:200, P2:90, P3:50}</code>. The driver chooses batch-42 end offsets <code>{P0:180, P1:240, P2:115, P3:70}</code>. Each range includes its start and excludes its end.</p>
 ${figure('spark-deep-ranges')}
 ${table(['Input range','Example task placement','What persists for recovery'],[
 ['orders-0 [120,180)','Task A on E1','The source’s start/end vector and required state version; E1’s identity is not the resume cursor'],
 ['orders-1 [200,240)','Task B on E1 in another slot','Its partition boundary in the same batch offset vector'],
 ['orders-2 [90,115)','Task C on E2','The same range can be retried on E3 or a replacement executor'],
 ['orders-3 [50,70)','Task D on E3','Completion belongs to the batch, not a Kafka commit from E3']])}
 <p>With the ordinary unsplit scan there is one source task per nonempty Kafka partition. <code>minPartitions</code> and <code>maxRecordsPerPartition</code> can produce several disjoint input ranges from one Kafka partition. If eight tasks are planned and the cluster has six available task slots, some tasks wait for a slot. Adding executors does not change the durable input offsets or create additional Kafka partitions.</p>
 <p>Keep four counts separate: Kafka partitions, planned source ranges, shuffle/state partitions, and executor task slots. An offset span is also not always a user-record count: control records, compaction and transactions can create gaps or hide records under <code>read_committed</code>.</p>
 ${figure('spark-deep-batch')}
 <p>Within the ordinary query loop, batch 42 runs before batch 43 is constructed for execution. Parallel tasks inside batch 42 provide concurrency; they do not turn that loop into independent per-executor checkpoint streams. The planned offset log is written before distributed work, and the completion log follows successful sink execution.</p>
 ${refs('spk-source-ranges','spk-range-splitting','spk-task-offers','spk-batch-plan','spk-planned-log','spk-batch-complete','spk-test-ranges')}`),
 section('driver','Driver: source discovery, admission control and physical planning',`
 ${figure('spark-deep-calls')}
 ${table(['Step','Method / component','What actually happens'],[
 ['Create the query','StreamExecution.start / QueryExecutionThread','Start the driver-side stream loop. The application caller is not the executor task thread.'],
 ['Initialize source','KafkaMicroBatchStream.getOrCreateInitialPartitionOffsets','Read source initial metadata if present; otherwise apply startingOffsets/timestamp policy and persist the initial vector.'],
 ['Determine next end vector','MicroBatchExecution.constructNextBatch → KafkaMicroBatchStream.latestOffset','Discover current source offsets and apply read limits such as maxOffsetsPerTrigger.'],
 ['Discover Kafka boundaries','KafkaOffsetReaderAdmin → Admin.listOffsets','Driver contacts Kafka for metadata/offset boundaries; the deprecated consumer offset reader is a configuration-dependent alternative.'],
 ['Declare the batch','MicroBatchExecution.markMicroBatchStart → offsetLog.add','Atomically publish the chosen input end vector and batch timing/watermark metadata. This log stores boundaries, not copies of Kafka payloads.'],
 ['Plan the scan','KafkaMicroBatchStream.planInputPartitions','Resolve [start,end) ranges and construct KafkaBatchInputPartition objects with reader settings.'],
 ['Plan operators','MicroBatchExecution.runBatch / IncrementalExecution','Build the physical plan, exchanges, state information and sink execution for this batch.'],
 ['Schedule work','DAGScheduler → TaskSchedulerImpl','Create stage tasks and choose executor slots. Cluster resources and data locality affect placement.']])}
 <p>The driver does not collect every source record and then distribute it. It sends task descriptions that reference the required source range and physical work. Executors fetch the actual Kafka records. The V2 write path’s <code>nextBatch.collect()</code> in the runtime forces distributed write execution; it does not make the driver the Kafka data sink.</p>
 ${details('Fresh start, restart and newly discovered partitions',`<p><code>startingOffsets</code> selects a fresh source position. Once the source initial offset file and batch logs exist, restart reuses checkpoint progress. Changing that option does not reset the existing query. Source metadata such as <code>sources/0/0</code> and engine end-offset logs such as <code>offsets/42</code> serve different purposes.</p><p>Partition discovery can change the source offset vector. The offset reader resolves newly added and deleted partitions and applies data-loss policy where relevant; this is not executor group rebalancing. The inspected AvailableNow failure test covers an unfinished batch and a changed Kafka partition set. Preserve checkpoint compatibility when changing source subscriptions or query structure.</p>`)}
 ${refs('spk-query-thread','spk-initial-source','spk-source-end','spk-admin-offsets','spk-source-ranges','spk-planned-log','spk-state-version','spk-test-available-retry')}`),
 section('executor','Executor: reader, consumer pool, iterator and task thread',`
 ${table(['Layer','Low-level path','Ownership and behavior'],[
 ['Control receipt','CoarseGrainedExecutorBackend receives LaunchTask','Decode a TaskDescription and delegate to Executor; the RPC handling thread does not execute the whole query.'],
 ['Task execution','Executor.launchTask → TaskRunner → threadPool','A pool thread runs the task and its RDD/physical operator iterator. Several tasks can run in one JVM.'],
 ['Scan iterator','DataSourceRDD.compute → reader factory','Create the partition reader for the planned InputPartition, with task completion cleanup.'],
 ['Kafka range reader','KafkaBatchPartitionReader.next','Read within this range and project ConsumerRecord data into Spark rows. Its cursor advances as records are delivered.'],
 ['Consumer acquisition','KafkaDataConsumer.acquire','Borrow a client with pool key (group.id, TopicPartition); parameters are checked separately. Pool/fetched-data reuse avoids repeated setup.'],
 ['Network read','InternalKafkaConsumer.createConsumer → KafkaConsumer.assign; seek → poll','Assign a topic partition explicitly, seek to Spark’s requested offset and fetch from the Kafka leader.'],
 ['Reattempt','KafkaDataConsumer task-attempt handling','Invalidate failed-attempt cache state and acquire suitable client state for rereading the range.'],
 ['Finish','ExecutorBackend.statusUpdate(FINISHED, result)','Return task result/commit information to the driver; this is not a Kafka source OffsetCommit.']])}
 <p><code>assign</code> gives this reader explicit partition access. The Kafka consumer group coordinator does not distribute these Spark source tasks across executors. A <code>group.id</code> may identify clients or support Kafka authorization; it does not replace the Spark scheduler or checkpoint cursor.</p>
 <p>Pool reuse is an optimization with task-attempt rules. It does not mean one consumer is called concurrently by all tasks. The inspected consumer test specifically exercises several tasks reading different ranges of the same topic partition inside one executor, and the retry test checks invalidation/new-instance behavior.</p>
 <p>Narrow transformations can run in the same task iterator without another remote process hop. An exchange creates a new partitioning/stage boundary. Stateful operators and output tasks may then execute on different executors from the source scan.</p>
 ${refs('spk-executor-receive','spk-task-thread','spk-reader-create','spk-kafka-reader','spk-kafka-fetch','spk-kafka-assign','spk-consumer-key','spk-retry-consumer','spk-task-result','spk-test-consumer-retry','spk-test-multi-task-consumer')}`),
 section('networking','Network and RPC boundaries',`
 ${figure('spark-deep-network')}
 ${table(['Plane','Payload / calls','Failure handling'],[
 ['Spark control RPC','Executor registration, LaunchTask, StatusUpdate, heartbeats and coordinator messages over Spark RpcEnv/Netty','Scheduler detects lost executors/task results and retries work under configured limits.'],
 ['Kafka input/output protocol','Metadata, ListOffsets, Fetch, Produce and their responses over Kafka client connections','Kafka client refreshes broker/leader metadata and retries eligible requests. Spark retries tasks/batches separately.'],
 ['Shuffle block transfer','Serialized map output rows, fetched using shuffle client/Netty from executors or configured shuffle service','Missing blocks cause FetchFailed and stage/map-output recomputation.'],
 ['Checkpoint filesystem I/O','Offset/commit metadata files and state deltas/snapshots/changelogs','Storage errors can fail the task/query; recovery needs an intact compatible checkpoint.'],
 ['State coordination RPC','Active provider location and coordination metadata','Driver location memory helps placement; the checkpoint provides recoverable state contents.']])}
 <p>Kafka protocol calls are not Spark RPC messages. Spark task status is not a Kafka group offset acknowledgement. Shuffle files are not replicated Kafka logs. If a broker leader changes, executor clients recover the Kafka connection while keeping Spark’s requested range; if an executor disappears, Spark can schedule that range on another process.</p>
 <p>An external shuffle service can continue serving local shuffle files after an executor process exits. Losing the worker host/disk can still lose those blocks. Default microbatch recovery recomputes missing map outputs when needed; an executor heap or local shuffle directory is not a substitute for durable source retention and checkpoint storage.</p>
 ${refs('spk-launch-rpc','spk-executor-receive','spk-task-result','spk-shuffle-fetch','spk-fetch-failure','spk-state-coordinator','spk-test-shuffle-retry')}`),
 section('state-partitioning','Transform, shuffle, state and sink are separate stages of progress',`
 <p>For <code>groupBy(customerId).count()</code>, source partition ordering does not by itself put all records for a customer in one state partition. The physical plan exchanges rows using the configured key partitioning. The reducer/state tasks update the state for the keys they receive.</p>
 ${figure('spark-deep-shuffle')}
 <p>In this example the four source tasks feed two state partitions. Batch 42 loads state version 42 and persists version 43. The state-store identity uses checkpoint location, operator ID and state partition ID, with query-run provider identity and optional checkpoint IDs. State partition 0 is unrelated to Kafka partition 0.</p>
 <p><code>StateStoreRDD</code> asks for preferred locations where a state provider is already loaded. That can reduce cold-load latency, but placement is not permanent. A task moved from E2 to E3 opens the same required durable operator-partition state rather than receiving E2’s JVM heap. The state partition count can be restored from checkpoint metadata and must remain compatible with the recovered plan.</p>
 <p>State is persisted per task/operator partition. Its next version can exist while another task is still running or the overall batch later fails. Only the query completion log establishes that the batch and its sink execution completed. <a href="spark-checkpoint-failure-recovery.html#state">Follow the exact state replay example</a>.</p>
 ${refs('spk-state-layout','spk-state-version','spk-state-rdd','spk-state-locality','spk-hdfs-state-commit','spk-test-state-replay')}`),
 section('no-data-and-performance','Empty input, latency and throughput',`
 ${figure('spark-deep-no-new-data')}
 <p>No new source rows and no data loss are different situations. <code>constructNextBatch</code> can schedule a batch with unchanged offsets when stateful timeout/watermark work needs it. If neither new input nor another stateful batch is required, the query waits for another trigger. A no-data batch can still have state, sink and checkpoint consequences.</p>
 ${table(['Tuning dimension','Effect','Tradeoff'],[
 ['Trigger interval','Sets when the query checks/schedules the next microbatch','Short intervals reduce waiting but add planning, scheduling and checkpoint overhead. A slow batch still delays subsequent ordinary batches.'],
 ['maxOffsetsPerTrigger','Limits chosen source offset spans across partitions','Bounds batch work approximately; it is not an exact visible-record count or byte budget.'],
 ['minPartitions / maxRecordsPerPartition','Creates more source ranges/tasks from large partitions','Can increase parallelism; adds scheduling, client and task overhead.'],
 ['Executor slots','Permits more independent task attempts at once','Source/shuffle/state partitioning and skew still constrain useful concurrency.'],
 ['Shuffle/state partitioning','Distributes aggregation/join work','More partitions can reduce per-task size but add metadata/files/coordination; recovery must preserve compatible state layout.'],
 ['Warm state/consumer cache','Avoids repeated setup or cold state loads','Cache loss increases recovery latency without being the authoritative loss of durable state.']])}
 <p>Useful batch-time decomposition: offset discovery + planning/log publication + source/shuffle/state/sink critical path + completion-log publication. It is not the sum of every executor’s CPU time: many tasks overlap, and the slowest required task/stage often determines completion latency. Track input rates, batch duration, offsets and state-store load/commit metrics alongside executor/task failures.</p>
 <p>For a presentation, keep the ownership chain visible: driver chooses a vector → tasks run on executor slots → any exchange repartitions keys → state/sink operations succeed → driver records batch completion. Then use <a href="spark-checkpoint-failure-recovery.html">the recovery chapter</a> to place a failure between any two arrows.</p>
 ${refs('spk-batch-plan','spk-source-end','spk-range-splitting','spk-state-layout','spk-test-no-data-replay')}`)
 ]},
 {file:'spark-checkpoint-failure-recovery.html',number:26,title:'Spark: checkpoints, state and failure recovery',intro:'Explain why Kafka group offset commits are unnecessary for this source, how each failure is replayed across executors, and what must hold for retained input and sink effects to survive.',sections:[
 section('summary','What recovery can promise, and what it needs',`
 <p>Spark’s Kafka source reads explicit ranges recorded in Spark’s checkpoint. Its source <code>commit(end)</code> is empty, and internal consumers have auto commit disabled. Kafka group offsets are therefore not the recovery ledger for this query.</p>
 ${table(['Question','Answer in the ordinary synchronous microbatch path'],[
 ['How is input progress durable?','offsets/batchId fixes the planned end vector; commits/batchId identifies successful batch completion.'],
 ['What does an executor failure do?','Retry the task/range using the required state version, possibly on a different executor.'],
 ['What does a driver restart do?','Read the same checkpoint, reconcile offset and completion logs, then replay the incomplete batch or begin the next one.'],
 ['Does that avoid skipped input?','Under retained/replayable Kafka input, durable compatible checkpoint/state storage, correct restart and successful execution, failure recovery can replay unfinished input.'],
 ['Does that prevent duplicate output?','Only if the sink provides replay-safe effects through supported manifest, idempotency or atomic finalization. The built-in Kafka sink is at-least-once across Spark replay.'],
 ['What if Kafka has deleted the required records?','A checkpoint stores positions and state, not a copy of all Kafka records. Detection can stop or warn/skip according to policy, but cannot restore deleted input.']])}
 ${figure('spark-deep-loss-conditions')}
 <p>“No data loss” here concerns recovery of retained input after failures. Query logic can intentionally filter, aggregate, deduplicate or drop late records according to watermarks. That semantic choice should be assessed separately from skipped input caused by failures.</p>
 ${refs('spk-no-kafka-commit','spk-v1-commit','spk-kafka-config','spk-planned-log','spk-batch-complete','spk-driver-recovery','spk-data-loss','spk-reader-loss')}`),
 section('checkpoint','Checkpoint storage: what is inside and who writes it?',`
 ${figure('spark-deep-storage')}
 ${code(`checkpoint/                       illustrative directory shape
  metadata                        persistent query identity
  sources/0/0                     initial source offsets
  offsets/41                      end offsets for completed batch 41
  offsets/42                      planned end offsets for batch 42
  commits/41                      completion of batch 41
  commits/42                      exists only after successful batch 42
  state/<operator>/<partition>/   versioned state and provider metadata

output/_spark_metadata/           file sink's separate output manifest`)}
 <p>Paths and serialization depend on source naming and checkpoint format/provider. The tree is illustrative, not a byte-exact checkpoint dump. Newer formats can include source names, checkpoint IDs, state/schema metadata and different version headers. Do not fabricate those fields as always present.</p>
 ${table(['Artifact','Writer','Meaning'],[
 ['Source initial-offset metadata','Driver source initialization','Starting vector selected for a fresh source; differs from each later batch boundary.'],
 ['Offset log','Driver before ordinary batch execution','Planned end vector plus batch timing/watermark/session metadata used for replay.'],
 ['Versioned state','Executor state-store provider','Operator-partition data needed by future execution and failed-batch replay.'],
 ['Commit log','Driver after sink success','Completed batch and relevant recovery metadata; not a distributed transaction enclosing all external writes.'],
 ['Sink manifest / batch ledger','Sink implementation or application','Which output is logically finalized, independently of query checkpoint completion.']])}
 <p><code>HDFSMetadataLog</code> writes via <code>CheckpointFileManager.createAtomic</code>, then closes to publish or cancels on error. The rename-based manager writes a temporary file and renames it to the final name. Compatible storage must satisfy complete-file atomic visibility and durability requirements; a partial final checkpoint file would break the recovery protocol.</p>
 <p>The driver and executors must reach the same durable checkpoint storage after replacement. A driver-local ephemeral directory cannot support recovery after that disk disappears. For object storage, use a filesystem/manager implementation with the required semantics; do not assume every rename implementation behaves like an atomic HDFS rename. The inspected manager explicitly warns about non-atomic fallback behavior.</p>
 <p>Use one active query owner for a checkpoint and retain checkpoint-compatible source, state and query configuration. Conflict checks prevent certain concurrent log overwrites; they are not a distributed multi-writer application design. Deleting the checkpoint removes the original query’s recovery authority.</p>
 ${refs('spk-initial-source','spk-log-atomic','spk-file-manager','spk-rename-manager','spk-batch-complete','spk-state-ids','spk-file-manifest','spk-test-atomic-file','spk-test-log-restart')}`),
 section('state','State commit, cache loss and executor relocation',`
 ${figure('spark-deep-state-restore')}
 <p>For batch <code>b</code>, <code>IncrementalExecution</code> supplies state version <code>b</code>; providers produce <code>b+1</code> after applying the batch. With batch 42, a stateful task reads version 42 and commits version 43. A version-43 artifact may exist before the global batch succeeds.</p>
 <p>If the batch later fails, recovery asks for the state corresponding to the required replay boundary, not whichever version filename is largest. The inspected state test commits a later version, evicts the earlier cache, simulates global failure, then reloads the earlier durable state and regenerates the abandoned next version.</p>
 ${table(['Provider / component','Working state','Durable commit and restore'],[
 ['HDFSBackedStateStoreProvider','Executor-local versioned maps/cache','Finalize the next delta through atomic checkpoint I/O; load an appropriate snapshot and deltas through the requested version on cache miss.'],
 ['RocksDBStateStoreProvider / RocksDB','Local RocksDB working files on the executor','Persist a remote checkpoint snapshot or durable changelog. Cold recovery reconstructs from remote artifacts.'],
 ['RocksDB changelog mode','Local DB plus changelog tracking','Per-batch changelog commits establish durability; periodic snapshot uploads may run separately. Forced snapshots use the commit path.'],
 ['StateStoreCoordinator','Driver RPC endpoint and active-provider location map','Helps placement and coordination/optional validation; it is not the durable state database.'],
 ['StateStoreRDD','Executor task access plus preferred locations','Build provider identity and load the required operator-partition state on the chosen executor.']])}
 <p>Driver <code>StateStoreCoordinator</code> registers a Spark RPC endpoint. Executors report active provider instances and ask about preferred locations. The driver can lose this in-memory map while the checkpoint state remains intact; restarted providers register again. Do not draw this coordinator as a replicated cluster state topic.</p>
 <p>Checkpoint-format caveat: at this pin state format 1 is the default, and numeric state versions are central. Format 2 or later enables state checkpoint IDs, which help identify selected attempts/lineage. State metadata in the query commit log therefore depends on the enabled format. The replay diagrams acknowledge optional IDs rather than showing UUIDs as mandatory fields.</p>
 ${details('Why asynchronous RocksDB snapshot upload can still be durable',`<p>With changelog checkpointing enabled, the batch commit persists its changelog synchronously, while a periodic snapshot may be uploaded asynchronously. Durability follows the durable changelog and valid base snapshot/lineage, rather than waiting for every periodic snapshot. This can reduce commit upload cost but increase cold-restore replay work. Without changelog checkpointing, snapshot checkpoint upload is part of ordinary commit. Local RocksDB files alone cannot survive loss of the executor’s disk.</p>`)}
 ${refs('spk-state-version','spk-state-rdd','spk-state-locality','spk-hdfs-state-commit','spk-hdfs-state-load','spk-rocksdb-commit','spk-state-coordinator','spk-state-ids','spk-test-state-replay','spk-test-state-restart')}`),
 section('driver-restart','Driver restart: replay the unfinished batch',`
 ${figure('spark-deep-recovery')}
 <p>Worked example: <code>offsets/41</code> has <code>{120,200,90,50}</code>, and <code>offsets/42</code> has <code>{180,240,115,70}</code>. If only <code>commits/41</code> exists, Spark treats 42 as unfinished and reruns those fixed ranges with required prior state. If <code>commits/42</code> exists, its end vector becomes the next start vector and Spark moves to batch 43.</p>
 <p>The new driver also restores captured watermark/session/checkpoint metadata and selected state checkpoint IDs when present. Replaying a completed batch is not the intended normal path. Replaying an unfinished batch is intentional, even if some of its external output already exists.</p>
 <p>Executor JVMs remaining alive do not elect a new query driver or advance the query checkpoint themselves. The deployment must restart the driver/query with access to the same checkpoint. Spark task retry, cluster-manager executor replacement and driver application restart are separate recovery actions.</p>
 <p>Missing previous offsets, incompatible state metadata or inconsistent log entries can stop recovery. The correct response is to repair or restore a valid supported checkpoint/source state; silently moving to newer offsets can skip input. Changing <code>startingOffsets</code> cannot rewrite the progress of an existing checkpoint.</p>
 ${refs('spk-driver-recovery','spk-initial-source','spk-batch-complete','spk-test-log-inconsistent','spk-test-no-data-replay','spk-test-available-retry')}`),
 section('failure-matrix','Failure cases across multiple executors',`
 ${table(['Failure point','Durable evidence','Recovery path','Output / availability consequence'],[
 ['Driver before offsets/42 is published','Completed batch 41 remains recorded','Restart and construct the next batch from intact progress','End offsets may be discovered again; no completed 42 is inferred'],
 ['Driver after offsets/42, before tasks','Planned 42 and completed 41','Restart replays the fixed batch-42 ranges','Extra recovery time, with input preserved if retained'],
 ['Executor during source read / transform','Planned ranges and last durable state','Retry the task on an available executor','Previously issued external side effects can repeat'],
 ['One state task commits 43, another task fails','Some next-state files exist; commits/42 absent','Retry from required version 42 and regenerate next state','A partition state commit does not establish global batch completion'],
 ['Executor with warm state cache disappears','Remote state version remains','Reload snapshot/deltas/changelog on another executor','Cold-load latency and storage bandwidth cost'],
 ['Local shuffle blocks disappear','Source replay boundaries and durable state remain','FetchFailed handling recomputes missing map/stage work under retry limits','Lost local blocks are recomputable work, not durable source progress'],
 ['Partial Kafka/custom sink output then task failure','Some accepted output exists; no global completion','Retry task or unfinished batch','At-least-once effects unless sink protects repeats'],
 ['Sink success then driver loss before commits/42','Output exists, checkpoint still unfinished','Restart replays 42','Kafka appends can duplicate; manifest/ledger sinks can recognize prior finalization'],
 ['Driver loss after commits/42 publication','42 completion exists','Begin 43 from 42 end offsets','Relies on the sink success contract meaning durable completion'],
 ['Checkpoint I/O failure','Last intact files remain; newer external effects may exist','Fail task/query and recover from intact supported progress','Replay window grows; storage availability can stop progress'],
 ['Kafka leader/network failure','Broker replicas plus checkpoint range boundaries','Client leader rediscovery/retries and possibly Spark task retry','Availability pauses; broker durability/retention determine replayability'],
 ['Kafka retention deletes needed input','Checkpoint positions still exist, records do not','Fail on detected loss with true; best-effort warn/skip with false','Missing records cannot be reconstructed by checkpointing'],
 ['Checkpoint deleted or ephemeral storage lost','Recovery authority missing','Fresh-query semantics or restore an external checkpoint backup','Existing query recovery guarantee no longer applies'],
 ['Concurrent/incompatible checkpoint use','Metadata may conflict or no longer fit the plan','Integrity/configuration errors or unsupported recovery','Do not assume a shared checkpoint makes independent queries coordinated']])}
 <p>Retry limits and restart policy matter. A correct replay path does not guarantee infinite availability during permanent storage or network failure. Adequate Kafka retention must cover source backlog plus outage plus recovery/catch-up time, with margin; size-based retention, compaction, topic deletion and disk failure require separate consideration.</p>
 <p><code>failOnDataLoss=true</code> changes the response to detected missing data; it is not a retention mechanism. With false, the reader can move to still-available offsets inside the requested range or return no record if no overlap remains. Gaps caused by control/aborted records should not be confused with proof that every offset integer was a user record.</p>
 ${refs('spk-fetch-failure','spk-test-shuffle-retry','spk-retry-consumer','spk-test-state-replay','spk-driver-recovery','spk-reader-loss','spk-data-loss','spk-test-retention-policy')}`),
 section('sink','Sink success and the batch completion gap',`
 ${figure('spark-deep-crash-window')}
 ${table(['Sink','What its commit means','Replay behavior'],[
 ['Built-in Kafka streaming sink','Executor sends records, flushes and checks errors; driver commit is empty','Already accepted output can be appended again during task/batch replay. Producer idempotence does not deduplicate new send calls from replay.'],
 ['Streaming file sink','Publish successful task file list in output/_spark_metadata','Skip batches already present in the sink log. Logical append output can be replay-safe with compatible storage and metadata-aware readers.'],
 ['foreachBatch with ordinary external writes','User function returns after whatever it considers success','At-least-once unless destination/application supplies repeat protection; parallel per-partition transactions do not create one global batch transaction.'],
 ['Custom transactional / idempotent sink','Destination recognizes a stable completion identity atomically with effects','Can provide exactly-once effects within that destination’s supported atomicity and concurrency contract.'],
 ['Generic V2 streaming writer','Executor writer messages plus driver commit(epochId,messages)','Connector must implement replay-safe epoch commit; using the interface alone does not supply it.'],
 ['Console / memory','Debug output in console or driver state','Not a durable production sink for end-to-end preservation.']])}
 <p><code>KafkaDataWriter.write</code> sends rows before its <code>commit()</code>. Commit flushes, checks callback errors and returns a writer message. Abort does not roll those sends back. <code>KafkaStreamingWrite.useCommitCoordinator</code> is false, and its driver commit/abort hooks are empty. A stable Kafka key also does not by itself deduplicate append-log output; repeated sends remain records unless readers or a destination apply a defined deduplication/upsert policy.</p>
 <p>Kafka sink durability also depends on producer acknowledgments, topic replication and ISR policy. Spark passes producer options through; flushing an acknowledgment-free send cannot establish durable broker output before the query completion log advances.</p>
 <p>Spark’s <code>OutputCommitCoordinator</code> arbitrates task-attempt permission for connectors that use it. It is a driver RPC/in-memory decision, not a durable transaction spanning query logs and sink output. The V2 task path writes rows before asking for commit permission, so the coordinator cannot undo effects already exposed by an incompatible writer. Kafka streaming bypasses that arbitration.</p>
 <p>File sink output has a separate persistent manifest. After a completed output batch but a missing query commit, replay checks the manifest and skips the already published batch. Partial tasks can leave physical files, and cleanup is best effort. Metadata-aware readers define the logical output; raw directory globbing can include orphan/unpublished files.</p>
 ${refs('spk-kafka-send','spk-kafka-task-commit','spk-kafka-driver-commit','spk-producer-options','spk-writer-authorization','spk-commit-arbitration','spk-file-sink','spk-file-manifest','spk-test-task-arbitration','spk-test-file-abort')}`),
 section('custom-sink','A replay-safe foreachBatch design',`
 ${figure('spark-deep-sink-ledger')}
 <p>Use a stable query identity and Spark batch ID to identify a batch’s effects. The query’s restart-specific run ID is unsuitable for recognizing the same batch after restart. A destination can apply output and a completion ledger in one transaction, or atomically publish a staged output set associated with that batch.</p>
 ${code(`Illustrative application protocol, not a Spark built-in transaction:

begin destination transaction
enforce unique (stableQueryId, batchId) and concurrency-safe finalization
if ledger contains (stableQueryId, batchId):
    finish without applying the same effects again
else:
    apply all output covered by this batch identity
    insert (stableQueryId, batchId) into durable ledger
    atomically commit output plus ledger
on conflicting finalization: roll back all this attempt's output,
    then verify the winning attempt's durable completion

return from foreachBatch only after recognized durable success`)}
 <p>Writing “done” before the output can lose effects on a crash; writing it after output in an unrelated transaction can duplicate them on replay. A driver transaction around a ledger cannot automatically enclose JDBC transactions opened by separate executor tasks. Use a destination-supported staging/finalization scheme, durable per-record idempotency, or another protocol that covers all partial and concurrent attempts.</p>
 <p>Do not launch asynchronous writes and return from the callback before their success is known. Do not swallow failures and report successful completion. For stateful plans, consume the required data/state partitions; this pin has eligible state-store commit validation for incomplete foreachBatch execution, but it cannot certify that arbitrary external side effects succeeded.</p>
 <p>If a custom callback writes several destinations, finalizing each independently does not supply cross-destination atomicity. Decide which completion identities each destination honors and how partial success is repaired.</p>
 ${refs('spk-foreach-batch','spk-query-identity','spk-batch-sink','spk-batch-complete','spk-test-foreach-state')}`),
 section('modes-and-mental-model','Execution modes and presentation memory aids',`
 ${table(['Mode','Progress timing','Use this chapter’s timeline?'],[
 ['Ordinary synchronous microbatch','Chosen offsets before execution; completion after sink success','Yes: the main diagrams are scoped to this path'],
 ['Asynchronous progress tracking','Separate subclass writes progress asynchronously; replay ranges can change after failure','No: this removes the ordinary end-to-end exactly-once model, and this pin rejects stateful queries'],
 ['Continuous processing','Separate continuous execution and epoch coordination','No: do not substitute the microbatch sequence; assess its at-least-once guarantees and supported operators'],
 ['RealTimeTrigger at this pin','Processed offsets gathered from source tasks and logged at batch end; additional state/checkpoint constraints','No: it uses MicroBatchExecution but overrides the ordinary timing and source/shuffle behavior']])}
 <p>During a presentation, point to three durable ledgers: Kafka input records, Spark query/state checkpoint, and sink output/finalization. Then ask where the failure occurs. Executor failure changes a task attempt; driver failure changes the query process; neither should change a recorded unfinished batch’s intended range under ordinary synchronous recovery.</p>
 <p>Remember five facts: the driver chooses offset vectors; executors fetch directly; state follows operator partitions; completion logs decide replay; sinks decide whether repeated execution repeats effects. Kafka source <code>commit()</code> is a lifecycle hook with an empty implementation here, while Spark’s checkpoint <code>commits/42</code> is actual query progress evidence.</p>
 <p>Replay requires retained source and valid durable state. Exactly-once effects also require deterministic/compatible replay and sink repeat protection. Spark records certain batch timestamp/watermark metadata, but arbitrary randomness, mutable external lookups and fire-and-forget user code can change results across attempts.</p>
 <p>The source/test appendix records what was inspected. Documentation rendering and layout checks were run; Spark engine tests, multi-executor fault injection, example execution and performance benchmarks were not run.</p>
 ${refs('spk-batch-plan','spk-planned-log','spk-batch-complete','spk-async-progress','spk-async-guarantees','spk-no-kafka-commit','spk-driver-recovery')}`)
 ]}
 ];
}
