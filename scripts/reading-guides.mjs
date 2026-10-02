const guide = (gap, model, example, steps, check, answer) => ({gap, model, example, steps, check, answer});

export const readingGuides = {
 'index.html': guide(
  'Several entry-point lists compete before the reader knows which runtime to follow.',
  'Choose one processing route first. Kafka is the input log; Streams, Connect, Flink and Spark are different ways to do work; each destination has its own publication rule.',
  'Use purchase-7 as a business event. Its Kafka input offset, Flink checkpoint ID and Iceberg snapshot ID identify different things; none is a universal transaction number.',
  [['Read input','flink-kafka-connector.html#fk-emit','KafkaRecordEmitter advances a partition-local next offset after emission.'],['Recover computation','flink-state-checkpoint-recovery.html#flink-checkpoint','CheckpointCoordinator tracks a consistent job recovery point.'],['Publish a table','iceberg-storage-commits.html#iceberg-flink','IcebergFilesCommitter publishes checkpoint work through a table snapshot.']],
  'Must a pipeline use every product in this guide?', 'No. Pick the runtime and sink you use. Start with the reading routes and glossary, then follow that route into code.'),
 '01-kafka-record-lifecycle.html': guide(
  'LEO, HW, LSO and application progress are introduced together and are easy to treat as one cursor.',
  'A Kafka partition is an ordered log with several boundaries. A producer reply describes a write; a consumer bookmark describes where that application should resume.',
  'If LEO=120, HW=116 and an unresolved transaction begins at 110, read_committed is bounded at 110. A group bookmark of 105 is independent: it says this group resumes at 105.',
  [['Queue a send','#kafka-producer','KafkaProducer hands work to its accumulator; returning a future is not broker success.'],['Append and replicate','#kafka-partition','Partition handles leader append and replication requirements.'],['Move the replication boundary','#kafka-hw','High-watermark logic is separate from application processing progress.']],
  'Does a successful send mean the consumer finished processing?', 'No. Append, replication, transactional visibility and downstream processing are separate milestones.'),
 'kafka-transactions-recovery.html': guide(
  'Idempotent retry and transaction atomicity need a side-by-side example before the failure taxonomy.',
  'Idempotence recognizes a producer retry. A transaction couples a set of Kafka writes and supported progress records to one outcome.',
  'Worker W writes output for input 104 and includes next offset 105 in the same Kafka transaction. Abort hides that output from read_committed and does not advance the committed input bookmark. A fresh business send after a successful commit is different work to Kafka.',
  [['Enroll input progress','#streams-commit','StreamsProducer passes consumed offsets into its producer transaction.'],['Decide','#kafka-txn','TransactionCoordinator processes EndTxn and persists the decision.'],['Apply at participants','#kafka-markers','KafkaApis handles markers at participant leaders.']],
  'Does retrying a business event with a new producer identity deduplicate it?', 'No. Producer protocol retry protection is not a durable business-event uniqueness constraint.'),
 '02-kafka-streams.html': guide(
  'Task ownership and local-store recovery are described without a concrete loss of the owning host.',
  'A Streams task bundles processing and local state. Its recoverable history lives in Kafka; another application instance can take over the task.',
  'A task changes customer c42 from 90 to 100 and produces an output. If its transaction aborts, replacement work must recover committed state and input progress before replaying. The old local value 100 alone does not establish a committed result.',
  [['Manage local stores','#streams-state','ProcessorStateManager associates stores with their recovery metadata.'],['Commit Kafka effects','#streams-commit','StreamsProducer commits output/changelog work with consumed offsets.'],['Restore on a new owner','#streams-restore','StoreChangelogReader reconstructs state from changelog history.']],
  'Can a local RocksDB directory replace the changelog recovery contract?', 'No. A surviving directory may speed restore; a lost or dirty directory cannot establish committed state by itself.'),
 '03-kafka-connect.html': guide(
  'Source offsets, sink offsets and worker status are easy to confuse because all three are called progress.',
  'Connect source tasks bring external data into Kafka; sink tasks deliver Kafka data elsewhere. Their completion records live in different places.',
  'Source example: database position L42 and its Kafka output can share a source-worker transaction. Sink example: a database insert succeeds for Kafka offset 104, then the worker dies before committing 105. The sink can see 104 again.',
  [['Assign a task','#connect-group','DistributedHerder coordinates worker/task ownership.'],['Publish source data plus source position','#connect-source-eos','ExactlyOnceWorkerSourceTask supplies the source transaction boundary.'],['Report safe sink progress','#connect-precommit','WorkerSinkTask uses the connector preCommit contract before committing Kafka offsets.']],
  'Does SinkTask.put returning always mean the database write is durable?', 'No. A connector may buffer. Its preCommit/flush and destination contract must identify what is safe to acknowledge.'),
 '04-flink-runtime.html': guide(
  'The process, endpoint, slot and task terms need a concrete placement example before the RPC list.',
  'JobMaster schedules work; TaskExecutor is a worker control endpoint; a task thread runs operators. A slot is an allocation unit, not another process.',
  'Two TaskManagers can each run several task attempts. Losing one JVM loses all its tasks. The replacement needs new allocations and durable checkpoint state; it does not receive the old JVM heap.',
  [['Request resources','#flink-requirements','The job slot pool declares required resources.'],['Offer allocated slots','#flink-slot-offer','TaskExecutor offers slots to the JobMaster.'],['Launch an attempt','#flink-deploy','TaskExecutor validates and accepts the deployment descriptor.']],
  'Does task-submission acknowledgement certify completed processing?', 'No. It acknowledges deployment handling. Task execution, snapshot acknowledgement and sink completion happen later.'),
 '01-plan-and-deploy.html': guide(
  'Four graph names are abstract until connected to one pipeline and its physical boundaries.',
  'The same program is represented at different planning levels. Chaining combines compatible operators into one task; parallelism creates instances of those tasks.',
  'Take source -> map -> keyBy(customer) -> sum. If source/map can chain at parallelism two, they can form two tasks. The keyed exchange routes each customer to the downstream owner; it is not a direct call merely because tasks share a machine.',
  [['Choose chaining','#flink-graph','StreamingJobGraphGenerator decides which edges can be chained.'],['Assign keyed state','#flink-keygroup','KeyGroupRangeAssignment maps keys through key groups to subtasks.'],['Deploy the result','#flink-deploy','TaskExecutor receives the physical task attempt.']],
  'Does adding slots split one hot customer across independent sums?', 'No. Keeping one keyed aggregate correct requires a single owner per key in that computation; changing that requires an algorithmic design.'),
 '02-records-time-and-backpressure.html': guide(
  'Mailbox, backpressure and watermarks need separate examples so transport progress is not mistaken for event-time progress.',
  'Backpressure limits how fast records move. Watermarks describe event-time progress. The mailbox coordinates task actions; it cannot make a blocking user function finish.',
  'Input A has watermark 12:00:10 and active input B has 12:00:02. Their combined progress is held at 12:00:02 even if A is fast. Separately, a slow sink can exhaust buffers and slow both inputs.',
  [['Order task work','#flink-mailbox','MailboxProcessor runs control actions and input work.'],['Wait for capacity','#flink-input','StreamTask suspends normal input on availability futures.'],['Combine input time','#flink-watermark','StatusWatermarkValve accounts for input watermarks and status.']],
  'Will larger buffers fix a permanently slower sink?', 'No. They absorb a burst and increase queue capacity; sustained arrival above service capacity still accumulates work.'),
 'flink-state-checkpoint-recovery.html': guide(
  'Checkpoint trigger, task snapshot and globally completed checkpoint need a single failure timeline.',
  'A completed checkpoint is a recoverable cut across the job. A barrier marks that cut in the data flow; a task acknowledgement contributes durable state handles to it.',
  'CP41 contains next offset 100 and total 70. Work through offset 104 produces total 100. If CP42 never completes, restore CP41 and replay from 100. If CP42 completes with next offset 105, restore its state and recover its pending sink commit.',
  [['Take the task cut','#flink-barrier','SubtaskCheckpointCoordinatorImpl coordinates barrier and snapshot work.'],['Establish job completion','#flink-checkpoint','CheckpointCoordinator aggregates required state and records completion.'],['Restore attempts','#flink-restore','DefaultScheduler drives restart and state restoration.']],
  'Can the source snapshot alone authorize skipping input after failure?', 'No. Recovery needs the completed job checkpoint with matching operator and sink recovery state.'),
 'flink-kafka-connector.html': guide(
  'Two post-checkpoint actions look like a single Kafka commit unless compared using actual offsets.',
  'Flink owns its recovery bookmark. Its optional Kafka group-offset publication and its Kafka output transaction are separate operations.',
  'CP42 stores next offset 105. The fetcher has already polled through 119. Recovery from CP42 uses 105, not 120. Even if Kafka group progress still says 100, CP42 remains the recovery authority.',
  [['Emit and advance source state','#fk-emit','KafkaRecordEmitter updates the split after emission.'],['Snapshot the bookmark','#fk-state','KafkaSourceReader saves checkpoint split offsets.'],['Finalize output','#fk-commit','KafkaCommitter completes the checkpoint-linked output transaction.']],
  'Does Kafka group offset 105 prove the sink transaction committed?', 'No. Check the sink committable/transaction path independently; the source offset publication is separate bookkeeping.'),
 'spark-scheduler-networking.html': guide(
  'Task retry and stage recomputation need an example that distinguishes executor loss from lost shuffle data.',
  'The driver schedules attempts. Executors run them and exchange shuffle blocks. Lost intermediate work can be recomputed from the upstream plan.',
  'Map tasks on E1 and E2 feed a reduce task on E3. If E2 loses required local shuffle blocks, retrying only the reducer may be insufficient: Spark must regenerate the missing map output before the reduce can finish.',
  [['Launch a task','#spark-launch','CoarseGrainedSchedulerBackend sends work to an executor.'],['Read shuffle blocks','#spark-shuffle','ShuffleBlockFetcherIterator retrieves upstream outputs.'],['Handle lost work','#spark-retry','DAGScheduler handles failures and resubmission.']],
  'Can retry or speculation run my side-effecting function twice?', 'Yes. A destination needs attempt-safe commit or idempotency; task scheduling does not undo an external action.'),
 '05-spark-structured-streaming.html': guide(
  'The word commit names several unrelated operations; the two driver logs need a minimal example first.',
  'The offset log says what a batch intends to read. The completion log says the batch finished. Sink behavior decides whether replay repeats an external effect.',
  'Batch 42 plans P0 [120,180). offsets/42 exists but commits/42 does not. After restart, batch 42 may run again even if Kafka output from the first attempt already exists.',
  [['Record the plan','#spark-offset','MicroBatchExecution records the chosen batch boundaries.'],['Write Kafka output','#spark-kafka','KafkaSink writes records; its local latest-batch tracking is not a durable cross-system transaction.'],['Record completion','#spark-end','MicroBatchExecution publishes batch completion after sink execution.']],
  'Does KafkaMicroBatchStream.commit update the Kafka group offset?', 'No in this pinned source. It is an empty lifecycle hook; Spark checkpoint logs govern this source recovery.'),
 'iceberg-storage-commits.html': guide(
  'Metadata files, uploaded data and published snapshots need one concrete visibility example.',
  'Files contain rows; the table metadata determines which files belong to a visible snapshot. A successful upload is only one step toward publication.',
  'A writer uploads file F and prepares S900 from S899. Before metadata publication, readers of S899 do not gain F. If the commit reply is lost, F might already be referenced by S900, so deleting it as an orphan is unsafe.',
  [['Prepare and publish metadata','#iceberg-snapshot','SnapshotProducer coordinates snapshot commit and eligible retry.'],['Resolve restored sink work','#iceberg-recover','IcebergFilesCommitter reconciles prior checkpoint publication.'],['Plan a read','#iceberg-scan','DataTableScan selects files for a chosen table snapshot.']],
  'Does finding a Parquet file in the bucket mean it is a committed table row?', 'No. The selected snapshot and applicable delete/filter semantics determine the logical table contents.'),
 'datafusion-query-engine.html': guide(
  'Local execution partitions sound like distributed workers unless the host responsibility is introduced first.',
  'DataFusion executes a query inside its host process. A physical partition is a unit of local plan execution; distribution and durable recovery require a host design.',
  'A host executes a filter/projection stream and consumes one Arrow batch at a time. If the process disappears, its channels and buffered batches disappear too. Retrying requires the host to reconstruct the query from retained inputs.',
  [['Start a partition','#df-execute','ExecutionPlan::execute returns a batch stream.'],['Redistribute locally','#df-repartition','RepartitionExec routes batches within this process.'],['Reserve working memory','#df-memory','MemoryReservation participates in a memory-pool budget.']],
  'Does a stream guarantee the first result without buffering the input?', 'No. A filter can often yield early; sort and join phases may need retained or spilled input.'),
 'comet-spark-native.html': guide(
  'Planning fallback, JNI execution and Spark recovery are three different boundaries that need a single trace.',
  'Spark owns distributed attempts. Comet executes supported plan segments inside an executor through a native library; JNI is an in-process boundary.',
  'Spark launches a task on E2; its Comet iterator creates a native plan and pulls batches. If E2 dies, Spark retries the task elsewhere. The native plan handle is not transferred as durable state.',
  [['Choose native segments','#comet-extension','CometSparkSessionExtensions installs planning rules.'],['Create per-task native execution','#comet-create','CometExecIterator passes plan and task context through JNI.'],['Release resources','#comet-close','Iterator close handles native/task resource cleanup.']],
  'Does unsupported planning fallback guarantee recovery from every native crash?', 'No. Compatibility fallback and runtime failure are different. An executor crash follows Spark recovery.'),
 'rust-arrow-runtime.html': guide(
  'Ownership primitives are listed before showing the memory lifetime they protect.',
  'Rust ownership and Arrow buffers describe local lifetime. They do not record durable progress or roll back a remote effect.',
  'A small slice can share a large Arrow allocation. Dropping the original view does not free that allocation while the slice still owns it. Losing the process removes both; Arc never made a remote replica.',
  [['Own an allocation','#arrow-buffer','Buffer retains shared backing storage.'],['Create another view','#arrow-slice','Slicing can retain storage without copying every value.'],['Cross a language boundary','#arrow-ffi','The Arrow FFI contract carries memory ownership and release responsibilities.']],
  'Does cancelling a future undo a completed HTTP write?', 'No. Cancellation governs local work; the remote operation needs its own transaction, reconciliation or idempotency contract.'),
 'end-to-end-example.html': guide(
  'Kafka and Iceberg alternatives can read as one automatically atomic two-sink pipeline.',
  'Track input replay, computation state and output publication separately. Follow one destination before combining destinations.',
  'At offset 104, purchase-7 changes c42 from 90 to 100. CP42 records next offset 105 and sink recovery information. For the Iceberg route, S900 publishes the table output; for the Kafka route, T42 resolves output visibility.',
  [['Record the input boundary','#fk-state','KafkaSourceReader snapshots the next offset.'],['Finalize the Iceberg route','#iceberg-flink','IcebergFilesCommitter publishes retained file work.'],['Finalize the Kafka route','#fk-commit','KafkaCommitter resolves the prepared transaction.']],
  'If T42 is visible first, must S900 already be visible?', 'No. Independent destinations have independent publication decisions. A common checkpoint is not atomic cross-destination visibility.'),
 '06-delivery-and-recovery.html': guide(
  'Many tuning formulas arrive together without telling readers which decision to make first.',
  'First choose the effect that must survive or not repeat. Then identify its durable boundary; only then tune waiting, batching and recovery budgets.',
  'At 100,000 arrivals/s and 80,000 completions/s, backlog grows by 20,000/s. An extra 200,000-record queue buys about ten seconds at those fixed rates; it does not fix the capacity deficit.',
  [['Name the recovery point','#fk-state','Source state belongs to the engine checkpoint.'],['Name the publication step','#fk-commit','Sink transaction completion determines the output boundary.'],['Locate a capacity stall','#flink-input','Input availability propagates pressure from downstream.']],
  'Is faster CPU enough when output waits five seconds for a checkpoint cut?', 'It can help throughput, but reducing a 50 ms computation alone cannot remove the five-second waiting term.'),
 '07-code-evidence.html': guide(
  'A long catalogue of snippets does not explain how to test a claim or distinguish inspected code from an executed test.',
  'Read a claim as a chain: caller -> state change -> durable operation -> completion callback -> recovery. A method name alone is not evidence for the entire chain.',
  'To check “Flink source offsets recover the job,” inspect snapshotState and restore/seek. To check “Kafka output is committed,” inspect KafkaCommitter and Kafka markers. An optional source OffsetCommit cannot substitute for either proof.',
  [['Inspect source state','#fk-state','Check which offset is saved and when.'],['Inspect external publication','#fk-commit','Check success, retry and fatal error handling.'],['Check broker completion','#kafka-markers','Follow the participant marker path rather than stopping at EndTxn.']],
  'Were all engines tested together at these commits?', 'No. These are separate source snapshots. The newer share-transaction chapter records its own limited test run; its results do not validate the whole guide.'),
 'kafka-flink-distributed-runtime.html': guide(
  'The comprehensive chapter is too dense to serve simultaneously as a beginner introduction and a protocol reference.',
  'Follow one record on the data path, then revisit the same owners on the checkpoint path. A pod, service, task and thread are different boundaries.',
  'Source and map may share one task thread. keyBy can send the record to a task on another worker. The sink producer sends output to Kafka, while checkpoint control messages travel separately through Flink.',
  [['Cross the source thread boundary','#f-emit','KafkaRecordEmitter runs on the task path after fetcher handoff.'],['Cross the operator boundary','#f-chain','ChainingOutput uses a direct call only when the operators are chained.'],['Cross the output commit boundary','#f-commit','KafkaCommitter finishes a prepared sink transaction.']],
  'Does one diagram box imply one pod or one RPC per record?', 'No. Use the chapter’s placement and transport tables to identify which boxes share a process and which arrows are calls, queues or network messages.'),
 'kafka-leadership-metadata.html': guide(
  'Several epochs and elections are introduced before demonstrating which ownership actually changes.',
  'The controller maintains the ownership map. Partition replicas contain the data. Changing the map cannot recreate data absent from every valid replica.',
  'B1 leads orders/P0 and an offsets shard; B3 leads a transaction shard. If B1 dies, P0 and the offsets shard can move independently. B3 need not stop being the transaction coordinator.',
  [['Choose eligible ownership','#ev-partition-election','Controller logic chooses a partition leader under the configured policy.'],['Apply the new map','#ev-broker-publisher','BrokerMetadataPublisher updates local roles and coordinator hooks.'],['Check quorum progress','#ev-raft-majority','Metadata commits require the controller quorum, distinct from user-topic ISR rules.']],
  'Does electing a new controller mean every coordinator is ready?', 'No. Metadata must be applied, internal partitions must have leaders, and coordinator shards must finish loading.'),
 'kafka-group-share-coordinators.html': guide(
  'Membership, acquisition and durable acknowledgement use three owners with similar names.',
  'Membership answers who may participate. Acquisition answers who temporarily holds a record. Durable share state answers which work is already completed.',
  'Worker A holds record 11 while B completes record 12. Recording 12’s ACK does not require declaring 11 complete. If 11 is released, another worker may acquire it; record 12 remains completed in the group’s durable share state.',
  [['Validate group membership','#ev-group-share','The group coordinator owns membership.'],['Track record acquisition','#ev-share-cache','SharePartition lives at the source-serving broker.'],['Persist delivery state','#ev-share-persist','The persister connects source delivery work to the share-state owner.']],
  'Is this ordinary ACK path already atomic with Kafka output?', 'No. The separate KIP-1289 chapter uses a different source revision and records its implementation limits.'),
 'kafka-offset-commits-flink-spark.html': guide(
  'The cross-runtime comparison is interrupted by client-version caveats before a reader has chosen a runtime.',
  'Ask which durable bookmark the runtime restores. Choosing sync versus async only answers how a plain Kafka offset caller waits.',
  'Suppose poll returned through 149 but only records through 119 are safely finished. Committing 150 can skip unfinished work. In Flink, use the completed checkpoint position; in ordinary Spark micro-batches, use its recorded batch boundaries and completion.',
  [['Store a Kafka bookmark','#ev-coordinator-record','OffsetMetadataManager creates offset records for the group.'],['Publish a Flink checkpoint position','#ev-flink-notify','KafkaSourceReader selects the saved checkpoint map.'],['Recover a Spark batch','#ev-spark-recovery','MicroBatchExecution reconciles its own logs.']],
  'Does commitSync wait for my worker-pool futures?', 'No. Your application must first establish a contiguous safe frontier. Waiting for an offset write cannot make unfinished effects durable.'),
 'kafka-rpc-atlas.html': guide(
  'The complete API catalogue needs a task-oriented entry route; reading every schema obscures the protocol journey.',
  'Use this page as a dictionary after identifying the caller, owner and operation. Schema JSON describes the binary wire layout; it is not the actual message format sent over TCP.',
  'For “commit returned but output is invisible,” look at EndTxn, then WriteTxnMarkers, then Fetch visibility fields. For “where does OffsetCommit go,” start with FindCoordinator and the group owner.',
  [['Record a transaction decision','#rpc-26','EndTxn goes to the transaction coordinator.'],['Deliver the decision','#rpc-27','WriteTxnMarkers goes to participant leaders.'],['Read eligible output','#rpc-1','Fetch carries isolation and visibility-related response fields.']],
  'Can I send a schema’s newest version to every cluster?', 'No. Client support, endpoint ApiVersions and feature configuration constrain the version. This atlas is pinned to one source snapshot.'),
 'kafka-transaction-marker-recovery.html': guide(
  'The extensive failure catalogue needs a minimal timeline to distinguish decision, visibility and cleanup.',
  'The coordinator records a decision, participant leaders apply it, and the coordinator records completion. These are separate durable events.',
  'T42 writes results/P0 on B1 and results/P1 on B2; its coordinator is B3. B1 may apply COMMIT before B2. A replacement B3 must finish COMMIT after recovering PREPARE_COMMIT; it cannot choose ABORT to avoid waiting.',
  [['Record the decision','#ev-txn-end-v2','The EndTxn path persists prepare state.'],['Deliver to participant leaders','#ev-txn-markers','TransactionMarkerChannelManager queues completion work.'],['Decide read visibility','#ev-lso','UnifiedLog bounds stable reads independently for each partition.']],
  'Does EndTxn success provide a simultaneous cross-partition snapshot?', 'No. Markers and replication converge independently; an earlier open transaction can also hold a partition’s LSO back.'),
 'kafka-share-transactions-multi-broker.html': guide(
  'The four-broker example can be mistaken for an integration test despite the staging limitation.',
  'Separate the intended distributed ownership model from the code that currently connects those owners. The marker path assumes acknowledgements were successfully staged first.',
  'If the source leader is B2 and the group coordinator is B1, this branch’s local membership read on B2 can reject staging. No commit diagram can repair that missing route. After valid staging, share-state and output participants apply the durable decision independently.',
  [['Check the staging prerequisite','#ev-local-validation','GroupCoordinatorService schedules a local membership read.'],['Finalize share state','#ev-share-finalize','ShareCoordinatorShard matches staged producer identity and epoch.'],['Refresh a remote source cache','#ev-cache','A later fetch can reload cached TX_PENDING state.']],
  'Do passing shard tests prove arbitrary broker placements work?', 'No. Read the documented staging, fencing and cache gaps plus the exact test results; multi-broker fault injection was not run.'),
 'spark-distributed-execution.html': guide(
  'Kafka partitions, scan ranges, state partitions and executor slots are four different counts.',
  'The driver chooses ranges of input; executor tasks read them. A shuffle can repartition those rows into different state owners before output.',
  'Four Kafka partitions can yield four ordinary source tasks, placed on three executors. Those tasks may feed two state partitions. Losing E2 moves task attempts; it does not renumber Kafka partitions or turn state partition 0 into Kafka P0.',
  [['Plan source ranges','#ev-spk-source-ranges','KafkaMicroBatchStream creates input partitions from the driver’s boundaries.'],['Run task attempts','#ev-spk-task-thread','Executor runs work in its task thread pool.'],['Load operator state','#ev-spk-state-rdd','StateStoreRDD opens the required operator-partition state.']],
  'Does Kafka group assignment decide which executor reads a range?', 'No for this source path. Spark plans tasks and its readers explicitly assign/seek Kafka partitions.'),
 'spark-checkpoint-failure-recovery.html': guide(
  'Per-task state-store commit is easily mistaken for completed batch output.',
  'The query’s completion log chooses the recovery boundary. A newer state file or successful executor task alone does not complete the whole batch.',
  'Batch 42 loads state version 42 and produces version 43. If another task fails and commits/42 is absent, replay needs the state required for batch 42, not whichever state filename is largest. Kafka output from the failed run can already exist.',
  [['Load the required state','#ev-spk-state-version','IncrementalExecution sets the version for the batch.'],['Recover query progress','#ev-spk-driver-recovery','MicroBatchExecution compares offset and completion logs.'],['Understand sink success','#ev-spk-kafka-task-commit','KafkaDataWriter flushes sends; this is not an atomic query-log transaction.']],
  'Does a stable Kafka output key eliminate replayed append records?', 'No. Duplicate keys can remain separate log records. Logical deduplication requires an explicit destination or reader policy.')
};

const esc = value => value.replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;');
export function addReadingGuide(html,file) {
 const g=readingGuides[file];
 if(!g || !html.includes('class="hero"'))return html;
 const block=`<!-- reading-guide:start --><section class="reading-guide" id="reading-guide" aria-label="Mental model and worked example"><p class="eyebrow">Before the detail</p><h2>The mental model</h2><p>${esc(g.model)}</p><details><summary>Worked example: connect the system view to the code</summary><p>${esc(g.example)}</p><ol>${g.steps.map(([label,href,text])=>`<li><a href="${esc(href)}">${esc(label)}</a><p>${esc(text)}</p></li>`).join('')}</ol><p class="example-scope">Illustrative reasoning example using this chapter's source scope; not a recorded deployment or executed experiment.</p></details><details><summary>${esc(g.check)}</summary><p>${esc(g.answer)}</p></details><p class="reading-help"><a href="reading-paths.html">Choose a reading path</a> · <a href="reading-paths.html#glossary">Terms and progress boundaries</a> · <a href="reading-paths.html#review">Clarity review</a></p></section><!-- reading-guide:end -->`;
 if(html.includes('<!-- reading-guide:start -->'))return html.replace(/<!-- reading-guide:start -->[\s\S]*?<!-- reading-guide:end -->/,()=>block);
 if(!/<div class="hero">[\s\S]*?<\/div>/.test(html))throw Error(`Missing hero in ${file}`);
 return html.replace(/<div class="hero">[\s\S]*?<\/div>/,hero=>hero+block);
}
