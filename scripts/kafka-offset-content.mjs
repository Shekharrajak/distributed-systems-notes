export function chapter({section,table,figure,refs,code,details}){
 return {file:'kafka-offset-commits-flink-spark.html',number:24,title:'Offset commits: Kafka, Flink and Spark',intro:'What commitSync and commitAsync really acknowledge, which thread runs them, what survives a crash, and why Flink and Spark recover from different durable records.',sections:[
 section('summary','Start here: whose progress is being committed?',`
 <p><strong>commitSync and commitAsync write the same kind of Kafka group-offset record.</strong> “Sync” changes how the caller waits and retries; it does not select a stronger server-side storage format. Neither method commits your database writes, Flink state or Spark output.</p>
 ${table(['Runtime','Kafka source offset API','Recovery authority','What a successful offset commit does not prove'],[
 ['Plain KafkaConsumer','commitSync or commitAsync, or auto commit','Committed group offsets, unless the application supplies another offset store','That the sink output and offset were atomic'],
 ['Modern Flink KafkaSource','Optional commitAsync after checkpoint completion','Completed Flink checkpoint: source splits + operator state + sink recovery state','That a Kafka sink transaction has completed'],
 ['Spark Structured Streaming Kafka source','Neither API for source progress','Spark checkpoint offsets/commit logs and state','Kafka group offsets are not its progress ledger'],
 ['Legacy Spark direct Kafka DStreams','CanCommitOffsets.commitAsync queues a later Kafka commitAsync','Depends on selected strategy: Kafka commits, DStream checkpoint, or an external atomic store','That enqueueing completed a broker write or made output exactly-once']])}
 ${figure('offset-authority')}${refs('api-sync','api-async','flink-client','flink-snapshot','spark-noop','spark-recovery','dstream-queue')}
 <p>Scope: ordinary consumer-group offset commits, modern FLIP-27 KafkaSource, Spark’s default synchronous microbatch execution, and the separate legacy DStream API. Share-consumer record acknowledgements are a different protocol; see <a href="kafka-group-share-coordinators.html">group versus share coordinators</a>. Internal Kafka client versions bundled by Flink/Spark may differ from the Kafka source snapshot shown here.</p>`),
 section('positions','Four numbers that must not be confused',`
 ${table(['Number','Meaning','Example for one partition'],[
 ['Kafka log end / fetch visibility boundary','What the log contains, and what this isolation level can read; not application progress','Log end 200; read_committed may stop at an earlier stable boundary'],
 ['Consumer position','Next fetch position after records returned by poll; records may still be unprocessed','poll returned through 149; position may be 150'],
 ['Safe application frontier','Next position after the contiguous prefix whose required effects are safe','Only through 119 is safely processed: frontier 120'],
 ['Durable restart progress','The last stored offset or engine checkpoint that recovery actually uses','Kafka group offset 100, or Flink checkpoint next offset 120']])}
 <p>Commit the <strong>next record to process</strong>, not the last processed record. For a fully handled batch, the pinned Kafka API recommends <code>ConsumerRecords.nextOffsets()</code>, which also carries leader epochs. Kafka offsets need not be consecutive: compaction, transactions and control batches can create gaps. Track the safe frontier in the delivered sequence, not an assumption that every integer is a user record.</p>
 ${figure('offset-frontier')}
 <p><code>commitSync()</code> with no map uses the consumer’s consumed positions. It does not wait for your worker pool, database futures or asynchronous sink writes. <code>commitAsync()</code> has the same limitation. Committing 150 while record 120 is still unfinished can skip 120 on restart.</p>
 <p>An offset commit does not seek the current consumer, delete source records, acknowledge individual share records, or force an input-topic transaction to commit. <code>seek</code> changes the live position; OffsetCommit changes stored group progress. They solve different problems.</p>${refs('api-async','classic-sync','classic-async','coordinator-replay')}`),
 section('sync-async','commitSync versus commitAsync: exact contract',`
 ${table(['Concern','commitSync','commitAsync'],[
 ['Caller waiting','Waits for success, a non-retriable failure, or timeout/interruption','Does not wait for the broker acknowledgement; reports completion through callback'],
 ['Retries in inspected clients','Retries expected retriable failures within the API deadline','Does not automatically retry an errored explicit commit attempt'],
 ['Result','Normal return means this commit succeeded; failure throws','Method return is not success; inspect callback exception'],
 ['Server storage','OffsetCommit → group coordinator → replicated offsets log','Same RPC, validation and durable log'],
 ['Ordering','Can be used as a bounded final progress checkpoint while ownership remains valid','Calls and their callbacks are ordered within the consumer API contract'],
 ['Cost','Blocks the owner’s application work and next poll','Overlaps work with commit latency but still consumes broker, network and callback capacity']])}
 ${figure('offset-sync-async')}
 <p>“Async” is not “no local waiting of any kind.” The inspected <code>AsyncKafkaConsumer.commit</code> waits for <code>offsetsReady</code> so the network thread captures positions before subsequent fetch progress changes them. It also executes already queued callbacks. The promise is not to synchronously await this broker commit result.</p>
 ${details('Subtle version caveat: does commitSync drain all earlier callbacks?',`<p>The pinned <code>commitAsync</code> Javadoc documents ordered invocations/callbacks and completion before a subsequent sync call returns. The pinned <code>commitSync</code> Javadoc separately qualifies the earlier-callback guarantee to the <code>consumer</code> group protocol. The newer delegate explicitly waits for <code>lastPendingAsyncCommit</code> and drains callbacks. The classic delegate also drains completed callbacks and has a pending-async path for empty maps, but do not turn this into a stronger cross-version guarantee than the API documents.</p><p>A timeout or other exceptional exit is not a successful drain. Nor does waiting for an earlier async attempt mean that attempt succeeded: its error still belongs to its callback. Inspect the exact client version and group protocol used by your application.</p>`)}
 ${refs('api-order-caveat','async-handoff','async-sync','classic-sync','commit-manager-sync','commit-manager-async','test-async-no-retry','test-sync-retry','test-async-drain')}`),
 section('threads','Which process, thread and module runs the work?',`
 <p><code>AsyncKafkaConsumer</code> is a client implementation name selected by <code>group.protocol=consumer</code>; it supports <em>both</em> commit methods. <code>ClassicKafkaConsumer</code> supports both too. Choosing <code>commitAsync</code> does not switch delegates or spawn a pod.</p>
 ${figure('offset-threads')}
 ${table(['Boundary','Code path','Ownership rule'],[
 ['Classic application thread','KafkaConsumer → ClassicKafkaConsumer → ConsumerCoordinator → ConsumerNetworkClient','Ordinary API calls drive commit I/O/completion. The classic membership heartbeat thread is not a dedicated commit callback executor.'],
 ['New consumer application thread','AsyncKafkaConsumer → application-event queue','Sync waits on a future; async queues callback delivery. KafkaConsumer remains a single-owner API.'],
 ['New consumer network thread','ApplicationEventProcessor → CommitRequestManager → NetworkClientDelegate','Sends the OffsetCommit request and receives its result; queues user callback work.'],
 ['User callback execution','OffsetCommitCallbackInvoker; classic completion queue','Runs on the consumer’s application thread during consumer operations. Keep callbacks short.'],
 ['Flink TaskManager JVM','Source task/mailbox → split-fetcher task queue → KafkaPartitionSplitReader','The fetcher owns the source consumer. Do not call its commit methods concurrently from the mailbox.'],
 ['Spark executor JVM','KafkaDataConsumer assign/seek/poll','Spark-selected offset ranges drive reads; there is no source group-offset commit to offload.']])}
 <p>On Kubernetes, a broker, JobManager, TaskManager, Spark driver or executor is commonly deployed as a separate process/pod. The boxes above are <strong>logical thread/module boundaries</strong>, not a required pod-per-box layout. Multiple source subtasks and operator chains can share a TaskManager JVM. Broker request handling, coordinator execution and storage callbacks share a broker process, with pools/queues between them.</p>
 ${refs('delegate','network-event','callback-thread','classic-callback','flink-fetcher','spark-fetch')}`),
 section('wire','Follow one OffsetCommit through the distributed cluster',`
 <p>Suppose group <code>payments</code> consumes <code>orders-0</code> led by broker B1, but its <code>__consumer_offsets</code> partition is led by B2. Fetch goes to B1; OffsetCommit goes to B2. B2 is the group coordinator for this group because it leads the backing offsets partition. If B1 and B2 happen to be the same broker, these remain different logical operations.</p>
 ${figure('offset-rpc')}
 ${table(['Wire field / response','Meaning'],[
 ['API key 8: OffsetCommit','Same Kafka binary request over the broker listener’s TCP/TLS connection for both Java methods; not gRPC and not a controller RPC.'],
 ['GroupId + MemberId + GenerationIdOrMemberEpoch','Group identity and ownership validation; classic generation versus consumer-protocol member epoch. Manual assignment does not imply ordinary membership/rebalance fencing.'],
 ['GroupInstanceId','Optional static membership identity in supported versions.'],
 ['Topic + PartitionIndex + CommittedOffset','The topic-partition resume point. Names through v9; IDs in v10 at this source pin.'],
 ['CommittedLeaderEpoch + CommittedMetadata','Input-partition epoch metadata and optional user metadata; this epoch is not the consumer member epoch.'],
 ['Response throttle time + partition ErrorCode','Each partition reports an outcome. A multi-partition request is not an atomic sink transaction; validation may reject some partitions and accept others.']])}
 <p>Broker flow: <code>KafkaApis.handleOffsetCommitRequest</code> authorizes and validates → <code>GroupCoordinatorService.commitOffsets</code> schedules a write keyed by the group’s backing partition → <code>OffsetMetadataManager.commitOffset</code> creates records → coordinator runtime appends them → replication advances the internal partition’s high watermark → deferred response completes.</p>
 <p>The durable key is the group/topic/partition offset key, and the value includes the committed next offset and metadata. The in-memory coordinator view is reconstructed by replay; it is not the durability boundary. Normal offset writes are not stored in KRaft’s cluster metadata log. KRaft records leadership and cluster metadata, while the offset topic replicates through broker partition replication.</p>
 <p>A successful commit is replication-backed, not a promise of one physical <code>fsync</code> per API call. Resilience still depends on replicas, ISR/min-ISR policy, eligible leadership, disk survival and clean recovery. Unavailable sufficient replicas can reject or delay writes; choosing async cannot fix that. See <a href="kafka-leadership-metadata.html">leadership and metadata failure recovery</a> and the <a href="kafka-rpc-atlas.html">complete versioned RPC schemas</a>.</p>
 ${refs('wire-request','wire-response','broker-commit','coordinator-commit','coordinator-record','coordinator-replay','coordinator-hw')}`),
 section('failure','Failure windows and the stale retry trap',`
 ${figure('offset-stale-retry')}
 <p>The coordinator stores accepted updates in log order; it is not a compare-and-set on the largest offset. A deliberate rewind can be valid. Therefore the client/application must not submit an obsolete retry after a newer frontier. The sequence above does not contradict FIFO callback ordering: the old retry is a <em>new</em> later invocation.</p>
 ${table(['Failure window','What can be true','Recovery / safe response'],[
 ['Output succeeded, process dies before offset commit','Sink contains effects; Kafka still holds an older restart offset','Replay is expected. Use idempotent output or an atomic output+progress design.'],
 ['Offset committed before output finishes','Restart skips records whose effects were not durable','Data can be lost from the application result. Waiting with commitSync does not repair the wrong order.'],
 ['Coordinator appended/replicated but response is lost','Client sees timeout/disconnect although the offset may be durable','Outcome is uncertain, not proven rollback. Re-establish ownership and use a current safe frontier.'],
 ['Coordinator broker fails before replication completes','A speculative append may not survive failover','New offsets leader loads committed state; retry/rediscover within valid ownership.'],
 ['NOT_COORDINATOR / loading / unavailable','Cached coordinator may be stale or its shard not ready','Rediscovery and bounded retry. Sync handles retriable failures; async exposes its failed attempt.'],
 ['Rebalance / stale member / fenced static instance','This consumer may no longer own the partitions','Do not blindly loop on the old map. Resolve assignment; stop the fenced instance.'],
 ['Mixed partition errors','Some offsets can have succeeded even though the API reports an exception','Do not assume all-or-nothing. Track safe progress per partition.'],
 ['Commit callback never observed before process death','Async call returned but result was not observed','Restore from durable progress, not the application’s last submitted request.'],
 ['Source data expires before replay','A saved offset cannot recreate deleted Kafka records','Retention must cover outage/recovery lag. Fail or reset according to policy; neither commit method restores missing data.'],
 ['Stored group offset expires or a fresh group is used','No committed Kafka resume point exists','Plain consumers need explicit starting/reset policy. Engine checkpoints are separate, but still need retained source data.']])}
 <p>For manually assigned consumers, a shared <code>group.id</code> is not an application-wide lock: independent clients can overwrite each other’s offsets. For subscribe-based consumers, distinguish a recoverable coordinator move from a membership error. On partition revocation, commit only a known-safe frontier while ownership permits it; after partitions are lost, do not assume you can commit them.</p>
 ${refs('coordinator-replay','commit-manager-sync','commit-manager-async','broker-commit','wire-response')}`),
 section('examples','Plain Java consumer: correct order before API choice',`
 <p>Illustrative Java sketch for the pinned API; imports/setup and shutdown/error policy omitted. It assumes <code>enable.auto.commit=false</code>, one consumer owner thread, sequential batch processing, and a sink function that returns only when the whole batch’s required effects are durable. These snippets were not compiled or executed.</p>
 ${code(`ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(250));
writeBatchAndWaitForDurability(records);
consumer.commitSync(records.nextOffsets(), Duration.ofSeconds(5));`)}
 <p>This is at-least-once output under a crash between sink success and commit success. To overlap commit latency, change the last line—not the safe frontier:</p>
 ${code(`Map<TopicPartition, OffsetAndMetadata> safe = Map.copyOf(records.nextOffsets());
consumer.commitAsync(safe, (attempted, error) -> {
    if (error != null) {
        recordCommitFailure(attempted, error);
    }
});`)}
 <p>Continue driving the consumer API so callbacks execute. Publish the next <em>current safe</em> frontier under a bounded policy; do not recursively retry the callback’s stale <code>attempted</code> map. Treat fatal authorization/fencing failures explicitly. A graceful final <code>commitSync(currentSafe, timeout)</code> can reduce duplicate replay, but only while ownership remains valid, and it cannot protect a hard crash.</p>
 ${details('What changes with a worker pool?',`<p>Retain a per-partition ordered completion tracker. Dispatch work while maintaining consumer ownership on one thread; pause partitions if necessary to bound unfinished work, continue required polling, and advance the commit frontier only over work known complete. A completion at offset 104 cannot cover an unfinished 102. A queue of “largest finished offset” values is not sufficient. On rebalance, stop accepting work for revoked partitions and prevent late workers from advancing an obsolete ownership epoch.</p><p>Use Kafka producer transactions for Kafka-to-Kafka atomic output+offset commits through <code>sendOffsetsToTransaction</code>, not either ordinary consumer commit API. An external database instead needs its own atomic offset+output transaction, idempotency or another compatible recovery protocol.</p>`)}
 ${refs('api-async','callback-thread','api-order-caveat','coordinator-replay')}`),
 section('flink','Flink: checkpoint authority, async offset publication',`
 ${figure('offset-flink')}
 ${table(['Step','Code / thread','Meaning'],[
 ['1. Read and emit','KafkaPartitionSplitReader fetcher → source mailbox → KafkaRecordEmitter','A prefetched record is not yet a checkpointed source offset. Emitter advances split state after deserialization/emission.'],
 ['2. Snapshot checkpoint 42','KafkaSourceReader.snapshotState(42), source task','Save split next-offsets. Also keep offsetsToCommit[42] if enabled. Do not use the consumer’s latest fetched position.'],
 ['3. Complete the global checkpoint','Flink checkpoint lifecycle','Recovery state and completion metadata become authoritative after the required tasks acknowledge. An individual source snapshot alone is not global completion.'],
 ['4. Notify source completion','KafkaSourceReader.notifyCheckpointComplete(42)','Select exactly the saved map for checkpoint 42; absent/empty maps are handled without inventing offsets.'],
 ['5. Submit commit work','KafkaSourceFetcherManager.enqueueOffsetsCommitTask','Marshal access to the fetcher-owned Kafka consumer; create a fetcher if the old one has ended.'],
 ['6. Call Kafka','KafkaPartitionSplitReader.notifyCheckpointComplete → consumer.commitAsync','Send ordinary OffsetCommit; record callback success/failure metrics and prune bookkeeping on success.'],
 ['7. Restore','Checkpoint split offsets → assign/seek','Recovery uses saved next-offsets even if the Kafka group commit never succeeded.']])}
 <p>Worked example: checkpoint 42 contains source next offset 120. The source has since fetched up to 200 and emitted up to 179. Its checkpoint-42 Kafka commit must publish 120, not 180 or 200. If the task fails and checkpoint 42 is the recovery point, it resumes from 120 with matching operator state. Alignment/channel-state handling belongs to the Flink checkpoint protocol, not this consumer offset call.</p>
 <p><strong>Failure of this Kafka offset commit does not invalidate the completed Flink checkpoint.</strong> The connector logs and counts callback errors. Pending bookkeeping can remain until a later successful completion subsumes it. Do not describe that as an infinite automatic retry of the exact same failed request.</p>
 <p>Configuration: <code>commit.offsets.on.checkpoint</code> defaults true; the builder defaults it false when <code>group.id</code> is absent. Kafka auto commit defaults false here, but the builder’s conditional override allows explicit configuration—so “Flink always forces auto commit off” would be too strong. With checkpointing enabled, keep source progress publication tied to completed checkpoints rather than Kafka’s poll-based auto-commit clock. Without completed checkpoints there are no checkpoint-completion notifications to publish.</p>
 <p>Fresh bootstrap may use an <code>OffsetsInitializer.committedOffsets</code> policy. That does not make Kafka group offsets authoritative when restoring an existing checkpoint/savepoint. Flink owns split assignment; a source consumer’s group ID does not make the Kafka group coordinator schedule Flink subtasks.</p>
 ${details('Source commit versus exactly-once Kafka sink commit',`<p>The source calls <code>KafkaConsumer.commitAsync</code> for ordinary group progress. The exactly-once sink’s <code>KafkaCommitter</code> calls <code>KafkaProducer.commitTransaction</code> for transactional output. They can target different coordinator brokers and complete at different times. There is no single Kafka transaction joining these ordinary source offsets to the output in this connector path.</p><p>Flink’s completed checkpoint and recoverable sink committables connect input replay, operator state and sink finalization. A failed source offset publication is monitoring/bookkeeping loss; a sink transaction that cannot be finalized is a sink correctness/recovery problem with separate handling. See <a href="flink-state-checkpoint-recovery.html">checkpoint recovery</a> and <a href="kafka-transaction-marker-recovery.html">transaction marker recovery</a>.</p>`)}
 ${refs('flink-emitter','flink-snapshot','flink-notify','flink-fetcher','flink-client','flink-commit-failure','flink-seek','flink-defaults','flink-options','flink-sink','test-flink-complete','test-flink-disable')}`),
 section('spark','Spark Structured Streaming: commit logs, not consumer commits',`
 <p>For the built-in Kafka source, the answer to “does Spark use commitSync or commitAsync?” is <strong>neither for source progress</strong>. <code>KafkaSourceProvider</code> disables auto commit on its consumers and rejects the user option. <code>KafkaMicroBatchStream.commit(end)</code> is empty. The word <code>commit</code> in Spark is not evidence of an OffsetCommit RPC.</p>
 ${figure('offset-spark')}
 ${table(['Artifact or method','Meaning','Crash implication'],[
 ['checkpoint/offsets/42','Chosen end-offset vector for microbatch 42','Defines replayable input range, not proof that output completed'],
 ['Executor KafkaDataConsumer','Assigns/seeks/fetches the range planned by the driver','Task retry can reread input; no executor source offset commit is needed'],
 ['Sink write / writer commit','Sink-specific successful batch output','External effects may already exist before the driver writes its completion log'],
 ['checkpoint/commits/42','Batch completion metadata, including relevant state checkpoint information','Recovery can advance past batch 42 if its offset/commit records agree'],
 ['checkpoint/state/...','Stateful operator recovery data when applicable','A source offset by itself cannot reconstruct aggregation state without replay/state recovery'],
 ['KafkaMicroBatchStream.commit(end)','No-op source lifecycle hook','Does not update __consumer_offsets']])}
 <p>Example: offsets/42 defines <code>[120,180)</code>. If the driver dies after output is written but before commits/42 is durable, recovery sees planned input without a matching completion and re-executes that batch. If commits/42 is durable, recovery advances. The sink must make repeated execution safe if you need exactly-once results.</p>
 <p>The built-in Kafka sink’s <code>KafkaDataWriter.commit</code> flushes producer sends and checks errors. It does not atomically commit source offsets with those writes. Spark’s Kafka sink is at-least-once across query/task retries; producer idempotence does not deduplicate arbitrary application replay across new producer sessions. For <code>foreachBatch</code>, a durable batch-ID/output deduplication or sink transaction scheme is the application’s responsibility.</p>
 <p>A configured <code>kafka.group.id</code> therefore does not turn consumer-group lag into Spark’s query progress. Use query start/end offsets, input/processing rates, batch duration and checkpoint status. Restart using the same valid checkpoint resumes its recorded progress; deleting it and setting startingOffsets is a new bootstrap decision, not normal failure recovery.</p>
 <p>Scope caveat from the inspected source: <code>RealTimeTrigger</code> delays offset logging until batch end, and async progress tracking has its own implementation. The sequence above deliberately describes the ordinary synchronous microbatch path. Neither “asynchronous progress tracking” nor a generic Spark writer <code>commit()</code> should be confused with Kafka consumer <code>commitAsync</code>.</p>
 ${refs('spark-config','spark-noop','spark-fetch','spark-start','spark-output-order','spark-end','spark-recovery','spark-source-commit','spark-sink','spark-doc','test-spark-config','test-spark-log')}`),
 section('dstreams','Legacy Spark DStreams: the exception you may remember',`
 ${figure('offset-dstream')}
 <p>The older <code>org.apache.spark.streaming.kafka010</code> integration exposes <code>CanCommitOffsets</code> on the original direct stream. Its <code>commitAsync</code> is thread-safe because it enqueues offset ranges; that does not make the underlying KafkaConsumer thread-safe.</p>
 <p>The driver’s later <code>DirectKafkaInputDStream.compute</code> calls <code>commitAll</code>. This drains the queue, takes the maximum exclusive <code>untilOffset</code> per partition in that drain, and invokes the driver Kafka consumer’s <code>commitAsync</code>. Only the most recently supplied callback is retained. Enqueueing is therefore neither a broker acknowledgement nor a one-callback-per-enqueue guarantee.</p>
 ${code(`stream.foreachRDD { rdd =>
  val ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
  writeOutputAndWait(rdd)
  stream.asInstanceOf[CanCommitOffsets].commitAsync(ranges)
}`)}
 <p>Illustrative Scala sketch, not executed. Capture ranges from the original Kafka RDD before transformations that remove <code>HasOffsetRanges</code>. Disable auto commit. Call after output durability, not after merely constructing a lazy transformation. A crash after output and before durable commit still replays, so the sink must tolerate duplicates. Out-of-order asynchronous output completion also needs a contiguous-batch frontier: coalescing max(untilOffset) cannot detect unfinished earlier output.</p>
 <p>Legacy DStreams support more than one recovery strategy. If you select Kafka offsets, they supply restart progress; if you restore a DStream checkpoint or store offsets atomically with output elsewhere, that strategy controls recovery. Do not carry a blanket “Spark commits to Kafka” statement into a Structured Streaming presentation.</p>
 ${refs('dstream-queue','dstream-drain','dstream-compute','test-dstream')}`),
 section('tradeoffs','Latency, throughput and operational tradeoffs',`
 ${table(['Choice','Benefit','Cost / boundary'],[
 ['Sync commit every batch','Simple sequential control flow and explicit completion','Adds commit round-trip/replication/queueing delay to the owner’s processing cycle; long stalls affect polling liveness'],
 ['Async periodic commit','Overlaps useful work with offset-write latency','More uncommitted progress at crash, callback/error bookkeeping, and no sink atomicity'],
 ['Larger commit interval','Fewer coordinator writes and callbacks','Larger duplicate replay window and slower Kafka group progress visibility'],
 ['Smaller Flink checkpoint interval','Fresher coordinated recovery points and potentially earlier transactional sink visibility','More state snapshots, checkpoint storage traffic, coordinator work and transaction overhead'],
 ['Larger Spark microbatch','Amortizes scheduling and checkpoint-log overhead','More end-to-end waiting and a larger batch to replay after failure'],
 ['More partitions/parallel tasks','More processing and source bandwidth capacity','More per-partition state, connections/requests, checkpoint metadata and coordination overhead'],
 ['More replicas / stricter durability policy','Better resilience to specified failures','More replication traffic and potential write unavailability when required replicas are absent']])}
 <p>Back-of-envelope only: for a sequential batch with useful work time P and commit wait C, a sync-commit loop needs roughly P+C plus fetch/scheduling overhead. Async can overlap C with later work, but does not remove C from broker completion latency. If records arrive at rate r and durable progress lags by T seconds, roughly r×T records may be candidates for replay; multi-partition skew, checkpoint duration and failures change the actual number. These are reasoning aids, not measured benchmarks.</p>
 <p>Separate three latency questions in a presentation: when was output first written, when was output safely visible to the intended reader, and when was the recovery point durable? Kafka group committed lag, Flink checkpoint age, Spark batch completion and Kafka transactional visibility answer different questions.</p>
 ${refs('classic-sync','commit-manager-async','flink-notify','spark-start','spark-end')}`),
 section('mental-model','Presentation mental model: three questions, five verbs',`
 <p><strong>Ask: who owns the work, where is progress durable, and what happens if the acknowledgement is lost?</strong></p>
 ${table(['Verb','Remember it as'],[
 ['poll / fetch','Move data into the runtime; not proof of completed output'],
 ['seek','Move this consumer’s live read position'],
 ['commitSync / commitAsync','Publish Kafka group restart progress; wait now versus learn later'],
 ['Flink checkpoint / Spark checkpoint logs','Persist the engine’s coordinated recovery knowledge'],
 ['commitTransaction / sink commit','Finalize output under that sink’s protocol; not automatically the same as source progress']])}
 <p>Thirty-second version: “Both Kafka commit methods write the same replicated bookmark. Sync waits; async reports later. Flink’s real bookmark lives in its checkpoint and it optionally mirrors source offsets to Kafka with commitAsync. Spark Structured Streaming keeps progress in its own offset and commit logs, not Kafka consumer commits. Legacy Spark DStreams can queue Kafka commits. Exactly-once is about coordinating output with recovery progress, not choosing the method with ‘Sync’ in its name.”</p>
 ${details('What evidence was verified, and what was not?',`<p>The appendix links exact repository revisions, methods and inspected test bodies. The documentation build checks that evidence needles exist at those revisions, Mermaid renders, labels do not geometrically overlap, links resolve, and desktop/mobile layouts work. Kafka, Flink and Spark engine tests, the snippets, performance benchmarks and live broker/pod fault injection were <strong>not run</strong>. The failure matrix describes consequences derived from the code and protocol, not newly measured failure experiments.</p>`)}
 ${refs('api-sync','api-async','flink-snapshot','spark-recovery','dstream-queue')}`)
 ]};
}
