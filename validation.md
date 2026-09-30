# Guide validation

- 27 chapters; 100 rendered SVG diagrams; 298 code anchors.
- All source markers and referenced test files resolve in the recorded local revisions.
- 2098 static-site links and anchors checked; 0 broken.
- Desktop (1440 px) and mobile (390 px) layouts checked on every chapter.
- Font loaded before diagram layout; embedded in SVGs for offline viewing.
- Flowchart edge-label/node and edge-label/edge-label collision checks passed.
- Default diagram scale stays at least 90%; wide diagrams scroll inside their frame.
- Zoom, fit-width, chapter navigation and source expansion checked.
- Checks passed: true. See validation.json for full results.

Source tests were inspected or located, not executed. No engine integration tests, benchmarks or fault-injection runs were performed. Geometry checks do not replace visual inspection.

Kafka RPC addition: 94 request/response pairs (90 active, 4 retired), both common headers, 11 diagrams and 45 code/test references. Schema search, direct links, source expansion and desktop/mobile layouts passed. See [the additional validation record](kafka-guide-validation.json). The pod layout is illustrative, not a runtime observation.

Kafka recovery addition: three chapters, 12 diagrams and 62 pinned code/test references covering KRaft metadata/elections, group/share coordinators, transaction markers, epochs and failure windows. All 17 documentation tests, 48 desktop/mobile page layouts and six new-page interaction checks passed. Diagrams were rendered with embedded Ubuntu, checked for label overlap and visually reviewed. See [the recovery validation record](kafka-recovery-validation.json). Kafka tests were inspected, not executed; no live cluster failover was performed.

<!-- kafka-offset-validation -->

Offset-commit addition: one chapter, nine diagrams and 52 pinned references across Kafka, Flink Kafka connector and Spark. Covers sync/async contracts, thread ownership, OffsetCommit RPC/storage, failure windows, Flink checkpoint publication, Structured Streaming logs, legacy DStreams and presentation notes. All 23 documentation tests, 50 desktop/mobile page layouts and two new-page interaction checks passed. Embedded-font rendering, geometry checks and visual review passed for all nine diagrams. See [the offset validation record](kafka-offset-validation.json). Engine tests and snippets were not executed; no live fault injection or performance benchmarks were run.

<!-- spark-deep-validation -->

Spark deep dive: two chapters, 13 diagrams and 71 pinned source/test references. Covers multiple executors, task threads, V2/V1 readers, RPC versus Kafka and shuffle traffic, state versions, checkpoint atomicity, task/driver failure, replay and sink durability/deduplication. All 23 existing documentation tests, 54 desktop/mobile layouts and four new-page interaction checks passed. Embedded-font rendering, overlap/clipped-text checks and visual review passed for all 13 diagrams. See [the Spark validation record](spark-guide-validation.json). Source/test bodies were inspected; Spark engine tests, example execution, benchmarks and live fault injection were not run.

<!-- /spark-deep-validation -->
