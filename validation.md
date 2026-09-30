# Guide validation

- 21 chapters; 66 rendered SVG diagrams; 113 code anchors.
- All source markers and referenced test files resolve in the recorded local revisions.
- 1274 static-site links and anchors checked; 0 broken.
- Desktop (1440 px) and mobile (390 px) layouts checked on every chapter.
- Font loaded before diagram layout; embedded in SVGs for offline viewing.
- Flowchart edge-label/node and edge-label/edge-label collision checks passed.
- Default diagram scale stays at least 90%; wide diagrams scroll inside their frame.
- Zoom, fit-width, chapter navigation and source expansion checked.
- Checks passed: true. See validation.json for full results.

Source tests were inspected or located, not executed. No engine integration tests, benchmarks or fault-injection runs were performed. Geometry checks do not replace visual inspection.

Kafka RPC addition: 94 request/response pairs (90 active, 4 retired), both common headers, 11 diagrams and 45 code/test references. Schema search, direct links, source expansion and desktop/mobile layouts passed. See [the additional validation record](kafka-guide-validation.json). The pod layout is illustrative, not a runtime observation.
