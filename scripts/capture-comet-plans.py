import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess

from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument('--engine', choices=['jvm', 'comet'], required=True)
parser.add_argument('--output', required=True)
parser.add_argument('--repo', required=True)
parser.add_argument('--revision', required=True)
args = parser.parse_args()
output = Path(args.output)
output.mkdir(parents=True, exist_ok=True)
spark = SparkSession.builder.appName(f'Iceberg plan atlas - {args.engine}').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
tables = ['customer', 'lineitem', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier']
snapshot_file = output / 'snapshots.json'
snapshots = json.loads(snapshot_file.read_text()) if snapshot_file.exists() else {}
records = []
try:
    for table in tables:
        identifier = f'local.tpch.{table}'
        if table not in snapshots:
            iceberg = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
                spark._jsparkSession, identifier)
            snapshots[table] = {'table': identifier, 'id': str(iceberg.currentSnapshot().snapshotId())}
        spark.read.format('iceberg').option('snapshot-id', snapshots[table]['id']).load(
            identifier).createOrReplaceTempView(table)
    snapshot_file.write_text(json.dumps(snapshots, indent=2) + '\n')
    for mode, threshold in [('normal', '10485760'), ('no-broadcast', '-1')]:
        spark.conf.set('spark.sql.autoBroadcastJoinThreshold', threshold)
        for query in range(1, 23):
            key = f'{mode}-q{query}-{args.engine}'
            sql = subprocess.check_output(
                ['git', 'show', f'{args.revision}:benchmarks/tpc/queries/tpch/q{query}.sql'],
                cwd=args.repo, text=True)
            (output / f'q{query}.sql').write_text(sql)
            statements = [s.strip() for s in sql.split(';') if s.strip()]
            executable = sql
            if query == 15:
                assert len(statements) == 3
                temporary = re.sub(r'\bcreate view revenue0\b',
                                   'create or replace temporary view revenue0', statements[0],
                                   flags=re.IGNORECASE)
                assert temporary != statements[0]
                spark.sql(temporary)
                executable = statements[1]
            else:
                assert len(statements) == 1
                first = re.sub(r'--[^\n]*', '', sql).strip().lower()
                assert first.startswith(('select', 'with'))
            spark.sparkContext.setJobGroup(key, f'Plan only: {key}')
            try:
                df = spark.sql(executable)
                plan = spark._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), 'formatted')
                (output / f'{key}.plan.txt').write_text(plan)
                records.append({'key': key, 'query': query, 'engine': args.engine, 'mode': mode,
                                'file': f'{key}.plan.txt', 'sha256': hashlib.sha256(plan.encode()).hexdigest(),
                                'schema': json.loads(df.schema.json()),
                                'jobIdsDuringPlanning': list(spark.sparkContext.statusTracker().getJobIdsForGroup(key)),
                                'status': 'planned'})
                print(f'PLAN {key}', flush=True)
            except Exception as failure:
                records.append({'key': key, 'query': query, 'engine': args.engine, 'mode': mode,
                                'status': 'planning-failed', 'error': str(failure)})
                print(f'PLAN_FAILED {key}: {type(failure).__name__}', flush=True)
            finally:
                if query == 15:
                    spark.catalog.dropTempView('revenue0')
    report = {'capturedAt': datetime.now(timezone.utc).isoformat(), 'engine': args.engine,
              'sparkVersion': spark.version, 'appId': spark.sparkContext.applicationId,
              'queryRevision': args.revision, 'snapshots': snapshots, 'queryResultsExecuted': False,
              'q15Adaptation': 'Session temporary view replaces persistent CREATE VIEW; SELECT unchanged.',
              'config': {k: spark.conf.get(k) for k in ['spark.sql.adaptive.enabled', 'spark.sql.shuffle.partitions',
                         'spark.memory.offHeap.enabled', 'spark.memory.offHeap.size']},
              'master': spark.sparkContext.master, 'records': records}
    (output / f'{args.engine}-capture.json').write_text(json.dumps(report, indent=2) + '\n')
finally:
    spark.stop()
