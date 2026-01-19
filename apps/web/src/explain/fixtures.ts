export const EXPLAIN_TREE_FIXTURE_SIMPLE = `[00]:[0: ResultSink]||[Fragment: 0]||VRESULT SINK||   MYSQL_PROTOCAL||
--[00]:[0: VUNION]||[Fragment: 0]||



========== STATISTICS ==========
`;

export const EXPLAIN_TREE_FIXTURE_MULTI_FRAGMENT = `Explain String(Nereids Planner)
[05]:[5: ResultSink]||[Fragment: 0]||VRESULT SINK||MYSQL_PROTOCAL||
--[05]:[5: VMERGING-EXCHANGE]||[Fragment: 0]||offset: 0||
----[09]:[9: DataStreamSink]||[Fragment: 1]||STREAM DATA SINK||EXCHANGE ID: 05||UNPARTITIONED
------[04]:[4: VTOP-N]||[Fragment: 1]||
--------[03]:[3: VAGGREGATE (merge finalize)]||[Fragment: 1]||cardinality=3||
----------[02]:[2: VEXCHANGE]||[Fragment: 1]||offset: 0||
------------[02]:[2: DataStreamSink]||[Fragment: 2]||STREAM DATA SINK||EXCHANGE ID: 02||HASH_PARTITIONED
--------------[00]:[0: VOlapScanNode]||[Fragment: 2]||TABLE: tpch.lineitem(lineitem)||cardinality=149,996,355||afterFilter=1,841,539||PREDICATES: 2||
========== STATISTICS ==========
`;
