#!/usr/bin/env python3
"""
FastFlow Caterpillar DAG Detector and Flow Parallelism Analyzer

Implements the caterpillar DAG detection algorithm from the FastFlow paper.
Uses JSON patterns and CSV data relationships to:
1. Extract data lineage patterns from JSON input/output patterns
2. Group tasks by actual data lineage
3. Detect independent caterpillar DAGs for parallel execution

Caterpillar DAGs: Independent data lineages that can execute in parallel
without dependencies between them, enabling workflow acceleration through
the FastFlow segmentation approach.
"""

import pandas as pd
import json
import networkx as nx
import matplotlib.pyplot as plt
from typing import Dict, List, Set, Tuple, Optional
import re
from collections import defaultdict
import argparse

class CaterpillarDAGDetector:
    """
    FastFlow caterpillar DAG detector and flow parallelism analyzer
    using only JSON patterns and CSV data relationships
    """

    def __init__(self):
        self.workflow_data = None
        self.dependencies = None
        self.dag = nx.DiGraph()
        self.data_lineage_patterns = {}
        self.caterpillar_dags = []

    def load_data(self, csv_file: str, json_file: str, num_nodes: Optional[int] = None):
        """Load and filter workflow data"""
        print(f"Loading workflow data...")

        # Load CSV
        self.workflow_data = pd.read_csv(csv_file)
        print(f"  CSV: {len(self.workflow_data)} total operations")

        # Filter by nodes if specified
        if num_nodes is not None:
            self.workflow_data = self.workflow_data[self.workflow_data['numNodes'] == num_nodes].copy()
            print(f"  Filtered for {num_nodes} nodes: {len(self.workflow_data)} operations")

        # Load JSON
        with open(json_file, 'r') as f:
            self.dependencies = json.load(f)
        print(f"  JSON: {len(self.dependencies)} workflow stages")

        return self.workflow_data, self.dependencies

    def extract_lineage_patterns_from_json(self) -> Dict[str, List[str]]:
        """
        Extract data lineage patterns from JSON input/output patterns
        This identifies what kind of data groupings to look for
        """
        print(f"\nExtracting lineage patterns from JSON dependencies...")

        lineage_patterns = []

        # Collect all input/output patterns from JSON
        for stage_name, stage_info in self.dependencies.items():
            # Input patterns from predecessors
            predecessors = stage_info.get('predecessors', {})
            for pred_stage, pred_info in predecessors.items():
                input_patterns = pred_info.get('inputs', [])
                lineage_patterns.extend(input_patterns)

            # Output patterns
            output_patterns = stage_info.get('outputs', [])
            lineage_patterns.extend(output_patterns)

        # Remove duplicates and clean patterns
        unique_patterns = list(set(lineage_patterns))

        print(f"Found {len(unique_patterns)} unique patterns:")
        for pattern in unique_patterns:
            print(f"  {pattern}")

        self.data_lineage_patterns = unique_patterns
        return unique_patterns

    def identify_lineage_variable_parts(self, patterns: List[str]) -> List[str]:
        """
        Identify variable parts in patterns that could indicate different lineages
        For example, in "chr.*n-.*-.*\\.tar\\.gz", the ".*" parts could be lineage identifiers
        """
        print(f"\nAnalyzing pattern variable parts...")

        # Look for common regex patterns that indicate variables
        variable_indicators = [
            r'\.\*',      # .* (any characters)
            r'\.\+',      # .+ (one or more characters)
            r'\[.*?\]',   # [abc] (character classes)
            r'\(.*?\)',   # (abc) (groups)
            r'\\d\+',     # \d+ (digits)
            r'\\w\+',     # \w+ (word characters)
        ]

        patterns_with_variables = []

        for pattern in patterns:
            has_variables = any(re.search(indicator, pattern) for indicator in variable_indicators)
            if has_variables:
                patterns_with_variables.append(pattern)
                print(f"  Pattern with variables: {pattern}")

        return patterns_with_variables

    def extract_lineage_groups_from_actual_files(self, variable_patterns: List[str]) -> Dict[str, Set[str]]:
        """
        Use variable patterns to group actual files from CSV into lineages
        """
        print(f"\nGrouping actual files by lineage using patterns...")

        # Get all unique files from CSV
        all_files = set(self.workflow_data['fileName'].unique())
        print(f"Total unique files: {len(all_files)}")

        lineage_groups = defaultdict(set)

        # For each file, try to match it against variable patterns
        for filename in all_files:
            assigned_lineage = None

            # Try each variable pattern
            for pattern in variable_patterns:
                # Convert JSON regex pattern to Python regex
                # Remove escape characters that are for JSON
                python_pattern = pattern.replace('\\\\', '\\')

                try:
                    match = re.search(python_pattern, filename)
                    if match:
                        # Extract the variable parts that could identify lineage
                        # Look for the first capturing group or the part that varies
                        lineage_id = self.extract_lineage_identifier_from_match(match, filename, python_pattern)

                        if lineage_id:
                            assigned_lineage = lineage_id
                            break
                except re.error:
                    # If regex is malformed, skip
                    continue

            # If matched a pattern, assign to that lineage
            if assigned_lineage:
                lineage_groups[assigned_lineage].add(filename)
            else:
                # Check if it's likely a shared/input file (doesn't match output patterns)
                is_shared = True
                for pattern in variable_patterns:
                    python_pattern = pattern.replace('\\\\', '\\')
                    try:
                        if re.search(python_pattern, filename):
                            is_shared = False
                            break
                    except re.error:
                        continue

                if is_shared:
                    lineage_groups['shared'].add(filename)
                else:
                    # Fallback grouping
                    lineage_groups['other'].add(filename)

        print(f"Identified {len(lineage_groups)} lineage groups:")
        for lineage_id, files in lineage_groups.items():
            print(f"  {lineage_id}: {len(files)} files")
            # Show sample files
            sample_files = list(files)[:3]
            for sample in sample_files:
                print(f"    {sample}")
            if len(files) > 3:
                print(f"    ... and {len(files) - 3} more")

        return dict(lineage_groups)

    def extract_lineage_identifier_from_match(self, match, filename: str, pattern: str) -> Optional[str]:
        """
        Extract lineage identifier from regex match
        Look for parts that could identify different parallel processing streams
        """
        # Strategy 1: Use capturing groups if they exist
        if match.groups():
            # Take the first meaningful group
            for group in match.groups():
                if group and len(group) > 0:
                    return f"lineage_{group}"

        # Strategy 2: Look for common identifier patterns in the filename
        # Look for number sequences, alphanumeric codes, etc.
        common_id_patterns = [
            r'(\d+)',           # Numbers
            r'([A-Za-z]+\d+)',  # Letters followed by numbers
            r'(\d+[A-Za-z]+)',  # Numbers followed by letters
        ]

        for id_pattern in common_id_patterns:
            id_matches = re.findall(id_pattern, filename)
            if id_matches:
                # Use the first significant identifier
                identifier = id_matches[0]
                if len(identifier) >= 1:  # At least 1 character
                    return f"lineage_{identifier}"

        # Strategy 3: If pattern suggests structure but no clear ID, use hash
        if '.*' in pattern or '.+' in pattern:
            # Use a hash of the filename to group similar files
            return f"lineage_{hash(filename) % 100:02d}"

        return None

    def map_tasks_to_lineages(self, lineage_groups: Dict[str, Set[str]]) -> Dict[str, Dict[str, List[str]]]:
        """
        Map task instances to lineages based on which files they operate on
        """
        print(f"\nMapping tasks to lineages...")

        lineage_tasks = defaultdict(lambda: defaultdict(list))

        # Group task operations by taskName + taskPID
        task_instances = self.workflow_data.groupby(['taskName', 'taskPID'])

        for (task_name, task_pid), task_ops in task_instances:
            task_files = set(task_ops['fileName'].unique())

            # Determine which lineage this task belongs to
            best_lineage = 'shared'
            best_overlap = 0

            for lineage_id, lineage_files in lineage_groups.items():
                if lineage_id == 'shared':
                    continue

                # Count overlap between task files and lineage files
                overlap = len(task_files & lineage_files)
                if overlap > best_overlap:
                    best_overlap = overlap
                    best_lineage = lineage_id

            # If no significant overlap with any specific lineage, assign to shared
            if best_overlap == 0:
                best_lineage = 'shared'

            task_id = f"{task_name}_{task_pid}"
            lineage_tasks[best_lineage][task_name].append(task_id)

        # Print mapping results
        for lineage_id, stages in lineage_tasks.items():
            print(f"  {lineage_id}:")
            for stage_name, tasks in stages.items():
                print(f"    {stage_name}: {len(tasks)} tasks")

        return lineage_tasks

    def build_workflow_dag(self):
        """
        Build DAG using JSON patterns and CSV data
        """
        print(f"\nBuilding workflow DAG...")

        self.dag = nx.DiGraph()

        # Extract lineage patterns from JSON
        patterns = self.extract_lineage_patterns_from_json()
        variable_patterns = self.identify_lineage_variable_parts(patterns)

        # Group files by lineage using patterns
        lineage_groups = self.extract_lineage_groups_from_actual_files(variable_patterns)

        # Map tasks to lineages
        lineage_tasks = self.map_tasks_to_lineages(lineage_groups)

        # Add data nodes with lineage information
        all_files = set(self.workflow_data['fileName'].unique())
        for filename in all_files:
            file_data = self.workflow_data[self.workflow_data['fileName'] == filename]

            # Determine data type
            has_writes = (file_data['operation'] == 'write').any()
            has_reads = (file_data['operation'] == 'read').any()

            if not has_writes and has_reads:
                data_type = 'input'
            elif has_writes and has_reads:
                data_type = 'intermediate'
            elif has_writes and not has_reads:
                data_type = 'output'
            else:
                data_type = 'unknown'

            # Find lineage
            lineage_id = 'shared'
            for lid, files in lineage_groups.items():
                if filename in files:
                    lineage_id = lid
                    break

            self.dag.add_node(f"data_{filename}",
                             node_type='data',
                             filename=filename,
                             data_type=data_type,
                             lineage=lineage_id,
                             size_mb=file_data['aggregateFilesizeMB'].mean())

        # Add task nodes with lineage information
        for lineage_id, stages in lineage_tasks.items():
            for stage_name, task_list in stages.items():
                stage_info = self.dependencies.get(stage_name, {})

                for task_id in task_list:
                    self.dag.add_node(f"task_{task_id}",
                                     node_type='task',
                                     task_name=stage_name,
                                     lineage=lineage_id,
                                     stage_order=stage_info.get('stage_order', 0),
                                     parallelism=stage_info.get('parallelism', 1))

        # Add edges from CSV operations
        for _, row in self.workflow_data.iterrows():
            task_node = f"task_{row['taskName']}_{row['taskPID']}"
            data_node = f"data_{row['fileName']}"

            if not self.dag.has_node(task_node) or not self.dag.has_node(data_node):
                continue

            flow_stats = {
                'operation': row['operation'],
                'random_offset': row['randomOffset'],
                'transfer_size': row['transferSize'],
                'aggregate_size_mb': row['aggregateFilesizeMB'],
                'num_tasks': row['numTasks'],
                'total_time': row['totalTime'],
                'num_nodes': row['numNodes'],
                'tasks_per_node': row['tasksPerNode'],
                'parallelism': row['parallelism'],
                'throughput_mib': row['trMiB'],
                'storage_type': row['storageType']
            }

            if row['operation'] == 'read':
                self.dag.add_edge(data_node, task_node, edge_type='consumer', **flow_stats)
            elif row['operation'] == 'write':
                self.dag.add_edge(task_node, data_node, edge_type='producer', **flow_stats)

        print(f"Built DAG with:")
        print(f"  Nodes: {self.dag.number_of_nodes()}")
        print(f"  Edges: {self.dag.number_of_edges()}")

    def detect_caterpillar_dags(self) -> List[Dict]:
        """
        Detect independent flow parallelism using proper data flow analysis
        """
        print(f"\nDetecting flow parallelism...")

        # Group nodes by lineage
        lineage_subgraphs = defaultdict(lambda: {'tasks': [], 'data': []})

        for node, attrs in self.dag.nodes(data=True):
            lineage = attrs.get('lineage', 'unknown')

            if node.startswith('task_'):
                lineage_subgraphs[lineage]['tasks'].append(node)
            elif node.startswith('data_'):
                lineage_subgraphs[lineage]['data'].append(node)

        self.caterpillar_dags = []

        for lineage_id, nodes in lineage_subgraphs.items():
            if lineage_id == 'shared' or not nodes['tasks']:
                continue

            # Check for cross-lineage data dependencies (the correct way)
            is_independent = self.check_caterpillar_independence(lineage_id, nodes)

            # Compute metrics
            lineage_nodes = set(nodes['tasks'] + nodes['data'])
            # Include shared data this lineage reads from
            for task_node in nodes['tasks']:
                for pred in self.dag.predecessors(task_node):
                    if self.dag.nodes[pred].get('lineage') == 'shared':
                        lineage_nodes.add(pred)

            subgraph = self.dag.subgraph(lineage_nodes).copy()

            total_time = sum(
                data.get('total_time', 0) for u, v, data in subgraph.edges(data=True)
            )

            # Critical path
            try:
                critical_path = nx.dag_longest_path(subgraph, weight='total_time')
            except:
                critical_path = []

            # Workflow stages
            stages = set()
            for task_node in nodes['tasks']:
                task_name = self.dag.nodes[task_node].get('task_name', '')
                if task_name:
                    stages.add(task_name)

            flow_info = {
                'flow_id': lineage_id,
                'is_parallel': is_independent,
                'task_count': len(nodes['tasks']),
                'data_count': len(nodes['data']),
                'total_execution_time': total_time,
                'critical_path_length': len(critical_path),
                'workflow_stages': sorted(list(stages))
            }

            self.caterpillar_dags.append(flow_info)

        # Sort by execution time
        self.caterpillar_dags.sort(key=lambda t: t['total_execution_time'], reverse=True)

        parallel_count = sum(1 for t in self.caterpillar_dags if t['is_parallel'])
        print(f"Detected {len(self.caterpillar_dags)} data flows:")
        print(f"  Parallel: {parallel_count}")
        print(f"  Sequential: {len(self.caterpillar_dags) - parallel_count}")

        return self.caterpillar_dags

    def check_caterpillar_independence(self, lineage_id: str, nodes: Dict) -> bool:
        """
        Correctly check if a lineage is independent by analyzing data flow dependencies
        """
        lineage_tasks = set(nodes['tasks'])
        lineage_data = set(nodes['data'])

        # A lineage is independent if:
        # 1. Its tasks only read from shared inputs or its own data
        # 2. Its tasks only write to its own data (not shared or other lineages)
        # 3. No other lineage's tasks read from its data

        for task_node in lineage_tasks:
            # Check what this task reads from
            for pred in self.dag.predecessors(task_node):
                if pred.startswith('data_'):
                    pred_lineage = self.dag.nodes[pred].get('lineage', 'unknown')
                    # Task can read from shared inputs or its own lineage data
                    if pred_lineage != 'shared' and pred_lineage != lineage_id:
                        return False  # Reads from another lineage

            # Check what this task writes to
            for succ in self.dag.successors(task_node):
                if succ.startswith('data_'):
                    succ_lineage = self.dag.nodes[succ].get('lineage', 'unknown')
                    # Task should only write to its own lineage
                    if succ_lineage != lineage_id:
                        return False  # Writes to shared or another lineage

        # Check if other lineages read from this lineage's data
        for data_node in lineage_data:
            for succ in self.dag.successors(data_node):
                if succ.startswith('task_'):
                    succ_lineage = self.dag.nodes[succ].get('lineage', 'unknown')
                    if succ_lineage != lineage_id:
                        return False  # Another lineage reads from this data

        return True

    def compute_lineage_statistics(self) -> Dict[str, Dict]:
        """Compute aggregate statistics for each lineage"""
        print(f"\nComputing lineage statistics...")

        lineage_stats = {}

        for flow in self.caterpillar_dags:
            lineage_id = flow['flow_id']

            # Get all edges for this lineage
            lineage_edges = []
            for node, attrs in self.dag.nodes(data=True):
                if attrs.get('lineage') == lineage_id and node.startswith('task_'):
                    # Get all edges connected to this task
                    for pred in self.dag.predecessors(node):
                        edge_data = self.dag.edges[pred, node]
                        lineage_edges.append(edge_data)
                    for succ in self.dag.successors(node):
                        edge_data = self.dag.edges[node, succ]
                        lineage_edges.append(edge_data)

            if not lineage_edges:
                continue

            # Compute aggregates
            total_transfer = sum(edge.get('transfer_size', 0) for edge in lineage_edges)
            total_time = sum(edge.get('total_time', 0) for edge in lineage_edges)
            total_data_mb = sum(edge.get('aggregate_size_mb', 0) for edge in lineage_edges)

            # Average metrics
            avg_throughput = sum(edge.get('throughput_mib', 0) for edge in lineage_edges) / len(lineage_edges)
            avg_parallelism = sum(edge.get('parallelism', 0) for edge in lineage_edges) / len(lineage_edges)
            avg_nodes = sum(edge.get('num_nodes', 0) for edge in lineage_edges) / len(lineage_edges)

            # Storage types used
            storage_types = set(edge.get('storage_type', 'unknown') for edge in lineage_edges)

            # Operation counts
            read_ops = sum(1 for edge in lineage_edges if edge.get('operation') == 'read')
            write_ops = sum(1 for edge in lineage_edges if edge.get('operation') == 'write')

            lineage_stats[lineage_id] = {
                'total_operations': len(lineage_edges),
                'read_operations': read_ops,
                'write_operations': write_ops,
                'total_transfer_bytes': total_transfer,
                'total_transfer_mb': total_transfer / (1024 * 1024) if total_transfer > 0 else 0,
                'total_execution_time': total_time,
                'total_data_mb': total_data_mb,
                'avg_throughput_mib': avg_throughput,
                'avg_parallelism': avg_parallelism,
                'avg_nodes_used': avg_nodes,
                'storage_types': list(storage_types),
                'efficiency_mb_per_sec': (total_data_mb / total_time) if total_time > 0 else 0
            }

            print(f"  {lineage_id}:")
            print(f"    Operations: {len(lineage_edges)} ({read_ops} reads, {write_ops} writes)")
            print(f"    Total transfer: {total_transfer / (1024*1024):.1f} MB")
            print(f"    Avg throughput: {avg_throughput:.1f} MiB/s")
            print(f"    Storage types: {', '.join(storage_types)}")

        return lineage_stats

    def export_results(self, output_prefix: str = "workflow_dag"):
        """Export DAG and thread analysis"""

        # Compute lineage statistics
        lineage_stats = self.compute_lineage_statistics()

        # Export DAG
        dag_data = {
            'metadata': {
                'total_nodes': self.dag.number_of_nodes(),
                'total_edges': self.dag.number_of_edges(),
            },
            'nodes': {
                node: {**self.dag.nodes[node], 'node_id': node}
                for node in self.dag.nodes()
            },
            'edges': [
                {
                    'source': u,
                    'target': v,
                    **self.dag.edges[u, v]
                } for u, v in self.dag.edges()
            ]
        }

        dag_file = f"{output_prefix}_dag.json"
        with open(dag_file, 'w') as f:
            json.dump(dag_data, f, indent=2, default=str)

        # Export flows with enhanced statistics
        flow_data = {
            'metadata': {
                'total_flows': len(self.caterpillar_dags),
                'parallel_flows': sum(1 for f in self.caterpillar_dags if f['is_parallel'])
            },
            'flows': self.caterpillar_dags,
            'lineage_statistics': lineage_stats
        }

        flow_file = f"{output_prefix}_flows.json"
        with open(flow_file, 'w') as f:
            json.dump(flow_data, f, indent=2, default=str)

        # Export statistics summary
        stats_file = f"{output_prefix}_statistics.json"
        with open(stats_file, 'w') as f:
            json.dump(lineage_stats, f, indent=2, default=str)

        print(f"Results exported:")
        print(f"  DAG: {dag_file}")
        print(f"  Flows: {flow_file}")
        print(f"  Statistics: {stats_file}")

        return dag_file, flow_file, stats_file

def main():
    parser = argparse.ArgumentParser(description="Workflow-Agnostic DAG Constructor and Flow Parallelism Detector")

    parser.add_argument('csv_file', help='CSV file with workflow execution data')
    parser.add_argument('json_file', help='JSON file with workflow dependencies')
    parser.add_argument('-n', '--num-nodes', type=int, help='Filter CSV for specific number of nodes')
    parser.add_argument('-o', '--output-prefix', default='clean_workflow', help='Output file prefix')

    args = parser.parse_args()

    print("=== FastFlow Caterpillar DAG Detection ===\n")

    analyzer = CaterpillarDAGDetector()
    analyzer.load_data(args.csv_file, args.json_file, args.num_nodes)
    analyzer.build_workflow_dag()
    flows = analyzer.detect_caterpillar_dags()

    print(f"\n=== Results ===")
    parallel_flows = [f for f in flows if f['is_parallel']]

    for i, flow in enumerate(parallel_flows):
        print(f"Parallel Flow {i+1}: {flow['flow_id']}")
        print(f"  Tasks: {flow['task_count']}")
        print(f"  Execution time: {flow['total_execution_time']:.1f}s")
        print(f"  Stages: {', '.join(flow['workflow_stages'])}")

    if len(parallel_flows) > 1:
        total_time = sum(f['total_execution_time'] for f in flows)
        max_time = max(f['total_execution_time'] for f in parallel_flows)
        speedup = total_time / max_time

        print(f"\n=== Flow Parallelism Potential ===")
        print(f"Parallel flows: {len(parallel_flows)}")
        print(f"Estimated speedup: {speedup:.2f}x")
       

    analyzer.export_results(args.output_prefix)

if __name__ == "__main__":
    main()