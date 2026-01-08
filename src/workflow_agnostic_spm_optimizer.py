#!/usr/bin/env python3
"""
Workflow-Agnostic SPM Storage Optimizer

Integrates SPM (Storage Performance Matching) analysis with flow parallelism detection
without any hardcoded workflow assumptions. Works with any workflow structure.

Key concepts:
- DAG edge demand: Performance requirements extracted from actual workflow data
- SPM storage supply: Available storage configurations and their performance characteristics
- Task grouping: Optimal placement based on data-driven analysis, not hardcoded rules
"""

import pandas as pd
import json
import numpy as np
from typing import Dict, List, Tuple, Optional, Set
import argparse
from collections import defaultdict

from execution_plan_generator import ExecutionPlanGenerator

class WorkflowAgnosticSPMOptimizer:
    """
    Workflow-agnostic storage optimizer using SPM analysis results
    """

    def __init__(self, enable_segmentation=False, storage_constraints_config=None):
        self.spm_data = None
        self.dag_flows = None
        self.dag_statistics = None
        self.dag_data = None
        self.enable_segmentation = enable_segmentation
        self.horizontal_segments = {}
        self.vertical_segments = {}
        # Storage constraints configuration with defaults
        self.storage_constraints_config = storage_constraints_config or self._get_default_storage_constraints()

    def _get_default_storage_constraints(self):
        """
        Default storage constraint profiles for horizontal segmentation.
        These represent per-node resource allocations that encourage meaningful task grouping.
        """
        return {
            'constraint_mode': 'per_node_allocation',  # 'per_node_allocation' or 'total_capacity'
            'enable_horizontal_segmentation': True,
            'storage_profiles': {
                'tmpfs': {
                    'bandwidth_limit_mib': 300,     # Per-node RAM bandwidth allocation
                    'capacity_limit_mb': 1024,      # Per-node RAM capacity allocation
                    'iops_limit': 15000,            # Per-node IOPS allocation
                    'latency_ms': 0.001,            # Memory latency
                    'cost_factor': 3.0,             # High cost for RAM usage
                    'description': 'Memory-based storage with high performance but limited capacity'
                },
                'ssd': {
                    'bandwidth_limit_mib': 200,     # Per-node SSD bandwidth allocation
                    'capacity_limit_mb': 2048,      # Per-node SSD capacity allocation
                    'iops_limit': 10000,            # Per-node IOPS allocation
                    'latency_ms': 0.1,              # SSD latency
                    'cost_factor': 1.5,             # Medium cost
                    'description': 'SSD storage with balanced performance and capacity'
                },
                'beegfs': {
                    'bandwidth_limit_mib': 150,     # Per-node network bandwidth allocation
                    'capacity_limit_mb': 4096,      # Per-node network storage allocation
                    'iops_limit': 3000,             # Per-node network IOPS allocation
                    'latency_ms': 5.0,              # Network latency
                    'cost_factor': 1.0,             # Low cost baseline
                    'description': 'Network filesystem with large capacity but lower performance'
                },
                'beegfs-ssd': {
                    'bandwidth_limit_mib': 250,     # Per-node hybrid bandwidth allocation
                    'capacity_limit_mb': 3072,      # Per-node hybrid capacity allocation
                    'iops_limit': 8000,             # Per-node hybrid IOPS allocation
                    'latency_ms': 2.0,              # Hybrid latency
                    'cost_factor': 1.2,             # Moderate cost
                    'description': 'Hybrid storage with balanced performance and cost'
                }
            },
            'segmentation_settings': {
                # Horizontal segmentation settings
                'min_tasks_per_segment': 1,         # Minimum tasks in a horizontal segment
                'max_tasks_per_segment': 50,        # Maximum tasks in a horizontal segment
                'prefer_even_distribution': True,   # Try to balance segments
                'bandwidth_utilization_target': 0.8, # Target 80% bandwidth utilization
                'capacity_utilization_target': 0.7,  # Target 70% capacity utilization
                'iops_utilization_target': 0.8,     # Target 80% IOPS utilization
                # Vertical segmentation settings
                'max_vertical_segment_data_mb': 50,  # Maximum data volume per vertical segment
                'max_vertical_segment_tasks': 10,    # Maximum tasks per vertical segment
                'enable_producer_consumer_pairing': True,  # Enable FastFlow-style task pairing
                'locality_optimization_strategy': 'data_volume_driven'  # Strategy for co-location
            }
        }

    def update_storage_constraints(self, storage_constraints_config: Dict):
        """
        Update storage constraints configuration

        Args:
            storage_constraints_config: Dictionary containing storage profiles and segmentation settings
        """
        self.storage_constraints_config = storage_constraints_config

    def get_storage_constraints_config(self) -> Dict:
        """
        Get current storage constraints configuration
        """
        return self.storage_constraints_config.copy()

    def load_spm_data(self, spm_csv_path: str):
        """Load SPM analysis results"""
        print(f"Loading SPM data from {spm_csv_path}...")
        self.spm_data = pd.read_csv(spm_csv_path)
        print(f"  Loaded {len(self.spm_data)} SPM configurations")

        # Extract unique producer-consumer pairs from SPM data (workflow-agnostic)
        unique_pairs = self.spm_data[['producer', 'consumer']].drop_duplicates()
        print(f"  Found {len(unique_pairs)} unique producer-consumer pairs in SPM data")

        return self.spm_data

    def load_dag_analysis(self, flows_json: str, stats_json: str, dag_json: str):
        """Load DAG flow analysis, statistics, and full DAG structure"""
        print(f"Loading DAG analysis...")

        with open(flows_json, 'r') as f:
            flow_data = json.load(f)
            self.dag_flows = flow_data['flows']
            print(f"  Loaded {len(self.dag_flows)} parallel flows")

        with open(stats_json, 'r') as f:
            self.dag_statistics = json.load(f)
            print(f"  Loaded statistics for {len(self.dag_statistics)} lineages")

        with open(dag_json, 'r') as f:
            self.dag_data = json.load(f)
            print(f"  Loaded DAG with {self.dag_data['metadata']['total_nodes']} nodes and {self.dag_data['metadata']['total_edges']} edges")

        return self.dag_flows, self.dag_statistics, self.dag_data

    def extract_workflow_stage_transitions(self) -> Dict[Tuple[str, str], List[Dict]]:
        """
        Extract actual stage transitions from DAG edges, with JSON fallback for missing stages
        """
        print(f"Extracting workflow stage transitions from DAG...")

        stage_transitions = defaultdict(list)

        # APPROACH 1: Analyze actual edges in the DAG to find producer-consumer relationships
        # We need to trace through data nodes to find task-to-task stage transitions
        for edge in self.dag_data['edges']:
            source_node = edge['source']
            target_node = edge['target']

            # Get node information
            source_info = self.dag_data['nodes'].get(source_node, {})
            target_info = self.dag_data['nodes'].get(target_node, {})

            # Handle task -> data -> task transitions
            if source_info.get('node_type') == 'task' and target_info.get('node_type') == 'data':
                # This is a task producing data
                producer_stage = source_info.get('task_name', 'unknown')
                data_node = target_node

                # Find all tasks that consume this data
                for consumer_edge in self.dag_data['edges']:
                    if consumer_edge['source'] == data_node:
                        consumer_node = consumer_edge['target']
                        consumer_info = self.dag_data['nodes'].get(consumer_node, {})

                        if consumer_info.get('node_type') == 'task':
                            consumer_stage = consumer_info.get('task_name', 'unknown')

                            if producer_stage != 'unknown' and consumer_stage != 'unknown' and producer_stage != consumer_stage:
                                transition = (producer_stage, consumer_stage)
                                stage_transitions[transition].append({
                                    'producer_task': source_node,
                                    'consumer_task': consumer_node,
                                    'data_file': data_node,
                                    'producer_lineage': source_info.get('lineage', 'unknown'),
                                    'consumer_lineage': consumer_info.get('lineage', 'unknown'),
                                    'edge_data': edge
                                })

        # APPROACH 2: Fallback to JSON dependencies if DAG analysis found no transitions
        # This handles workflows where stages are missing from DAG due to incomplete CSV data
        if len(stage_transitions) == 0:
            print(f"  No transitions found in DAG, extracting from JSON dependencies...")
            json_transitions = self._extract_transitions_from_json_dependencies()
            stage_transitions.update(json_transitions)

        print(f"  Found {len(stage_transitions)} unique stage transitions:")
        for transition, edges in stage_transitions.items():
            print(f"    {transition[0]} -> {transition[1]}: {len(edges)} edges")

        return dict(stage_transitions)

    def _extract_transitions_from_json_dependencies(self) -> Dict[Tuple[str, str], List[Dict]]:
        """
        Workflow-agnostic extraction of stage transitions from JSON dependencies
        """
        stage_transitions = defaultdict(list)

        # Load original JSON dependencies from DAG metadata
        if 'metadata' in self.dag_data and 'original_dependencies' in self.dag_data['metadata']:
            dependencies = self.dag_data['metadata']['original_dependencies']
        else:
            print("    Warning: No JSON dependencies found in DAG metadata")
            return {}

        # Extract stage transitions from JSON predecessor relationships
        for stage_name, stage_info in dependencies.items():
            predecessors = stage_info.get('predecessors', {})

            for predecessor_stage, predecessor_info in predecessors.items():
                # Skip initial_data and other non-stage dependencies
                if predecessor_stage != 'initial_data' and predecessor_stage in dependencies:
                    transition = (predecessor_stage, stage_name)

                    # Create a synthetic transition entry
                    stage_transitions[transition].append({
                        'producer_task': f"json_stage_{predecessor_stage}",
                        'consumer_task': f"json_stage_{stage_name}",
                        'data_file': f"json_dependency_{predecessor_stage}_to_{stage_name}",
                        'producer_lineage': 'json_derived',
                        'consumer_lineage': 'json_derived',
                        'edge_data': {
                            'source': f"json_stage_{predecessor_stage}",
                            'target': f"json_stage_{stage_name}",
                            'dependency_type': 'json_defined'
                        }
                    })

        print(f"    Extracted {len(stage_transitions)} transitions from JSON dependencies")
        return dict(stage_transitions)

    def match_dag_transitions_to_spm_pairs(self, dag_transitions: Dict) -> Dict:
        """
        Match DAG stage transitions to available SPM producer-consumer pairs
        """
        print(f"\\nMatching DAG transitions to SPM pairs...")

        # Get all available SPM pairs
        spm_pairs = set(zip(self.spm_data['producer'], self.spm_data['consumer']))
        dag_stage_pairs = set(dag_transitions.keys())

        matches = {}
        unmatched_dag = []

        for dag_pair in dag_stage_pairs:
            # Try exact match first
            if dag_pair in spm_pairs:
                matches[dag_pair] = dag_pair
                print(f"  Exact match: {dag_pair[0]} -> {dag_pair[1]}")
            else:
                # Try partial/fuzzy matching
                best_match = self.find_best_spm_match(dag_pair, spm_pairs)
                if best_match:
                    matches[dag_pair] = best_match
                    print(f"  Fuzzy match: {dag_pair[0]} -> {dag_pair[1]} ≈ {best_match[0]} -> {best_match[1]}")
                else:
                    unmatched_dag.append(dag_pair)
                    print(f"  No match: {dag_pair[0]} -> {dag_pair[1]}")

        if unmatched_dag:
            print(f"  Warning: {len(unmatched_dag)} DAG transitions have no SPM data")

        return matches

    def find_best_spm_match(self, dag_pair: Tuple[str, str], spm_pairs: set) -> Optional[Tuple[str, str]]:
        """
        Find best SPM pair match for a DAG transition using similarity
        """
        dag_producer, dag_consumer = dag_pair

        best_match = None
        best_score = 0

        for spm_producer, spm_consumer in spm_pairs:
            # Calculate similarity score (simple string similarity)
            producer_score = self.calculate_string_similarity(dag_producer, spm_producer)
            consumer_score = self.calculate_string_similarity(dag_consumer, spm_consumer)

            combined_score = (producer_score + consumer_score) / 2

            if combined_score > best_score and combined_score > 0.5:  # Threshold for acceptable match
                best_score = combined_score
                best_match = (spm_producer, spm_consumer)

        return best_match

    def calculate_string_similarity(self, str1: str, str2: str) -> float:
        """
        Calculate simple string similarity (can be improved with better algorithms)
        """
        if str1 == str2:
            return 1.0

        # Simple substring matching
        str1_lower = str1.lower()
        str2_lower = str2.lower()

        if str1_lower in str2_lower or str2_lower in str1_lower:
            return 0.8

        # Check for common words
        words1 = set(str1_lower.split('_'))
        words2 = set(str2_lower.split('_'))

        if words1 & words2:  # Common words
            return 0.6

        return 0.0

    def analyze_edge_performance_demand(self) -> Dict:
        """
        Analyze performance demand from DAG edges (workflow-agnostic)
        """
        print(f"\\nAnalyzing edge performance demand...")

        demand_analysis = {
            'flow_demands': {},
            'edge_characteristics': {},
            'performance_requirements': {}
        }

        # Analyze performance characteristics for each flow
        for flow in self.dag_flows:
            flow_id = flow['flow_id']

            if flow_id not in self.dag_statistics:
                continue

            stats = self.dag_statistics[flow_id]

            # Extract performance demand characteristics
            demand = {
                'total_transfer_mb': stats.get('total_transfer_mb', 0),
                'avg_throughput_mib': stats.get('avg_throughput_mib', 0),
                'total_operations': stats.get('total_operations', 0),
                'read_operations': stats.get('read_operations', 0),
                'write_operations': stats.get('write_operations', 0),
                'efficiency_mb_per_sec': stats.get('efficiency_mb_per_sec', 0),
                'storage_types_used': stats.get('storage_types', [])
            }

            # Calculate derived metrics
            if demand['write_operations'] > 0:
                demand['read_write_ratio'] = demand['read_operations'] / demand['write_operations']
            else:
                demand['read_write_ratio'] = float('inf')

            demand['io_intensity'] = demand['total_operations'] / max(1, flow['task_count'])

            demand_analysis['flow_demands'][flow_id] = demand

            print(f"  {flow_id}:")
            print(f"    Transfer: {demand['total_transfer_mb']:.1f} MB")
            print(f"    Throughput: {demand['avg_throughput_mib']:.1f} MiB/s")
            print(f"    Operations: {demand['total_operations']} ({demand['read_operations']} reads, {demand['write_operations']} writes)")
            print(f"    IO intensity: {demand['io_intensity']:.1f} ops/task")

        return demand_analysis

    def find_optimal_storage_configurations(self, demand_analysis: Dict) -> Dict:
        """
        Find optimal storage configurations based on performance demand
        """
        print(f"\\nFinding optimal storage configurations...")

        optimization_results = {}

        for flow_id, demand in demand_analysis['flow_demands'].items():

            # Classify demand level based on metrics
            demand_level = self.classify_demand_level(demand)

            # Find best storage configurations for this demand level
            optimal_configs = self.get_storage_configs_for_demand(demand_level, demand)

            optimization_results[flow_id] = {
                'demand_level': demand_level,
                'demand_characteristics': demand,
                'recommended_configs': optimal_configs
            }

            print(f"  {flow_id} ({demand_level} demand):")
            if optimal_configs:
                best = optimal_configs[0]
                print(f"    Best config: {best['producer_storage']} -> {best['consumer_storage']}")
                print(f"    SPM score: {best['spm_score']:.3f}")
                print(f"    Tasks/node: {best['producer_tasks_per_node']}->{best['consumer_tasks_per_node']}")

        return optimization_results

    def classify_demand_level(self, demand: Dict) -> str:
        """
        Classify demand level based on performance characteristics
        """
        throughput = demand['avg_throughput_mib']
        io_intensity = demand['io_intensity']
        transfer_size = demand['total_transfer_mb']

        # Dynamic thresholds based on data distribution
        if throughput > 1000 or io_intensity > 10 or transfer_size > 100:
            return 'high'
        elif throughput > 500 or io_intensity > 5 or transfer_size > 50:
            return 'medium'
        else:
            return 'low'

    def get_storage_configs_for_demand(self, demand_level: str, demand: Dict) -> List[Dict]:
        """
        Get storage configurations suitable for the given demand level
        """
        # Define storage preferences based on demand level (not workflow-specific)
        if demand_level == 'high':
            preferred_storage = ['tmpfs', 'ssd', 'beegfs-tmpfs', 'beegfs-ssd', 'ssd-ssd']
        elif demand_level == 'medium':
            preferred_storage = ['ssd', 'beegfs-ssd', 'tmpfs', 'ssd-beegfs', 'ssd-ssd']
        else:  # low
            preferred_storage = ['beegfs', 'ssd', 'beegfs-ssd', 'beegfs-beegfs']

        configs = []

        # Search SPM data for configurations with preferred storage types
        for prod_storage in preferred_storage:
            for cons_storage in preferred_storage:
                matching = self.spm_data[
                    (self.spm_data['producerStorageType'] == prod_storage) &
                    (self.spm_data['consumerStorageType'] == cons_storage)
                ]

                if not matching.empty:
                    # Get best configuration (minimum SPM)
                    best = matching.loc[matching['SPM'].idxmin()]

                    configs.append({
                        'producer_storage': prod_storage,
                        'consumer_storage': cons_storage,
                        'smp_score': best['SPM'],
                        'producer_tasks_per_node': best['producerTasksPerNode'],
                        'consumer_tasks_per_node': best['consumerTasksPerNode'],
                        'producer_stage': best['producer'],
                        'consumer_stage': best['consumer']
                    })

        # Sort by SPM score (lower is better)
        configs.sort(key=lambda x: x['smp_score'])

        return configs[:5]  # Return top 5 configurations

    def design_node_task_allocation(self, optimization_results: Dict) -> Dict:
        """
        Design task allocation per node based on SPM analysis (workflow-agnostic)
        """
        print(f"\\nDesigning node task allocation...")

        allocation_plan = {
            'node_assignments': {},
            'resource_utilization': {},
            'parallelism_recommendations': {}
        }

        for flow_id in optimization_results:
            flow_info = next(f for f in self.dag_flows if f['flow_id'] == flow_id)
            total_tasks = flow_info['task_count']

            configs = optimization_results[flow_id]['recommended_configs']

            if not configs:
                continue

            # Use best configuration for task allocation
            best_config = configs[0]

            # Calculate optimal node allocation
            recommended_allocation = self.calculate_node_allocation(
                total_tasks,
                best_config['producer_tasks_per_node'],
                best_config['consumer_tasks_per_node']
            )

            allocation_plan['node_assignments'][flow_id] = recommended_allocation

            print(f"  {flow_id} ({total_tasks} tasks):")
            print(f"    Recommended: {recommended_allocation['nodes_needed']} nodes")
            print(f"    Tasks per node: {recommended_allocation['effective_tasks_per_node']}")
            print(f"    Resource efficiency: {recommended_allocation['efficiency']:.1%}")

        return allocation_plan

    def calculate_node_allocation(self, total_tasks: int, prod_tasks_per_node: int, cons_tasks_per_node: int) -> Dict:
        """
        Calculate optimal node allocation for given task counts
        """
        # Use the more restrictive tasks per node limit
        effective_tasks_per_node = min(prod_tasks_per_node, cons_tasks_per_node)

        if effective_tasks_per_node <= 0:
            effective_tasks_per_node = total_tasks  # Fallback

        # Calculate minimum nodes needed
        nodes_needed = max(1, int(np.ceil(total_tasks / effective_tasks_per_node)))

        # Calculate efficiency (how well we utilize the nodes)
        total_capacity = nodes_needed * effective_tasks_per_node
        efficiency = total_tasks / total_capacity

        return {
            'nodes_needed': nodes_needed,
            'effective_tasks_per_node': effective_tasks_per_node,
            'total_capacity': total_capacity,
            'efficiency': efficiency
        }

    def calculate_inter_stage_transfer_costs(self) -> Dict:
        """
        Calculate data transfer costs between workflow stages
        """
        print(f"\\nCalculating inter-stage transfer costs...")

        transfer_analysis = {
            'storage_transition_costs': {},
            'stage_transition_costs': {},
            'optimization_opportunities': {}
        }

        # Analyze storage transition costs from SPM data
        transitions = self.spm_data.groupby(['producerStorageType', 'consumerStorageType']).agg({
            'SPM': ['mean', 'min', 'max', 'std', 'count']
        }).round(4)

        transitions.columns = ['mean_spm', 'min_smp', 'max_smp', 'std_spm', 'count']
        transitions = transitions.sort_values('mean_spm')

        print(f"  Storage transition costs (mean SPM):")
        for (prod, cons), row in transitions.head(10).iterrows():
            cost_info = {
                'mean_cost': row['mean_smp'],
                'min_cost': row['min_spm'],
                'max_cost': row['max_smp'],
                'std_cost': row['std_smp'],
                'sample_count': row['count']
            }

            transfer_analysis['storage_transition_costs'][f"{prod}->{cons}"] = cost_info
            print(f"    {prod:12s} -> {cons:12s}: {row['mean_smp']:6.3f} (±{row['std_smp']:5.3f})")

        return transfer_analysis

    def build_workflow_pipeline_graph(self, dag_transitions: Dict) -> Dict:
        """
        Build workflow pipeline graph to understand stage ordering and dependencies
        """
        print(f"\\nBuilding workflow pipeline graph...")

        pipeline = {
            'stages': set(),
            'transitions': [],
            'stage_order': {},
            'dependencies': defaultdict(list)
        }

        # Extract all unique stages and their transitions
        for (producer, consumer), edges in dag_transitions.items():
            pipeline['stages'].add(producer)
            pipeline['stages'].add(consumer)
            pipeline['transitions'].append((producer, consumer))
            pipeline['dependencies'][consumer].append(producer)

        # Determine stage ordering (topological sort)
        stage_order = self.determine_stage_order(pipeline['stages'], pipeline['transitions'])
        pipeline['stage_order'] = stage_order

        print(f"  Workflow pipeline stages: {list(pipeline['stages'])}")
        print(f"  Stage order: {[stage for stage, _ in sorted(stage_order.items(), key=lambda x: x[1])]}")

        return pipeline

    def determine_stage_order(self, stages: set, transitions: List[Tuple[str, str]]) -> Dict[str, int]:
        """
        Determine stage order using topological sorting
        """
        # Build dependency graph
        dependencies = defaultdict(set)
        dependents = defaultdict(set)

        for producer, consumer in transitions:
            dependencies[consumer].add(producer)
            dependents[producer].add(consumer)

        # Topological sort
        stage_order = {}
        visited = set()
        order_counter = 0

        def visit_stage(stage):
            nonlocal order_counter
            if stage in visited:
                return stage_order.get(stage, 0)

            visited.add(stage)

            # Visit all dependencies first
            max_dep_order = -1
            for dep in dependencies[stage]:
                dep_order = visit_stage(dep)
                max_dep_order = max(max_dep_order, dep_order)

            # Assign order after all dependencies
            stage_order[stage] = max_dep_order + 1
            order_counter = max(order_counter, stage_order[stage])
            return stage_order[stage]

        # Process all stages
        for stage in stages:
            visit_stage(stage)

        return stage_order

    def generate_end_to_end_storage_configurations(self, pipeline: Dict, spm_matches: Dict) -> List[Dict]:
        """
        Generate all possible end-to-end storage configurations across the workflow
        """
        print(f"\\nGenerating end-to-end storage configurations...")

        # Get ordered stages
        ordered_stages = sorted(pipeline['stage_order'].items(), key=lambda x: x[1])
        stage_sequence = [stage for stage, _ in ordered_stages]

        print(f"  Stage sequence: {' -> '.join(stage_sequence)}")

        # Get storage options for each transition
        transition_options = {}
        for (producer, consumer) in pipeline['transitions']:
            if (producer, consumer) in spm_matches:
                spm_pair = spm_matches[(producer, consumer)]

                # Get all storage configurations for this SPM pair
                matching_configs = self.spm_data[
                    (self.spm_data['producer'] == spm_pair[0]) &
                    (self.spm_data['consumer'] == spm_pair[1])
                ]

                if not matching_configs.empty:
                    configs = []
                    for _, config in matching_configs.iterrows():
                        configs.append({
                            'producer_storage': config['producerStorageType'],
                            'consumer_storage': config['consumerStorageType'],
                            'producer_tasks_per_node': config['producerTasksPerNode'],
                            'consumer_tasks_per_node': config['consumerTasksPerNode'],
                            'spm_cost': config['SPM'],
                            'est_time_prod': config.get('estT_prod', 0),
                            'est_time_cons': config.get('estT_cons', 0)
                        })

                    transition_options[(producer, consumer)] = configs

        print(f"  Found storage options for {len(transition_options)} transitions")

        # Generate combinations of storage assignments across the workflow
        end_to_end_configs = self.generate_storage_combinations(transition_options, stage_sequence)

        print(f"  Generated {len(end_to_end_configs)} end-to-end configurations")
        return end_to_end_configs

    def generate_storage_combinations(self, transition_options: Dict, stage_sequence: List[str]) -> List[Dict]:
        """
        Generate combinations of storage configurations ensuring consistency
        """
        from itertools import product

        # Build transition sequence
        transitions = []
        for i in range(len(stage_sequence) - 1):
            transition = (stage_sequence[i], stage_sequence[i + 1])
            if transition in transition_options:
                transitions.append(transition)

        if not transitions:
            return []

        # Get all option combinations
        option_lists = [transition_options[trans] for trans in transitions]

        # Limit combinations to avoid explosion (take best N options per transition)
        max_options_per_transition = 5
        limited_option_lists = [options[:max_options_per_transition] for options in option_lists]

        combinations = []
        for combo in product(*limited_option_lists):
            config = {
                'transitions': [],
                'total_spm_cost': 0,
                'storage_consistency_violations': 0,
                'data_transfer_penalties': 0
            }

            for i, (transition, option) in enumerate(zip(transitions, combo)):
                transition_config = {
                    'transition': transition,
                    'storage_config': option,
                    'stage_order': i
                }
                config['transitions'].append(transition_config)
                config['total_spm_cost'] += option['spm_cost']

            # Check storage consistency and add transfer penalties
            config = self.evaluate_storage_consistency(config)

            combinations.append(config)

        return combinations

    def evaluate_storage_consistency(self, config: Dict) -> Dict:
        """
        Evaluate storage consistency across transitions and add transfer penalties
        """
        transitions = config['transitions']
        violations = 0
        transfer_penalties = 0

        for i in range(len(transitions) - 1):
            current_trans = transitions[i]
            next_trans = transitions[i + 1]

            # Check if consumer storage of current matches producer storage of next
            current_consumer_storage = current_trans['storage_config']['consumer_storage']
            next_producer_storage = next_trans['storage_config']['producer_storage']

            if current_consumer_storage != next_producer_storage:
                violations += 1

                # Add transfer penalty based on storage type mismatch
                penalty = self.calculate_transfer_penalty(current_consumer_storage, next_producer_storage)
                transfer_penalties += penalty

        config['storage_consistency_violations'] = violations
        config['data_transfer_penalties'] = transfer_penalties
        config['adjusted_total_cost'] = config['total_spm_cost'] + transfer_penalties

        return config

    def calculate_transfer_penalty(self, from_storage: str, to_storage: str) -> float:
        """
        Calculate penalty for data transfer between different storage types
        """
        # Define transfer costs based on storage type characteristics
        transfer_costs = {
            ('beegfs', 'ssd'): 2.0,       # Network to local
            ('beegfs', 'tmpfs'): 3.0,     # Network to memory
            ('ssd', 'beegfs'): 1.5,       # Local to network
            ('ssd', 'tmpfs'): 1.0,        # Local to memory
            ('tmpfs', 'beegfs'): 2.5,     # Memory to network
            ('tmpfs', 'ssd'): 1.0,        # Memory to local
            ('beegfs', 'beegfs'): 0.1,    # Same storage
            ('ssd', 'ssd'): 0.1,          # Same storage
            ('tmpfs', 'tmpfs'): 0.05      # Same storage (fastest)
        }

        # Handle compound storage types
        from_base = from_storage.split('-')[0] if '-' in from_storage else from_storage
        to_base = to_storage.split('-')[0] if '-' in to_storage else to_storage

        return transfer_costs.get((from_base, to_base), 1.0)  # Default penalty

    def generate_caterpillar_segments(self, storage_configs: List[Dict]) -> Dict:
        """
        Generate caterpillar segments (horizontal and vertical) based on SPM constraints
        """
        if not self.enable_segmentation:
            return {'horizontal_segments': {}, 'vertical_segments': {}, 'segmentation_analysis': {}}

        print(f"\nGenerating caterpillar segments based on SPM constraints...")

        segmentation_results = {
            'horizontal_segments': {},
            'vertical_segments': {},
            'segmentation_analysis': {}
        }

        # Generate segments for each flow
        for flow in self.dag_flows:
            flow_id = flow['flow_id']

            # Generate horizontal segments (parallelism-focused, IO constraint-based)
            horizontal_segs = self.generate_horizontal_segments(flow_id, storage_configs)
            segmentation_results['horizontal_segments'][flow_id] = horizontal_segs

            # Generate vertical segments (locality-focused, file placement)
            vertical_segs = self.generate_vertical_segments(flow_id, storage_configs)
            segmentation_results['vertical_segments'][flow_id] = vertical_segs

            print(f"  {flow_id}: {len(horizontal_segs)} horizontal, {len(vertical_segs)} vertical segments")

        # Analyze segmentation trade-offs
        segmentation_results['segmentation_analysis'] = self.analyze_segmentation_tradeoffs(
            segmentation_results['horizontal_segments'],
            segmentation_results['vertical_segments']
        )

        return segmentation_results

    def generate_horizontal_segments(self, flow_id: str, storage_configs: List[Dict]) -> List[Dict]:
        """
        SPM-FIRST APPROACH: Generate horizontal segments based on SPM storage assignments
        and comprehensive storage constraints (bandwidth, capacity, IOPS, latency)
        """
        horizontal_segments = []

        if not storage_configs or flow_id not in self.dag_statistics:
            return horizontal_segments

        flow_stats = self.dag_statistics[flow_id]
        stage_tasks = self.group_tasks_by_stage(flow_id)

        # STEP 1: SPM-First - Use SPM storage assignments as foundation
        best_storage_config = storage_configs[0]  # Top-ranked SPM configuration

        # STEP 2: Find stages with multiple tasks (candidates for horizontal segmentation)
        parallel_stages = {stage: tasks for stage, tasks in stage_tasks.items() if len(tasks) > 1}

        if not parallel_stages:
            return horizontal_segments

        # STEP 3: Extract storage assignments for parallel stages from SPM transitions
        stage_storage_assignments = self._extract_stage_storage_assignments(best_storage_config, parallel_stages)

        # STEP 4: Generate horizontal segments for each parallel stage
        for stage_name, tasks_in_stage in parallel_stages.items():
            # Get SPM storage assignment for this stage
            spm_storage_info = stage_storage_assignments.get(stage_name, {
                'producer_storage': 'ssd',
                'consumer_storage': 'ssd',
                'spm_cost': 0.1,
                'expected_throughput_mib': 200,
                'producer_tasks_per_node': 1
            })

            # STEP 5: Extract comprehensive storage constraints from SPM
            storage_constraints = self.extract_storage_constraints(spm_storage_info, flow_stats)

            # STEP 6: Calculate per-task resource demands
            task_demands = self.calculate_per_task_demands(flow_id, stage_name, tasks_in_stage, flow_stats)

            # STEP 7: Determine optimal task grouping based on ALL storage constraints
            task_groups = self.create_storage_constrained_task_groups(
                tasks_in_stage, task_demands, storage_constraints
            )

            # STEP 8: Create horizontal segments from task groups
            for group_id, task_group in enumerate(task_groups):
                segment_demands = self.calculate_segment_demands(task_group, task_demands)

                horizontal_segments.append({
                    'segment_type': 'horizontal',
                    'segment_id': f"{stage_name}_group_{group_id}",
                    'stage_name': stage_name,
                    'stage_type': 'parallel_stage',
                    'spm_storage_assignment': {
                        'producer_storage': spm_storage_info.get('producer_storage'),
                        'consumer_storage': spm_storage_info.get('consumer_storage'),
                        'spm_cost': spm_storage_info.get('spm_cost', 0)
                    },
                    'task_group': {
                        'task_count': len(task_group),
                        'task_ids': task_group[:5],  # Sample for readability
                        'tasks_per_node': len(task_group)
                    },
                    'storage_constraints_satisfied': {
                        'bandwidth_utilization': segment_demands['bandwidth_mib'] / storage_constraints['bandwidth_limit_mib'],
                        'capacity_utilization': segment_demands['storage_mb'] / storage_constraints['capacity_limit_mb'],
                        'iops_utilization': segment_demands['iops'] / storage_constraints['iops_limit'],
                        'within_limits': self.check_storage_limits(segment_demands, storage_constraints)
                    },
                    'performance_characteristics': {
                        'expected_bandwidth_mib': segment_demands['bandwidth_mib'],
                        'storage_required_mb': segment_demands['storage_mb'],
                        'iops_required': segment_demands['iops'],
                        'estimated_latency_ms': segment_demands['latency_ms']
                    },
                    'parallelism_analysis': {
                        'max_parallelism': len(task_group),
                        'bandwidth_bottleneck': segment_demands['bandwidth_mib'] >= storage_constraints['bandwidth_limit_mib'] * 0.8,
                        'capacity_bottleneck': segment_demands['storage_mb'] >= storage_constraints['capacity_limit_mb'] * 0.8,
                        'optimization_strategy': self.determine_optimization_strategy(segment_demands, storage_constraints)
                    }
                })

        return horizontal_segments

    def _extract_stage_storage_assignments(self, best_storage_config: Dict, parallel_stages: Dict) -> Dict:
        """
        Extract storage assignments for parallel stages from SPM transition configurations
        """
        stage_storage_assignments = {}

        # Map stage names to storage assignments from SPM transitions
        for transition_config in best_storage_config.get('transitions', []):
            stage_transition = transition_config.get('transition', [])
            if len(stage_transition) != 2:
                continue

            producer_stage, consumer_stage = stage_transition[0], stage_transition[1]
            spm_storage_info = transition_config['storage_config']

            # Assign storage info to parallel stages involved in transitions
            if producer_stage in parallel_stages:
                stage_storage_assignments[producer_stage] = spm_storage_info
            if consumer_stage in parallel_stages:
                stage_storage_assignments[consumer_stage] = spm_storage_info

        return stage_storage_assignments

    def extract_storage_constraints(self, spm_storage_info: Dict, flow_stats: Dict) -> Dict:
        """
        Extract comprehensive storage constraints from SPM data and storage type characteristics
        """
        storage_type = spm_storage_info.get('producer_storage', 'ssd')

        # Get storage profiles from configuration
        storage_profiles = self.storage_constraints_config.get('storage_profiles', {})
        segmentation_settings = self.storage_constraints_config.get('segmentation_settings', {})

        # Use configured storage profiles or defaults
        default_profile = {
            'bandwidth_limit_mib': 200,
            'capacity_limit_mb': 2048,
            'iops_limit': 10000,
            'latency_ms': 0.1,
            'cost_factor': 1.5
        }

        profile = storage_profiles.get(storage_type, default_profile)

        # Apply SPM-specific adjustments if available
        spm_bandwidth = spm_storage_info.get('expected_throughput_mib', profile['bandwidth_limit_mib'])
        spm_tasks_per_node = spm_storage_info.get('producer_tasks_per_node', 1)

        return {
            'storage_type': storage_type,
            'bandwidth_limit_mib': min(spm_bandwidth, profile['bandwidth_limit_mib']),
            'capacity_limit_mb': profile['capacity_limit_mb'],
            'iops_limit': profile['iops_limit'],
            'latency_ms': profile['latency_ms'],
            'cost_factor': profile['cost_factor'],
            'spm_tasks_per_node': spm_tasks_per_node,
            'spm_cost': spm_storage_info.get('spm_cost', 0),
            # Include configurable segmentation settings
            'min_tasks_per_segment': segmentation_settings.get('min_tasks_per_segment', 1),
            'max_tasks_per_segment': segmentation_settings.get('max_tasks_per_segment', 50),
            'bandwidth_utilization_target': segmentation_settings.get('bandwidth_utilization_target', 0.8),
            'capacity_utilization_target': segmentation_settings.get('capacity_utilization_target', 0.7),
            'iops_utilization_target': segmentation_settings.get('iops_utilization_target', 0.8)
        }

    def calculate_per_task_demands(self, flow_id: str, stage_name: str, tasks: List[str], flow_stats: Dict) -> Dict:
        """
        Calculate resource demands per task based on workflow statistics
        """
        total_operations = flow_stats.get('total_operations', 0)
        total_transfer_mb = flow_stats.get('total_transfer_mb', 0)
        avg_throughput_mib = flow_stats.get('avg_throughput_mib', 100)

        # Estimate per-task demands (workflow-agnostic)
        task_count = len(tasks)
        if task_count == 0:
            return {'bandwidth_mib': 0, 'storage_mb': 0, 'iops': 0, 'latency_ms': 0}

        # Calculate per-task resource requirements
        ops_per_task = total_operations / task_count
        transfer_per_task = total_transfer_mb / task_count
        bandwidth_per_task = avg_throughput_mib / task_count if task_count > 1 else avg_throughput_mib

        # Estimate storage requirements (input + intermediate + output)
        storage_per_task = transfer_per_task * 2.5  # Factor for intermediate files

        # Estimate IOPS (operations per second per task)
        iops_per_task = ops_per_task / 10.0  # Rough estimate: 10 ops per second

        return {
            'bandwidth_mib': bandwidth_per_task,
            'storage_mb': storage_per_task,
            'iops': iops_per_task,
            'latency_ms': 1.0,  # Base latency requirement
            'operations': ops_per_task
        }

    def create_storage_constrained_task_groups(self, tasks: List[str], task_demands: Dict,
                                             storage_constraints: Dict) -> List[List[str]]:
        """
        Create task groups that respect ALL storage constraints
        """
        if not tasks:
            return []

        # Calculate maximum tasks per group based on each constraint
        bandwidth_max = int(storage_constraints['bandwidth_limit_mib'] / max(0.1, task_demands['bandwidth_mib']))
        capacity_max = int(storage_constraints['capacity_limit_mb'] / max(0.1, task_demands['storage_mb']))
        iops_max = int(storage_constraints['iops_limit'] / max(0.1, task_demands['iops']))

        # The most restrictive constraint determines group size
        max_tasks_per_group = min(bandwidth_max, capacity_max, iops_max, len(tasks))
        max_tasks_per_group = max(1, max_tasks_per_group)  # At least 1 task per group

        # Create groups
        task_groups = []
        for i in range(0, len(tasks), max_tasks_per_group):
            group = tasks[i:i + max_tasks_per_group]
            task_groups.append(group)

        return task_groups

    def calculate_segment_demands(self, task_group: List[str], task_demands: Dict) -> Dict:
        """
        Calculate total resource demands for a segment (task group)
        """
        group_size = len(task_group)

        return {
            'bandwidth_mib': task_demands['bandwidth_mib'] * group_size,
            'storage_mb': task_demands['storage_mb'] * group_size,
            'iops': task_demands['iops'] * group_size,
            'latency_ms': task_demands['latency_ms'],  # Latency doesn't scale with group size
            'task_count': group_size
        }

    def check_storage_limits(self, segment_demands: Dict, storage_constraints: Dict) -> bool:
        """
        Check if segment demands are within storage limits
        """
        bandwidth_ok = segment_demands['bandwidth_mib'] <= storage_constraints['bandwidth_limit_mib']
        capacity_ok = segment_demands['storage_mb'] <= storage_constraints['capacity_limit_mb']
        iops_ok = segment_demands['iops'] <= storage_constraints['iops_limit']

        return bandwidth_ok and capacity_ok and iops_ok

    def determine_optimization_strategy(self, segment_demands: Dict, storage_constraints: Dict) -> str:
        """
        Determine the optimal strategy based on which constraint is most limiting
        """
        bandwidth_util = segment_demands['bandwidth_mib'] / storage_constraints['bandwidth_limit_mib']
        capacity_util = segment_demands['storage_mb'] / storage_constraints['capacity_limit_mb']
        iops_util = segment_demands['iops'] / storage_constraints['iops_limit']

        max_util = max(bandwidth_util, capacity_util, iops_util)

        if max_util == bandwidth_util:
            return 'bandwidth_limited'
        elif max_util == capacity_util:
            return 'capacity_limited'
        elif max_util == iops_util:
            return 'iops_limited'
        else:
            return 'balanced'

    def group_tasks_by_stage(self, flow_id: str) -> Dict[str, List[str]]:
        """
        Group tasks by stage within a specific flow/lineage
        """
        stage_tasks = {}

        for node_id, node_attrs in self.dag_data['nodes'].items():
            if (node_attrs.get('lineage') == flow_id and
                node_attrs.get('node_type') == 'task'):

                # Extract stage from task name like 'task_individuals_merge_15918-dc026'
                parts = node_id.split('_')
                if len(parts) >= 3:
                    stage = '_'.join(parts[1:-1])  # Everything between 'task_' and final ID
                    if stage not in stage_tasks:
                        stage_tasks[stage] = []
                    stage_tasks[stage].append(node_id)

        return stage_tasks

    def calculate_io_constrained_tasks_per_node(self, storage_info: Dict, total_transfer_mb: float,
                                              avg_throughput_mib: float, total_operations: int) -> int:
        """
        Calculate maximum tasks per node without violating IO constraints
        """
        # Base tasks per node from SPM analysis
        spm_producer_tasks = storage_info.get('producer_tasks_per_node', 1)
        smp_consumer_tasks = storage_info.get('consumer_tasks_per_node', 1)

        # Use more restrictive constraint
        base_tasks_per_node = min(spm_producer_tasks, smp_consumer_tasks)

        # Apply IO constraint scaling based on storage type performance characteristics
        storage_type = storage_info.get('producer_storage', 'unknown')

        # Storage type IO scaling factors (based on typical performance)
        io_scaling = {
            'tmpfs': 1.0,      # Memory - highest performance
            'ssd': 0.8,        # SSD - high performance
            'beegfs': 0.6,     # Network storage - medium performance
            'beegfs-ssd': 0.7, # Hybrid - balanced performance
            'ssd-beegfs': 0.7  # Hybrid - balanced performance
        }

        scaling_factor = io_scaling.get(storage_type, 0.5)  # Default conservative

        # Calculate IO-constrained tasks per node
        io_constrained_tasks = max(1, int(base_tasks_per_node * scaling_factor))

        # Additional constraint: ensure we don't exceed throughput capacity
        if avg_throughput_mib > 0 and total_transfer_mb > 0:
            # Estimate per-task transfer requirements
            transfer_per_task = total_transfer_mb / max(1, total_operations / 10)  # Rough estimate

            # Ensure node can handle required throughput
            max_throughput_tasks = max(1, int(avg_throughput_mib / max(1, transfer_per_task)))
            io_constrained_tasks = min(io_constrained_tasks, max_throughput_tasks)

        return io_constrained_tasks

    def generate_vertical_segments(self, flow_id: str, storage_configs: List[Dict]) -> List[Dict]:
        """
        Generate vertical segments: FastFlow-style producer-consumer task co-location

        Groups specific producer tasks with their consumer tasks for data locality optimization.
        This implements the core FastFlow vertical segmentation concept where tasks that
        share data are co-located on the same compute node to minimize data movement.
        """
        vertical_segments = []

        if not storage_configs or flow_id not in self.dag_statistics:
            return vertical_segments

        # STEP 1: Identify producer-consumer task pairs from DAG edges
        producer_consumer_pairs = self._find_producer_consumer_task_pairs(flow_id)

        if not producer_consumer_pairs:
            return vertical_segments

        # STEP 2: Use SPM storage assignment for co-location decisions
        best_storage_config = storage_configs[0]

        # STEP 3: Group producer-consumer pairs into co-location segments
        co_location_segments = self._create_co_location_segments(
            flow_id, producer_consumer_pairs, best_storage_config
        )

        # STEP 4: Generate vertical segment metadata for each co-location group
        for segment_id, segment_info in co_location_segments.items():
            producer_tasks = segment_info['producer_tasks']
            consumer_tasks = segment_info['consumer_tasks']
            data_volume = segment_info['data_volume_mb']

            # Calculate locality benefits
            transfer_saved = self._calculate_transfer_savings(segment_info)
            resource_efficiency = self._calculate_resource_efficiency(segment_info)

            vertical_segments.append({
                'segment_type': 'vertical',
                'segment_id': segment_id,
                'co_location_strategy': 'producer_consumer_pairing',
                'producer_consumer_pairs': segment_info['task_pairs'],
                'producer_tasks': {
                    'count': len(producer_tasks),
                    'stage_distribution': self._get_stage_distribution(producer_tasks),
                    'sample_tasks': producer_tasks[:3]  # Sample for readability
                },
                'consumer_tasks': {
                    'count': len(consumer_tasks),
                    'stage_distribution': self._get_stage_distribution(consumer_tasks),
                    'sample_tasks': consumer_tasks[:3]  # Sample for readability
                },
                'data_locality_optimization': {
                    'shared_data_volume_mb': data_volume,
                    'transfer_eliminated_mb': transfer_saved,
                    'locality_score': min(1.0, transfer_saved / max(1.0, data_volume)),
                    'network_savings_percent': (transfer_saved / max(1.0, data_volume)) * 100
                },
                'resource_efficiency': {
                    'compute_node_utilization': resource_efficiency,
                    'memory_locality_benefit': 'high' if data_volume > 10 else 'medium',
                    'io_optimization': 'local_storage_access'
                },
                'implementation_guidance': {
                    'node_assignment': 'co_locate_on_same_compute_node',
                    'storage_strategy': 'shared_local_storage',
                    'execution_order': 'producer_then_consumer'
                }
            })

        return vertical_segments

    def _find_producer_consumer_task_pairs(self, flow_id: str) -> List[Dict]:
        """
        Find specific producer-consumer task pairs from DAG edges

        Returns list of task pairs where producer output is consumed by consumer
        """
        task_pairs = []

        if flow_id not in self.dag_statistics:
            return task_pairs

        # Find all edges for this flow
        for edge in self.dag_data['edges']:
            source_id = edge['source']
            target_id = edge['target']

            # Get node information
            source_node = self.dag_data['nodes'].get(source_id, {})
            target_node = self.dag_data['nodes'].get(target_id, {})

            # Check if both nodes belong to this flow
            if (source_node.get('lineage') == flow_id and
                target_node.get('lineage') == flow_id):

                # Extract task and stage information
                producer_task = source_node.get('task_name', source_id)
                consumer_task = target_node.get('task_name', target_id)
                producer_stage = source_node.get('stage', 'unknown')
                consumer_stage = target_node.get('stage', 'unknown')

                # Estimate data volume (from edge attributes or node outputs)
                data_volume = edge.get('data_size_mb', 0.1)  # Default small size

                task_pairs.append({
                    'producer_task': producer_task,
                    'consumer_task': consumer_task,
                    'producer_stage': producer_stage,
                    'consumer_stage': consumer_stage,
                    'data_volume_mb': data_volume,
                    'producer_node_id': source_id,
                    'consumer_node_id': target_id,
                    'edge_info': edge
                })

        return task_pairs

    def _create_co_location_segments(self, flow_id: str, producer_consumer_pairs: List[Dict],
                                   best_storage_config: Dict) -> Dict[str, Dict]:
        """
        Group producer-consumer pairs into co-location segments

        Creates segments where multiple producer-consumer pairs can be co-located
        on the same compute node for maximum data locality benefit
        """
        co_location_segments = {}

        # Simple grouping strategy: group by data volume and resource requirements
        current_segment_id = 0
        current_segment_data = 0
        current_segment_tasks = set()
        current_segment_pairs = []

        # Get configurable limits for co-location from storage constraints config
        segmentation_settings = self.storage_constraints_config.get('segmentation_settings', {})
        max_segment_data_mb = segmentation_settings.get('max_vertical_segment_data_mb', 50)
        max_tasks_per_segment = segmentation_settings.get('max_vertical_segment_tasks', 10)

        for pair in producer_consumer_pairs:
            pair_data_volume = pair['data_volume_mb']
            producer_task = pair['producer_task']
            consumer_task = pair['consumer_task']

            # Check if this pair can fit in current segment
            would_exceed_data = (current_segment_data + pair_data_volume) > max_segment_data_mb
            would_exceed_tasks = len(current_segment_tasks) + 2 > max_tasks_per_segment
            has_task_conflict = (producer_task in current_segment_tasks or
                               consumer_task in current_segment_tasks)

            # Start new segment if needed
            if (would_exceed_data or would_exceed_tasks or has_task_conflict) and current_segment_pairs:
                # Finalize current segment
                segment_id = f"vertical_segment_{current_segment_id}"
                co_location_segments[segment_id] = {
                    'task_pairs': current_segment_pairs.copy(),
                    'producer_tasks': [p['producer_task'] for p in current_segment_pairs],
                    'consumer_tasks': [p['consumer_task'] for p in current_segment_pairs],
                    'data_volume_mb': current_segment_data,
                    'task_count': len(current_segment_tasks)
                }

                # Start new segment
                current_segment_id += 1
                current_segment_data = 0
                current_segment_tasks = set()
                current_segment_pairs = []

            # Add pair to current segment
            current_segment_pairs.append(pair)
            current_segment_data += pair_data_volume
            current_segment_tasks.add(producer_task)
            current_segment_tasks.add(consumer_task)

        # Finalize last segment
        if current_segment_pairs:
            segment_id = f"vertical_segment_{current_segment_id}"
            co_location_segments[segment_id] = {
                'task_pairs': current_segment_pairs,
                'producer_tasks': [p['producer_task'] for p in current_segment_pairs],
                'consumer_tasks': [p['consumer_task'] for p in current_segment_pairs],
                'data_volume_mb': current_segment_data,
                'task_count': len(current_segment_tasks)
            }

        return co_location_segments

    def _calculate_transfer_savings(self, segment_info: Dict) -> float:
        """
        Calculate data transfer savings from co-location
        """
        # In co-location, all intermediate data stays local (100% transfer savings)
        return segment_info['data_volume_mb']

    def _calculate_resource_efficiency(self, segment_info: Dict) -> float:
        """
        Calculate resource utilization efficiency for co-located tasks
        """
        # Simple efficiency model based on task count and data locality
        task_count = segment_info['task_count']
        data_volume = segment_info['data_volume_mb']

        # Higher efficiency for more data per task (better locality benefit)
        if task_count > 0:
            data_per_task = data_volume / task_count
            efficiency = min(1.0, data_per_task / 10.0)  # Normalize to 0-1
        else:
            efficiency = 0.0

        return efficiency

    def _get_stage_distribution(self, tasks: List[str]) -> Dict[str, int]:
        """
        Get distribution of tasks across workflow stages (workflow-agnostic)
        """
        stage_dist = {}

        # Use DAG node data to get actual stage assignments
        for task in tasks:
            # Find the task in DAG nodes to get its stage
            task_stage = 'unknown'
            for node_id, node_attrs in self.dag_data['nodes'].items():
                if node_attrs.get('task_name', node_id) == task:
                    task_stage = node_attrs.get('stage', 'unknown')
                    break

            stage_dist[task_stage] = stage_dist.get(task_stage, 0) + 1

        return stage_dist

    def find_producer_consumer_stage_chains(self, flow_id: str) -> Dict[str, Dict]:
        """
        WORKFLOW-AGNOSTIC: Discover stage chains from actual DAG structure
        """
        stage_chains = {}

        # Step 1: Discover all stages in this flow
        stages_in_flow = set()
        stage_tasks = self.group_tasks_by_stage(flow_id)
        stages_in_flow = set(stage_tasks.keys())

        # Step 2: Discover stage dependencies by analyzing task->data->task flows
        stage_dependencies = self.discover_stage_dependencies(flow_id, stage_tasks)

        # Step 3: Build stage chains from dependencies (workflow-agnostic)
        stage_chains = self.build_stage_chains_from_dependencies(stage_dependencies, flow_id)

        return stage_chains

    def discover_stage_dependencies(self, flow_id: str, stage_tasks: Dict[str, List[str]]) -> Dict[str, Set[str]]:
        """
        Discover which stages depend on which other stages
        """
        stage_dependencies = {stage: set() for stage in stage_tasks.keys()}

        # Analyze task->data->task relationships to find stage dependencies
        for edge in self.dag_data['edges']:
            source_node = edge['source']
            target_node = edge['target']

            source_attrs = self.dag_data['nodes'].get(source_node, {})
            target_attrs = self.dag_data['nodes'].get(target_node, {})

            # Only look within this flow
            if (source_attrs.get('lineage') == flow_id and
                target_attrs.get('lineage') == flow_id):

                # Extract stages from node names
                source_stage = self.extract_stage_from_node(source_node)
                target_stage = self.extract_stage_from_node(target_node)

                # If we have a task->data or data->task relationship across stages
                if (source_stage and target_stage and source_stage != target_stage and
                    source_stage in stage_tasks and target_stage in stage_tasks):
                    stage_dependencies[target_stage].add(source_stage)

        return stage_dependencies

    def extract_stage_from_node(self, node_id: str) -> str:
        """
        WORKFLOW-AGNOSTIC: Extract stage name from any node ID
        """
        if node_id.startswith('task_'):
            parts = node_id.split('_')
            if len(parts) >= 3:
                return '_'.join(parts[1:-1])  # Everything between 'task_' and final ID
        elif node_id.startswith('data_'):
            # For data nodes, we can't directly extract stage, return None
            return None
        return None

    def build_stage_chains_from_dependencies(self, stage_dependencies: Dict[str, Set[str]], flow_id: str) -> Dict[str, Dict]:
        """
        WORKFLOW-AGNOSTIC: Build meaningful stage chains from discovered dependencies
        """
        stage_chains = {}
        chain_id = 0

        # Find stages with no dependencies (root stages)
        root_stages = [stage for stage, deps in stage_dependencies.items() if len(deps) == 0]

        # Build chains starting from each root stage
        for root_stage in root_stages:
            chains_from_root = self.trace_stage_chains(root_stage, stage_dependencies, set())

            for chain in chains_from_root:
                if len(chain) > 1:  # Only include chains with multiple stages
                    chain_name = f"chain_{chain_id}"
                    data_flow = self.calculate_data_flow_for_stages(flow_id, chain)
                    stage_chains[chain_name] = {
                        'stages': chain,
                        'data_flow': data_flow,
                        'description': f"Pipeline: {' -> '.join(chain)}"
                    }
                    chain_id += 1

        # If no chains found, create single-stage segments
        if not stage_chains:
            for stage in stage_dependencies.keys():
                chain_name = f"single_stage_{chain_id}"
                data_flow = self.calculate_data_flow_for_stages(flow_id, [stage])
                stage_chains[chain_name] = {
                    'stages': [stage],
                    'data_flow': data_flow,
                    'description': f"Single stage: {stage}"
                }
                chain_id += 1

        return stage_chains

    def trace_stage_chains(self, current_stage: str, dependencies: Dict[str, Set[str]], visited: Set[str]) -> List[List[str]]:
        """
        WORKFLOW-AGNOSTIC: Recursively trace stage dependency chains
        """
        if current_stage in visited:
            return [[current_stage]]

        visited.add(current_stage)
        chains = []

        # Find stages that depend on the current stage
        dependent_stages = [stage for stage, deps in dependencies.items() if current_stage in deps]

        if not dependent_stages:
            # This is a terminal stage
            chains.append([current_stage])
        else:
            # Continue tracing each dependent stage
            for next_stage in dependent_stages:
                next_chains = self.trace_stage_chains(next_stage, dependencies, visited.copy())
                for next_chain in next_chains:
                    chains.append([current_stage] + next_chain)

        return chains

    def calculate_data_flow_for_stages(self, flow_id: str, stages: List[str]) -> Dict:
        """
        WORKFLOW-AGNOSTIC: Calculate data flow volume and characteristics for any sequence of stages
        """
        total_mb = 0
        files = []

        # Get flow statistics
        if flow_id in self.dag_statistics:
            flow_stats = self.dag_statistics[flow_id]

            # Count total stages in this flow to calculate proportion
            all_stages = set()
            stage_tasks = self.group_tasks_by_stage(flow_id)
            all_stages = set(stage_tasks.keys())

            if len(all_stages) > 0:
                stage_ratio = len(stages) / len(all_stages)
                total_mb = flow_stats.get('total_transfer_mb', 0) * stage_ratio

        # Find data nodes that might be intermediate files for these stages
        for node_id, node_attrs in self.dag_data['nodes'].items():
            if (node_attrs.get('lineage') == flow_id and
                node_attrs.get('node_type') == 'data'):
                files.append(node_id)

        return {
            'total_mb': total_mb,
            'files': files[:10],  # Sample files
            'stage_count': len(stages)
        }

    def calculate_stage_chain_locality(self, stages: List[str], data_flow: Dict, config: Dict) -> Tuple[float, float]:
        """
        WORKFLOW-AGNOSTIC: Calculate locality score and transfer cost for co-locating a stage chain
        """
        # Higher locality score for shorter chains (easier to co-locate)
        chain_length = len(stages)
        locality_base_score = 1.0 / chain_length  # Shorter chains = higher locality potential

        # Transfer cost based on data volume and storage transitions
        data_mb = data_flow.get('total_mb', 0)

        # Calculate benefits based on chain characteristics (workflow-agnostic)
        if chain_length <= 2:
            transfer_cost = data_mb * 0.1  # 90% reduction for short chains
            locality_score = min(0.95, locality_base_score + 0.3)
        elif chain_length <= 3:
            transfer_cost = data_mb * 0.3  # 70% reduction for medium chains
            locality_score = min(0.85, locality_base_score + 0.2)
        else:
            transfer_cost = data_mb * 0.5  # 50% reduction for longer chains
            locality_score = min(0.75, locality_base_score + 0.1)

        return locality_score, transfer_cost

    def get_optimal_storage_for_chain(self, stages: List[str], config: Dict) -> Dict:
        """
        WORKFLOW-AGNOSTIC: Get optimal storage configuration for any stage chain
        """
        # For short chains, use fastest storage (tmpfs/ssd)
        # For longer chains, balance performance and capacity
        if len(stages) <= 2:
            return {'primary_storage': 'tmpfs', 'fallback_storage': 'ssd', 'strategy': 'speed_optimized'}
        else:
            return {'primary_storage': 'ssd', 'fallback_storage': 'beegfs', 'strategy': 'balanced'}

    def extract_data_flow_paths(self, flow_id: str) -> List[Dict]:
        """
        Extract data flow paths for vertical segment analysis
        """
        data_paths = []

        # Find all nodes for this flow in the DAG
        flow_nodes = []
        for node_id, node_attrs in self.dag_data['nodes'].items():
            if node_attrs.get('lineage') == flow_id:
                flow_nodes.append((node_id, node_attrs))

        # Build data flow sequences: task -> data -> task chains
        for edge in self.dag_data['edges']:
            source = edge['source']
            target = edge['target']

            source_attrs = self.dag_data['nodes'].get(source, {})
            target_attrs = self.dag_data['nodes'].get(target, {})

            # Look for producer-consumer data flows within this lineage
            if (source_attrs.get('lineage') == flow_id and
                target_attrs.get('lineage') == flow_id):

                data_paths.append({
                    'source_node': source,
                    'target_node': target,
                    'source_type': source_attrs.get('node_type'),
                    'target_type': target_attrs.get('node_type'),
                    'edge_data': edge
                })

        return data_paths

    def optimize_file_placement_locality(self, data_paths: List[Dict], storage_config: Dict) -> Dict:
        """
        Optimize file placement for maximum data locality
        """
        locality_groups = {}

        # Group data paths by storage locality potential
        transition_storage = {}
        for trans_config in storage_config.get('transitions', []):
            transition = trans_config['transition']
            storage_info = trans_config['storage_config']
            transition_storage[transition] = storage_info

        # Analyze each data flow path for locality optimization
        group_counter = 0
        for path in data_paths:
            # Determine optimal storage placement for this path
            optimal_placement = self.determine_optimal_file_placement(path, transition_storage)

            if optimal_placement:
                group_id = f"locality_group_{group_counter}"
                locality_groups[group_id] = optimal_placement
                group_counter += 1

        return locality_groups

    def determine_optimal_file_placement(self, data_path: Dict, transition_storage: Dict) -> Dict:
        """
        Determine optimal file placement for a data flow path
        """
        # Get edge characteristics
        edge_data = data_path.get('edge_data', {})

        # Find best storage assignment based on transfer costs and locality
        storage_sequence = []
        total_transfer_cost = 0

        # Analyze storage transitions for this path
        for transition, storage_info in transition_storage.items():
            producer_storage = storage_info.get('producer_storage')
            consumer_storage = storage_info.get('consumer_storage')

            # Calculate transfer cost for this storage transition
            transfer_cost = self.calculate_transfer_penalty(producer_storage, consumer_storage)
            total_transfer_cost += transfer_cost

            storage_sequence.append({
                'transition': transition,
                'producer_storage': producer_storage,
                'consumer_storage': consumer_storage,
                'transfer_cost': transfer_cost
            })

        # Calculate locality score (lower transfer cost = higher locality)
        locality_score = 1.0 / (1.0 + total_transfer_cost)

        return {
            'files': [data_path],
            'storage_sequence': storage_sequence,
            'locality_score': locality_score,
            'flow_path': data_path,
            'transfer_cost': total_transfer_cost
        }

    def analyze_segmentation_tradeoffs(self, horizontal_segments: Dict, vertical_segments: Dict) -> Dict:
        """
        Analyze trade-offs between horizontal (parallelism) and vertical (locality) segmentation
        """
        tradeoff_analysis = {
            'horizontal_benefits': {},
            'vertical_benefits': {},
            'optimal_strategy': {},
            'performance_predictions': {}
        }

        # Analyze each flow's segmentation options
        for flow_id in horizontal_segments:
            h_segments = horizontal_segments.get(flow_id, [])
            v_segments = vertical_segments.get(flow_id, [])

            # Calculate horizontal benefits (parallelism gains)
            h_parallelism_gain = sum(seg.get('parallelism_potential', 1) for seg in h_segments)
            h_io_efficiency = sum(seg.get('io_constraint_basis', {}).get('throughput_requirement_mib', 0)
                                for seg in h_segments)

            # Calculate vertical benefits (locality gains)
            v_locality_gain = sum(seg.get('locality_score', 0) for seg in v_segments)
            v_transfer_savings = sum(seg.get('transfer_minimization', 0) for seg in v_segments)

            tradeoff_analysis['horizontal_benefits'][flow_id] = {
                'parallelism_gain': h_parallelism_gain,
                'io_efficiency': h_io_efficiency,
                'segment_count': len(h_segments)
            }

            tradeoff_analysis['vertical_benefits'][flow_id] = {
                'locality_gain': v_locality_gain,
                'transfer_savings': v_transfer_savings,
                'segment_count': len(v_segments)
            }

            # Determine optimal strategy
            if h_parallelism_gain > v_locality_gain:
                optimal_strategy = 'horizontal_focused'
                confidence = h_parallelism_gain / max(1, v_locality_gain)
            else:
                optimal_strategy = 'vertical_focused'
                confidence = v_locality_gain / max(1, h_parallelism_gain)

            tradeoff_analysis['optimal_strategy'][flow_id] = {
                'strategy': optimal_strategy,
                'confidence': confidence,
                'reasoning': f"{'Parallelism' if optimal_strategy == 'horizontal_focused' else 'Locality'} benefits outweigh alternatives"
            }

        return tradeoff_analysis

    def rank_storage_configurations(self, end_to_end_configs: List[Dict]) -> List[Dict]:
        """
        Rank all storage configurations by total workflow cost
        """
        print(f"\\nRanking storage configurations by total workflow cost...")

        # Sort by adjusted total cost (SPM + transfer penalties)
        ranked_configs = sorted(end_to_end_configs, key=lambda x: x['adjusted_total_cost'])

        print(f"  Top 10 storage configurations:")
        for i, config in enumerate(ranked_configs[:10]):
            print(f"    {i+1:2d}. Total Cost: {config['adjusted_total_cost']:6.3f} "
                  f"(SPM: {config['total_spm_cost']:6.3f}, Transfer: {config['data_transfer_penalties']:6.3f}, "
                  f"Violations: {config['storage_consistency_violations']})")

        return ranked_configs

    def create_storage_assignment_recommendations(self, ranked_configs: List[Dict], top_n: int = 5) -> Dict:
        """
        Create ranked storage assignment recommendations for users
        """
        print(f"\\nCreating storage assignment recommendations...")

        recommendations = {
            'top_configurations': [],
            'analysis_summary': {},
            'implementation_guidance': {}
        }

        for i, config in enumerate(ranked_configs[:top_n]):
            recommendation = {
                'rank': i + 1,
                'total_cost': config['adjusted_total_cost'],
                'spm_cost': config['total_spm_cost'],
                'transfer_penalty': config['data_transfer_penalties'],
                'consistency_violations': config['storage_consistency_violations'],
                'storage_assignment': {},
                'transition_details': [],
                'implementation_complexity': 'low'  # Will be calculated
            }

            # Extract storage assignment for each stage
            storage_by_stage = {}
            for trans_config in config['transitions']:
                producer, consumer = trans_config['transition']
                storage = trans_config['storage_config']

                if producer not in storage_by_stage:
                    storage_by_stage[producer] = storage['producer_storage']
                storage_by_stage[consumer] = storage['consumer_storage']

                recommendation['transition_details'].append({
                    'from_stage': producer,
                    'to_stage': consumer,
                    'from_storage': storage['producer_storage'],
                    'to_storage': storage['consumer_storage'],
                    'tasks_per_node_from': storage['producer_tasks_per_node'],
                    'tasks_per_node_to': storage['consumer_tasks_per_node'],
                    'spm_cost': storage['spm_cost']
                })

            recommendation['storage_assignment'] = storage_by_stage

            # Calculate implementation complexity
            unique_storage_types = len(set(storage_by_stage.values()))
            if unique_storage_types > 3 or config['storage_consistency_violations'] > 2:
                recommendation['implementation_complexity'] = 'high'
            elif unique_storage_types > 2 or config['storage_consistency_violations'] > 0:
                recommendation['implementation_complexity'] = 'medium'

            recommendations['top_configurations'].append(recommendation)

            print(f"    Rank {i+1}: Cost {config['adjusted_total_cost']:.3f}, "
                  f"Storage types: {list(set(storage_by_stage.values()))}, "
                  f"Complexity: {recommendation['implementation_complexity']}")

        # Add analysis summary
        recommendations['analysis_summary'] = {
            'total_configurations_evaluated': len(ranked_configs),
            'best_cost': ranked_configs[0]['adjusted_total_cost'] if ranked_configs else 0,
            'worst_cost': ranked_configs[-1]['adjusted_total_cost'] if ranked_configs else 0,
            'cost_improvement_range': (
                ranked_configs[-1]['adjusted_total_cost'] - ranked_configs[0]['adjusted_total_cost']
                if len(ranked_configs) > 1 else 0
            )
        }

        return recommendations

    def export_optimization_results(self, output_prefix: str = "workflow_agnostic_smp"):
        """Export all optimization results including end-to-end analysis"""

        print(f"\\nPerforming complete end-to-end optimization analysis...")

        # Extract workflow transitions from DAG
        dag_transitions = self.extract_workflow_stage_transitions()

        # Build workflow pipeline
        pipeline = self.build_workflow_pipeline_graph(dag_transitions)

        # Match to SPM data
        spm_matches = self.match_dag_transitions_to_spm_pairs(dag_transitions)

        # Generate end-to-end storage configurations
        end_to_end_configs = self.generate_end_to_end_storage_configurations(pipeline, spm_matches)

        # Rank configurations by total workflow cost
        ranked_configs = self.rank_storage_configurations(end_to_end_configs)

        # Create user recommendations
        recommendations = self.create_storage_assignment_recommendations(ranked_configs)

        # Generate caterpillar segments if enabled
        segmentation_results = None
        if self.enable_segmentation:
            segmentation_results = self.generate_caterpillar_segments(ranked_configs[:5])

        # Analyze performance demand (for completeness)
        demand_analysis = self.analyze_edge_performance_demand()

        # Compile comprehensive results
        results = {
            'metadata': {
                'total_parallel_flows': len(self.dag_flows),
                'smp_configurations_analyzed': len(self.spm_data),
                'dag_stage_transitions': len(dag_transitions),
                'matched_spm_pairs': len(spm_matches),
                'end_to_end_configurations_evaluated': len(end_to_end_configs),
                'optimization_timestamp': pd.Timestamp.now().isoformat()
            },
            'workflow_structure': {
                'pipeline': {
                    'stages': list(pipeline['stages']),
                    'stage_order': pipeline['stage_order'],
                    'transitions': pipeline['transitions']
                },
                'dag_transitions': {str(k): len(v) for k, v in dag_transitions.items()},
                'spm_matches': {str(k): str(v) for k, v in spm_matches.items()}
            },
            'optimization_analysis': {
                'demand_analysis': demand_analysis,
                'end_to_end_configurations': ranked_configs[:20],  # Top 20 for detailed analysis
                'storage_recommendations': recommendations,
                'caterpillar_segmentation': segmentation_results if self.enable_segmentation else None
            }
        }

        # Export to JSON
        results_file = f"{output_prefix}_results.json"
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)

        # Export simplified recommendations for users
        recommendations_file = f"{output_prefix}_recommendations.json"
        with open(recommendations_file, 'w') as f:
            json.dump(recommendations, f, indent=2, default=str)

        print(f"\\nWorkflow-agnostic optimization results exported to:")
        print(f"  Full analysis: {results_file}")
        print(f"  User recommendations: {recommendations_file}")

        return results_file, recommendations_file






def main():
    parser = argparse.ArgumentParser(description="Workflow-Agnostic SPM Storage Optimizer")

    parser.add_argument('spm_csv', help='SPM analysis results CSV file')
    parser.add_argument('flows_json', help='Flow parallelism analysis JSON file')
    parser.add_argument('stats_json', help='Lineage statistics JSON file')
    parser.add_argument('dag_json', help='DAG structure JSON file')
    parser.add_argument('-o', '--output-prefix', default='workflow_agnostic_spm', help='Output file prefix')
    parser.add_argument('--segmented', action='store_true', help='Enable caterpillar segmentation analysis')
    parser.add_argument('--execution-plan', choices=['nextflow', 'slurm', 'cwl', 'snakemake'],
                       help='Generate execution plan for specified scheduler')

    args = parser.parse_args()

    mode_text = "with Caterpillar Segmentation" if args.segmented else "(Storage-only mode)"
    print(f"=== Workflow-Agnostic SPM Storage Optimization {mode_text} ===\\n")

    optimizer = WorkflowAgnosticSPMOptimizer(enable_segmentation=args.segmented)
    optimizer.load_spm_data(args.spm_csv)
    optimizer.load_dag_analysis(args.flows_json, args.stats_json, args.dag_json)

    results_file, recommendations_file = optimizer.export_optimization_results(args.output_prefix)

    # Generate execution plans if requested
    execution_plan_file = None
    if args.execution_plan:
        print(f"\\nGenerating {args.execution_plan} execution plan...")

        # Load optimization results to get segmentation data
        with open(results_file, 'r') as f:
            optimization_results = json.load(f)

        storage_configs = optimization_results.get('optimization_analysis', {}).get('end_to_end_configurations', [])
        caterpillar_data = optimization_results.get('optimization_analysis', {}).get('caterpillar_segmentation', {})
        segmentation_results = {
            'horizontal_segments': caterpillar_data.get('horizontal_segments', {}),
            'vertical_segments': caterpillar_data.get('vertical_segments', {})
        }

        # Generate execution plans using the separate module
        execution_plan_generator = ExecutionPlanGenerator()
        execution_plans = execution_plan_generator.generate_execution_plans(
            storage_configs, segmentation_results, args.execution_plan
        )

        # Save execution plans
        execution_plan_file = f"{args.output_prefix}_execution_plan_{args.execution_plan}.json"
        with open(execution_plan_file, 'w') as f:
            json.dump(execution_plans, f, indent=2, default=str)

        print(f"  Execution plan saved to: {execution_plan_file}")

    print(f"\\n=== Summary ===")
    print(f"Workflow-agnostic SPM optimization completed.")
    if args.segmented:
        print(f"Caterpillar segmentation analysis included:")
        print(f"  - Horizontal segments: IO-constrained task fitting")
        print(f"  - Vertical segments: Data locality optimization")
        print(f"  - Trade-off analysis: Parallelism vs locality")
    else:
        print(f"Storage-only optimization completed.")
    if args.execution_plan:
        print(f"Execution plan generated for {args.execution_plan} scheduler.")
    
if __name__ == "__main__":
    main()