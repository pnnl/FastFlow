"""
Execution Plan Generator for FastFlow Segmentation Results

Translates FastFlow segmentation recommendations into execution-ready plans
for various workflow schedulers (Nextflow, Slurm, CWL, Snakemake).
"""

from datetime import datetime
from typing import Dict, List, Tuple
import json


class ExecutionPlanGenerator:
    """
    Generates scheduler-specific execution plans from FastFlow segmentation results
    """

    def __init__(self, storage_constraints_config: Dict = None):
        """
        Initialize execution plan generator

        Args:
            storage_constraints_config: Configuration for storage constraints
        """
        self.storage_constraints_config = storage_constraints_config or self._get_default_storage_constraints()

    def _get_default_storage_constraints(self) -> Dict:
        """Default storage constraint configuration"""
        return {
            'storage_profiles': {
                'ssd': {
                    'bandwidth_limit_mib': 200,
                    'capacity_limit_mb': 2048,
                    'iops_limit': 10000
                },
                'tmpfs': {
                    'bandwidth_limit_mib': 300,
                    'capacity_limit_mb': 1024,
                    'iops_limit': 15000
                },
                'beegfs': {
                    'bandwidth_limit_mib': 150,
                    'capacity_limit_mb': 4096,
                    'iops_limit': 3000
                }
            }
        }

    def generate_execution_plans(self, storage_configs: List[Dict], segmentation_results: Dict,
                                 output_format: str = 'nextflow') -> Dict:
        """
        Generate execution-ready plans for workflow schedulers

        Args:
            storage_configs: Storage optimization results from SPM
            segmentation_results: Horizontal and vertical segmentation results
            output_format: Target scheduler format ('nextflow', 'slurm', 'cwl', 'snakemake')

        Returns:
            Dictionary containing execution plans for the specified scheduler
        """
        execution_plans = {
            'format': output_format,
            'metadata': {
                'generated_at': datetime.now().isoformat(),
                'total_lineages': len(segmentation_results.get('horizontal_segments', {})),
                'segmentation_enabled': True,
                'storage_optimization': len(storage_configs) > 0
            },
            'storage_configuration': self._generate_storage_config(storage_configs),
            'node_assignments': self._generate_node_assignments(segmentation_results),
            'execution_scripts': self._generate_scheduler_scripts(segmentation_results, output_format),
            'resource_requirements': self._calculate_resource_requirements(segmentation_results)
        }

        return execution_plans

    def _generate_storage_config(self, storage_configs: List[Dict]) -> Dict:
        """Generate storage configuration for execution"""
        if not storage_configs:
            return {'storage_type': 'default', 'configuration': {}}

        best_config = storage_configs[0]
        storage_config = {
            'primary_storage': best_config.get('storage_assignment', {}).get('primary_storage', 'ssd'),
            'total_cost': best_config.get('total_cost', 0),
            'storage_transitions': [],
            'mount_requirements': {}
        }

        # Extract storage requirements from transitions
        for transition in best_config.get('transitions', []):
            producer_storage = transition.get('storage_config', {}).get('producer_storage', 'ssd')
            consumer_storage = transition.get('storage_config', {}).get('consumer_storage', 'ssd')

            storage_config['storage_transitions'].append({
                'from_stage': transition.get('transition', ['unknown', 'unknown'])[0],
                'to_stage': transition.get('transition', ['unknown', 'unknown'])[1],
                'producer_storage': producer_storage,
                'consumer_storage': consumer_storage,
                'performance_cost': transition.get('storage_config', {}).get('spm_cost', 0)
            })

            # Track mount requirements
            for storage_type in [producer_storage, consumer_storage]:
                if storage_type not in storage_config['mount_requirements']:
                    storage_config['mount_requirements'][storage_type] = self._get_storage_mount_config(storage_type)

        return storage_config

    def _get_storage_mount_config(self, storage_type: str) -> Dict:
        """Get mount configuration for storage type"""
        storage_mounts = {
            'tmpfs': {
                'mount_type': 'tmpfs',
                'mount_options': 'size=2G,noatime',
                'mount_point': '/tmp/workflow_tmpfs',
                'cleanup_required': True
            },
            'ssd': {
                'mount_type': 'local_ssd',
                'mount_options': 'defaults,noatime',
                'mount_point': '/mnt/ssd/workflow',
                'cleanup_required': False
            },
            'beegfs': {
                'mount_type': 'network_fs',
                'mount_options': 'defaults',
                'mount_point': '/mnt/beegfs/workflow',
                'cleanup_required': False
            }
        }
        return storage_mounts.get(storage_type, storage_mounts['ssd'])

    def _generate_node_assignments(self, segmentation_results: Dict) -> Dict:
        """Generate node assignment recommendations for tasks"""
        node_assignments = {
            'co_location_groups': {},
            'parallel_execution_groups': {},
            'resource_requirements_per_node': {}
        }

        # Process vertical segments (co-location)
        vertical_segments = segmentation_results.get('vertical_segments', {})
        for lineage_id, segments in vertical_segments.items():
            co_location_groups = []
            for segment in segments:
                if segment.get('segment_type') == 'vertical':
                    producer_tasks = segment.get('producer_tasks', {}).get('sample_tasks', [])
                    consumer_tasks = segment.get('consumer_tasks', {}).get('sample_tasks', [])

                    co_location_groups.append({
                        'segment_id': segment.get('segment_id'),
                        'tasks_to_co_locate': producer_tasks + consumer_tasks,
                        'data_locality_benefit': segment.get('data_locality_optimization', {}),
                        'node_assignment_strategy': 'same_node',
                        'execution_order': 'producer_then_consumer'
                    })

            node_assignments['co_location_groups'][lineage_id] = co_location_groups

        # Process horizontal segments (parallel execution)
        horizontal_segments = segmentation_results.get('horizontal_segments', {})
        for lineage_id, segments in horizontal_segments.items():
            parallel_groups = []
            for segment in segments:
                if segment.get('segment_type') == 'horizontal':
                    task_group = segment.get('task_group', {})

                    parallel_groups.append({
                        'segment_id': segment.get('segment_id'),
                        'parallel_tasks': task_group.get('task_ids', []),
                        'tasks_per_node': task_group.get('tasks_per_node', 1),
                        'storage_constraints': segment.get('storage_constraints_satisfied', {}),
                        'node_assignment_strategy': 'distribute_across_nodes',
                        'execution_order': 'parallel'
                    })

            node_assignments['parallel_execution_groups'][lineage_id] = parallel_groups

        return node_assignments

    def _generate_scheduler_scripts(self, segmentation_results: Dict, output_format: str) -> Dict:
        """Generate scheduler-specific execution scripts"""
        if output_format == 'nextflow':
            return self._generate_nextflow_script(segmentation_results)
        elif output_format == 'slurm':
            return self._generate_slurm_script(segmentation_results)
        elif output_format == 'cwl':
            return self._generate_cwl_script(segmentation_results)
        elif output_format == 'snakemake':
            return self._generate_snakemake_script(segmentation_results)
        else:
            return {'error': f'Unsupported output format: {output_format}'}

    def _generate_nextflow_script(self, segmentation_results: Dict) -> Dict:
        """Generate Nextflow-compatible execution script"""
        nextflow_config = {
            'script_type': 'nextflow',
            'processes': {},
            'workflow_configuration': {
                'executor': 'slurm',
                'queue': 'normal',
                'clusterOptions': '--time=24:00:00'
            },
            'storage_directives': {},
            'co_location_directives': []
        }

        # Generate processes for horizontal segments (parallel execution)
        horizontal_segments = segmentation_results.get('horizontal_segments', {})
        for lineage_id, segments in horizontal_segments.items():
            for segment in segments:
                if segment.get('segment_type') == 'horizontal':
                    process_name = f"process_{segment.get('segment_id', 'unknown')}"
                    stage_name = segment.get('stage_name', 'unknown')
                    task_group = segment.get('task_group', {})
                    storage_assignment = segment.get('spm_storage_assignment', {})

                    nextflow_config['processes'][process_name] = {
                        'stage': stage_name,
                        'tasks': task_group.get('task_ids', []),
                        'cpus': task_group.get('tasks_per_node', 1),
                        'memory': '8.GB',
                        'time': '4.h',
                        'storage_type': storage_assignment.get('producer_storage', 'ssd'),
                        'scratch': True if storage_assignment.get('producer_storage') == 'tmpfs' else False,
                        'clusterOptions': f"--constraint={storage_assignment.get('producer_storage', 'ssd')}"
                    }

        # Generate co-location directives for vertical segments
        vertical_segments = segmentation_results.get('vertical_segments', {})
        for lineage_id, segments in vertical_segments.items():
            for segment in segments:
                if segment.get('segment_type') == 'vertical':
                    producer_tasks = segment.get('producer_tasks', {}).get('sample_tasks', [])
                    consumer_tasks = segment.get('consumer_tasks', {}).get('sample_tasks', [])

                    nextflow_config['co_location_directives'].append({
                        'segment_id': segment.get('segment_id'),
                        'producer_tasks': producer_tasks,
                        'consumer_tasks': consumer_tasks,
                        'scheduling_hint': 'same_node',
                        'shared_storage': True,
                        'data_volume_mb': segment.get('data_locality_optimization', {}).get('shared_data_volume_mb', 0)
                    })

        return nextflow_config

    def _generate_slurm_script(self, segmentation_results: Dict) -> Dict:
        """Generate Slurm-compatible execution script"""
        slurm_config = {
            'script_type': 'slurm',
            'job_scripts': {},
            'dependency_chain': [],
            'resource_allocations': {},
            'storage_setup_commands': []
        }

        # Generate SLURM job scripts for horizontal segments
        horizontal_segments = segmentation_results.get('horizontal_segments', {})
        job_id = 1
        for lineage_id, segments in horizontal_segments.items():
            for segment in segments:
                if segment.get('segment_type') == 'horizontal':
                    job_name = f"job_{job_id}_{segment.get('segment_id', 'unknown')}"
                    task_group = segment.get('task_group', {})
                    storage_assignment = segment.get('spm_storage_assignment', {})

                    slurm_config['job_scripts'][job_name] = {
                        'job_name': job_name,
                        'partition': 'normal',
                        'ntasks': task_group.get('tasks_per_node', 1),
                        'cpus_per_task': 1,
                        'memory': '8G',
                        'time': '04:00:00',
                        'constraint': storage_assignment.get('producer_storage', 'ssd'),
                        'tasks_to_execute': task_group.get('task_ids', []),
                        'storage_requirements': segment.get('storage_constraints_satisfied', {})
                    }
                    job_id += 1

        return slurm_config

    def _generate_cwl_script(self, segmentation_results: Dict) -> Dict:
        """Generate CWL-compatible execution script"""
        return {
            'script_type': 'cwl',
            'cwlVersion': 'v1.2',
            'class': 'Workflow',
            'requirements': {
                'ResourceRequirement': {},
                'DockerRequirement': {},
                'InitialWorkDirRequirement': {}
            },
            'inputs': {},
            'outputs': {},
            'steps': {}
        }

    def _generate_snakemake_script(self, segmentation_results: Dict) -> Dict:
        """Generate Snakemake-compatible execution script"""
        return {
            'script_type': 'snakemake',
            'rules': {},
            'cluster_config': {},
            'resource_specifications': {}
        }

    def _calculate_resource_requirements(self, segmentation_results: Dict) -> Dict:
        """Calculate total resource requirements for execution"""
        requirements = {
            'total_compute_nodes': 0,
            'storage_requirements': {},
            'memory_requirements': {},
            'estimated_execution_time': {},
            'cost_estimates': {}
        }

        # Calculate based on horizontal segments (parallel execution)
        horizontal_segments = segmentation_results.get('horizontal_segments', {})
        total_parallel_segments = 0
        for lineage_id, segments in horizontal_segments.items():
            total_parallel_segments += len(segments)

        requirements['total_compute_nodes'] = total_parallel_segments

        # Calculate storage requirements
        for lineage_id, segments in horizontal_segments.items():
            for segment in segments:
                if segment.get('segment_type') == 'horizontal':
                    storage_constraints = segment.get('storage_constraints_satisfied', {})
                    storage_type = segment.get('spm_storage_assignment', {}).get('producer_storage', 'ssd')

                    if storage_type not in requirements['storage_requirements']:
                        requirements['storage_requirements'][storage_type] = {
                            'bandwidth_mib_total': 0,
                            'storage_mb_total': 0,
                            'iops_total': 0
                        }

                    perf_chars = segment.get('performance_characteristics', {})
                    requirements['storage_requirements'][storage_type]['bandwidth_mib_total'] += perf_chars.get('expected_bandwidth_mib', 0)
                    requirements['storage_requirements'][storage_type]['storage_mb_total'] += perf_chars.get('storage_required_mb', 0)
                    requirements['storage_requirements'][storage_type]['iops_total'] += perf_chars.get('iops_required', 0)

        return requirements

    def save_execution_plan(self, execution_plan: Dict, output_file: str) -> str:
        """
        Save execution plan to file

        Args:
            execution_plan: Generated execution plan dictionary
            output_file: Path to output file

        Returns:
            Path to saved file
        """
        with open(output_file, 'w') as f:
            json.dump(execution_plan, f, indent=2, default=str)

        return output_file

    def generate_scheduler_scripts(self, execution_plan: Dict, output_dir: str = '.') -> List[str]:
        """
        Generate actual script files for schedulers

        Args:
            execution_plan: Generated execution plan
            output_dir: Directory to save scripts

        Returns:
            List of generated script file paths
        """
        import os

        scheduler_format = execution_plan.get('format', 'nextflow')
        generated_files = []

        if scheduler_format == 'nextflow':
            generated_files.extend(self._write_nextflow_files(execution_plan, output_dir))
        elif scheduler_format == 'slurm':
            generated_files.extend(self._write_slurm_files(execution_plan, output_dir))

        return generated_files

    def _write_nextflow_files(self, execution_plan: Dict, output_dir: str) -> List[str]:
        """Write Nextflow configuration and script files"""
        import os

        files = []

        # Write nextflow.config
        config_content = self._generate_nextflow_config_content(execution_plan)
        config_file = os.path.join(output_dir, 'nextflow.config')
        with open(config_file, 'w') as f:
            f.write(config_content)
        files.append(config_file)

        # Write main.nf workflow script
        workflow_content = self._generate_nextflow_workflow_content(execution_plan)
        workflow_file = os.path.join(output_dir, 'main.nf')
        with open(workflow_file, 'w') as f:
            f.write(workflow_content)
        files.append(workflow_file)

        return files

    def _write_slurm_files(self, execution_plan: Dict, output_dir: str) -> List[str]:
        """Write Slurm job script files"""
        import os

        files = []
        job_scripts = execution_plan.get('execution_scripts', {}).get('job_scripts', {})

        for job_name, job_config in job_scripts.items():
            script_content = self._generate_slurm_script_content(job_config)
            script_file = os.path.join(output_dir, f"{job_name}.sh")
            with open(script_file, 'w') as f:
                f.write(script_content)
            files.append(script_file)

        return files

    def _generate_nextflow_config_content(self, execution_plan: Dict) -> str:
        """Generate nextflow.config content"""
        storage_config = execution_plan.get('storage_configuration', {})

        config = f"""
// FastFlow-generated Nextflow configuration
// Generated: {execution_plan.get('metadata', {}).get('generated_at')}

executor {{
    name = 'slurm'
    queueSize = {execution_plan.get('resource_requirements', {}).get('total_compute_nodes', 100)}
}}

// Storage configuration
params {{
    primary_storage = '{storage_config.get('primary_storage', 'ssd')}'
    workDir = '{storage_config.get('mount_requirements', {}).get('ssd', {}).get('mount_point', '/mnt/ssd/workflow')}'
}}

// Process configuration
process {{
    executor = 'slurm'
    queue = 'normal'

    // Default resources
    cpus = 1
    memory = '8.GB'
    time = '4.h'
"""

        # Add storage-specific process configurations
        for process_name, process_config in execution_plan.get('execution_scripts', {}).get('processes', {}).items():
            storage_type = process_config.get('storage_type', 'ssd')
            config += f"""

    withName: '{process_name}' {{
        clusterOptions = '--constraint={storage_type}'
        scratch = {str(process_config.get('scratch', False)).lower()}
    }}"""

        config += "\n}\n"
        return config

    def _generate_nextflow_workflow_content(self, execution_plan: Dict) -> str:
        """Generate main.nf workflow content"""
        workflow = f"""
#!/usr/bin/env nextflow

// FastFlow-generated Nextflow workflow
// Generated: {execution_plan.get('metadata', {}).get('generated_at')}

"""

        # Add process definitions
        for process_name, process_config in execution_plan.get('execution_scripts', {}).get('processes', {}).items():
            workflow += f"""
process {process_name} {{
    cpus {process_config.get('cpus', 1)}
    memory '{process_config.get('memory', '8.GB')}'
    time '{process_config.get('time', '4.h')}'

    input:
    // Add your input channels here

    output:
    // Add your output channels here

    script:
    '''
    // Execute tasks: {', '.join(process_config.get('tasks', []))}
    // Stage: {process_config.get('stage', 'unknown')}
    // Storage: {process_config.get('storage_type', 'ssd')}

    echo "Executing {process_name}"
    // Add your actual task execution commands here
    '''
}}
"""

        # Add workflow definition
        workflow += """
workflow {
    // Define your workflow logic here
    // Connect processes based on data dependencies
}
"""
        return workflow

    def _generate_slurm_script_content(self, job_config: Dict) -> str:
        """Generate Slurm script content"""
        script = f"""#!/bin/bash
#SBATCH --job-name={job_config.get('job_name')}
#SBATCH --partition={job_config.get('partition', 'normal')}
#SBATCH --ntasks={job_config.get('ntasks', 1)}
#SBATCH --cpus-per-task={job_config.get('cpus_per_task', 1)}
#SBATCH --memory={job_config.get('memory', '8G')}
#SBATCH --time={job_config.get('time', '04:00:00')}
#SBATCH --constraint={job_config.get('constraint', 'ssd')}

# FastFlow-generated Slurm job script
# Storage requirements: {job_config.get('storage_requirements', {})}

# Set up working directory
export WORKDIR=/mnt/ssd/workflow
mkdir -p $WORKDIR
cd $WORKDIR

# Execute tasks
echo "Executing tasks: {', '.join(job_config.get('tasks_to_execute', []))}"

# TODO: Add your actual task execution commands here
for task in {' '.join(job_config.get('tasks_to_execute', []))}; do
    echo "Running task: $task"
    # ./run_task.sh $task
done

echo "Job {job_config.get('job_name')} completed"
"""
        return script