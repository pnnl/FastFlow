# FastFlow Implementation

Workflow-agnostic caterpillar DAG segmentation and SPM storage optimization implementation.



## Quick Start

```bash
# Setup environment
python3 -m venv fastflow_env
source fastflow_env/bin/activate
pip install -r requirements.txt

# Run FastFlow analysis (example with 1000 Genomes)
python src/caterpillar_dag_detector.py data/1000genomes/1kg_workflow_data.csv data/1000genomes/1kg_script_order.json -n 5 -o workflow_analysis
python src/workflow_agnostic_spm_optimizer.py data/1000genomes/1kg_filtered_spm_results.csv workflow_analysis_flows.json workflow_analysis_statistics.json workflow_analysis_dag.json --segmented --execution-plan nextflow -o optimization_results
```

## Workflows

### 1. 1000 Genomes 


```bash
# Step 1: DAG Detection
python src/caterpillar_dag_detector.py \
  data/1000genomes/1kg_workflow_data.csv \
  data/1000genomes/1kg_script_order.json \
  -n 5 -o genomes_analysis

# Step 2: Optimization
python src/workflow_agnostic_spm_optimizer.py \
  data/1000genomes/1kg_filtered_spm_results.csv \
  genomes_analysis_flows.json \
  genomes_analysis_statistics.json \
  genomes_analysis_dag.json \
  --segmented --execution-plan nextflow \
  -o genomes_optimization
```


### 2. Montage 


```bash
# Step 1: DAG Detection
python src/caterpillar_dag_detector.py \
  data/montage/montage_workflow_data.csv \
  data/montage/montage_script_order.json \
  -n 2 -o montage_analysis

# Step 2: Optimization
python src/workflow_agnostic_spm_optimizer.py \
  data/montage/montage_filtered_spm_results.csv \
  montage_analysis_flows.json \
  montage_analysis_statistics.json \
  montage_analysis_dag.json \
  --segmented --execution-plan nextflow \
  -o montage_optimization
```



### 3. DDMD 


```bash
# Step 1: DAG Detection
python src/caterpillar_dag_detector.py \
  data/ddmd/ddmd_4n_l_workflow_data.csv \
  data/ddmd/ddmd_script_order.json \
  -n 4 -o ddmd_analysis

# Step 2: Optimization
python src/workflow_agnostic_spm_optimizer.py \
  data/ddmd/ddmd_4n_l_filtered_spm_results.csv \
  ddmd_analysis_flows.json \
  ddmd_analysis_statistics.json \
  ddmd_analysis_dag.json \
  --segmented --execution-plan nextflow \
  -o ddmd_optimization
```


## Core Components

1. **`src/caterpillar_dag_detector.py`** - Workflow-agnostic DAG analysis and pattern detection
2. **`src/workflow_agnostic_spm_optimizer.py`** - SPM storage optimization with FastFlow segmentation
3. **`src/execution_plan_generator.py`** - Multi-scheduler execution plan generation

## Output Files

### Step 1 Outputs (DAG Detection)
- `<prefix>_flows.json` - Detected parallel flows and lineage information
- `<prefix>_statistics.json` - Performance statistics per lineage
- `<prefix>_dag.json` - Complete workflow DAG structure

### Step 2 Outputs (Optimization)
- `<prefix>_results.json` - Full segmentation analysis with detailed metrics
- `<prefix>_recommendations.json` - User-friendly storage optimization recommendations
- `<prefix>_execution_plan_<scheduler>.json` - Ready-to-run scheduler execution plan

## Command Line Options

### caterpillar_dag_detector.py
- `-n, --nodes` - Number of compute nodes to analyze (filters CSV data)
- `-o, --output` - Output prefix for generated files

### workflow_agnostic_spm_optimizer.py
- `--segmented` - Enable FastFlow caterpillar segmentation
- `--execution-plan` - Generate execution plan (nextflow, slurm, cwl, snakemake)
- `-o, --output` - Output prefix for optimization results

## Installation

### Prerequisites
- Python 3.7+
- Virtual environment support

### Setup
```bash
# 1. Clone repository
git clone <repository_url>
cd FastFlow-Release

# 2. Create virtual environment
python3 -m venv fastflow_env
source fastflow_env/bin/activate  # On Windows: fastflow_env\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt
```

### Dependencies
- pandas - Data manipulation and analysis
- numpy - Numerical computing
- networkx - Graph analysis
- matplotlib - Visualization (optional)

## Input Data Format

FastFlow requires three input files per workflow:

1. **Workflow CSV** (`*_workflow_data.csv`) - Execution traces with columns:
   - `operation`, `taskName`, `fileName`, `stageOrder`, `transferSize`, `totalTime`, etc.

2. **Dependencies JSON** (`*_script_order.json`) - Workflow structure:
   ```json
   {
     "stage_name": {
       "stage_order": 1,
       "parallelism": 100,
       "num_tasks": 100,
       "predecessors": {...},
       "outputs": [...]
     }
   }
   ```

3. **SPM CSV** (`*_filtered_spm_results.csv`) - Storage performance data:
   - `producer`, `consumer`, `producerStorageType`, `consumerStorageType`, `SPM`, etc.


## FastFlow Concepts

- **Caterpillar DAGs**: Independent parallel data lineages
- **Horizontal Segmentation**: Task grouping based on storage I/O constraints
- **Vertical Segmentation**: Producer-consumer task co-location for data locality
- **SPM (Storage Performance Matching)**: Storage assignment optimization