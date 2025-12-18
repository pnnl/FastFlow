# FastFlow Implementation

FastFlow caterpillar DAG segmentation and SPM storage optimization implementation.

## Quick Start

```bash
# Setup environment
python3 -m venv fastflow_env
source fastflow_env/bin/activate
pip install -r requirements.txt

# Run FastFlow analysis
python src/caterpillar_dag_detector.py data/1kg_workflow_data.csv data/1kg_script_order.json -n 5 -o enhanced_workflow
python src/workflow_agnostic_spm_optimizer.py data/1kg_filtered_spm_results.csv enhanced_workflow_flows.json enhanced_workflow_statistics.json enhanced_workflow_dag.json --segmented --execution-plan nextflow -o enhanced_execution_plan
```

## Core Components

1. **`src/caterpillar_dag_detector.py`** - Detects independent caterpillar DAGs for parallel execution
2. **`src/workflow_agnostic_spm_optimizer.py`** - SPM storage optimization with FastFlow segmentation
3. **`src/execution_plan_generator.py`** - Generates scheduler-specific execution plans

## Usage

### Step 1: Detect Caterpillar DAGs
```bash
python src/caterpillar_dag_detector.py data/1kg_workflow_data.csv data/1kg_script_order.json -n 5 -o enhanced_workflow
```

**Outputs:**
- `enhanced_workflow_flows.json`
- `enhanced_workflow_statistics.json`
- `enhanced_workflow_dag.json`

### Step 2: Generate Storage Optimization and Execution Plans
```bash
python src/workflow_agnostic_spm_optimizer.py \
  data/1kg_filtered_spm_results.csv \
  enhanced_workflow_flows.json \
  enhanced_workflow_statistics.json \
  enhanced_workflow_dag.json \
  --segmented \
  --execution-plan nextflow \
  -o enhanced_execution_plan
```

**Outputs:**
- `enhanced_execution_plan_results.json` - Full segmentation analysis
- `enhanced_execution_plan_recommendations.json` - Storage recommendations
- `enhanced_execution_plan_execution_plan_nextflow.json` - Nextflow execution plan

## Setup

### 1. Create Virtual Environment
```bash
python3 -m venv fastflow_env
source fastflow_env/bin/activate  # On Windows: fastflow_env\Scripts\activate
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

## Requirements

- Python 3.7+
- pandas
- numpy
- networkx
- matplotlib
- JSON input/output files following the specified schema

## FastFlow Concepts

- **Caterpillar DAGs**: Independent parallel data lineages
- **Horizontal Segmentation**: Task grouping based on storage I/O constraints
- **Vertical Segmentation**: Producer-consumer task co-location for data locality
- **SPM (Storage Performance Matching)**: Storage assignment optimization