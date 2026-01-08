<!-- -*-Mode: markdown;-*- -->
<!-- $Id: 8fa6cd46de144d84fc7d8e75968128d8d1c91c83 $ -->

FastFlow
=============================================================================

**Home**:
  - https://github.com/pnnl/DataLife
  
  - [Performance Lab for EXtreme Computing and daTa](https://github.com/perflab-exact)

  - Related: 
  [DataFlowDrs](https://github.com/pnnl/DataFlowDrs)

**About**: 

When distributed scientific workflows are not intelligently executed, they can fail time constraints. To improve workflow response time, FastFlow is a new method of scheduling that prioritizes critical flow paths and their interactions. The key insight is to use the global perspective of interacting critical flows to guide a fast (locally greedy) scheduler that uses data flow projections to select between the better of flow parallelism and flow locality. The result is a rapid, linear-time scheduling method that achieves high quality results and excels on data-intensive workflows.

**Contacts**: (_firstname_._lastname_@pnnl.gov)

  - Nathan R. Tallent ([www](https://nathantallent.github.io))
  - Jesun Firoz ([www](https://www.pnnl.gov/people/jesun-firoz))
  - Lenny Guo ([www](https://www.pnnl.gov/people/luanzheng-guo))
  - Meng Tang (Illinois Institute of Technology) ([www](https://scholar.google.com/citations?user=KXC9NesAAAAJ&hl=en))


References
-----------------------------------------------------------------------------

* H. Lee, L. Guo, M. Tang, J. Firoz, N. Tallent, A. Kougkas, and X.-H. Sun, “Data flow lifecycles for optimizing workflow coordination,” in Proc. of the Intl. Conf. for High Performance Computing, Networking, Storage and Analysis (SuperComputing), SC ’23, (New York, NY, USA), Association for Computing Machinery, November 2023. ([doi](https://doi.org/10.1145/3581784.3607104))

* M. Tang, J. Cernuda, J. Ye, L. Guo, N. R. Tallent, A. Kougkas, and X.-H. Sun, “DaYu: Optimizing distributed scientific workflows by decoding dataflow semantics and dynamics,” in Proc. of the 2024 IEEE Conf. on Cluster Computing, pp. 357–369, IEEE, September 2024. ([doi](https://doi.org/10.1109/CLUSTER59578.2024.00038))

* L. Guo, H. Lee, J. Firoz, M. Tang, and N. R. Tallent, “Improving I/O-aware workflow scheduling via data flow characterization and trade-off analysis,” in Seventh IEEE Intl. Workshop on Benchmarking, Performance Tuning and Optimization for Big Data Applications (Proc. of the IEEE Intl. Conf. on Big Data), IEEE Computer Society, December 2024.  ([doi](https://doi.org/10.1109/BigData62323.2024.10825855))

* H. Lee, J. Firoz, N. R. Tallent, L. Guo, and M. Halappanavar, “FlowForecaster: Automatically inferring detailed & interpretable workflow scaling models for forecasts,” in Proc. of the 39th IEEE Intl. Parallel and Distributed Processing Symp., IEEE Computer Society, June 2025. ([doi](https://doi.org/10.1109/IPDPS64566.2025.00045))

* J. Firoz, H. Lee, L. Guo, M. Tang, N. R. Tallent, and Z. Peng, “FastFlow: Rapid workflow response by prioritizing critical data flows and their interactions,” in Proc. of the 37th Intl. Conf. on Scalable Scientific Data Management, ACM, June 2025. ([doi](https://doi.org/10.1145/3733723.3733735))



Acknowledgements
-----------------------------------------------------------------------------

This work was supported by the U.S. Department of Energy's Office of
Advanced Scientific Computing Research:

- Orchestration for Distributed & Data-Intensive Scientific Exploration


FastFlow Implementation
=============================================================================

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
