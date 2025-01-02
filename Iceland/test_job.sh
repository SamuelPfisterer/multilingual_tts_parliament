#!/bin/bash
#SBATCH --mail-type=NONE
#SBATCH --output=/itet-stor/spfisterer/net_scratch/Downloading/Iceland/logs/%j.out
#SBATCH --error=/itet-stor/spfisterer/net_scratch/Downloading/Iceland/logs/%j.err
#SBATCH --mem=20G
#SBATCH --nodes=1
#SBATCH --cpus-per-task=4
#SBATCH --array=0-0  # Just run the first batch

ETH_USERNAME=spfisterer
PROJECT_NAME=Downloading/Iceland
DIRECTORY=/itet-stor/${ETH_USERNAME}/net_scratch/${PROJECT_NAME}
CONDA_ENVIRONMENT=video_processing

# For test, just process 3 rows
START_IDX=0
END_IDX=3

cd ${DIRECTORY}

# Activate conda
source /itet-stor/${ETH_USERNAME}/net_scratch/conda/bin/activate
conda activate ${CONDA_ENVIRONMENT}

# Create necessary directories
mkdir -p ${DIRECTORY}/logs

# Log start of job
echo "=== Test Job Information ===" >> logs/test_job_${SLURM_ARRAY_TASK_ID}.log
echo "Job ID: ${SLURM_JOB_ID}" >> logs/test_job_${SLURM_ARRAY_TASK_ID}.log
echo "Processing test batch (${START_IDX} to ${END_IDX})" >> logs/test_job_${SLURM_ARRAY_TASK_ID}.log
echo "Started at: $(date)" >> logs/test_job_${SLURM_ARRAY_TASK_ID}.log
echo "====================\n" >> logs/test_job_${SLURM_ARRAY_TASK_ID}.log

# Execute code
python scripts/main.py --start_idx ${START_IDX} --end_idx ${END_IDX} --csv_file danish_mp4_media_links_since_2018.csv 2>&1 | tee -a logs/test_job_${SLURM_ARRAY_TASK_ID}.log

# Log end of job
echo "\n====================" >> logs/test_job_${SLURM_ARRAY_TASK_ID}.log
echo "Job finished at: $(date)" >> logs/test_job_${SLURM_ARRAY_TASK_ID}.log
echo "====================" >> logs/test_job_${SLURM_ARRAY_TASK_ID}.log 