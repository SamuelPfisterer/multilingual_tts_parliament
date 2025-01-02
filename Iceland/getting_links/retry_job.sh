#!/bin/bash
#SBATCH --mail-type=NONE
#SBATCH --output=/itet-stor/spfisterer/net_scratch/Downloading/Iceland/logs/%j.out
#SBATCH --error=/itet-stor/spfisterer/net_scratch/Downloading/Iceland/logs/%j.err
#SBATCH --mem=20G
#SBATCH --nodes=1
#SBATCH --cpus-per-task=4
#SBATCH --array=0-63%20  # 64 batches (0-63) with max 20 concurrent jobs

ETH_USERNAME=spfisterer
PROJECT_NAME=Downloading/Iceland
DIRECTORY=/itet-stor/${ETH_USERNAME}/net_scratch/${PROJECT_NAME}
CONDA_ENVIRONMENT=video_processing
LINKS_PER_BATCH=1000  # Process 1000 links per batch (63720/1000 ≈ 64 batches)
TOTAL_ROWS=63720

# Calculate start and end indices for this job
START_IDX=$((SLURM_ARRAY_TASK_ID * LINKS_PER_BATCH))
if [ $SLURM_ARRAY_TASK_ID -eq 63 ]; then
    # Last batch handles remaining rows
    END_IDX=$TOTAL_ROWS
else
    END_IDX=$(((SLURM_ARRAY_TASK_ID + 1) * LINKS_PER_BATCH))
fi

cd ${DIRECTORY}

# Activate conda
source /itet-stor/${ETH_USERNAME}/net_scratch/conda/bin/activate
conda activate ${CONDA_ENVIRONMENT}

# Create necessary directories
mkdir -p ${DIRECTORY}/logs
mkdir -p ${DIRECTORY}/results/retry

# Log start of job
echo "=== Job Information ===" >> logs/retry_job_${SLURM_ARRAY_TASK_ID}.log
echo "Job ID: ${SLURM_JOB_ID}" >> logs/retry_job_${SLURM_ARRAY_TASK_ID}.log
echo "Array Task ID: ${SLURM_ARRAY_TASK_ID}" >> logs/retry_job_${SLURM_ARRAY_TASK_ID}.log
echo "Start Index: ${START_IDX}" >> logs/retry_job_${SLURM_ARRAY_TASK_ID}.log
echo "End Index: ${END_IDX}" >> logs/retry_job_${SLURM_ARRAY_TASK_ID}.log
echo "Working Directory: ${DIRECTORY}" >> logs/retry_job_${SLURM_ARRAY_TASK_ID}.log
echo "Started at: $(date)" >> logs/retry_job_${SLURM_ARRAY_TASK_ID}.log
echo "====================\n" >> logs/retry_job_${SLURM_ARRAY_TASK_ID}.log

# Execute code with all_failed_links.csv as input
python main.py --start_idx ${START_IDX} --end_idx ${END_IDX} 2>&1 | tee -a logs/retry_job_${SLURM_ARRAY_TASK_ID}.log

# Log end of job
echo "\n====================" >> logs/retry_job_${SLURM_ARRAY_TASK_ID}.log
echo "Job finished at: $(date)" >> logs/retry_job_${SLURM_ARRAY_TASK_ID}.log
echo "====================" >> logs/retry_job_${SLURM_ARRAY_TASK_ID}.log