#!/bin/bash
#SBATCH --mail-type=NONE
#SBATCH --output=/itet-stor/spfisterer/net_scratch/Downloading/Iceland/logs/%j.out
#SBATCH --error=/itet-stor/spfisterer/net_scratch/Downloading/Iceland/logs/%j.err
#SBATCH --mem=20G
#SBATCH --nodes=1
#SBATCH --cpus-per-task=4
#SBATCH --array=0-59%40  # 60 batches (0-59) with max 40 concurrent jobs

ETH_USERNAME=spfisterer
PROJECT_NAME=Downloading/Iceland
DIRECTORY=/itet-stor/${ETH_USERNAME}/net_scratch/${PROJECT_NAME}
CONDA_ENVIRONMENT=video_processing
LINKS_PER_BATCH=1000  # Process 1000 links per batch (59188/1000 ≈ 60 batches)
TOTAL_ROWS=59188

# Calculate start and end indices for this job
START_IDX=$((SLURM_ARRAY_TASK_ID * LINKS_PER_BATCH))
if [ $SLURM_ARRAY_TASK_ID -eq 59 ]; then
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

# Log start of job
echo "=== Job Information ===" >> logs/job_${SLURM_ARRAY_TASK_ID}.log
echo "Job ID: ${SLURM_JOB_ID}" >> logs/job_${SLURM_ARRAY_TASK_ID}.log
echo "Array Task ID: ${SLURM_ARRAY_TASK_ID}" >> logs/job_${SLURM_ARRAY_TASK_ID}.log
echo "Start Index: ${START_IDX}" >> logs/job_${SLURM_ARRAY_TASK_ID}.log
echo "End Index: ${END_IDX}" >> logs/job_${SLURM_ARRAY_TASK_ID}.log
echo "Started at: $(date)" >> logs/job_${SLURM_ARRAY_TASK_ID}.log
echo "====================\n" >> logs/job_${SLURM_ARRAY_TASK_ID}.log

# Execute code
python scripts/main.py --start_idx ${START_IDX} --end_idx ${END_IDX} --csv_file danish_mp4_media_links_since_2018.csv 2>&1 | tee -a logs/job_${SLURM_ARRAY_TASK_ID}.log

# Log end of job
echo "\n====================" >> logs/job_${SLURM_ARRAY_TASK_ID}.log
echo "Job finished at: $(date)" >> logs/job_${SLURM_ARRAY_TASK_ID}.log
echo "====================" >> logs/job_${SLURM_ARRAY_TASK_ID}.log