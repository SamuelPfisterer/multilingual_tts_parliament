import argparse
import pandas as pd
import tarfile
import json
import logging
from pathlib import Path
from pydub import AudioSegment
from pydub.exceptions import CouldntDecodeError
import os
import time
import io
from typing import Tuple, List, Optional, Dict, Set
import shutil
import traceback
import signal
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- Status Constants ---
STATUS_PENDING_LOCAL_TAR = "pending_local_tar"
STATUS_COMPLETED_LOCAL_TAR = "completed_local_tar"
STATUS_ERROR_LOCAL_TAR_CER = "error_local_tar_cer"
STATUS_ERROR_LOCAL_TAR_PROCESSING = "error_local_tar_processing"
STATUS_ERROR_LOCAL_TAR_MISSING_SOURCE = "error_local_tar_missing_source"
STATUS_ERROR_LOCAL_TAR_AUDIO_LOAD = "error_local_tar_audio_load"

# Global variables for signal handling
manifest_df = None
country_manifest_path = None
manifest_updates_collector = []

def signal_handler(signum, frame):
    """Handle interruption signals by saving the manifest before exit."""
    if manifest_df is not None and country_manifest_path is not None and manifest_updates_collector:
        logging.info("Received interrupt signal. Saving manifest updates before exit...")
        # Apply any pending updates
        for original_idx, status, error_msg in manifest_updates_collector:
            manifest_df.loc[original_idx, "webdataset_status"] = status
            if pd.notna(error_msg) and error_msg:
                manifest_df.loc[original_idx, "error_message"] = error_msg
        save_manifest_atomically(manifest_df, country_manifest_path)
        logging.info("Manifest saved. Exiting...")
    sys.exit(0)

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def print_status_report(manifest_df: pd.DataFrame, splits_df: pd.DataFrame):
    """Print a detailed status report of WebDataset processing progress."""
    print("\n=== WebDataset Processing Status Report ===\n")
    
    # Get total counts
    total_segments = len(manifest_df)
    if total_segments == 0:
        print("No segments found in manifest.")
        return
        
    # Overall statistics
    print("Overall Progress:")
    print("-" * 50)
    status_counts = manifest_df['webdataset_status'].value_counts()
    for status, count in status_counts.items():
        percentage = (count / total_segments) * 100
        print(f"{status}: {count} segments ({percentage:.1f}%)")
    
    # Per-split statistics
    print("\nProgress by Split:")
    print("-" * 50)
    
    # Merge with splits information, explicitly dropping any split column from manifest_df
    merged_df = pd.merge(
        manifest_df.drop(columns=['split'], errors='ignore'),
        splits_df[['source_audio_path', 'split']],
        on='source_audio_path',
        how='left'
    )
    
    for split in ['test', 'validation', 'train']:
        split_df = merged_df[merged_df['split'] == split]
        if len(split_df) == 0:
            print(f"\n{split.capitalize()} Split: No segments found")
            continue
            
        print(f"\n{split.capitalize()} Split:")
        split_total = len(split_df)
        split_status_counts = split_df['webdataset_status'].value_counts()
        
        for status, count in split_status_counts.items():
            percentage = (count / split_total) * 100
            print(f"  {status}: {count} segments ({percentage:.1f}%)")
            
        # Show error breakdown if there are errors
        error_df = split_df[split_df['webdataset_status'].str.startswith('error_')]
        if not error_df.empty:
            print("\n  Error Breakdown:")
            error_types = error_df['webdataset_status'].value_counts()
            for error_type, count in error_types.items():
                percentage = (count / split_total) * 100
                print(f"    {error_type}: {count} segments ({percentage:.1f}%)")
    
    print("\n" + "=" * 50)

def save_manifest_atomically(df, path):
    """Saves a DataFrame to CSV atomically."""
    temp_path = path.with_suffix(f"{path.suffix}.tmp.{os.getpid()}")
    df.to_csv(temp_path, index=False)
    os.replace(temp_path, path)
    logging.info(f"Manifest saved to {path}")

def add_segment_to_tar(
    tar_obj, 
    segment_row, 
    segment_audio_bytes,
    audio_format, 
    manifest_updates_list
) -> int:
    """Adds a single segment (audio and JSON) to an open TAR object."""
    segment_key = segment_row["key"]
    original_manifest_index = segment_row.name

    try:
        # Create metadata JSON
        metadata = {
            "key": segment_key,
            "country": segment_row["country"],
            "language": segment_row["language"],
            "video_id": segment_row["video_id"],
            "transcript_id": segment_row["transcript_id"],
            "source_audio_path": segment_row["source_audio_path"],
            "start_seconds": segment_row["start_seconds"],
            "end_seconds": segment_row["end_seconds"],
            "duration_seconds": segment_row["duration_seconds"],
            "asr_transcript": segment_row["asr_transcript"],
            "human_transcript": segment_row["human_transcript"],
            "cer": segment_row["cer"],
            "wer": segment_row["wer"],
            "original_transcript_start_idx": segment_row["original_transcript_start_idx"],
            "original_transcript_end_idx": segment_row["original_transcript_end_idx"],
        }
        metadata_json_str = json.dumps(metadata, indent=2)
        metadata_bytes = metadata_json_str.encode("utf-8")
        
        json_tar_info = tarfile.TarInfo(name=f"{segment_key}.json")
        json_tar_info.size = len(metadata_bytes)
        json_tar_info.mtime = int(time.time())
        tar_obj.addfile(json_tar_info, fileobj=io.BytesIO(metadata_bytes))

        # Add audio bytes
        audio_tar_info = tarfile.TarInfo(name=f"{segment_key}.{audio_format}")
        audio_tar_info.size = len(segment_audio_bytes)
        audio_tar_info.mtime = int(time.time())
        tar_obj.addfile(audio_tar_info, fileobj=io.BytesIO(segment_audio_bytes))
        
        manifest_updates_list.append((original_manifest_index, STATUS_COMPLETED_LOCAL_TAR, ""))
        return len(metadata_bytes) + len(segment_audio_bytes)
    except Exception as e:
        logging.error(f"Error adding segment {segment_key} to TAR: {e}")
        manifest_updates_list.append((original_manifest_index, STATUS_ERROR_LOCAL_TAR_PROCESSING, f"TAR add error: {e}"))
        return 0

def process_audio_file_segments(
    source_audio_path: str,
    segments_df: pd.DataFrame,
    tar_obj: tarfile.TarFile,
    audio_format: str,
    max_cer: Optional[float],
    manifest_updates_list: List[Tuple[int, str, str]],
    should_save_manifest: bool = True  # New parameter
) -> Tuple[int, int]:
    """
    Process all pending segments from a single audio file.
    
    Args:
        source_audio_path: Path to the source audio file
        segments_df: DataFrame containing segments for this audio file
        tar_obj: Open TAR file object to write to
        audio_format: Target audio format
        max_cer: Maximum allowed CER value (None for no filtering)
        manifest_updates_list: List to collect manifest updates
        should_save_manifest: Whether to save the manifest updates
        
    Returns:
        Tuple of (total_bytes_added, segments_processed)
    """
    if not Path(source_audio_path).exists():
        error_msg = f"Source audio file not found: {source_audio_path}"
        logging.error(error_msg)
        for idx in segments_df.index:
            manifest_updates_list.append((idx, STATUS_ERROR_LOCAL_TAR_MISSING_SOURCE, error_msg))
        if should_save_manifest:
            save_manifest_updates()
        return 0, 0

    try:
        audio_obj = AudioSegment.from_file(source_audio_path)
    except Exception as e:
        error_status = STATUS_ERROR_LOCAL_TAR_AUDIO_LOAD
        error_msg = f"Failed to load audio: {str(e)}\nTraceback:\n{traceback.format_exc()}"
        logging.error(f"{error_msg} - {source_audio_path}")
        for idx in segments_df.index:
            manifest_updates_list.append((idx, error_status, error_msg))
        if should_save_manifest:
            save_manifest_updates()
        return 0, 0

    total_bytes_added = 0
    segments_processed = 0

    # Process each segment from this audio file
    for idx, segment in segments_df.iterrows():
        # Check CER if needed
        if max_cer is not None and segment.get("cer", 1.0) > max_cer:
            manifest_updates_list.append((idx, STATUS_ERROR_LOCAL_TAR_CER, f"CER {segment['cer']:.4f} > {max_cer}"))
            continue

        # Extract segment
        start_ms = int(segment["start_seconds"] * 1000)
        end_ms = int(segment["end_seconds"] * 1000)
        if start_ms < 0: start_ms = 0
        if start_ms >= end_ms:
            manifest_updates_list.append((idx, STATUS_ERROR_LOCAL_TAR_PROCESSING, "Start time >= end time"))
            continue

        try:
            segment_audio = audio_obj[start_ms:end_ms]
            audio_bytes_io = io.BytesIO()
            segment_audio.export(audio_bytes_io, format=audio_format)
            segment_audio_bytes = audio_bytes_io.getvalue()
            audio_bytes_io.close()

            bytes_added = add_segment_to_tar(tar_obj, segment, segment_audio_bytes, audio_format, manifest_updates_list)
            if bytes_added > 0:
                total_bytes_added += bytes_added
                segments_processed += 1

        except Exception as e:
            logging.error(f"Error processing segment {segment['key']}: {e}")
            manifest_updates_list.append((idx, STATUS_ERROR_LOCAL_TAR_PROCESSING, f"Segment processing error: {e}"))

    # Save manifest updates after processing each audio file
    if should_save_manifest:
        save_manifest_updates()
        
    return total_bytes_added, segments_processed

def save_manifest_updates():
    """Apply collected updates to manifest and save it."""
    global manifest_df, country_manifest_path, manifest_updates_collector
    if manifest_df is not None and country_manifest_path is not None and manifest_updates_collector:
        for original_idx, status, error_msg in manifest_updates_collector:
            manifest_df.loc[original_idx, "webdataset_status"] = status
            if pd.notna(error_msg) and error_msg:
                manifest_df.loc[original_idx, "error_message"] = error_msg
        save_manifest_atomically(manifest_df, country_manifest_path)
        manifest_updates_collector.clear()

def verify_tar_contents(tar_path: Path) -> Set[str]:
    """
    Verify contents of a TAR file and return set of successfully processed segment keys.
    
    Args:
        tar_path: Path to the TAR file
        
    Returns:
        Set of segment keys that are properly stored in the TAR (have both .json and audio file)
    """
    if not tar_path.exists():
        return set()
        
    try:
        completed_segments = set()
        with tarfile.open(tar_path, 'r') as tar:
            members = tar.getmembers()
            # Group files by segment key
            segments = {}
            for member in members:
                # Extract segment key (remove extension)
                key = os.path.splitext(member.name)[0]
                if key not in segments:
                    segments[key] = set()
                segments[key].add(os.path.splitext(member.name)[1])
                
            # Check which segments have both .json and audio file
            for key, extensions in segments.items():
                if '.json' in extensions and any(ext in extensions for ext in ['.opus', '.wav', '.mp3']):
                    completed_segments.add(key)
                    
        return completed_segments
    except Exception as e:
        logging.error(f"Error verifying TAR {tar_path}: {e}")
        return set()

def verify_and_update_manifest(
    manifest_df: pd.DataFrame,
    country_output_dir: Path,
    splits_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Verify all TAR files and update manifest status based on actual contents.
    
    Args:
        manifest_df: DataFrame containing the manifest
        country_output_dir: Root directory containing TAR files
        splits_df: DataFrame containing split information
        
    Returns:
        Updated manifest DataFrame
    """
    logging.info("Verifying TAR contents and updating manifest...")
    
    # Create a copy of the manifest to update
    updated_manifest = manifest_df.copy()
    
    # Track statistics
    total_segments = 0
    verified_segments = 0
    mismatched_segments = 0
    
    # Check each split directory
    for split in ['test', 'validation', 'train']:
        split_dir = country_output_dir / split
        if not split_dir.exists():
            continue
            
        # Find all TAR files in this split
        tar_files = list(split_dir.glob('*.tar'))
        if not tar_files:
            continue
            
        logging.info(f"Checking {len(tar_files)} TAR files in {split} split...")
        
        # Verify each TAR file
        for tar_path in tar_files:
            completed_segments = verify_tar_contents(tar_path)
            if completed_segments:
                # Update status for segments that are actually in the TAR
                mask = updated_manifest['key'].isin(completed_segments)
                total_segments += len(completed_segments)
                
                # Check for mismatches
                current_status = updated_manifest.loc[mask, 'webdataset_status']
                mismatches = (current_status != STATUS_COMPLETED_LOCAL_TAR).sum()
                mismatched_segments += mismatches
                
                if mismatches > 0:
                    logging.info(f"Found {mismatches} segments in {tar_path.name} marked incorrectly in manifest")
                
                # Update status
                updated_manifest.loc[mask, 'webdataset_status'] = STATUS_COMPLETED_LOCAL_TAR
                updated_manifest.loc[mask, 'error_message'] = ''
                verified_segments += len(completed_segments)
    
    logging.info(f"TAR verification complete:")
    logging.info(f"- Total segments found in TARs: {total_segments}")
    logging.info(f"- Verified complete segments: {verified_segments}")
    logging.info(f"- Status mismatches fixed: {mismatched_segments}")
    
    return updated_manifest

def main():
    global manifest_df, country_manifest_path, manifest_updates_collector
    
    parser = argparse.ArgumentParser(
        description="Create local WebDataset TAR shards from audio segments."
    )
    parser.add_argument("--country", required=True, help="Country name (e.g., germany)")
    parser.add_argument(
        "--manifest-dir",
        type=Path,
        default="./manifests",
        help="Directory containing _manifest.csv and _splits.csv files.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default="./webdataset_output",
        help="Root directory to save the local TAR shards.",
    )
    parser.add_argument(
        "--max-videos",
        type=int,
        default=None,
        help="Optional: Process segments from at most N video_ids for testing.",
    )
    parser.add_argument(
        "--max-shard-size-gb",
        type=float,
        default=1.0,
        help="Maximum size of each TAR shard in GB.",
    )
    parser.add_argument(
        "--max-cer",
        type=float,
        default=None,
        help="Optional: Maximum Character Error Rate to include a segment.",
    )
    parser.add_argument(
        "--audio-format",
        type=str,
        default="opus",
        help="Audio format for segments within TARs. Source audio is assumed to be opus.",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show processing status report and exit.",
    )
    parser.add_argument(
        "--check-tar",
        action="store_true",
        help="When used with --status, verify TAR contents and update manifest accordingly.",
    )
    args = parser.parse_args()

    if args.audio_format != "opus":
        logging.warning(
            f"Script is optimized for opus source. Outputting as {args.audio_format} using pydub export, "
            f"which will re-encode if source is opus and target is different."
        )

    country_manifest_path = args.manifest_dir / f"{args.country}_manifest.csv"
    country_splits_path = args.manifest_dir / f"{args.country}_splits.csv"
    country_output_dir = args.output_dir / args.country

    if not country_manifest_path.exists():
        logging.error(f"Manifest file not found: {country_manifest_path}")
        return
    if not country_splits_path.exists():
        logging.error(f"Splits file not found: {country_splits_path}")
        return

    args.output_dir.mkdir(parents=True, exist_ok=True)
    country_output_dir.mkdir(parents=True, exist_ok=True)

    try:
        manifest_df = pd.read_csv(country_manifest_path, keep_default_na=False)
        logging.info(f"Loaded manifest: {country_manifest_path} with {len(manifest_df)} segments.")
    except Exception as e:
        logging.error(f"Error loading manifest {country_manifest_path}: {e}")
        return
    
    try:
        splits_df = pd.read_csv(country_splits_path, keep_default_na=False)
        logging.info(f"Loaded splits: {country_splits_path} with {len(splits_df)} entries.")
        logging.info(f"Splits DataFrame columns: {splits_df.columns.tolist()}")
    except Exception as e:
        logging.error(f"Error loading splits {country_splits_path}: {e}")
        return

    # If --status flag is used, show status report and exit
    if args.status:
        if args.check_tar:
            # Verify TAR contents and update manifest if needed
            manifest_df = verify_and_update_manifest(manifest_df, country_output_dir, splits_df)
            # Save updated manifest if changes were made
            save_manifest_atomically(manifest_df, country_manifest_path)
        print_status_report(manifest_df, splits_df)
        return

    # Handle webdataset_status column - set to pending if:
    # 1. Column doesn't exist
    # 2. Column exists but value is NA/None
    # 3. Column exists but value is empty string
    if "webdataset_status" not in manifest_df.columns:
        manifest_df["webdataset_status"] = STATUS_PENDING_LOCAL_TAR
        logging.info("Created new webdataset_status column with all pending")
    else:
        # Debug information before changes
        empty_status_count = (manifest_df["webdataset_status"] == "").sum()
        na_status_count = manifest_df["webdataset_status"].isna().sum()
        unique_statuses = manifest_df["webdataset_status"].unique()
        logging.info(f"Webdataset status column stats before update:")
        logging.info(f"- Empty string status count: {empty_status_count}")
        logging.info(f"- NA status count: {na_status_count}")
        logging.info(f"- Unique status values: {unique_statuses}")
        
        # Replace both NA and empty strings with pending status
        manifest_df.loc[manifest_df["webdataset_status"].isna() | (manifest_df["webdataset_status"] == ""), "webdataset_status"] = STATUS_PENDING_LOCAL_TAR
        
        # Debug information after changes
        logging.info(f"Webdataset status column stats after update:")
        logging.info(f"- Unique status values: {manifest_df['webdataset_status'].unique()}")
    if "error_message" not in manifest_df.columns:
        manifest_df["error_message"] = ""
    if "cer" not in manifest_df.columns:
        logging.warning("'cer' column not found in manifest. Will not filter by CER.")
        manifest_df["cer"] = 1.0
    else:
        manifest_df["cer"] = pd.to_numeric(manifest_df["cer"], errors='coerce').fillna(1.0)

    all_pending_df = manifest_df[manifest_df["webdataset_status"] == STATUS_PENDING_LOCAL_TAR].copy()
    
    if all_pending_df.empty:
        logging.info(f"No segments with status '{STATUS_PENDING_LOCAL_TAR}' found. Exiting.")
        return
    logging.info(f"Found {len(all_pending_df)} segments with status '{STATUS_PENDING_LOCAL_TAR}'.")

    if args.max_videos is not None and args.max_videos > 0:
        if "video_id" not in all_pending_df.columns:
            logging.error("Cannot apply --max-videos: 'video_id' column not found.")
            return
        unique_video_ids = all_pending_df["video_id"].unique()
        videos_to_process = unique_video_ids[: args.max_videos]
        all_pending_df = all_pending_df[all_pending_df["video_id"].isin(videos_to_process)]
        if all_pending_df.empty:
            logging.info(f"No pending segments for the selected {len(videos_to_process)} video_ids after filtering. Exiting.")
            return
        logging.info(f"Filtered to {len(all_pending_df)} segments from {len(videos_to_process)} video_ids for --max-videos.")

    # Merge with splits_df to get split information
    splits_df_subset = splits_df[["source_audio_path", "split"]].copy()
    logging.info(f"Splits subset columns before merge: {splits_df_subset.columns.tolist()}")
    
    pending_df_merged = pd.merge(
        all_pending_df, 
        splits_df_subset, 
        on="source_audio_path", 
        how="left",
        suffixes=('_to_drop', '')  # This ensures the split from splits_df is kept without suffix
    )
    
    # Drop any duplicate columns from the manifest if they exist
    cols_to_drop = [col for col in pending_df_merged.columns if col.endswith('_to_drop')]
    if cols_to_drop:
        pending_df_merged.drop(columns=cols_to_drop, inplace=True)
        
    logging.info(f"Merged DataFrame columns: {pending_df_merged.columns.tolist()}")
    logging.info(f"Number of rows after merge: {len(pending_df_merged)}")
    
    if "split" not in pending_df_merged.columns:
        logging.error("'split' column missing after merge! Available columns:")
        for col in pending_df_merged.columns:
            logging.error(f"  - '{col}'")

    if pending_df_merged["split"].isnull().any():
        unmapped_count = pending_df_merged["split"].isnull().sum()
        logging.warning(f"{unmapped_count} pending segments could not be mapped to a split and will be skipped.")
        pending_df_merged.dropna(subset=["split"], inplace=True)
        if pending_df_merged.empty:
            logging.info("No pending segments remaining after removing unmapped ones. Exiting.")
            return

    max_shard_size_bytes = args.max_shard_size_gb * (1024**3)
    manifest_updates_collector = []

    # --- Process Test and Validation Splits (One TAR each) ---
    for split_name_simple in ["test", "validation"]:
        current_split_segments_df = pending_df_merged[pending_df_merged["split"] == split_name_simple]
        if current_split_segments_df.empty:
            logging.info(f"No pending segments for split '{split_name_simple}'.")
            continue
        
        logging.info(f"Processing {len(current_split_segments_df)} pending segments for split '{split_name_simple}'.")
        output_split_dir = country_output_dir / split_name_simple
        output_split_dir.mkdir(parents=True, exist_ok=True)
        tar_path = output_split_dir / f"{args.country}-{split_name_simple}.tar"
        temp_tar_path = tar_path.with_suffix(f".{split_name_simple}.tmp.{os.getpid()}")

        try:
            with tarfile.open(temp_tar_path, "w") as tar_obj:
                # Group by source_audio_path and process each file's segments
                for source_audio_path, file_segments_df in current_split_segments_df.groupby("source_audio_path"):
                    bytes_added, segments_processed = process_audio_file_segments(
                        source_audio_path,
                        file_segments_df,
                        tar_obj,
                        args.audio_format,
                        args.max_cer,
                        manifest_updates_collector,
                        should_save_manifest=True
                    )
                    if segments_processed > 0:
                        logging.info(f"Added {segments_processed} segments ({bytes_added/1024/1024:.2f} MB) from {source_audio_path}")

            os.replace(temp_tar_path, tar_path)
            logging.info(f"Finished shard {tar_path} for split '{split_name_simple}'")
        except Exception as e:
            logging.error(f"Failed to create TAR {temp_tar_path} for split '{split_name_simple}': {e}")
            if temp_tar_path.exists(): os.remove(temp_tar_path)
    
    # Apply updates for test/validation and save manifest
    for original_idx, status, error_msg in manifest_updates_collector:
        manifest_df.loc[original_idx, "webdataset_status"] = status
        if pd.notna(error_msg) and error_msg: manifest_df.loc[original_idx, "error_message"] = error_msg
    if manifest_updates_collector:
        save_manifest_atomically(manifest_df, country_manifest_path)
    manifest_updates_collector.clear()

    # --- Process Train Split (Potentially multiple TARs) ---
    train_segments_df = pending_df_merged[pending_df_merged["split"] == "train"]
    active_train_tar_obj = None
    if train_segments_df.empty:
        logging.info("No pending segments for train split(s). Exiting train processing.")
    else:
        logging.info(f"Processing {len(train_segments_df)} pending segments for train split(s)...")
        output_train_dir = country_output_dir / "train"
        output_train_dir.mkdir(parents=True, exist_ok=True)

        # Find the highest existing train shard index
        train_shard_idx = 0
        last_train_tar = None
        for existing_file in output_train_dir.glob(f"{args.country}-train-*.tar"):
            try:
                idx_part = existing_file.stem.split("-")[-1]
                if idx_part.isdigit():
                    current_idx = int(idx_part)
                    if current_idx > train_shard_idx:
                        train_shard_idx = current_idx
                        last_train_tar = existing_file
            except: pass

        # If we found an existing TAR, check its size
        if last_train_tar is not None:
            try:
                active_train_tar_current_size_bytes = last_train_tar.stat().st_size
                if active_train_tar_current_size_bytes < max_shard_size_bytes:
                    # Last TAR exists and has space, create temp copy to append to
                    active_train_tar_temp_path = last_train_tar.with_suffix(f".tmp.{os.getpid()}")
                    shutil.copy2(last_train_tar, active_train_tar_temp_path)
                    active_train_tar_obj = tarfile.open(active_train_tar_temp_path, "a")
                    logging.info(f"Continuing with existing train shard {last_train_tar}")
            except Exception as e:
                logging.error(f"Error checking train shard size: {e}")

        active_train_tar_current_size_bytes = 0

        # Group by source_audio_path and process each file's segments
        for source_audio_path, file_segments_df in train_segments_df.groupby("source_audio_path"):
            # Estimate total size needed for this file's segments
            avg_segment_size = 500 * 1024  # Rough estimate: 500KB per segment
            estimated_total_size = len(file_segments_df) * avg_segment_size

            # If adding this file's segments might exceed shard size, or if no active TAR exists
            if active_train_tar_obj is None or \
               (active_train_tar_current_size_bytes > 0 and 
                active_train_tar_current_size_bytes + estimated_total_size > max_shard_size_bytes):
                
                # Close current TAR if it exists
                if active_train_tar_obj is not None:
                    active_train_tar_obj.close()
                    if active_train_tar_temp_path and active_train_tar_temp_path.exists():
                        final_path = Path(str(active_train_tar_temp_path).replace(f".tmp.{os.getpid()}", ".tar"))
                        os.replace(active_train_tar_temp_path, final_path)
                        logging.info(f"Finished train shard {final_path}")
                    train_shard_idx += 1  # Only increment when we actually need a new file

                # Create new TAR
                current_train_shard_path = output_train_dir / f"{args.country}-train-{train_shard_idx:05d}.tar"
                active_train_tar_temp_path = current_train_shard_path.with_suffix(f".tmp.{os.getpid()}")
                logging.info(f"Starting new train shard: {current_train_shard_path}")
                active_train_tar_obj = tarfile.open(active_train_tar_temp_path, "w")
                active_train_tar_current_size_bytes = 0

            # Process this file's segments
            bytes_added, segments_processed = process_audio_file_segments(
                source_audio_path,
                file_segments_df,
                active_train_tar_obj,
                args.audio_format,
                args.max_cer,
                manifest_updates_collector,
                should_save_manifest=True
            )
            
            if segments_processed > 0:
                active_train_tar_current_size_bytes += bytes_added
                logging.info(f"Added {segments_processed} segments ({bytes_added/1024/1024:.2f} MB) from {source_audio_path}")

        # Close final train TAR if it exists
        if active_train_tar_obj is not None:
            active_train_tar_obj.close()
            if active_train_tar_temp_path and active_train_tar_temp_path.exists():
                final_path = Path(str(active_train_tar_temp_path).replace(f".tmp.{os.getpid()}", ".tar"))
                os.replace(active_train_tar_temp_path, final_path)
                logging.info(f"Finished final train shard {final_path}")

        # Apply updates for train and save manifest
        for original_idx, status, error_msg in manifest_updates_collector:
            manifest_df.loc[original_idx, "webdataset_status"] = status
            if pd.notna(error_msg) and error_msg: manifest_df.loc[original_idx, "error_message"] = error_msg
        if manifest_updates_collector:
            save_manifest_atomically(manifest_df, country_manifest_path)
        manifest_updates_collector.clear()

    logging.info("All processing complete.")

if __name__ == "__main__":
    main() 