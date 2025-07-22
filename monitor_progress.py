import time
import os
import glob
from datetime import datetime

def check_progress():
    """Check the current progress of the production run"""
    print(f"\n{'='*60}")
    print(f"📊 PRODUCTION PROGRESS UPDATE - {datetime.now().strftime('%H:%M:%S')}")
    print(f"{'='*60}")
    
    # Check for batch files
    batch_files = glob.glob("batch_result_IMPROVED_*.csv")
    batch_files.sort()
    
    if batch_files:
        print(f"✅ Completed batches: {len(batch_files)}")
        
        # Show latest batch info
        latest_batch = batch_files[-1]
        batch_num = latest_batch.split('_')[-1].replace('.csv', '')
        print(f"📈 Latest completed batch: {batch_num}")
        
        # Calculate progress
        total_batches = 3496
        progress_percent = (len(batch_files) / total_batches) * 100
        print(f"🎯 Overall progress: {progress_percent:.2f}% ({len(batch_files)}/{total_batches} batches)")
        
        # Estimate completion time
        if len(batch_files) > 1:
            # Rough estimate based on current rate
            estimated_hours = (total_batches - len(batch_files)) / len(batch_files) * 5 / 60  # 5 min intervals
            print(f"⏱️  Estimated time remaining: ~{estimated_hours:.1f} hours")
        
        # Check file sizes to see if batches are being processed
        latest_size = os.path.getsize(latest_batch)
        print(f"💾 Latest batch file size: {latest_size:,} bytes")
        
    else:
        print("⏳ No completed batches yet - still processing first batch")
        print("   This is normal for the first 5-10 minutes")
    
    # Check if any error files exist
    error_files = glob.glob("*error*.log")
    if error_files:
        print(f"⚠️  Error files found: {len(error_files)}")
    else:
        print("✅ No error files detected")
    
    print(f"{'='*60}")

def monitor_production_run():
    """Monitor the production run with 5-minute intervals"""
    print("🔄 STARTING 5-MINUTE PROGRESS MONITORING")
    print("Press Ctrl+C to stop monitoring")
    
    try:
        while True:
            check_progress()
            print("💤 Waiting 5 minutes for next update...")
            time.sleep(300)  # 5 minutes = 300 seconds
            
    except KeyboardInterrupt:
        print("\n\n🛑 Monitoring stopped by user")
        print("📊 Final progress check:")
        check_progress()

if __name__ == "__main__":
    monitor_production_run()
