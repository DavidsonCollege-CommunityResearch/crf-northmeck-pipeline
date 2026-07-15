# ==============================================================================
# PIPELINE ORCHESTRATOR
# Author: Paul Park, Gemini Code
# Objective: Execute all data pipelines sequentially to build the Data Warehouse
# Architecture: Direct-to-Cloud (MotherDuck) with Local Sync
# ==============================================================================

import subprocess
import os

env = os.environ.copy()
env["PYTHONPATH"] = os.getcwd()

print("🚀 Starting North Meck Insights Data Warehouse Pipeline (Direct-to-Cloud Mode)...\n")

try:
    
    print("Executing Census Pipeline...")
    subprocess.run(["python", "pipelines/nmidw_census.py"], env=env, check=True)

    print("Executing Geom Pipeline...")
    subprocess.run(["python", "pipelines/nmidw_geom.py"], env=env, check=True)

    print("\nExecuting Zillow Pipeline...")
    subprocess.run(["python", "pipelines/nmidw_zillow.py"], env=env, check=True)

    print("\nExecuting ALICE Pipeline...")
    subprocess.run(["python", "pipelines/nmidw_alice.py"], env=env, check=True)

    print("\nExecuting Charlotte ami Pipeline...")
    subprocess.run(["python", "pipelines/nmidw_charlotte.py"], env=env, check=True)

    print("\nExecuting MH/SU Facilities Pipeline...")
    subprocess.run(["python", "pipelines/nmidw_mh_su_facilities.py"], env=env, check=True)

    print("\nExecuting MH/SU Facilities Pipeline...")
    subprocess.run(["python", "pipelines/nmidw_cdc_places.py"], env=env, check=True)

    print("\nExecuting School Assessment Pipeline...")
    subprocess.run(["python", "pipelines/nmidw_schools.py"], env=env, check=True)

    print("\nExecuting Gold Pipeline...")
    subprocess.run(["python", "pipelines/nmidw_gold.py"], env=env, check=True)

    print("\nExecuting Aggregate Pipeline...")
    subprocess.run(["python", "pipelines/nmidw_aggregate.py"], env=env, check=True)

    print("\nExecuting Neighborhood Aggregate Pipeline...")
    subprocess.run(["python", "pipelines/nmidw_neighborhood_aggregate.py"], env=env, check=True)



    print("\n🌟 All Pipelines Finished! Cloud DW is live, and local backup is ready!")

except subprocess.CalledProcessError as e:
    print(f"\n❌ Pipeline execution failed during script: {e.cmd}")
    print("Please review the specific error message above to troubleshoot.")