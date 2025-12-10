#!/bin/bash

# Script to run the dbt demo in the correct order
# This script demonstrates the proper sequence to avoid the "relation does not exist" error

echo "Starting dbt demo..."

# Step 1: Install dependencies (if any)
echo "Step 1: Installing dbt dependencies..."
dbt deps

# Step 2: Load seeds first (this creates the example_source table)
echo "Step 2: Loading seeds..."
dbt seed

# Step 3: Run models (this will now work because the seed table exists)
echo "Step 3: Running models..."
dbt run

echo "Demo completed successfully!"