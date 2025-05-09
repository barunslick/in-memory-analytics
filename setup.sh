#!/bin/bash

# Create necessary directories for the in-memory-analytics project
echo "Creating necessary directories for in-memory-analytics..."

# Creating database directory
mkdir -p db
echo "Created db/ directory for storing database files"

# Creating files directory for data files
mkdir -p files
echo "Created files/ directory for storing input/output data files"

# Creating reports directory
mkdir -p reports
echo "Created reports/ directory for storing report files"

echo "Directory structure created successfully!"
