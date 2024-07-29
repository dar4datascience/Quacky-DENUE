# Quacky-DENUE
Ingest Historical DENUE data into duckdb

```cmd
# Create a new conda environment named "quacky-denue"
conda create --name quacky-denue python==3.12

# Activate the new environment
conda activate quacky-denue

# Add the conda-forge and microsoft channels
conda config --add channels conda-forge
conda config --add channels microsoft

# Install the required packages
conda install playwright duckdb
```

```cmd
conda env export --name quacky-denue --file environment.yml
```