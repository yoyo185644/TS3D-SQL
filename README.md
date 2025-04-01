# SQL Dependency Analysis Toolkit

## Overview
This project provides tools for analyzing SQL dependencies, parsing SQL statements, and performing dynamic SQL analysis to support database management and optimization.

## Project Structure

📂 RA
├── DynamicSQLGraph.py # Constructs dynamic SQL dependency graphs
├── sqlParse.py # Parses SQL statements
├── topoAnalysis.py # Performs topological analysis on SQL dependencies

📂 utils
├── parse.py # Helper functions for parsing data
├── preprocess.py # Preprocessing utilities for SQL data for construct sql templates

## Project Structure
file_path = "/home/yyy/mysql/data_2days/data_m2m/logs/sqls.txt"
group_data, keys = pre_process(file_path)

### ours 
graph = DynamicSQLGraph(timeout_threshold=1000)
graph.detect_abnormal_chains()

### baseline（topological_sort_optimized and distributed_topological_sort in topoAnalysis.py）
topo_data(processed_data)
