# Robot Log Pipeline

A data engineering project that turns raw robot logs into clean,
tested tables, so a team can ask how often a robot stops itself,
when, and under what conditions.

## Data

The public UR3 CobotOps dataset from the UCI Machine Learning
Repository. It stands in for real robot system logs. This project is
not affiliated with any company.

## Status

- Done: loading the raw data and profiling it (7,409 rows, 24 columns)
- In progress: cleaning, testing and scheduling the pipeline

## What profiling found

- Timestamps stored as text, 863 of them wrapped in stray quote marks
- 54 rows with no stop value and almost no sensor data
- Recording gaps: about 7.3 hours covered by about 2 hours of rows
