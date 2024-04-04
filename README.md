# Database Schema Extraction and Neo4j Integration

This repository contains a Python script for extracting schema information from different types of databases (SQL Server, PostgreSQL, Oracle) and integrating the extracted schema into a Neo4j graph database.

## Table of Contents

- [Introduction](#introduction)
- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)


## Introduction

The provided Python script allows users to extract schema information such as tables, columns, stored procedures, functions, and foreign key relationships from SQL Server, PostgreSQL, and Oracle databases. It then integrates this extracted schema information into a Neo4j graph database for visualization and analysis.

## Requirements

- Python 3.x
- PyODBC
- Psycopg2
- cx_Oracle
- Neo4j Python Driver

## Installation

1. Clone this repository to your local machine:

    ```bash
    git clone https://github.com/your-username/your-repository.git
    ```

2. Install the required Python packages:

    ```bash
    pip install -r requirements.txt
    ```

3. Set up your Neo4j instance and note down the URI and credentials.

## Usage

1. Run the Python script `main.py`.
2. Follow the prompts to enter the necessary credentials for your database(s) and Neo4j instance.
3. The script will extract schema information from the specified database(s) and integrate it into your Neo4j database.
4. Access your Neo4j instance to visualize and analyze the imported schema.

