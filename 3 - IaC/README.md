# Infrastructure as Code (IaC)

This folder contains all **Terraform configurations** to provision **AWS infrastructure** for the Air Quality Indicators project. It follows modular, scalable, and production-ready practices. 

## Project Architecture
The infrastructure is split into reusable modules and environment-based configurations, making it easy to scale, test, and promote changes.

### Key Components
- S3 Buckets for:

  - Raw ingestion from APIs

  - Processed data storage

  - Airflow DAG and logs

- IAM Roles & Policies for:

  - Lambda functions

  - MWAA (Airflow)

  - Access to S3 and other services

- Lambda Functions for:

  - Ingestion from external APIs

  - Preprocessing before storage

- MWAA (Airflow) for:

  - DAG orchestration and monitoring

- VPC / Networking for:

  - Private MWAA setup

  - Better security and control

## Goals
- Reusable modules with minimal duplication.

- Fully automated deployments.

- Least-privilege security model.

- Clearly separated dev/prod environments.

- Production-grade Airflow orchestration.

## Remote State
Terraform state is stored in a dedicated S3 bucket with locking via DynamoDB to avoid race conditions.
Defined in `backend.tf`.