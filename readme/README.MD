# Multi-Environment Application Deployment and Event-Driven Architecture Setup

## Achievements:
1. SNS topic and SQS Queue
2. RDS with PostgreSQL
3. Lambda to connect with RDS (triggered by SQS)
4. Python Application (reads API message)
5. ArgoCD
6. ECR and EKS (deployment and scaling)
7. S3 bucket for logging
8. Automation

## Github Repo:
- https://github.com/krishchadha/remoteapi

## Overview:
A Multi-environment application deployment using AWS, Docker (ECR), Kubernetes (EKS), and ArgoCD for CI/CD.

1. **SNS topic and SQS Queue**
![Screenshot 2024-10-13 095548](https://github.com/user-attachments/assets/42a298ba-9eef-4c20-86e1-5281bd8518f6)
![image](https://github.com/user-attachments/assets/16cb5afa-b813-460b-bef8-24535368ddc9)

2. RDS with PostgreSQL
![image](https://github.com/user-attachments/assets/d95b1ed1-65dc-4453-a008-dc74c3ff25ec)

3. Lambda to connect with RDS (triggered by SQS)
![image](https://github.com/user-attachments/assets/71d78402-67cf-4c53-a813-c250dc91c5ca)
