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
Github: https://github.com/krishchadha/lambdawithrdspostgres

![image](https://github.com/user-attachments/assets/71d78402-67cf-4c53-a813-c250dc91c5ca)

4. Python Application (reads API message)

![image](https://github.com/user-attachments/assets/c2fcb611-6f09-44cc-b80a-76688dde3a77)

5. ArgoCD

![Screenshot 2024-10-13 085059](https://github.com/user-attachments/assets/12af4904-2645-49bc-88ec-d6190e95946e)

6. ECR and EKS (deployment and scaling)
- ECR:

![image](https://github.com/user-attachments/assets/7c9e734f-6f70-49c0-b6da-407c048bea6e)

- EKS:
![image](https://github.com/user-attachments/assets/10aa7b8e-63db-4bf8-82ee-ea2a3d443161)

-- Different for staging and production environment:

![image](https://github.com/user-attachments/assets/74dd7cfc-44d6-4409-a6b2-7b2d8fc4f7b7)
