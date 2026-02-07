# Azure Architecture

## Overview
The platform uses Azure-native services to ingest, validate, transform, and serve analytics with strong security controls.

## Suggested Services
- Fabric Data Factory for orchestration and scheduling.
- Microsoft Fabric OneLake for scalable processing.
- Microsoft Fabric SQL endpoint (or Warehouse) for serving dimensional tables.
- Azure Key Vault for secrets and encryption keys.
- Azure Monitor and Log Analytics for observability.

## Rationale
- Fabric Data Factory enables managed orchestration with lineage and retry policies.
- Fabric SQL endpoint supports dimensional queries for analytics and reporting.
- Key Vault centralizes secrets and encryption keys with audit trails.
- Monitor and Log Analytics provide operational visibility and alerting.

## Security Practices
- Private endpoints for storage and compute.
- Managed identities for service-to-service authentication.
- Role-based access control on all data assets.


## Service Justification
- SFTP and Fabric Data Factory enables secure, automated, and scalable data ingestion, with robust orchestration with seamless integration across Azure services and diverse data sources
- Fabric OneLake serves as unified data lake for entire architecture, providing centralised, secure and scalable storage for all Fabric experiences (data engineering, analytics) with seamless integration to Power BI
- Power BI delivers interactive, AI driven visualisations with seamless integration across data sources, enabling self-service analytics and secure, scalable reporting business insights
- Key Vault centralizes secrets and supports managed identities for secure access.
- Azure Monitor and Log Analytics centralize metrics, logs, and alerting for operations.
