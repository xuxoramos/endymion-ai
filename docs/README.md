# Endymion-AI Documentation Index

Complete documentation for the Endymion-AI event-sourced cattle management system.

## 📚 Documentation Library

### For Developers

- **[Developer Guide](./DEVELOPER.md)** ⭐ START HERE
  - Getting started & installation
  - Architecture overview
  - Local development setup
  - Testing strategies
  - Common development tasks
  - Troubleshooting guide
  
- **[API Reference](./API.md)**
  - Complete REST API documentation
  - Request/response examples
  - Event payload schemas
  - OpenAPI specification
  - Client libraries

### For Operators

- **[Operations Guide](./OPERATIONS.md)**
  - Production deployment (Kubernetes, Docker)
  - Health checks & monitoring
  - Backup & restore procedures
  - Scaling considerations
  - Disaster recovery
  - Security best practices

### Architecture & Design

- **[Architecture Diagram](../ARCHITECTURE.md)**
  - System architecture visualization
  - Data flow sequences
  - Layer responsibilities
  - Technology stack

- **[Frontend Documentation](../frontend/README.md)**
  - React UI components
  - Interactive architecture view
  - Setup & deployment

### Monitoring & Observability

- **[Monitoring Guide](../backend/monitoring/README.md)**
  - Prometheus metrics
  - Grafana dashboards
  - Alert configuration
  - Health checks

### Demos & Examples

- **[Demo Scripts](../demo/README.md)**
  - Interactive demonstration
  - Timeline visualization
  - Sample workflows

## 🚀 Quick Start Paths

### "I want to run the system locally"

1. Read: [Developer Guide - Getting Started](./DEVELOPER.md#getting-started)
2. Run: `./demo/run_demo.sh`
3. Access: http://localhost:3000

### "I want to understand the architecture"

1. Read: [Architecture Diagram](../ARCHITECTURE.md)
2. View: http://localhost:3000/architecture (interactive)
3. Explore: [Developer Guide - Architecture Overview](./DEVELOPER.md#architecture-overview)

### "I want to deploy to production"

1. Read: [Operations Guide - Production Deployment](./OPERATIONS.md#production-deployment)
2. Review: [Operations Guide - Security](./OPERATIONS.md#security)
3. Setup: [Operations Guide - Monitoring](./OPERATIONS.md#monitoring)

### "I want to use the API"

1. Read: [API Reference](./API.md)
2. Access: http://localhost:8000/docs (OpenAPI)
3. Try: [API Reference - Testing Examples](./API.md#testing-examples)

## 📖 Documentation by Role

### Software Engineer

**Essential Reading:**
- [Developer Guide](./DEVELOPER.md) - Complete development workflow
- [API Reference](./API.md) - API contracts
- [Architecture Diagram](../ARCHITECTURE.md) - System design

**Reference:**
- Testing section in Developer Guide
- Troubleshooting section
- Common tasks section

### DevOps Engineer

**Essential Reading:**
- [Operations Guide](./OPERATIONS.md) - Deployment & operations
- [Monitoring Guide](../backend/monitoring/README.md) - Observability

**Reference:**
- Kubernetes manifests in Operations Guide
- Backup & restore procedures
- Scaling considerations

### Data Engineer

**Essential Reading:**
- [Architecture Diagram](../ARCHITECTURE.md) - Data flow
- [Developer Guide - Delta Lake Queries](./DEVELOPER.md#delta-lake-queries)

**Reference:**
- Bronze/Silver/Gold layer details
- Sync job documentation
- PySpark processing logic

### QA Engineer

**Essential Reading:**
- [Developer Guide - Testing](./DEVELOPER.md#testing)
- [API Reference](./API.md) - API contracts
- [Demo Scripts](../demo/README.md) - Test scenarios

**Reference:**
- Integration test examples
- Demo script flows
- Health check endpoints

## 🎯 Documentation by Task

### Setup & Installation

- [Install locally](./DEVELOPER.md#installation-steps)
- [Deploy to Kubernetes](./OPERATIONS.md#kubernetes-deployment)
- [Docker Compose setup](./OPERATIONS.md#docker-compose-simple-deployment)

### Development

- [Run services](./DEVELOPER.md#running-services)
- [Debug issues](./DEVELOPER.md#debugging-tips)
- [Write tests](./DEVELOPER.md#testing)
- [Add features](./DEVELOPER.md#common-tasks)

### Operations

- [Monitor system](./OPERATIONS.md#monitoring)
- [Backup data](./OPERATIONS.md#backup--restore)
- [Scale services](./OPERATIONS.md#scaling-considerations)
- [Recover from failure](./OPERATIONS.md#disaster-recovery)

### API Integration

- [Make API calls](./API.md#endpoints)
- [Handle events](./API.md#event-payload-schemas)
- [Use client libraries](./API.md#client-libraries)
- [Test endpoints](./API.md#testing-examples)

## 📊 Diagrams & Visuals

### Interactive

- **Architecture View**: http://localhost:3000/architecture
  - Click layers for details
  - Real-time data flow animation
  - Live sync status

### Static

- **System Architecture**: [ARCHITECTURE.md](../ARCHITECTURE.md)
  - Component diagram
  - Data flow sequences
  - Deployment architecture

- **Mermaid Diagrams**: Throughout documentation
  - Use GitHub rendering or Mermaid Live Editor
  - VS Code Mermaid extension

## 🔍 Search Documentation

### By Keyword

| Keyword | Documents |
|---------|-----------|
| **Event Sourcing** | Architecture, Developer Guide |
| **CQRS** | Architecture, Developer Guide |
| **Delta Lake** | Architecture, Developer Guide, Operations |
| **Sync Job** | Developer Guide, Operations, API |
| **Monitoring** | Operations, Monitoring Guide |
| **Backup** | Operations |
| **Kubernetes** | Operations |
| **API** | API Reference, Developer Guide |
| **Testing** | Developer Guide |
| **Troubleshooting** | Developer Guide, Operations |

### By Error Message

| Error | Location |
|-------|----------|
| "Sync lag is high" | [Troubleshooting](./DEVELOPER.md#sync-lag-is-high) |
| "Events stuck in outbox" | [Troubleshooting](./DEVELOPER.md#events-stuck-in-outbox) |
| "Silver state looks wrong" | [Troubleshooting](./DEVELOPER.md#silver-state-looks-wrong) |
| "SQL projection diverged" | [Troubleshooting](./DEVELOPER.md#sql-projection-diverged) |

## 📝 Contributing to Documentation

### Style Guide

- Use clear, concise language
- Include code examples
- Add diagrams where helpful
- Link to related sections
- Keep examples up-to-date

### Documentation Structure

```
docs/
├── README.md           # This file - documentation index
├── DEVELOPER.md        # Developer guide
├── API.md             # API reference
└── OPERATIONS.md      # Operations guide
```

### Updating Documentation

1. Edit Markdown files
2. Test code examples
3. Update diagrams (Mermaid)
4. Update version and date
5. Submit pull request

## 🆘 Getting Help

### Documentation Issues

- **Can't find what you need?** Open a GitHub issue
- **Documentation unclear?** Submit a PR with improvements
- **Found an error?** Report it in Issues

### Support Channels

1. **Documentation**: You're here! 📚
2. **GitHub Issues**: Bug reports & feature requests
3. **Slack**: #endymion-ai-dev (internal)
4. **Email**: dev@endymion-ai.example.com

## 📦 Additional Resources

### External Documentation

- [FastAPI Docs](https://fastapi.tiangolo.com/)
- [SQL Server Docs](https://learn.microsoft.com/sql/)
- [Delta Lake Docs](https://docs.delta.io/)
- [PySpark Docs](https://spark.apache.org/docs/latest/api/python/)
- [React Docs](https://react.dev/)
- [Kubernetes Docs](https://kubernetes.io/docs/)

### Related Projects

- [Event Sourcing Examples](https://github.com/cer/event-sourcing-examples)
- [CQRS Journey](https://docs.microsoft.com/en-us/previous-versions/msp-n-p/jj554200(v=pandp.10))
- [Delta Lake Examples](https://github.com/delta-io/delta)

## 📅 Documentation Versions

| Version | Date | Changes |
|---------|------|---------|
| 1.0.0 | Jan 2026 | Initial comprehensive documentation |

---

## Quick Links

**Most Popular:**
- [Getting Started](./DEVELOPER.md#getting-started) ⭐
- [API Endpoints](./API.md#endpoints)
- [Architecture Diagram](../ARCHITECTURE.md)
- [Troubleshooting](./DEVELOPER.md#troubleshooting)

**For Production:**
- [Deployment Guide](./OPERATIONS.md#production-deployment)
- [Monitoring Setup](./OPERATIONS.md#monitoring)
- [Backup Procedures](./OPERATIONS.md#backup--restore)

**For Development:**
- [Local Setup](./DEVELOPER.md#installation-steps)
- [Testing Guide](./DEVELOPER.md#testing)
- [Common Tasks](./DEVELOPER.md#common-tasks)

---

**Last Updated:** January 2026  
**Maintainers:** Endymion-AI Team  
**License:** MIT
