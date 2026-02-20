"""
Monitoring and alerting for sync jobs.

Provides:
- Health checks for sync lag and data freshness
- Prometheus-style metrics
- Simple web dashboard
"""

__all__ = ["health_check", "metrics", "dashboard"]
