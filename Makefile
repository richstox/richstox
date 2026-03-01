# ==============================================================================
# RICHSTOX Makefile - CI/CD Commands
# ==============================================================================
# Layer 4: API Call Prevention System
# ==============================================================================

.PHONY: audit-api audit-scheduler lint check ci

# API Call Audit - verifies all EODHD calls are in allowlist
audit-api:
	@echo "Running API Call Audit..."
	@python /app/scripts/audit_external_calls.py
	@echo "API Call Audit passed"

# Scheduler Audit - verifies scheduler configuration
audit-scheduler:
	@echo "Running Scheduler Audit..."
	@python /app/scripts/audit_scheduler.py
	@echo "Scheduler Audit passed"

# Combined audit
audit: audit-api audit-scheduler

# Lint backend
lint-backend:
	@cd /app/backend && ruff check . --ignore E501

# Quick check (run before commit)
check: audit-api
	@echo "All checks passed"

# Full CI pipeline
ci: audit lint-backend
	@echo "CI Pipeline passed"
