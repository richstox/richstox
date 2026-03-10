# RICHSTOX — GitHub Copilot Coding Agent Instructions

> This file is read by GitHub Copilot at the start of every session (IDE and Coding Agent).
> All project governance rules are in `/AGENTS.md` — read it first, every session.

---

## How the GitHub Copilot Coding Agent workflow works

Each agent session is tied to exactly **one GitHub issue** and produces **one PR**.
When the PR is merged, the session ends automatically — this is by design, not a bug.

**To start a new task:**
1. Richard creates a new GitHub issue describing the task
2. GitHub Copilot Coding Agent is assigned to that issue
3. Agent creates a branch, implements the task, opens a PR
4. Richard reviews, merges → session ends
5. For the next task, repeat from step 1

**Key implication:** Every agent session starts fresh. To minimize ramp-up time,
always read the files below before doing anything else.

---

## First steps for every new session (mandatory)

1. Read `/AGENTS.md` — hard rules, governance, architecture
2. Read `/TASKS.md` — current open task and backlog
3. Read `/PROJECT_STATE.md` — product rules and canonical decisions
4. Read `/memory/TECHNICAL_DOCUMENTATION.md` — architecture details
5. Check `git log --oneline -10` to see recent work

---

## Project summary

**RICHSTOX** is a mobile-first, long-term investing + portfolio tracking app.
- Backend: FastAPI (Python 3.12), port 8000, MongoDB
- Frontend: Expo 54 / React Native Web, port 8081
- Repo root: `/workspace/` (prod: `/app/`)

### Non-negotiable rules (enforce always)
- No autonomous commits without Richard's explicit `GO (commit+push)`
- Propose changes FIRST, wait for Richard's approval, then implement
- One task at a time — never start a second task until the first is closed
- Frontend NEVER calls EODHD API directly
- All metrics are computed locally from raw data, never stored provider-computed values
- Communicate with Richard in **Czech**; all code/UI/commits in **English**

### Current open task
See `/TASKS.md` → **CURRENT FOCUS** section.

### Key files
| File                                   | Purpose                          |
|----------------------------------------|----------------------------------|
| `/AGENTS.md`                           | Hard rules, architecture, workflow |
| `/TASKS.md`                            | Task registry (current + backlog) |
| `/PROJECT_STATE.md`                    | Canonical product decisions       |
| `/memory/TECHNICAL_DOCUMENTATION.md`  | Technical docs                    |
| `/memory/PRD.md`                       | Product requirements              |
| `/memory/DATA_SOURCES_CALCULATIONS.md`| Metric calculation specs          |
| `/backend/`                            | FastAPI backend                   |
| `/frontend/`                           | Expo frontend                     |

### Linting & testing
```bash
# Backend lint
cd backend && ~/.local/bin/ruff check . --ignore E501

# Backend tests
cd backend && pytest

# Frontend lint
cd frontend && yarn lint
```

---

## Why a new agent is needed for each task

GitHub Copilot Coding Agent is stateless between PRs — each PR is an isolated
session. This is intentional: it keeps each change focused, reviewable, and
reversible. The trade-off is that each session requires a new issue assignment.

To make each new session as productive as possible, this project stores context in:
- `/AGENTS.md` — rules and architecture (read every session)
- `/TASKS.md` — task registry (updated after every completed task)
- `/memory/` — technical documentation and state
- `/PROJECT_STATE.md` — canonical product decisions

Keep these files up to date so future agents start fully informed.
