# CLAUDE.md — Project Instructions

> This file governs how Claude Code behaves in this workspace.

---

## CRITICAL: AGENT & SKILL PROTOCOL (MANDATORY)

Before **any** implementation, code edit, or design work, you MUST:

1. **Read `.agent/ARCHITECTURE.md`** — understand available agents, skills, and workflows.
2. **Select the appropriate agent** from `.agent/agents/` based on the request domain.
3. **Load the agent's required skills** from its frontmatter `skills:` field (read each `SKILL.md`).
4. **Announce** which agent expertise is being applied:

```
🤖 Applying knowledge of @[agent-name]...
```

### Agent Routing (quick reference)

| Domain | Agent | Skills |
|---|---|---|
| Python / pipeline / data | `backend-specialist` | `python-patterns`, `database-design` |
| SQL / schema / DuckDB | `database-architect` | `database-design` |
| Debugging / root cause | `debugger` | `systematic-debugging` |
| Planning / breakdown | `project-planner` | `plan-writing`, `brainstorming` |
| Performance | `performance-optimizer` | `performance-profiling` |
| Multi-domain tasks | `orchestrator` | `parallel-agents`, `behavioral-modes` |

Full agent list: `.agent/agents/` (20 agents)
Full skill list: `.agent/skills/` (36 skills)

### Skill Loading Protocol

```
User Request → Match domain → Read .agent/agents/{agent}.md
                                         ↓
                              Check frontmatter skills:
                                         ↓
                              Read .agent/skills/{skill}/SKILL.md
                                         ↓
                              Apply principles (not just copy patterns)
```

**Rules:**
- Read `SKILL.md` first; only read sub-files matching the task.
- Rule priority: `CLAUDE.md` (P0) > agent `.md` (P1) > `SKILL.md` (P2). All are binding.
- Never write code without identifying and applying the correct agent first.

---

## AGENT ROUTING CHECKLIST (before every code/design response)

| Step | Check |
|---|---|
| 1 | Identified correct agent for this domain? |
| 2 | Read the agent's `.md` file? |
| 3 | Announced `🤖 Applying knowledge of @[agent]...`? |
| 4 | Loaded required skills from agent frontmatter? |

❌ Writing code without completing this checklist = **PROTOCOL VIOLATION**

---

## UNIVERSAL RULES (Always Active)

### Clean Code
All code must follow `@[skills/clean-code]` — concise, self-documenting, no over-engineering.

### File Dependency Awareness
Before modifying any file: identify dependents and update all affected files together.

### Read → Understand → Apply
```
❌ WRONG: Read agent file → Start coding
✅ CORRECT: Read → Understand WHY → Apply PRINCIPLES → Code
```

---

## PROJECT CONTEXT

- **Stack**: Python, DuckDB, pandas, Sensor Tower API
- **Domain**: Mobile game market intelligence — benchmarking, LTV, cohort retention
- **Key schema rule**: `app_id` (platform-specific) ≠ `unified_app_id` (cross-platform); bridge via `dim_apps`
- **IAP split**: Company revenue includes web/direct payments; ST only tracks IAP — apply `IAP_PCT` from `config.py`
- **Region mapping**: `Sing-Malay → [SG, MY]`, `TW-HK → [TW, HK]` (see `COMPANY_MARKET_ST_COUNTRIES` in `config.py`)

---

## WORKFLOWS (slash commands)

| Command | Purpose |
|---|---|
| `/brainstorm` | Socratic discovery |
| `/plan` | Task breakdown |
| `/debug` | Root cause analysis |
| `/orchestrate` | Multi-agent coordination |
| `/create` | New feature build |

---

> **Architecture reference**: `.agent/ARCHITECTURE.md`
> **Full rules reference**: `.agent/rules/GEMINI.md` (adapt for Claude where applicable)
