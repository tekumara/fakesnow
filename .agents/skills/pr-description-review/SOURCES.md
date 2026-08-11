# PR Description Review Sources

## Source Inventory

| Source | Trust | Confidence | Contribution | Usage constraints |
| --- | --- | --- | --- | --- |
| User-provided PR description rules in the current conversation, 2026-07-01 | Authoritative | High | Core rules, required format, anti-patterns, missing-intent behavior | Treat as policy; do not infer unstated exceptions. |
| User-provided error and issue-link request for PR #349 | High | High | Happy-path example of user-supplied intent and intentional `Fixes #347` link | Public PR/issue reference; use only as an example pattern. |
| Existing project skill `.agents/skills/snowflake-check/SKILL.md` | Local convention | Medium | Confirms project skill root and concise workflow style | Do not copy domain-specific Snowflake workflow. |
| `skill-writer` runtime references | Authoring guidance | High | Skill layout, SPEC, provenance, validation, description optimization | Maintenance-only; do not copy long authoring workflow into runtime skill. |

## Source Adaptation Notes

| Decision | Record |
| --- | --- |
| Source intent | Ensure PR descriptions explain actual user intent and avoid clutter or fabricated motivation. |
| Local target | A project-scoped Agent Skill under `.agents/skills/` that reviews, drafts, or updates PR descriptions. |
| Fidelity boundary | Preserve all non-negotiable rules: ask for missing intent, no file lists, no code narration, no speculative risks, no test plan, required format, intentional issue links only. |
| Local replacement | Converted prompt prose into a compact runtime workflow, checklist, and examples. |
| Omitted material | Optional details-block handling was removed at user request. |
| Rights and attribution | User-authored instructions in the current session; no external licensed content bundled. |

## Decisions

| Decision | Status | Rationale |
| --- | --- | --- |
| Skill class: `workflow-process` | adopted | The skill enforces an ordered review/drafting workflow with clear safety boundaries. |
| Primary execution shape: inline guidance with optional examples reference | adopted | One coherent policy handles normal invocations; examples are optional calibration material. |
| Create `SPEC.md` | adopted | New skill needs a maintenance contract for scope, evidence, and validation. |
| Create scripts | rejected | No deterministic parsing or API automation is required beyond normal agent/tool use. |
| Provider-specific hooks | rejected | Prompt-level guidance is sufficient and more portable. |
| Ask before drafting when intent is missing | adopted | Directly prevents the highest-risk failure: fabricated motivation. |

## Coverage Matrix

| Coverage pass | Status | Evidence |
| --- | --- | --- |
| Core behavior | covered | Workflow and required body format in `SKILL.md`. |
| Edge behavior | covered | Missing intent, accidental issue links, GitHub update permission. |
| Negative behavior | covered | Anti-pattern rules and examples. |
| Repair patterns | covered | Corrected-body workflow and anti-pattern correction example. |
| Version variance | not applicable | PR body policy is not tied to a software version. |
| Shape mechanics | covered | Inline workflow plus optional examples reference. |

## Description Optimization

Should trigger:
- "review this PR description"
- "write a PR body for this branch"
- "update PR #349 description"
- "make this PR description follow our rules"
- "prepare a GitHub PR description without test plan"

Should not trigger:
- "review the code in this PR"
- "suggest a PR title"
- "write release notes"
- "summarize the diff"
- "create a commit message"

Final description chosen because it includes review/draft/update use cases and excludes broad code-review wording.

## Retrieval Stopping Rationale

The user supplied the complete policy to encode. Existing project skill prior art established location. Further source collection would be low-yield unless a repository-level PR description policy is later provided.

## Open Gaps

- No persistent holdout corpus exists yet; add anonymized examples only if the skill misfires.

## Changelog

- 2026-07-01: Removed optional details-block handling at user request.
- 2026-07-01: Initial skill created from user-provided PR description policy.
