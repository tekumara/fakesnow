---
name: pr-description-review
description: Review, draft, or update pull request descriptions so they explain user-stated intent without narrating the diff. Use when a user asks to review a PR description, write or update a PR body, enforce PR description rules, or prepare a GitHub PR description.
---

# PR Description Review

Review, draft, or update PR descriptions using only intent the user actually supplied. The PR body should explain why the change is needed, not narrate the implementation.

## Core Rules

- Never fabricate intent. If the user did not explicitly state the problem being solved and why the change is needed, ask before creating or updating the PR description.
- Do not treat a requested change as intent. `Add X`, `fix Y`, or `update Z` describes what changed, not why it matters.
- Never include file lists, file-change summaries, or `Files Updated` / `Files Changed` sections.
- Never narrate code changes in human language. The diff already shows the implementation.
- Never speculate about risks. Include risks only when the user explicitly mentioned them.
- Never include `Test plan`, test checklists, QA instructions, or test sections.
- Use `#NUMBER` only for intentional issue references supplied by the user or unambiguous PR metadata. Otherwise rephrase to avoid accidental issue links.

## Workflow

1. Collect the minimum context.
   - Use the PR URL, branch diff, current PR body, session history, and user messages as needed.
   - Inspect the diff only to ensure the PR description matches actual changes; do not convert the diff into a change narration.
   - If asked to apply changes to GitHub, first confirm the final body satisfies this skill.

2. Identify intent.
   - Accept intent from explicit user explanation, linked issue text, or a concrete error/problem the user supplied.
   - If intent is missing, ask exactly:

     ```text
     Before creating this PR, I need to understand the intent behind this change.

     What problem does this solve, and why is this change needed?
     ```

   - Stop after asking. Do not draft a final PR description until the user answers.

3. Produce or review the PR description.
   - For review requests, report rule violations briefly and provide a corrected body when intent is available.
   - For create/update requests, output only the final PR body unless the user asked for review notes.
   - Apply the body to GitHub only when the user explicitly asks to update/edit the PR.

## Required Body Format

```markdown
### Why?

[The problem being solved, using user-supplied intent only. Include intentional issue links such as `Fixes #123` when supplied.]

### How?

[High-level approach in 1-2 sentences. Do not list files, list changes, or narrate implementation details.]
```

## Review Checklist

| Check | Pass condition |
| --- | --- |
| Intent | `Why?` states a real problem and need from user-provided evidence. |
| Actuality | Description matches the actual PR changes without merely repeating discussion. |
| No diff narration | `How?` is conceptual and short, not an implementation walkthrough. |
| No file list | Body has no changed-file inventory or file-based section. |
| No tests section | Body has no test plan, checklist, or testing instructions. |
| No speculative risk | Risks are absent unless explicitly user-supplied. |
| Issue links | `#NUMBER` appears only for intentional references. |

## Examples

Open `references/examples.md` when calibrating review output quality or updating this skill from examples.
