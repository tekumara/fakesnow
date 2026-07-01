# PR Description Review Specification

## Intent

This skill helps agents review, draft, and update pull request descriptions that explain user-supplied intent while avoiding noisy restatements of the diff. It exists to prevent fabricated motivation, file lists, code narration, speculative risk sections, and test-plan clutter in PR bodies.

## Scope

In scope:
- Reviewing existing PR descriptions against the policy.
- Drafting or rewriting PR descriptions when intent is available.
- Updating a GitHub PR body when the user explicitly asks to apply the change.
- Asking for missing intent before producing a final PR body.

Out of scope:
- Reviewing code quality or implementation correctness.
- Writing release notes, changelog entries, commit messages, or issue descriptions.
- Inventing business or technical motivation from the diff.
- Adding test plans or risk sections unless explicitly requested and supported by user-provided context.

## Users And Trigger Context

- Primary users: maintainers and coding agents preparing GitHub PR descriptions.
- Common user requests: "review this PR description", "write/update the PR body", "make this PR description follow the rules", "explain PR description", "edit PR #123 body".
- Should not trigger for: code review, diff review, release note generation, commit title suggestions, or general documentation writing without a PR description target.

## Runtime Contract

- Required first actions: determine whether intent is explicitly present; ask for it if missing.
- Required outputs: either the missing-intent question, concise review findings plus a corrected body, or the final PR body in the required format.
- Non-negotiable constraints: do not fabricate intent, list files, narrate code changes, speculate about risks, add a test plan, or use accidental issue links.
- Expected bundled files loaded at runtime: `SKILL.md`; `references/examples.md` only for calibration or maintenance.

## Source And Evidence Model

Authoritative sources:
- User-provided rules in the originating conversation.
- Explicit user-provided intent in the current task.
- Current PR body, PR diff, linked issue content, and session history.

Useful improvement sources:
- positive examples: accepted PR bodies that follow this structure.
- negative examples: rejected bodies that fabricate intent, narrate diffs, or add forbidden sections.
- commit logs/changelogs: only to verify actual scope, not to infer motivation.
- issue or PR feedback: useful when it states intent or corrections.
- validation results: manual checklist pass/fail notes.

Data that must not be stored:
- secrets.
- customer data.
- private repository URLs, issue text, or identifiers not needed for reproduction.

## Reference Architecture

- `SKILL.md` contains: trigger metadata, runtime rules, workflow, output format, and checklist.
- `references/` contains: optional example calibrations.
- `references/evidence/` contains: no files initially; add only anonymized persistent examples when maintaining the skill.
- `scripts/` contains: no files initially.
- `assets/` contains: no files initially.

## Validation

- Lightweight validation: inspect `SKILL.md` frontmatter, required sections, direct reference links, and the review checklist.
- Deeper validation: run the skill against happy-path, missing-intent, and anti-pattern examples.
- Holdout examples: maintain anonymized accepted/rejected PR body examples if future behavior regresses.
- Acceptance gates: the skill must ask for missing intent and must not produce final bodies with forbidden sections.

## Known Limitations

- The skill cannot know whether an issue reference is intentional unless the user, PR metadata, or linked issue context makes that explicit.

## Maintenance Notes

- Update `SKILL.md` when runtime rules, required format, trigger wording, or forbidden sections change.
- Update `SOURCES.md` when adding new source material, decisions, gaps, or changelog entries.
- Update `references/evidence/` only with anonymized examples that reveal a recurring false positive, false negative, or correction pattern.
