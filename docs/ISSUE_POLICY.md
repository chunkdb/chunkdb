# Issue Intake and Triage Policy

This policy keeps issue intake structured, low-noise, and contributor-friendly.

## When to Open an Issue

Open an issue when you need one of these:
- bug fix with reproducible steps
- scoped feature request
- docs or user-facing UX problem

Use the issue chooser and lightweight templates:
- Bug report
- Feature request

## When to Send a Direct PR Instead

Open a direct PR (with tests/docs) for clearly scoped, low-risk fixes where:
- behavior is unambiguous
- acceptance criteria are obvious
- issue discussion is not required for scope decisions

If uncertain, open an issue first.

## Reporter Expectations (Low Friction)

Issue templates are intentionally short.

Please include what you can, especially:
- clear title
- concise reproduction steps (for bugs)
- expected behavior

Maintainers may request additional details with `status:needs-info` when needed.

## Labeling Policy

Every open issue should have:
- exactly one type label
- exactly one priority label
- exactly one status label
- at least one platform label when platform-specific

### Type labels
- `type:bug`
- `type:performance`
- `type:feature`
- `type:docs-ux`

### Priority labels
- `priority:high`
- `priority:medium`
- `priority:low`

### Status labels
- `status:needs-triage`
- `status:triaged`
- `status:in-progress`
- `status:blocked`
- `status:needs-info`

### Platform labels
- `platform:windows`
- `platform:linux`
- `platform:macos`
- `platform:cross-platform`

Area labels are optional but recommended.

## Triage SLA

- First maintainer response: within 3 business days.
- Initial triage (labels + next action): within 5 business days.
- Ownership:
  - assign an owner when available
  - otherwise leave assignee empty and add explicit note: `owner: unassigned`

## Close Reason Taxonomy

Close with one of these reasons and a final comment:
- duplicate (link canonical issue)
- fixed (link commit/PR)
- not planned (scope/trade-off rationale)
- cannot reproduce (state attempted environment and commands)
- needs info timeout (missing required details after follow-up window)

For stale `status:needs-info` issues, close after 14 days without reporter response.

## Noise Reduction Rules

- Do not keep multiple overlapping self-tracking issues open.
- For related internal tasks, keep one scoped tracking issue with a checklist.
- Close duplicates with clear canonical links.

## Maintainer Triage Checklist

For each open issue:
- apply type/priority/status labels
- add platform label when needed
- assign owner or mark unassigned explicitly
- record next concrete action in a comment
- close immediately if resolved/duplicate/not planned with reason and links
