## What upstream does

Describe the upstream behavior or seam this PR is adapting.

## What the fork does

Describe the smallest intentional downstream divergence.

## Why we are intentionally different

State the RAM-authority, journal, residency, or authority invariant being established.

Architecture may be criticized. Individual contributors may not.

## Receipts

- [ ] focused tests
- [ ] `git diff --check`
- [ ] Local backend behavior preserved where applicable
- [ ] RamJournal invariant test added/updated where applicable
- [ ] persistent-I/O contract checked where applicable
- [ ] upstream seam comments updated
- [ ] `FORK_DELTA.md` updated for every new permanent touchpoint

## Upstream seam review

List upstream symbols touched by this PR.

If the answer is "a lot", this PR is probably trying to become a personality.
