# RAM Authority Branch and PR Plan

## Feature train

```text
fork/ram-00-contract
fork/ram-01-store
fork/ram-02-journal
fork/ram-03-residency
fork/ram-04-runtime-stores
fork/ram-05-authority
fork/ram-06-strict
fork/ram-07-lineage
fork/ram-08-migrate
fork/ram-09-default
fork/ram-10-cleanup
```

Each phase starts from the previous accepted phase. Do not squash the phase history after it reaches the maintained line.

A future bisect should answer a question, not return "RAM stuff happened here."

## Commit discipline

One semantic change per commit.

Each commit body states what changed, what upstream does, why the fork differs, what invariant is established, and what proves it.

## Sync branches

Use `sync/upstream-YYYY-MM-DD`.

Never mix feature work into upstream-sync PRs.

We are maintaining a fork, not collecting merge-conflict folklore.
