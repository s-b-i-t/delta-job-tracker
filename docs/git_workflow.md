# Git Workflow

## Rules
- Never push directly to `master`.
- Always work on a feature branch and merge via Pull Request.
- If `master` moved while you were working, rebase your branch before opening/merging PR.

## Recommended Git Config
```bash
git config --global pull.ff only
```

## Recover a Diverged Local `master`
Use this when local `master` has diverged and you want to match remote exactly.

```bash
git fetch origin
git checkout master
git reset --hard origin/master
```
