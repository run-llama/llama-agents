# Workflow dependency pinning

Block review when new or changed external GitHub Actions dependencies in `uses:` references are not pinned to full 40-character commit hashes. This includes actions and reusable workflows. Tags, branches, and shortened hashes are not sufficient because mutable references allow retag attacks. Local `./` references are exempt.
