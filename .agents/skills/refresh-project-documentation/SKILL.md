---
name: refresh-project-documentation
description: Audit and synchronize project documentation with the current code, manifests, release version, dependencies, supported environments, commands, features, and behavior. Use after version bumps, dependency or MSRV changes, API or configuration changes, renamed packages, CI changes, releases, or whenever README files and maintained guides may be stale.
---

# Refresh Project Documentation

Make documentation describe verifiable project reality. Use repository-native files and commands; assume no particular agent, model, editor, or hosting platform.

## 1. Establish sources of truth

1. Read repository guidance and check the working tree. Preserve unrelated edits, including untracked documentation.
2. Identify authoritative sources for each claim:
   - versions, MSRV, features, and dependencies: manifests and resolved metadata;
   - supported commands and matrices: CI workflows and task configuration;
   - APIs and behavior: public source, tests, and examples;
   - release procedure: packaging configuration and verified dry runs;
   - platform requirements: toolchain and target configuration.
3. Classify documentation before editing:
   - **Maintained:** README, contributor/setup guides, release guides, API guides, and current examples;
   - **Historical:** changelogs, completed plans, design snapshots, migration records, and archived examples;
   - **Generated:** API output, lockfiles, coverage, and tool-generated indexes.
4. Update maintained files. Preserve historical statements unless they falsely present themselves as current. Regenerate generated documentation with its owning tool rather than hand-editing it.

## 2. Audit for drift

Search maintained documentation for:

- old project and package versions;
- stale MSRV, toolchain, target, runtime, or operating-system requirements;
- renamed or removed crates, modules, features, environment variables, and commands;
- dependency snippets that no longer resolve to the intended release line;
- examples that do not compile or contradict the public API;
- CI/test instructions that omit required flags or name nonexistent packages;
- outdated support, security, stability, limitation, or release claims;
- links and paths to moved or missing files.

Search exact old values first, then inspect semantically related prose that may not contain the old literal. Do not replace every historical occurrence mechanically.

## 3. Update documentation

1. Prefer durable wording and point to a single source of truth instead of duplicating long version tables.
2. In installation snippets, show the current public release line when the repository is being prepared for that release.
3. State the actual MSRV separately from when a compilation target first became stable.
4. Keep commands copy-pasteable from the documented working directory and execution environment.
5. Update feature tables and examples only after checking their manifest definitions and public APIs.
6. Keep release examples internally consistent, but label illustrative versions when they are not the current release.
7. Avoid documenting internal dependency versions unless users or maintainers need them.
8. Do not invent guarantees from a successful local build. Distinguish compiled, unit-tested, integration-tested, and production-supported behavior.

## 4. Validate

Use the environment required by repository guidance.

1. Search again for stale values and explain any intentionally preserved historical matches.
2. Validate referenced files, package names, features, and commands against project metadata.
3. Run formatter or documentation checks when configured.
4. Compile documentation examples or run doc tests when the project supports them.
5. For release/version changes, run a package metadata check and confirm lockfiles reflect the new version.
6. Inspect the final diff for accidental edits, broken Markdown, contradictory claims, and unnecessary churn.

## 5. Report

List maintained files updated, the facts synchronized, checks performed, and historical files deliberately left unchanged. Call out claims that could not be verified or tests requiring unavailable services.
