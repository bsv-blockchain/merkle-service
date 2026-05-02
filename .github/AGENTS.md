# AGENTS.md

## 🎯 Purpose & Scope

This file defines the **baseline standards, workflows, and structure** for *all contributors and AI agents* operating within this repository. It serves as the root authority for engineering conduct, coding conventions, and collaborative norms.

It is designed to help AI assistants (e.g., Codex, Claude, Gemini) and human developers alike understand our practices, contribute clean and idiomatic code, and navigate the codebase confidently and effectively.

> Whether reading, writing, testing, or committing code, **you must adhere to the rules in this document.**

Additional `AGENTS.md` files **may exist in subdirectories** to provide more contextual or specialized guidance. These local agent files are allowed to **extend or override** the root rules to fit the needs of specific packages, services, or engineering domains—while still respecting the spirit of consistency and quality defined here.

<br><br>

## 📚 Technical Conventions

Our technical standards are organized into focused, portable documents in the `.github/tech-conventions/` directory:

### Core Development
* **[Go Essentials](tech-conventions/go-essentials.md)** - Context-first design, interfaces, goroutines, error handling
* **[Testing Standards](tech-conventions/testing-standards.md)** - Unit tests, coverage requirements, best practices
* **[Commenting & Documentation](tech-conventions/commenting-documentation.md)** - Code comments, package docs, markdown

### Version Control & Collaboration
* **[Commit & Branch Conventions](tech-conventions/commit-branch-conventions.md)** - Git workflow standards
* **[Pull Request Guidelines](tech-conventions/pull-request-guidelines.md)** - PR structure and review process
* **[Release Workflow & Versioning](tech-conventions/release-versioning.md)** - Semantic versioning and releases

### Project Management & Infrastructure
* **[Labeling Conventions](tech-conventions/labeling-conventions.md)** - GitHub label system
* **[Dependency Management](tech-conventions/dependency-management.md)** - Go modules and security
* **[Security Practices](tech-conventions/security-practices.md)** - Vulnerability reporting and secure coding
* **[GitHub Workflows Development](tech-conventions/github-workflows.md)** - Actions workflow best practices

> 💡 **Start with [tech-conventions/README.md](tech-conventions/README.md)** for a complete index with descriptions.

<br><br>

## 🛠️ Build & Task Automation

### Makefile (primary)

The project provides a [`Makefile`](../Makefile) at the repository root as the **primary interface** for common development tasks. Prefer `make <target>` over running raw commands directly.

| Target | Description |
|---|---|
| `make build` | Compile all packages (`go build ./...`) |
| `make test` | Run the full unit-test suite |
| `make test-e2e-postgres` | Run PostgreSQL-backed end-to-end tests (requires Docker/Podman) |
| `make lint` | Run `golangci-lint` |
| `make lint-store-imports` | Verify `cmd/` binaries do not import backend packages directly |
| `make docker-up` / `docker-down` | Start / stop local services via `podman-compose` |
| `make run` | Run the merkle-service binary |
| `make debug-dashboard` | Start the debug dashboard |
| `make scale-test` | Run the scale test suite (10 min timeout) |
| `make mega-scale-test` | Run the mega-scale suite (15 min timeout) |
| `make generate-mega-fixtures` | Generate large fixture data for mega-scale tests |

### magex (CI fallback)

For CI environments or platforms where `make` is unavailable, [**magex**](https://github.com/magefile/mage) provides a portable Go-native task runner equivalent. Mage targets mirror the Makefile and are resolved automatically when `magex` is present on `PATH`.

```sh
go install github.com/magefile/mage/magex@latest
magex <target>   # same targets as make
```

> Prefer `make` for local development. `magex` is intended as a fallback for environments that cannot rely on GNU Make (some Windows CI runners, hermetic sandboxes, etc.).

<br><br>

## 📁 Directory Structure

| Directory                   | Description                                             |
|-----------------------------|---------------------------------------------------------|
| `.github/`                  | Issue templates, workflows, and community documentation |
| `.github/actions/`          | GitHub composite actions for CI/CD and automation       |
| `.github/ISSUE_TEMPLATE/`   | Issue and pull request templates                        |
| `.github/tech-conventions/` | Technical conventions and standards for development     |
| `.github/workflows/`        | GitHub Actions workflows for CI/CD                      |
| `.vscode/`                  | VS Code settings and extensions for development         |
| `.` (root)                  | Source files and tests for the local package            |
