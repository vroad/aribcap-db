# Linting

## How to set up pre-commit hooks (optional)

This repository provides pre-commit hooks that run `cargo fmt`, `nixpkgs-fmt`,
and MegaLinter with the same configuration as the GitHub Actions workflow. Installing
it is optional. However, if you wish to contribute changes, enabling it is
highly recommended to catch lint and formatting errors before you commit.

Installing these hooks requires:

- [pre-commit]
- `cargo` and Rust's `rustfmt` component for the `cargo fmt` hook
- `nixpkgs-fmt` for the `nixpkgs-fmt` hook
- Docker for the MegaLinter hook

MegaLinter runs in Docker using `scripts/mega-linter.sh` and `compose.mega-linter.yml`. On Linux, we recommend
[Docker Rootless mode] for local runs to prevent the directories and files in
`megalinter-reports` from being owned by `root`. This is not a concern in
environments where file ownership inside the container does not map to the host
filesystem, such as Docker Desktop on macOS.

Once the prerequisites are installed, run the following to enable pre-commit hooks:

```shell
pre-commit install
```

The MegaLinter pre-commit hook lints only the staged files and applies any fixes it can make
automatically (`APPLY_FIXES=all`). When it reports an error or applies a fix, it
aborts the commit. After it applies fixes, re-stage the changed files and commit
again.

The `cargo fmt` hook runs `cargo fmt --all` whenever staged Rust files are present. If it
formats files, re-stage those changes and commit again.

The MegaLinter pre-commit hook also skips project-only linters, which cannot run on a file subset. We
therefore recommend a full-repository check before pushing
(see [Running MegaLinter manually](#running-megalinter-manually)).

If you need to commit while lint errors remain, pass `--no-verify` to
`git commit`. This skips pre-commit hooks for that single commit without uninstalling them.

### Running MegaLinter manually

> [!WARNING]
> Do not use `npx mega-linter-runner` to run MegaLinter locally. It may pull a
> different version of MegaLinter than the one used in the GitHub Actions
> workflow, which can cause inconsistent results.

You can also run MegaLinter directly through `scripts/mega-linter.sh`.
Unlike the MegaLinter pre-commit hook, it runs with `APPLY_FIXES=none` by default, so it
only reports problems without modifying any files.

Run it without arguments to lint the whole repository with all linters:

```shell
./scripts/mega-linter.sh
```

Pass file paths as arguments to lint only those files:

```shell
./scripts/mega-linter.sh compose.mega-linter.yml
```

Set `ENABLE_LINTERS` to run only a specific linter:

```shell
ENABLE_LINTERS=EDITORCONFIG_EDITORCONFIG_CHECKER ./scripts/mega-linter.sh compose.mega-linter.yml
```

See `compose.mega-linter.yml` for the full list of environment variables that can
be overridden.
