# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- CLI `run-workflow` dropped an `s` from `json.loads`, and the help text on the
  timout argument was
  incorrect. ([#5](https://github.com/cirrus-geo/cirrus-mgmt/pull/5)

## [v0.1.0] - 2023-08-01

### Added

- CLI and library functions to run a cirrus workflow, and collect its output.
  Also adds a `call` command which is similar to `exec` but uses a
  subprocess. And adds `invoke_lambda` which invokes lambdas that are part of
  the cirrus deployment. ([#4](https://github.com/cirrus-geo/cirrus-mgmt/pull/4)

## [v0.1.0a] - 2022-11-09

Initial release

[unreleased]: https://github.com/cirrus-geo/cirrus-mgmt/compare/v0.1.0...main
[v0.1.0]: https://github.com/cirrus-geo/cirrus-mgmt/compare/v0.1.0a...v0.1.0
[v0.1.0a]: https://github.com/cirrus-geo/cirrus-mgmt/releases/tag/v0.1.0a
