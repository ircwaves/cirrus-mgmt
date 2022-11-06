[![build-status-image]][build-status]
[![coverage-status-image]][codecov]
[![pypi-version]][pypi]

# cirrus-mgmt

cirrus-mgmt is a plugin for the [cirrus-geo] processing pipeline framework to
add deployment management commands to the `cirrus` cli. The commands allow
users to perform common tasks like getting payload states, processing payloads,
and testing workflows.

## Quickstart

cirrus-mgmt is `pip`-installable:

```
pip install cirrus-mgmt
```

When installed alonside [cirrus-geo], it will add several subcommands to the
`cirrus` cli:

```
cirrus deployments
cirrus manage
cirrus payload
```

See the [full plugin documentation][docs] for more information on use.

[docs]: https://cirrus-geo.github.io/cirrus-mgmt/
[cirrus-geo]: https://github.com/cirrus-geo/cirrus-geo
[build-status-image]: https://github.com/cirrus-geo/cirrus-mgmt/actions/workflows/python-test.yml/badge.svg
[build-status]: https://github.com/cirrus-geo/cirrus-mgmt/actions/workflows/python-test.yml
[coverage-status-image]: https://img.shields.io/codecov/c/github/cirrus-geo/cirrus-mgmt/master.svg
[codecov]: https://codecov.io/github/cirrus-geo/cirrus-mgmt?branch=main
[pypi-version]: https://img.shields.io/pypi/v/cirrus-mgmt.svg
[pypi]: https://pypi.org/project/cirrus-mgmt/
