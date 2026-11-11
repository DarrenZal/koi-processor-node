# Security policy

## Reporting a vulnerability

Please report security problems privately through GitHub:
[Report a vulnerability](https://github.com/DarrenZal/koi-processor-node/security/advisories/new)
(the **Security** tab, then **Report a vulnerability**).

Please do not open a public issue or pull request for a vulnerability until a fix is published.
Include the commit you tested, the steps to reproduce, and what you expected to happen.

## Supported versions

Only the current `main` branch is supported.

## How fixes are released

This repository is published as snapshots exported from a private development repository.
A fix is applied there first and reaches `main` with the next export.
Pull requests here are reviewed and ported into the source, not merged directly, and are then closed with a pointer to the export that contains them.
The reporter is credited unless they ask not to be.
