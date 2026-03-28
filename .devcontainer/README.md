# Dev Container

This directory contains Dev Container configuration for local development.

## Files

- `devcontainer.json`: active local Dev Container config
- `devcontainer.json.example`: example config with proxy settings

## Usage

1. Copy the example file:
   - `cp .devcontainer/devcontainer.json.example .devcontainer/devcontainer.json`
2. Adjust proxy settings in `containerEnv` if needed.
3. Reopen the project in the Dev Container.

## Proxy settings

The example includes these environment variables:

- `HTTP_PROXY`: HTTP proxy address
- `HTTPS_PROXY`: HTTPS proxy address
- `ALL_PROXY`: SOCKS proxy address for tools that support it
- `NO_PROXY`: hosts that should bypass the proxy

Default example values:

- `HTTP_PROXY=http://host.docker.internal:7890`
- `HTTPS_PROXY=http://host.docker.internal:7890`
- `ALL_PROXY=socks5://host.docker.internal:7891`
- `NO_PROXY=localhost,127.0.0.1,host.docker.internal`

`host.docker.internal` lets the container access a proxy running on the host machine. Update ports and values to match your local environment.
