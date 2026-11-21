# Curvine Web UI

The web console of the Curvine master. It is a Vue 3 single-page app (Vue CLI 5, Vue Router, Vuex, Bootstrap) with pages for the cluster overview, configuration, file browser, workers, and block locations.

## Prerequisites

- Node.js 22.13 or later, with the npm that ships with it. `build/check-env.sh` checks for this version, and `sass-loader` needs Node.js 22.11 or later.
- A running Curvine master, for example one started with the [Quick Start](../../README.md#-quick-start).

Run the commands below from `curvine-web/webui`. The `package.json` at the repository root does not install the UI dependencies.

## Install and run

```bash
cd curvine-web/webui
npm ci           # installs the versions pinned in package-lock.json; `npm install` also works
npm run serve    # dev server with hot reload, on http://localhost:8080 unless that port is taken
```

## Connect to a master

The UI calls relative `/api/...` URLs. In development, the dev server proxies them to the master's web server, using two variables from `.env`:

```dotenv
VUE_APP_BASE_URL = "http://127.0.0.1:9000"   # master web address
VUE_APP_BASE_API = "/api"                    # API path prefix
```

`vue.config.js` forwards `/api/<path>` to `${VUE_APP_BASE_URL}${VUE_APP_BASE_API}/<path>`, for example `/api/overview` to `http://127.0.0.1:9000/api/overview`.

The master serves its web UI and API on `[master] web_port` from the cluster config. The default is `9001` (`ClusterConf::DEFAULT_MASTER_WEB_PORT`), so set `VUE_APP_BASE_URL` to match your master. To point at another master without editing the committed `.env`, create `.env.local`, which is git-ignored:

```dotenv
VUE_APP_BASE_URL = "http://<master-host>:9001"
```

Restart `npm run serve` after you change these files.

## Production build

```bash
npm run build    # writes the bundle to dist/
```

You do not need to copy `dist/` by hand for a normal build. `make build` (or `make build ARGS='-p web'` for the UI only) runs `npm install` and `npm run build` here, then moves `dist/` to `build/dist/webui`.

The master serves the UI itself:

- `curvine-web/src/server/web_server.rs` serves static files from the `webui` directory relative to the master's working directory (`WEBUI_DIR`). `bin/launch-process.sh` starts the master from `CURVINE_HOME`, so the UI comes from `build/dist/webui` and is available at `http://<master-host>:<web_port>/`.
- The REST endpoints `/api/overview`, `/api/config`, `/api/browse`, `/api/block_locations` and `/api/workers` are routes in `curvine-master/src/master/router_handler.rs`.
- Every other path falls back to the static files, and unknown paths return `index.html`. This lets a page such as `/workers` load after a browser refresh. Explicit backend routes win over this fallback, so a UI route must not reuse a backend path (see [#2084](https://github.com/CurvineIO/curvine/issues/2084)).

## Lint

```bash
npm run lint                         # vue-cli-service lint; fixes what it can in place
npx vue-cli-service lint --no-fix    # report only
```

Run the linter before you open a pull request.

## Directory map

| Path | Contents |
| --- | --- |
| `src/main.js` | Entry point that mounts `App.vue` with the router and the store |
| `src/views/` | One component per page: `Overview.vue`, `Config.vue`, `Browse.vue`, `Workers.vue`, `Blocks.vue`, `AboutView.vue` |
| `src/components/` | Shared layout: `Header.vue`, `Footer.vue`, `Loading.vue` |
| `src/router/index.js` | Page routes: `/overview`, `/config`, `/browse`, `/workers`, `/blocks`, `/about` |
| `src/store/index.js` | Vuex store for shared state, such as the last request error |
| `src/api/client.js` | One function per master endpoint |
| `src/utils/request.js` | axios instance that uses `VUE_APP_BASE_API` as its base URL |
| `src/styles/`, `src/assets/` | SCSS and images |
| `public/` | `index.html` template and favicons, copied into the build as they are |

Each page loads its data through one function in `src/api/client.js`:

| Page | Function | Master endpoint |
| --- | --- | --- |
| Overview | `fetchOverviewData` | `GET /api/overview` |
| Config | `fetchConfigData` | `GET /api/config` |
| Browse | `fetchBrowseData` | `GET /api/browse` |
| Workers | `fetchWorkersData` | `GET /api/workers` |
| Blocks | `fetchBlocksData` | `GET /api/block_locations` |

## Troubleshooting

- **Proxy errors such as `ECONNREFUSED` while running `npm run serve`:** the dev server cannot reach `VUE_APP_BASE_URL`. Check that the master is running and that the port matches its `web_port` (default `9001`).
- **CORS errors in the browser console:** the browser is calling the master directly, and the master does not send CORS headers. Keep `VUE_APP_BASE_API` as the relative `/api` so requests go through the dev server proxy.
- **The master returns JSON or a 404 instead of the UI:** the master did not find a `webui` directory in its working directory. Build with `make build` and start the master with `bin/curvine-master.sh`, which runs from `CURVINE_HOME`.
- **The old UI still shows after a rebuild:** the master serves whatever is in `build/dist/webui`. If you ran `npm run build` yourself, copy `dist/` there, then hard-refresh the browser.
