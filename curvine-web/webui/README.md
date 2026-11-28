# Curvine Web UI

The web console of the Curvine master. It is a React 19 single-page app built with Vite 6, TypeScript, Tailwind CSS, and [shadcn/ui](https://ui.shadcn.com/) conventions. Pages cover the cluster overview, configuration, file browser, workers, block locations, and — when the server exposes the transfer API — transfer jobs and tenant summaries.

## Prerequisites

- Node.js 22.13 or later, with the npm that ships with it. `build/check-env.sh` checks for this version.
- A running Curvine master, for example one started with the [Quick Start](../../README.md#-quick-start).

Run the commands below from `curvine-web/webui`. The `package.json` at the repository root does not install the UI dependencies.

## Install and run

```bash
cd curvine-web/webui
npm ci           # installs the versions pinned in package-lock.json; `npm install` also works
npm run dev      # dev server with hot reload, on http://localhost:8080 unless that port is taken
```

## Connect to a master

The UI calls relative `/api/...` URLs. In development, the Vite dev server proxies them to the backend, using two variables from `.env`:

```dotenv
VITE_API_BASE=/api                        # API prefix used by the browser
VITE_API_TARGET=http://127.0.0.1:9001     # dev-server proxy target
```

`vite.config.ts` forwards `/api/<path>` to `VITE_API_TARGET`, for example `/api/overview` to `http://127.0.0.1:9001/api/overview`.

The master serves its web UI and API on `[master] web_port` from the cluster config. The default is `9001` (`ClusterConf::DEFAULT_MASTER_WEB_PORT`), so keep `VITE_API_TARGET` in sync with your master. To point elsewhere without editing the committed `.env`, create `.env.local`, which is git-ignored:

```dotenv
VITE_API_TARGET=http://<master-host>:9001
```

Restart `npm run dev` after you change these files.

## Master and transfer modes

The UI probes `GET /api/transfer/jobs?limit=1` at startup. If the response contains a `jobs` array, it runs in transfer mode and shows the Transfers and Tenants pages; otherwise it runs in master mode and shows Overview, Browse, Workers, and Configuration. Any non-transfer path redirects to `/transfers` in transfer mode.

## Production build

```bash
npm run build    # type-checks, then writes the bundle to dist/
```

You do not need to copy `dist/` by hand for a normal build. `make build` (or `make build ARGS='-p web'` for the UI only) runs `npm install` and `npm run build` here, then moves `dist/` to `build/dist/webui`.

The master serves the UI itself:

- `curvine-web/src/server/web_server.rs` serves static files from the `webui` directory relative to the master's working directory. `bin/launch-process.sh` starts the master from `CURVINE_HOME`, so the UI comes from `build/dist/webui` and is available at `http://<master-host>:<web_port>/`.
- Every other path falls back to `index.html`, so a page such as `/workers` loads after a browser refresh. Explicit backend routes win over this fallback, so a UI route must not reuse a backend path (see [#2084](https://github.com/CurvineIO/curvine/issues/2084)).

## Lint

```bash
npm run lint
```

Run the linter before you open a pull request.

## Directory map

| Path | Contents |
| --- | --- |
| `src/main.tsx` | Entry point that mounts `App.tsx` with the router and the auto-refresh provider |
| `src/App.tsx` | Route table |
| `src/pages/` | One component per page: `overview.tsx`, `workers.tsx`, `browse.tsx`, `blocks.tsx`, `config.tsx`, `transfers.tsx`, `transfer-detail.tsx`, `transfer-tenants.tsx` |
| `src/components/` | Layout and shared UI: `app-shell.tsx`, `transfer-state-badge.tsx`, `ui/` primitives |
| `src/lib/api.ts` | Typed client with one method per backend endpoint |
| `src/lib/types.ts`, `src/lib/transfer-types.ts` | Response shapes |
| `src/lib/capabilities.ts` | Master vs transfer mode detection |
| `src/lib/auto-refresh.tsx` | Shared 5-second refresh toggle, persisted in `localStorage` |
| `src/lib/navigation.ts` | Per-mode navigation and page titles |
| `src/assets/`, `public/` | Logo and favicons, copied into the build as they are |

Each page loads its data through `src/lib/api.ts`:

| Page | API method | Endpoint |
| --- | --- | --- |
| Overview | `overview` | `GET /api/overview` |
| Config | `config` | `GET /api/config` |
| Browse | `browse` | `GET /api/browse` |
| Workers | `workers` | `GET /api/workers` |
| Blocks | `blockLocations` | `GET /api/block_locations` |
| Transfers | `transferJobs` | `GET /api/transfer/jobs` |
| Transfer detail | `transferJob`, `transferTasks`, `cancelTransfer`, `retryTransfer` | `GET /api/transfer/jobs/:id`, `GET /api/transfer/jobs/:id/tasks`, `POST /api/transfer/jobs/:id/cancel`, `POST /api/transfer/jobs/:id/retry` |
| Tenants | `transferTenants` | `GET /api/transfer/tenants` |

## Troubleshooting

- **Proxy errors such as `ECONNREFUSED` while running `npm run dev`:** the dev server cannot reach `VITE_API_TARGET`. Check that the backend is running and that the port matches its `web_port` (default `9001`).
- **CORS errors in the browser console:** the browser is calling the backend directly, and it does not send CORS headers. Keep `VITE_API_BASE` as the relative `/api` so requests go through the dev server proxy.
- **The backend returns JSON or a 404 instead of the UI:** the master did not find a `webui` directory in its working directory. Build with `make build` and start the master with `bin/curvine-master.sh`, which runs from `CURVINE_HOME`.
- **The old UI still shows after a rebuild:** the master serves whatever is in `build/dist/webui`. If you ran `npm run build` yourself, copy `dist/` there, then hard-refresh the browser.
