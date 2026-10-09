# curvine-webui-next

Curvine 管理控制台前端(React 18 + TypeScript + Vite + Tailwind CSS v4 + shadcn/ui)。

这是 `curvine-web/webui`(Vue 3 + 手写 CSS)的现代化重构版本,目标是在**不改变功能模块、logo 与操作逻辑**的前提下,
提供更现代的视觉与更高的信息密度。旧模块已下线,仓库中只保留本模块。

## 开发

```bash
npm install          # 必须带 devDependencies(vite/tailwind 都是 devDependency)
npm run dev          # http://localhost:8080,/api 代理到 http://127.0.0.1:9000
npm run type-check   # tsc --noEmit
npm run test         # vitest
npm run build        # 产物在 dist/
```

`NODE_ENV=production` 环境下 `npm install` 会跳过 devDependencies,此时用 `npm ci --include=dev`。

环境变量(见 `.env.example`):

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `VITE_API_BASE` | `/api` | 前端请求前缀,同时是 dev proxy 的匹配路径 |
| `VITE_API_BASE_URL` | `http://127.0.0.1:9000` | dev server 代理目标 |

### 本地联调后端

后端 `curvine-web` 按目录提供静态资源,优先读 `CURVINE_WEBUI_DIR`:

```bash
cd curvine-web/webui-next && npm run build
CURVINE_WEBUI_DIR=$PWD/dist <curvine-web 二进制> start --conf <conf> --port 9000
```

也可以直接 `npm run dev` 走代理联调(推荐,有 HMR)。

## 构建与部署

`build/build.sh` 的 `web` 包构建本模块,并把 `dist/` 移动到 `build/dist/webui`:

```bash
build/build.sh -p web
# 或
make build
```

产物目录名仍是 `webui`,因此后端 `CURVINE_WEBUI_DIR`、部署脚本与镜像布局**无需任何改动**。

## 约定

- **路由路径不可随意更改**:后端 `curvine-web/src/router/admin_handler.rs` 硬编码了 SPA 路由白名单
  (`/login`、`/overview`、`/browse`、`/workers/:id` 等)。新增页面必须同步在该文件里加一条。
- 语义色令牌,不写裸色值;布局用 `flex`/`grid` + `gap`;控件默认 `h-8`,密集处用 `size="sm"`。
- 数据获取用 `@/lib/use-async` 的 `useAsync`;表单用受控 `useState` + `@/components/form-fields`;
  反馈用内联 `ErrorAlert`,不用 toast。
- 详情/创建/编辑用右侧 `Sheet`,危险操作用 `AlertDialog`。
- 每个可交互元素带 `data-testid`。
- 文案键集中在 `src/i18n/messages/`,由 `webui` 的既有翻译迁移而来;`en` 是 fallback,
  `zh-CN` 额外包含后端只发中文标题的键。
