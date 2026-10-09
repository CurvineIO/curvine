/// <reference types="vite/client" />

declare module '*.css'
declare module '*.svg' {
  const src: string
  export default src
}

interface ImportMetaEnv {
  readonly VITE_API_BASE?: string
  readonly VITE_API_BASE_URL?: string
}

interface ImportMeta {
  readonly env: ImportMetaEnv
}
