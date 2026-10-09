import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { BrowserRouter } from 'react-router-dom'

import { App } from './App'
import { AuthProvider } from '@/auth/auth-context'
import { I18nProvider } from '@/i18n'
import { MasterRuntimeProvider } from '@/master-runtime/master-runtime-context'
import { RefreshProvider } from '@/refresh/refresh-context'
import { ThemeProvider } from '@/theme/theme-context'
import { ToastProvider } from '@/components/ui/toast'
import { TooltipProvider } from '@/components/ui/tooltip'
import '@/styles/index.css'

const container = document.getElementById('root')
if (!container) {
  throw new Error('Root container #root is missing from index.html')
}

createRoot(container).render(
  <StrictMode>
    <BrowserRouter>
      <ThemeProvider>
        <I18nProvider>
          <AuthProvider>
            <MasterRuntimeProvider>
              <RefreshProvider>
                <ToastProvider>
                  <TooltipProvider>
                    <App />
                  </TooltipProvider>
                </ToastProvider>
              </RefreshProvider>
            </MasterRuntimeProvider>
          </AuthProvider>
        </I18nProvider>
      </ThemeProvider>
    </BrowserRouter>
  </StrictMode>
)
