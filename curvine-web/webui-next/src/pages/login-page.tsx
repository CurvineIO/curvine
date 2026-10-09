import { useState } from 'react'
import { useNavigate, useSearchParams } from 'react-router-dom'

import logoUrl from '@/assets/logo.svg'
import { useAuth } from '@/auth/auth-context'
import { ErrorAlert } from '@/components/data-state'
import { SubmitButton, TextField } from '@/components/form-fields'
import { Card, CardContent, CardHeader } from '@/components/ui/card'
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue
} from '@/components/ui/select'
import { localeOptions, useI18n, type AppLocale } from '@/i18n'

export function LoginPage() {
  const { t, locale, setLocale } = useI18n()
  const { signIn } = useAuth()
  const navigate = useNavigate()
  const [searchParams] = useSearchParams()

  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)

  async function submit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault()
    setError(null)

    if (!username || !password) {
      setError(t('login.required'))
      return
    }

    setSubmitting(true)
    try {
      await signIn(username, password)
      navigate(searchParams.get('redirect') || '/overview', { replace: true })
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : t('common.error'))
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <main className="grid min-h-svh place-items-center bg-background p-6">
      <Card className="w-full max-w-sm" data-testid="login-panel">
        <CardHeader>
          <div className="flex items-center gap-3">
            <img src={logoUrl} alt="Curvine" className="size-10 shrink-0" />
            <div className="flex flex-col gap-0.5">
              <h1 className="font-heading text-lg leading-tight font-semibold">
                {t('login.title')}
              </h1>
              <p className="text-xs text-muted-foreground">{t('login.subtitle')}</p>
            </div>
          </div>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          <div className="flex flex-col gap-1.5">
            <span className="text-sm font-medium">{t('common.language')}</span>
            <Select value={locale} onValueChange={(value) => setLocale(value as AppLocale)}>
              <SelectTrigger className="w-full" data-testid="login-locale-select">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectGroup>
                  {localeOptions.map((option) => (
                    <SelectItem key={option.value} value={option.value}>
                      {option.label}
                    </SelectItem>
                  ))}
                </SelectGroup>
              </SelectContent>
            </Select>
          </div>

          <form className="flex flex-col gap-4" onSubmit={submit} noValidate>
            <TextField
              label={t('login.username')}
              value={username}
              onChange={setUsername}
              placeholder={t('login.usernamePlaceholder')}
              autoComplete="username"
              autoFocus
              testId="login-username"
            />
            <TextField
              label={t('login.password')}
              type="password"
              value={password}
              onChange={setPassword}
              placeholder={t('login.passwordPlaceholder')}
              autoComplete="current-password"
              testId="login-password"
            />

            {error && <ErrorAlert message={error} />}

            <SubmitButton
              pending={submitting}
              pendingLabel={t('login.signingIn')}
              testId="login-submit"
            >
              {t('login.signIn')}
            </SubmitButton>
            <p className="text-xs text-muted-foreground">{t('login.hint')}</p>
          </form>
        </CardContent>
      </Card>
    </main>
  )
}
