<template>
  <main class="login-page">
    <section class="login-panel">
      <div class="login-brand">
        <img src="@/assets/logo.svg" alt="Curvine">
        <div>
          <h1>{{ $t('login.title') }}</h1>
          <p>{{ $t('login.subtitle') }}</p>
        </div>
      </div>

      <div class="login-locale">
        <label>
          {{ $t('common.language') }}
          <select :value="locale" class="admin-input" @change="onLocaleChange">
            <option v-for="item in localeOptions" :key="item.value" :value="item.value">{{ item.label }}</option>
          </select>
        </label>
      </div>

      <form class="login-form" @submit.prevent="submit">
        <label>{{ $t('login.username') }}<input v-model="username" autocomplete="username" :placeholder="$t('login.usernamePlaceholder')" autofocus></label>
        <label>{{ $t('login.password') }}<input v-model="password" type="password" autocomplete="current-password" :placeholder="$t('login.passwordPlaceholder')"></label>
        <button class="admin-button primary" type="submit" :disabled="loading">{{ loading ? $t('login.signingIn') : $t('login.signIn') }}</button>
        <p v-if="error" class="login-error">{{ error }}</p>
        <p class="login-hint">{{ $t('login.hint') }}</p>
      </form>
    </section>
  </main>
</template>

<script>
import { login } from '@/api/client'
import { i18n, localeOptions, setLocale } from '@/i18n'

export default {
  name: 'LoginPage',
  data() {
    return {
      username: '',
      password: '',
      loading: false,
      error: '',
      localeOptions
    }
  },
  computed: {
    locale() {
      return i18n.global.locale.value
    }
  },
  methods: {
    onLocaleChange(event) {
      setLocale(event.target.value)
    },
    async submit() {
      this.error = ''
      if (!this.username || !this.password) {
        this.error = this.$t('login.required')
        return
      }
      this.loading = true
      try {
        await login({ username: this.username, password: this.password })
        const redirect = this.$route.query.redirect || '/overview'
        this.$router.replace(String(redirect))
      } catch (error) {
        this.error = String(error)
      } finally { this.loading = false }
    }
  }
}
</script>

<style scoped>
.login-page {
  min-height: 100vh;
  display: grid;
  place-items: center;
  padding: 24px;
  background: var(--admin-bg);
}

.login-panel {
  width: min(420px, 100%);
  border: 1px solid var(--admin-border);
  border-radius: 8px;
  background: #fff;
  padding: 28px;
}

.login-brand {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 24px;
}

.login-brand img { width: 40px; height: 40px; }

.login-locale {
  margin-bottom: 16px;
}

.login-locale label {
  display: flex;
  flex-direction: column;
  gap: 6px;
  font-size: 0.9rem;
}

.login-form {
  display: grid;
  gap: 14px;
}

.login-form label {
  display: grid;
  gap: 6px;
  font-size: 0.9rem;
}

.login-form input {
  border: 1px solid var(--admin-border);
  border-radius: 6px;
  padding: 10px 12px;
}

.login-error {
  color: #c0392b;
  margin: 0;
}

.login-hint {
  color: var(--admin-muted);
  font-size: 0.85rem;
  margin: 0;
}
</style>
