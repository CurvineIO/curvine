module.exports = {
  extends: ['@commitlint/config-conventional'],
  rules: {
    // Type must be one of the following
    'type-enum': [
      2,
      'always',
      [
        'feat',     // New feature
        'fix',      // Bug fix
        'docs',     // Documentation updates
        'style',    // Code style changes (no logic changes)
        'refactor', // Code refactoring
        'perf',     // Performance optimization
        'test',     // Test related
        'chore',    // Build process or auxiliary tool changes
        'ci',       // CI/CD related
        'build',    // Build system or external dependency changes
        'revert'    // Revert commit
      ]
    ],
    // Type cannot be empty
    'type-empty': [2, 'never'],
    // Subject cannot be empty
    'subject-empty': [2, 'never'],
    // Subject cannot end with period
    'subject-full-stop': [2, 'never', '.'],
    // Header max length
    'header-max-length': [2, 'always', 120],
    // Body line max length
    'body-max-line-length': [2, 'always', 100],
    // Footer line max length  
    'footer-max-line-length': [2, 'always', 100]
  }
}; 