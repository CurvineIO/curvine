module.exports = {
  extends: ['@commitlint/config-conventional'],
  plugins: [
    {
      rules: {
        'require-issue-reference': (parsed) => {
          const { subject } = parsed;
          // Check if subject contains issue reference in format (#number)
          const issuePattern = /\(#\d+\)$/;
          
          if (!issuePattern.test(subject)) {
            return [
              false,
              'Commit message must contain issue reference in format (#number), e.g.: "feat: add new feature (#123)"'
            ];
          }
          
          // Check if subject (excluding issue reference) starts with lowercase
          const subjectWithoutIssue = subject.replace(/\s*\(#\d+\)$/, '');
          if (!/^[a-z]/.test(subjectWithoutIssue)) {
            return [
              false,
              'Commit subject must start with lowercase letter'
            ];
          }
          
          return [true];
        }
      }
    }
  ],
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
    // Disable default subject-case rule, use custom rule instead
    'subject-case': [0],
    // Header max length (increased to accommodate issue ID)
    'header-max-length': [2, 'always', 120],
    // Body line max length
    'body-max-line-length': [2, 'always', 100],
    // Footer line max length  
    'footer-max-line-length': [2, 'always', 100],
    // Use custom rule to validate issue reference
    'require-issue-reference': [2, 'always']
  }
}; 