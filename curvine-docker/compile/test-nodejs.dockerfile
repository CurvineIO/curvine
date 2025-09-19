# Test Dockerfile for Node.js installation
FROM rockylinux:9

# Install latest Node.js from NodeSource repository
RUN curl -sL https://rpm.nodesource.com/setup_current.x | bash - && \
    dnf install -y nodejs && \
    dnf clean all

# Test Node.js installation
RUN node --version && npm --version

CMD ["node", "--version"]
