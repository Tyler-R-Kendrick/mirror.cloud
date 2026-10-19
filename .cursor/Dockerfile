# Cursor Cloud Agent image for mirror.cloud.
# Go 1.23 on Debian bookworm (matches go.mod), Node 22 for the Grok/Kimi CLIs and
# JS tooling, plus the Grok Build CLI (setup copied from OpenSesame).
# Do not COPY the repo; Cursor checks it out separately.
FROM golang:1.23-bookworm

ENV PATH=/usr/local/go/bin:/go/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    git \
    curl \
    ca-certificates \
    sudo \
    build-essential \
  && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
  && apt-get install -y --no-install-recommends nodejs \
  && rm -rf /var/lib/apt/lists/*

# Grok Build CLI (tier-1 coding agent), installed system-wide like OpenSesame.
ENV GROK_BIN_DIR=/usr/local/bin
RUN curl -fsSL https://x.ai/cli/install.sh | bash \
  && /usr/local/bin/grok --version

# Kimi Code CLI (tier-2 fallback when the Grok weekly limit is exceeded).
ARG KIMI_CODE_VERSION=2.1.1
RUN npm install -g "@moonshot-ai/kimi-code@${KIMI_CODE_VERSION}" \
  && kimi --version

RUN echo 'export PATH="/usr/local/go/bin:${HOME}/go/bin:${PATH}"' > /etc/profile.d/go.sh

RUN useradd -m -u 1000 -s /bin/bash vscode \
  && echo "vscode ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/vscode \
  && chmod 0440 /etc/sudoers.d/vscode

USER vscode
