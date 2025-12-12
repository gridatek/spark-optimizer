# setup.py
from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="spark-optimizer",
    version="0.1.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="Optimize Spark job resources based on historical data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/gridatek/spark-optimizer",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=[
        "click>=8.0.0",
        "sqlalchemy>=1.4.0",
        "numpy>=1.21.0",
        "scikit-learn>=1.0.0",
        "pandas>=1.3.0",
        "tabulate>=0.9.0",
        "flask>=2.0.0",
        "pyyaml>=6.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=3.0.0",
            "black>=22.0.0",
            "flake8>=4.0.0",
            "mypy>=0.950",
            "sphinx>=4.5.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "spark-optimizer=spark_optimizer.cli.commands:cli",
        ],
    },
)

# ============================================================================
# requirements.txt
# ============================================================================
"""
click>=8.0.0
sqlalchemy>=1.4.0
numpy>=1.21.0
scikit-learn>=1.0.0
pandas>=1.3.0
tabulate>=0.9.0
flask>=2.0.0
pyyaml>=6.0
"""

# ============================================================================
# requirements-dev.txt
# ============================================================================
"""
-r requirements.txt
pytest>=7.0.0
pytest-cov>=3.0.0
pytest-mock>=3.10.0
black>=22.0.0
flake8>=4.0.0
mypy>=0.950
sphinx>=4.5.0
sphinx-rtd-theme>=1.0.0
"""

# ============================================================================
# .gitignore
# ============================================================================
"""
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# Virtual environments
venv/
env/
ENV/
.venv

# IDEs
.vscode/
.idea/
*.swp
*.swo
*~

# Testing
.pytest_cache/
.coverage
htmlcov/
.tox/

# Database
*.db
*.sqlite
*.sqlite3

# Logs
*.log

# OS
.DS_Store
Thumbs.db

# Project specific
spark_optimizer.db
event_logs/
sample_data/
"""

# ============================================================================
# docker-compose.yml
# ============================================================================
"""
version: '3.8'

services:
  postgres:
    image: postgres:14
    environment:
      POSTGRES_DB: spark_optimizer
      POSTGRES_USER: spark_user
      POSTGRES_PASSWORD: spark_password
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

  spark-optimizer-api:
    build: .
    command: spark-optimizer serve --port 8080 --host 0.0.0.0
    ports:
      - "8080:8080"
    environment:
      - DATABASE_URL=postgresql://spark_user:spark_password@postgres:5432/spark_optimizer
    depends_on:
      - postgres
    volumes:
      - ./event_logs:/event_logs:ro

volumes:
  postgres_data:
"""

# ============================================================================
# Dockerfile
# ============================================================================
"""
FROM python:3.10-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY src/ ./src/
COPY setup.py .
COPY README.md .

# Install package
RUN pip install -e .

# Create directory for event logs
RUN mkdir -p /event_logs

# Expose API port
EXPOSE 8080

# Default command
CMD ["spark-optimizer", "serve", "--port", "8080", "--host", "0.0.0.0"]
"""

# ============================================================================
# CONTRIBUTING.md
# ============================================================================
"""
# Contributing to Spark Resource Optimizer

Thank you for your interest in contributing!

## Development Setup

1. Clone the repository:
```bash
git clone https://github.com/gridatek/spark-optimizer.git
cd spark-optimizer
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\\Scripts\\activate
```

3. Install development dependencies:
```bash
pip install -e ".[dev]"
```

4. Run tests:
```bash
pytest tests/
```

## Code Style

- We use `black` for code formatting
- We use `flake8` for linting
- Run before committing:
```bash
black src/ tests/
flake8 src/ tests/
```

## Testing

- Write tests for new features
- Maintain test coverage above 80%
- Run tests with: `pytest tests/ --cov=spark_optimizer`

## Pull Request Process

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Code Review

All submissions require review. We use GitHub pull requests for this purpose.

## Reporting Bugs

Use GitHub Issues to report bugs. Include:
- Clear description
- Steps to reproduce
- Expected behavior
- Actual behavior
- System information

## Feature Requests

Feature requests are welcome! Please:
- Check existing issues first
- Explain the use case
- Describe the expected behavior

## Community

- Be respectful and inclusive
- Follow the code of conduct
- Help others when possible

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
"""

# ============================================================================
# LICENSE
# ============================================================================
"""
Apache License 2.0

Copyright 2024 [Your Name]

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""