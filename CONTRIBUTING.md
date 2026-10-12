# Contributing to MedEnroll834

Thank you for your interest in contributing to MedEnroll834! This document provides guidelines and instructions for contributing.

## Code of Conduct

By participating in this project, you agree to maintain a respectful and inclusive environment.

## How Can I Contribute?

### Reporting Bugs

1. Check if the bug has already been reported in [Issues](https://github.com/yourusername/medenroll834/issues)
2. If not, create a new issue using the Bug Report template
3. Provide detailed information to help reproduce the issue

### Suggesting Enhancements

1. Check if the enhancement has already been suggested
2. Create a new issue using the Feature Request template
3. Clearly describe the enhancement and its benefits

### Pull Requests

1. Fork the repository
2. Create a new branch: `git checkout -b feature/your-feature-name`
3. Make your changes following our coding standards
4. Write or update tests as needed
5. Update documentation as needed
6. Commit your changes: `git commit -m 'Add some feature'`
7. Push to the branch: `git push origin feature/your-feature-name`
8. Submit a pull request

## Development Setup

### Prerequisites

- Python 3.10+
- Java 11+ (for PySpark)
- Docker (optional)
- Make (optional)

### Local Development

```bash
# Clone the repository
git clone https://github.com/yourusername/medenroll834.git
cd medenroll834

# Create a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
pip install -e ".[dev]"

# Set up pre-commit hooks (optional)
pre-commit install
```

## Coding Standards

### Python Style Guide

- Follow PEP 8
- Use type hints where appropriate
- Maximum line length: 100 characters
- Use descriptive variable names

### Code Formatting

We use the following tools:
- **Black** for code formatting
- **Ruff** for linting
- **MyPy** for type checking

```bash
# Format code
make format

# Run linters
make lint
```

### Testing

- Write unit tests for all new features
- Maintain or improve code coverage
- Ensure all tests pass before submitting PR

```bash
# Run tests
make test

# Run tests with coverage
pytest tests/ -v --cov=src --cov-report=term-missing
```

## Commit Messages

Follow conventional commits format:

```
<type>(<scope>): <subject>

<body>

<footer>
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `chore`: Maintenance tasks

Example:
```
feat(parser): add support for 834 5010 version

Implemented parser logic to handle X12 834 version 5010
with improved error handling and validation.

Closes #123
```

## Documentation

- Update README.md if needed
- Add docstrings to all functions and classes
- Update API documentation in docs/api/
- Add examples for new features

## Review Process

1. All PRs require at least one approval
2. All tests must pass
3. Code coverage should not decrease
4. Documentation must be updated

## Questions?

Feel free to open an issue for questions or reach out to the maintainers.

Thank you for contributing! 🎉
