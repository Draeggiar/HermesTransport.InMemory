# CI/CD Pipeline Documentation

This repository includes a GitHub Actions pipeline for automated building, testing, and NuGet package publishing.

## Pipeline Overview

The CI/CD pipeline consists of two main jobs:

### 1. Build and Test (`build-and-test`)

**Triggers:**
- Push to `main` or `develop` branches
- Pull requests to `main` branch

**Steps:**
1. Checkout code
2. Setup .NET 8.0
3. Restore dependencies
4. Build solution in Release configuration
5. Run all tests with code coverage collection
6. Upload coverage reports to Codecov (optional)

### 2. NuGet Package Publishing (`publish-nuget`)

**Triggers:**
- Only when a GitHub release is published
- Depends on successful completion of build-and-test job

**Steps:**
1. Checkout code
2. Setup .NET 8.0
3. Restore dependencies
4. Extract version from release tag
5. Build and pack the NuGet package
6. Publish to NuGet.org

## Setup Requirements

### Repository Secrets

To enable automatic NuGet publishing, you need to configure the following secrets in your GitHub repository:

1. **`NUGET_API_KEY`** (Required for NuGet publishing)
   - Go to [NuGet.org](https://www.nuget.org)
   - Navigate to Account Settings → API Keys
   - Create a new API key with "Push new packages and package versions" permissions
   - Add this key as a repository secret

2. **`CODECOV_TOKEN`** (Optional for code coverage)
   - Sign up at [Codecov.io](https://codecov.io)
   - Add your repository
   - Copy the upload token
   - Add this token as a repository secret

### Release Process

To publish a new NuGet package:

1. **Create a Git Tag:**
   ```bash
   git tag v1.0.0
   git push origin v1.0.0
   ```

2. **Create a GitHub Release:**
   - Go to your repository on GitHub
   - Click "Releases" → "Create a new release"
   - Select your tag (e.g., `v1.0.0`)
   - Fill in release title and description
   - Click "Publish release"

3. **Automatic Process:**
   - GitHub Actions will automatically trigger
   - The package version will be extracted from the tag (e.g., `v1.0.0` → `1.0.0`)
   - NuGet package will be built and published

## Package Configuration

The NuGet package is configured in `src/HermesTransport.InMemory/HermesTransport.InMemory.csproj` with the following metadata:

- **Package ID:** `HermesTransport.InMemory`
- **Description:** Production-ready in-memory message broker implementation
- **Author:** Draeggiar
- **License:** MIT
- **Repository URL:** https://github.com/Draeggiar/HermesTransport.InMemory
- **Tags:** messaging, inmemory, broker, hermestransport, pubsub, cqrs, events, commands, dotnet

## Local Testing

You can test the package creation locally:

```bash
# Build and create package
dotnet pack src/HermesTransport.InMemory/HermesTransport.InMemory.csproj --configuration Release --output ./artifacts

# Test the package (if needed)
dotnet nuget push ./artifacts/*.nupkg --source "local-feed" --skip-duplicate
```

## Coverage Reports

Code coverage reports are automatically generated during test runs and can be uploaded to Codecov for detailed analysis and trend tracking.

## Pipeline Status

You can monitor the pipeline status through:
- GitHub Actions tab in your repository
- Status badges (can be added to README.md)
- Email notifications (configurable in GitHub settings)