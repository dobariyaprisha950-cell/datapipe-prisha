# GitHub and TestPyPI release setup

This repository is prepared to publish the `datapipe-prisha` distribution to **TestPyPI only**. The GitHub Actions publisher uses OpenID Connect (OIDC); it does not use a stored PyPI password or API token. The release workflow verifies the GitHub Release tag matches the project version, runs the checks, enforces the core branch-coverage threshold, builds the wheel and source distribution, validates their metadata, installs the wheel in a clean environment, then passes the built distributions to a separate least-privileged publishing job.

## 1. Create or select the GitHub repository

The current checkout has no Git remote, and no GitHub CLI or authenticated GitHub session is available in this environment. Public search did not identify an exact `datapipe-prisha` repository; private or unindexed repositories could not be checked. The repository name requested for this project is `datapipe-prisha`, but its owner and URL are not known here.

In the GitHub account that should own this project:

1. Check that account for an existing `datapipe-prisha` repository. If it exists, confirm that it is the intended repository. If it does not, create it without initializing it with a README, license, or `.gitignore` so its initial commit does not conflict with this history.
2. Copy the clone URL shown by GitHub and add it as `origin` from this checkout:

   ```powershell
   git remote add origin <CLONE-URL-COPIED-FROM-GITHUB>
   git push -u origin master
   ```

   Keep the existing local history. Replace the angle-bracket placeholder with the actual URL from GitHub; no repository URL is supplied or assumed by this project.
3. In repository **Settings → Environments**, create an environment named `testpypi`. Add required reviewers if releases should require human approval.

Do not add a PyPI password or token as a repository secret.

## 2. Configure the TestPyPI trusted publisher

Use TestPyPI, not production PyPI. Sign in to TestPyPI and use its **Publishing** settings:

- If `datapipe-prisha` already exists on TestPyPI, open that project's **Manage → Publishing** page and add a GitHub Actions publisher.
- For a new TestPyPI project, configure a **pending publisher** from the TestPyPI account's **Publishing** page. Set the project name to `datapipe-prisha`; a pending publisher does not reserve the name until its first successful publication.

Use these exact workflow identity fields, substituting the actual GitHub owner and repository name established in step 1:

| Trusted Publisher field | Value |
|---|---|
| Owner | Actual GitHub account or organization owner |
| Repository | `datapipe-prisha` |
| Workflow filename | `.github/workflows/release-testpypi.yml` |
| GitHub environment | `testpypi` |

The publisher must be configured on **TestPyPI**. Do not configure this workflow as a production PyPI publisher. The publishing job receives `id-token: write` only after its validation job succeeds and only while running in the `testpypi` environment.

PyPI's documentation covers [adding a trusted publisher](https://docs.pypi.org/trusted-publishers/adding-a-publisher/), [creating a project with a pending publisher](https://docs.pypi.org/trusted-publishers/creating-a-project-through-oidc/), and [publishing with OIDC](https://docs.pypi.org/trusted-publishers/using-a-publisher/).

## 3. Run a TestPyPI release

After the repository and publisher are configured:

1. Confirm CI is green on the pushed commit, including both POSIX signal tests on Ubuntu.
2. Ensure `pyproject.toml` has the intended release version. The currently configured version is `0.1.0`.
3. Create and publish a GitHub Release for that version. The `release` event starts `.github/workflows/release-testpypi.yml`.
4. Confirm the `validate` job passes and the `publish` job reports a successful TestPyPI upload. Then install the version from TestPyPI in a clean environment and exercise its CLI before considering a production release separately.

TestPyPI does not allow replacing an uploaded version. If a version was uploaded, increment the project version before another upload. This workflow never targets production PyPI.

## Current status

The release workflow and project metadata are prepared and locally validated. This checkout has no remote, repository owner, GitHub authentication, or TestPyPI publisher configuration. No repository was created, no push was made, and no package has been uploaded.
