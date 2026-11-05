# Security policy

Report security issues privately through a GitHub Security Advisory in this repository; do not publish exploitable details in public issues.

## Security design
- The extension does not request persistent access to all websites.
- Media scanning is user initiated and restricted to the active tab.
- The extension has no remote executable code or analytics.
- Native messaging is optional and requires a separately installed local host.
- Download filenames are sanitized before writing to the user's download directory.
- DRM and access-control bypass are out of scope.
