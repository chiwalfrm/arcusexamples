Recommended OS: Ubuntu 24.04.4 LTS server

**Required Packages**
- python3-pip, python3-venv (to install pip3 packages)
- redis (caches data) [OPTIONAL]

**Required Pip3 Libraries**
- aiohttp (public/wsorderbook.py creates a web server that serves the orderbook to clients)
- websockets
- cryptography
- redis [OPTIONAL]

**Documentation**
- private-commands-docs.txt - Documentation for the private/ commands
- public-commands-docs.txt - Documentation for the public/ commands

Note: private/ordersign.py is provided by dYdX Trading, Inc.  All rights reserved.
