---
"llama-agents-appserver": patch
"llama-agents-control-plane": patch
---
Rebuild the appserver and control plane images on the security relock (#869-#877, #885, #887), so they ship patched aiohttp 3.14.5, nltk 3.10.4, setuptools 84.2.1, virtualenv 21.15.3, pyjwt 2.15.1, gitpython 3.2.1 and urllib3 2.8.0, plus Debian's current openssl.
