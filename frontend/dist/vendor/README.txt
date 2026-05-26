Optional local Three.js fallback

This app tries to load Three.js in this order:
1) local npm package "three" (if available)
2) local script: /vendor/three.min.js
3) CDN: https://unpkg.com/three@0.160.0/build/three.min.js

To force local script mode without npm:
- place three.min.js in this folder (frontend/public/vendor/three.min.js)

To override URL at runtime:
- set window.__THREE_LOCAL_URL = 'https://your-host/path/to/three.min.js' before app starts.
