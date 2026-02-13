import React, { useEffect, useRef, useState } from 'react';
const THREE_CDN = 'https://unpkg.com/three@0.160.0/build/three.min.js';
const BASE_URL = import.meta.env.BASE_URL || '/';
const THREE_LOCAL_SCRIPT = `${BASE_URL}vendor/three.min.js`;
let threeLoaderPromise = null;

function normalizeThreeModule(candidate) {
  if (!candidate) return null;
  if (candidate.THREE) return candidate.THREE;
  if (candidate.default) return candidate.default;
  return candidate;
}

async function loadThreeFromLocalModule() {
  try {
    const dynamicImport = (0, eval)('(specifier) => import(specifier)');
    const module = await dynamicImport('three');
    return normalizeThreeModule(module);
  } catch (_err) {
    return null;
  }
}

function loadThreeFromScript(url) {
  if (typeof window === 'undefined') return Promise.resolve(null);
  if (window.THREE) return Promise.resolve(window.THREE);
  return new Promise((resolve) => {
    const existing = document.querySelector(`script[data-three-src="${url}"]`);
    if (existing) {
      existing.addEventListener('load', () => resolve(window.THREE || null), { once: true });
      existing.addEventListener('error', () => resolve(null), { once: true });
      return;
    }
    const script = document.createElement('script');
    script.src = url;
    script.async = true;
    script.dataset.threeSrc = url;
    script.onload = () => resolve(window.THREE || null);
    script.onerror = () => resolve(null);
    document.head.appendChild(script);
  });
}

function loadThree() {
  if (typeof window === 'undefined') return Promise.resolve(null);
  if (window.THREE) return Promise.resolve(window.THREE);
  if (threeLoaderPromise) return threeLoaderPromise;

  threeLoaderPromise = (async () => {
    const fromModule = await loadThreeFromLocalModule();
    if (fromModule) return fromModule;

    const localUrl = window.__THREE_LOCAL_URL || THREE_LOCAL_SCRIPT;
    const fromLocalScript = await loadThreeFromScript(localUrl);
    if (fromLocalScript) return fromLocalScript;

    return loadThreeFromScript(THREE_CDN);
  })();

  return threeLoaderPromise;
}

function applyExpression(eyes, mouth, expression) {
  const isBlink = expression === 'blink';
  const isSmile = expression === 'smile';
  const isFrown = expression === 'frown';

  eyes.scale.y = isBlink ? 0.15 : 1;
  mouth.scale.y = isSmile ? 1.15 : (isFrown ? 0.75 : 1);
  mouth.rotation.z = isFrown ? Math.PI : 0;
  mouth.position.y = isFrown ? -0.02 : -0.11;
}

function getViewConfig(variant) {
  if (variant === 'setup') {
    return { fov: 28, camY: 0.38, camZ: 7.2, lookY: 0.35, scale: 0.72, baseY: -0.56 };
  }
  if (variant === 'card') {
    return { fov: 33, camY: 0.34, camZ: 5.8, lookY: 0.36, scale: 0.96, baseY: -0.3 };
  }
  return { fov: 30, camY: 0.35, camZ: 6.2, lookY: 0.35, scale: 0.92, baseY: -0.32 };
}

export default function ThreeMascot({
  className = '',
  mood = 'idle',
  expression = 'neutral',
  variant = 'hero',
  fallbackSrc = '',
  renderMode = 'static',
}) {
  const rootRef = useRef(null);
  const [ready, setReady] = useState(false);
  const isStaticMode = renderMode === 'static';

  useEffect(() => {
    if (isStaticMode) {
      setReady(false);
      return () => {};
    }

    let disposed = false;
    let renderer;
    let rafId = 0;
    let ro;

    const init = async () => {
      const THREE = await loadThree();
      if (!THREE || disposed || !rootRef.current) {
        setReady(false);
        return;
      }

      const scene = new THREE.Scene();
      const view = getViewConfig(variant);
      const camera = new THREE.PerspectiveCamera(view.fov, 1, 0.1, 100);
      camera.position.set(0, view.camY, view.camZ);
      camera.lookAt(0, view.lookY, 0);

      renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true });
      renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2));
      renderer.setClearColor(0x000000, 0);
      if ('outputColorSpace' in renderer && THREE.SRGBColorSpace) {
        renderer.outputColorSpace = THREE.SRGBColorSpace;
      }
      rootRef.current.appendChild(renderer.domElement);

      const lightA = new THREE.DirectionalLight(0xffffff, 1.05);
      lightA.position.set(3, 4, 4);
      scene.add(lightA);
      scene.add(new THREE.AmbientLight(0xffffff, 0.72));

      const mascot = new THREE.Group();
      mascot.scale.set(view.scale, view.scale, view.scale);
      mascot.position.set(0, view.baseY, 0);
      scene.add(mascot);

      const skinMat = new THREE.MeshStandardMaterial({ color: 0x1f7de6, roughness: 0.35, metalness: 0.05 });
      const skinLightMat = new THREE.MeshStandardMaterial({ color: 0x58b0ff, roughness: 0.35, metalness: 0.02 });
      const whiteMat = new THREE.MeshStandardMaterial({ color: 0xf2f6ff, roughness: 0.5, metalness: 0.02 });
      const darkMat = new THREE.MeshStandardMaterial({ color: 0x172131, roughness: 0.45, metalness: 0.08 });
      const redMat = new THREE.MeshStandardMaterial({ color: 0xe21d2f, roughness: 0.55, metalness: 0.05 });
      const yellowMat = new THREE.MeshStandardMaterial({ color: 0xffd13f, roughness: 0.5, metalness: 0.04 });

      const body = new THREE.Mesh(new THREE.SphereGeometry(0.9, 36, 24), skinMat);
      body.position.set(0, -0.15, 0);
      mascot.add(body);

      const head = new THREE.Mesh(new THREE.SphereGeometry(0.76, 40, 30), skinLightMat);
      head.position.set(0, 1.03, 0);
      mascot.add(head);

      const earGeo = new THREE.SphereGeometry(0.23, 24, 20);
      const earL = new THREE.Mesh(earGeo, skinLightMat);
      earL.position.set(-0.66, 1.0, 0.08);
      earL.scale.set(1.35, 0.9, 1);
      mascot.add(earL);
      const earR = earL.clone();
      earR.position.x = 0.66;
      mascot.add(earR);

      const eyeWhiteGeo = new THREE.SphereGeometry(0.16, 20, 16);
      const eyeWhiteL = new THREE.Mesh(eyeWhiteGeo, whiteMat);
      eyeWhiteL.position.set(-0.22, 1.1, 0.63);
      mascot.add(eyeWhiteL);
      const eyeWhiteR = eyeWhiteL.clone();
      eyeWhiteR.position.x = 0.22;
      mascot.add(eyeWhiteR);

      const eyes = new THREE.Group();
      const pupilGeo = new THREE.SphereGeometry(0.07, 16, 12);
      const pupilL = new THREE.Mesh(pupilGeo, darkMat);
      pupilL.position.set(-0.22, 1.1, 0.78);
      eyes.add(pupilL);
      const pupilR = pupilL.clone();
      pupilR.position.x = 0.22;
      eyes.add(pupilR);
      mascot.add(eyes);

      const nose = new THREE.Mesh(new THREE.SphereGeometry(0.13, 18, 14), new THREE.MeshStandardMaterial({ color: 0x2a8eed }));
      nose.position.set(0, 0.9, 0.72);
      mascot.add(nose);

      const mouth = new THREE.Mesh(
        new THREE.TorusGeometry(0.18, 0.03, 12, 40, Math.PI),
        new THREE.MeshStandardMaterial({ color: 0x7a1c28 }),
      );
      mouth.position.set(0, -0.11, 0.72);
      mouth.rotation.set(Math.PI, 0, 0);
      head.add(mouth);

      const armGeo = new THREE.CapsuleGeometry(0.12, 0.58, 10, 16);
      const armL = new THREE.Mesh(armGeo, skinMat);
      armL.position.set(-0.92, 0.0, 0.06);
      armL.rotation.z = 1.3;
      mascot.add(armL);
      const armR = new THREE.Mesh(armGeo, skinMat);
      armR.position.set(0.92, 0.22, 0.06);
      armR.rotation.z = -0.7;
      mascot.add(armR);

      const hat = new THREE.Mesh(new THREE.ConeGeometry(0.58, 0.8, 28), whiteMat);
      hat.position.set(0, 1.86, 0.04);
      hat.rotation.z = -0.2;
      mascot.add(hat);
      const hatBall = new THREE.Mesh(new THREE.SphereGeometry(0.16, 18, 14), whiteMat);
      hatBall.position.set(0.22, 2.16, 0.16);
      mascot.add(hatBall);

      const pants = new THREE.Mesh(new THREE.CylinderGeometry(0.58, 0.58, 0.42, 28), whiteMat);
      pants.position.set(0, -0.98, 0.02);
      mascot.add(pants);

      const legGeo = new THREE.CapsuleGeometry(0.13, 0.42, 8, 14);
      const legL = new THREE.Mesh(legGeo, skinMat);
      legL.position.set(-0.22, -1.34, 0.02);
      mascot.add(legL);
      const legR = legL.clone();
      legR.position.x = 0.22;
      mascot.add(legR);

      const shoeGeo = new THREE.SphereGeometry(0.24, 22, 16);
      const shoeL = new THREE.Mesh(shoeGeo, whiteMat);
      shoeL.position.set(-0.32, -1.75, 0.12);
      shoeL.scale.set(1.35, 0.72, 1.1);
      mascot.add(shoeL);
      const shoeR = shoeL.clone();
      shoeR.position.x = 0.32;
      mascot.add(shoeR);

      const book = new THREE.Group();
      const cover = new THREE.Mesh(new THREE.BoxGeometry(0.52, 0.78, 0.12), darkMat);
      book.add(cover);
      const stripe1 = new THREE.Mesh(new THREE.BoxGeometry(0.46, 0.2, 0.02), darkMat);
      stripe1.position.set(0, 0.24, 0.07);
      book.add(stripe1);
      const stripe2 = new THREE.Mesh(new THREE.BoxGeometry(0.46, 0.2, 0.02), redMat);
      stripe2.position.set(0, 0.03, 0.07);
      book.add(stripe2);
      const stripe3 = new THREE.Mesh(new THREE.BoxGeometry(0.46, 0.24, 0.02), yellowMat);
      stripe3.position.set(0, -0.24, 0.07);
      book.add(stripe3);
      book.position.set(-1.42, -0.05, 0.2);
      book.rotation.z = 0.1;
      mascot.add(book);

      const pole = new THREE.Mesh(new THREE.CylinderGeometry(0.02, 0.02, 1.2, 14), whiteMat);
      pole.position.set(1.32, 0.62, 0.2);
      mascot.add(pole);
      const flag = new THREE.Mesh(new THREE.PlaneGeometry(0.9, 0.44, 18, 8), redMat);
      flag.position.set(1.78, 0.83, 0.2);
      flag.material = flag.material.clone();
      mascot.add(flag);

      const poop = new THREE.Mesh(
        new THREE.SphereGeometry(0.09, 14, 12),
        new THREE.MeshStandardMaterial({ color: 0x6d4518, roughness: 0.8, metalness: 0.05 }),
      );
      poop.visible = false;
      mascot.add(poop);

      const resize = () => {
        if (!rootRef.current) return;
        const w = Math.max(1, rootRef.current.clientWidth);
        const h = Math.max(1, rootRef.current.clientHeight);
        renderer.setSize(w, h, false);
        camera.aspect = w / h;
        camera.updateProjectionMatrix();
      };

      ro = new ResizeObserver(resize);
      ro.observe(rootRef.current);
      resize();

      setReady(true);
      const start = performance.now();
      const flagPos = flag.geometry.attributes.position;

      const tick = (now) => {
        if (disposed) return;
        const t = (now - start) / 1000;

        mascot.position.y = view.baseY + Math.sin(t * 2.4) * 0.055;
        mascot.position.x = 0;
        mascot.rotation.x = Math.sin(t * 1.4) * 0.03;
        mascot.rotation.y = Math.sin(t * 0.95) * 0.045;

        if (mood === 'correct') {
          mascot.position.y += Math.abs(Math.sin(t * 10.0)) * 0.08;
          armR.rotation.z = -0.65 + Math.sin(t * 8.0) * 0.2;
        } else {
          armR.rotation.z = -0.7;
        }
        if (mood === 'wrong' || mood === 'timeout') {
          mascot.rotation.z = Math.sin(t * 34.0) * 0.06;
          armL.rotation.z = 1.15 + Math.sin(t * 20.0) * 0.18;
        } else {
          mascot.rotation.z = 0;
          armL.rotation.z = 1.3;
        }

        for (let i = 0; i < flagPos.count; i += 1) {
          const x = flagPos.getX(i);
          const y = flagPos.getY(i);
          const wave = Math.sin(t * 5.0 + x * 6.0 + y * 3.0) * 0.05;
          flagPos.setZ(i, wave);
        }
        flagPos.needsUpdate = true;

        if (mood === 'wrong' || mood === 'timeout') {
          poop.visible = true;
          const tt = (t % 0.9) / 0.9;
          poop.position.set(0.3 + tt * 1.4, 0.2 + Math.sin(tt * Math.PI) * 0.35, 0.18 + tt * 1.4);
          poop.scale.setScalar(0.85 + tt * 1.1);
        } else {
          poop.visible = false;
        }

        applyExpression(eyes, mouth, expression);
        renderer.render(scene, camera);
        rafId = window.requestAnimationFrame(tick);
      };

      rafId = window.requestAnimationFrame(tick);
    };

    init();

    return () => {
      disposed = true;
      if (rafId) window.cancelAnimationFrame(rafId);
      if (ro) ro.disconnect();
      if (renderer) {
        renderer.dispose();
        if (renderer.domElement && renderer.domElement.parentNode) {
          renderer.domElement.parentNode.removeChild(renderer.domElement);
        }
      }
    };
  }, [mood, expression, variant, isStaticMode]);

  if (isStaticMode) {
    return (
      <div
        className={`mascot-three is-fallback ${className}`.trim()}
        aria-hidden="true"
      >
        {fallbackSrc && <img src={fallbackSrc} alt="Mascot" className="mascot-static-fallback" />}
      </div>
    );
  }

  return (
    <div
      ref={rootRef}
      className={`mascot-three ${ready ? 'is-ready' : 'is-fallback'} ${className}`.trim()}
      aria-hidden="true"
    >
      {!ready && fallbackSrc && <img src={fallbackSrc} alt="Mascot" className="mascot-static-fallback" />}
    </div>
  );
}
