import React, { useEffect, useRef, useState } from 'react';

const vertexShaderSource = `
attribute vec2 a_pos;
varying vec2 v_uv;
void main() {
  v_uv = a_pos * 0.5 + 0.5;
  gl_Position = vec4(a_pos, 0.0, 1.0);
}
`;

const fragmentShaderSource = `
precision mediump float;

varying vec2 v_uv;
uniform vec2 u_res;
uniform float u_time;
uniform float u_mood;
uniform float u_expression;

float smin(float a, float b, float k) {
  float h = max(k - abs(a - b), 0.0) / k;
  return min(a, b) - h * h * h * k * (1.0 / 6.0);
}

mat2 rot(float a) {
  float c = cos(a);
  float s = sin(a);
  return mat2(c, -s, s, c);
}

float sdSphere(vec3 p, float r) {
  return length(p) - r;
}

float sdCapsule(vec3 p, vec3 a, vec3 b, float r) {
  vec3 pa = p - a;
  vec3 ba = b - a;
  float h = clamp(dot(pa, ba) / dot(ba, ba), 0.0, 1.0);
  return length(pa - ba * h) - r;
}

float sdBox(vec3 p, vec3 b) {
  vec3 q = abs(p) - b;
  return length(max(q, 0.0)) + min(max(q.x, max(q.y, q.z)), 0.0);
}

vec2 mapScene(vec3 p) {
  float t = u_time;
  float bounce = sin(t * 2.2) * 0.06;
  float happy = step(0.5, u_mood) * (1.0 - step(1.5, u_mood));
  float upset = step(1.5, u_mood);

  vec3 q = p;
  q.y -= bounce + happy * abs(sin(t * 9.0)) * 0.05;
  q.x += upset * sin(t * 45.0) * 0.025;

  float d = 10.0;
  float id = 0.0;

  float body = sdSphere(q - vec3(0.0, -0.15, 0.0), 0.52);
  d = body;
  id = 1.0;

  float head = sdSphere(q - vec3(0.0, 0.62, 0.0), 0.42);
  if (head < d) { d = head; id = 1.0; }

  float earL = sdSphere(q - vec3(-0.38, 0.58, 0.03), 0.16);
  float earR = sdSphere(q - vec3(0.38, 0.58, 0.03), 0.16);
  if (earL < d) { d = earL; id = 1.0; }
  if (earR < d) { d = earR; id = 1.0; }

  float hatBase = sdSphere(q - vec3(0.0, 0.95, 0.02), 0.34);
  float hatTop = sdSphere(q - vec3(0.08, 1.2, 0.05), 0.19);
  float hat = smin(hatBase, hatTop, 0.2);
  if (hat < d) { d = hat; id = 2.0; }

  float armL = sdCapsule(q, vec3(-0.43, 0.06, 0.05), vec3(-0.82, -0.05, 0.18), 0.12);
  float armR = sdCapsule(q, vec3(0.43, 0.08, 0.05), vec3(0.78, 0.34, 0.15), 0.11);
  if (armL < d) { d = armL; id = 1.0; }
  if (armR < d) { d = armR; id = 1.0; }

  float legL = sdCapsule(q, vec3(-0.16, -0.56, 0.02), vec3(-0.18, -0.98, 0.08), 0.12);
  float legR = sdCapsule(q, vec3(0.16, -0.56, 0.02), vec3(0.18, -0.98, 0.08), 0.12);
  if (legL < d) { d = legL; id = 1.0; }
  if (legR < d) { d = legR; id = 1.0; }

  float pants = sdBox(q - vec3(0.0, -0.56, 0.05), vec3(0.32, 0.2, 0.22));
  if (pants < d) { d = pants; id = 2.0; }

  float shoeL = sdSphere(q - vec3(-0.22, -1.08, 0.22), 0.2);
  float shoeR = sdSphere(q - vec3(0.22, -1.08, 0.22), 0.2);
  if (shoeL < d) { d = shoeL; id = 2.0; }
  if (shoeR < d) { d = shoeR; id = 2.0; }

  vec3 bookP = q - vec3(-0.98, -0.08, 0.2);
  float book = sdBox(bookP, vec3(0.18, 0.27, 0.06));
  if (book < d) { d = book; id = 3.0; }

  vec3 poleP = q - vec3(0.92, 0.46, 0.26);
  float pole = sdCapsule(poleP, vec3(0.0, -0.36, 0.0), vec3(0.0, 0.42, 0.0), 0.02);
  if (pole < d) { d = pole; id = 4.0; }

  vec3 flagP = q - vec3(1.1, 0.76, 0.24);
  flagP.y += sin(flagP.x * 8.0 + t * 5.0) * 0.04;
  float flag = sdBox(flagP, vec3(0.26, 0.11, 0.015));
  if (flag < d) { d = flag; id = 5.0; }

  if (upset > 0.5) {
    vec3 poopP = q - vec3(0.32 + t * 1.15, 0.05 + sin(t * 6.0) * 0.12, -0.06 + t * 0.95);
    float poop = sdSphere(poopP, 0.09);
    if (poop < d) { d = poop; id = 6.0; }
  }

  return vec2(d, id);
}

vec3 getMaterialColor(float id, vec3 p) {
  if (id < 1.5) {
    float fres = 0.45 + 0.55 * smoothstep(-0.2, 0.6, p.y);
    return mix(vec3(0.07, 0.44, 0.86), vec3(0.49, 0.79, 1.0), fres);
  }
  if (id < 2.5) {
    return vec3(0.95, 0.97, 1.0);
  }
  if (id < 3.5) {
    if (p.y > 0.06) return vec3(0.11, 0.11, 0.12);
    if (p.y > -0.1) return vec3(0.9, 0.12, 0.2);
    return vec3(0.98, 0.84, 0.25);
  }
  if (id < 4.5) return vec3(0.86, 0.9, 0.95);
  if (id < 5.5) {
    float stripe = step(0.5, fract((p.y + 2.0) * 7.0));
    return mix(vec3(0.98, 0.1, 0.18), vec3(1.0), stripe);
  }
  return vec3(0.43, 0.27, 0.09);
}

vec3 calcNormal(vec3 p) {
  vec2 e = vec2(0.0018, 0.0);
  float x = mapScene(p + e.xyy).x - mapScene(p - e.xyy).x;
  float y = mapScene(p + e.yxy).x - mapScene(p - e.yxy).x;
  float z = mapScene(p + e.yyx).x - mapScene(p - e.yyx).x;
  return normalize(vec3(x, y, z));
}

void main() {
  vec2 uv = v_uv * 2.0 - 1.0;
  uv.x *= u_res.x / u_res.y;

  float t = u_time;
  vec3 ro = vec3(0.0, 0.06, 3.2);
  ro.y += sin(t * 0.8) * 0.04;
  vec3 ta = vec3(0.0, -0.05, 0.0);

  vec3 f = normalize(ta - ro);
  vec3 r = normalize(cross(vec3(0.0, 1.0, 0.0), f));
  vec3 u = cross(f, r);
  vec3 rd = normalize(f + uv.x * r + uv.y * u);

  vec3 col = vec3(0.94, 0.97, 1.0);
  col *= 1.0 - 0.1 * length(uv);

  float dist = 0.0;
  float hitId = -1.0;
  vec3 pos = ro;
  for (int i = 0; i < 80; i++) {
    pos = ro + rd * dist;
    vec2 h = mapScene(pos);
    if (h.x < 0.0015) {
      hitId = h.y;
      break;
    }
    dist += h.x;
    if (dist > 7.0) break;
  }

  if (hitId > -0.5) {
    vec3 n = calcNormal(pos);
    vec3 lightDir = normalize(vec3(0.7, 1.0, 0.6));
    float diff = max(dot(n, lightDir), 0.0);
    float hemi = 0.5 + 0.5 * n.y;
    float spec = pow(max(dot(reflect(-lightDir, n), -rd), 0.0), 28.0);
    vec3 base = getMaterialColor(hitId, pos);
    col = base * (0.22 + 0.78 * diff) + base * 0.26 * hemi + spec * vec3(1.0);

    if (hitId < 1.5) {
      float exprBlink = step(0.5, u_expression) * (1.0 - step(1.5, u_expression));
      float exprSmile = step(1.5, u_expression) * (1.0 - step(2.5, u_expression));
      float exprFrown = step(2.5, u_expression);

      vec3 eyeL = vec3(-0.12, 0.72, 0.32);
      vec3 eyeR = vec3(0.12, 0.72, 0.32);
      float eyeOpenL = smoothstep(0.055, 0.0, length(pos - eyeL));
      float eyeOpenR = smoothstep(0.055, 0.0, length(pos - eyeR));

      float blinkL = smoothstep(0.02, 0.0,
        abs(pos.y - eyeL.y) +
        max(abs(pos.x - eyeL.x) - 0.06, 0.0) +
        abs(pos.z - eyeL.z) * 1.2
      );
      float blinkR = smoothstep(0.02, 0.0,
        abs(pos.y - eyeR.y) +
        max(abs(pos.x - eyeR.x) - 0.06, 0.0) +
        abs(pos.z - eyeR.z) * 1.2
      );
      float eL = mix(eyeOpenL, blinkL, exprBlink);
      float eR = mix(eyeOpenR, blinkR, exprBlink);
      col = mix(col, vec3(0.05, 0.08, 0.16), max(eL, eR));

      vec3 nose = vec3(0.0, 0.55, 0.35);
      float nMask = smoothstep(0.08, 0.0, length(pos - nose));
      col = mix(col, vec3(0.13, 0.52, 0.9), nMask * 0.8);

      float front = smoothstep(0.07, 0.0, abs(pos.z - 0.34));
      float xGate = 1.0 - smoothstep(0.19, 0.23, abs(pos.x));
      float neutralMouth = smoothstep(0.02, 0.0, abs((pos.y - 0.40) + 0.28 * pos.x * pos.x - 0.005)) * front * xGate;
      float smileMouth = smoothstep(0.02, 0.0, abs((pos.y - 0.405) + 0.78 * pos.x * pos.x - 0.02)) * front * xGate;
      float frownMouth = smoothstep(0.02, 0.0, abs((pos.y - 0.35) - 0.78 * pos.x * pos.x + 0.01)) * front * xGate;

      float mouth = neutralMouth;
      mouth = mix(mouth, smileMouth, exprSmile);
      mouth = mix(mouth, frownMouth, exprFrown);
      col = mix(col, vec3(0.55, 0.09, 0.18), mouth * 0.85);
    }

    float fog = exp(-0.08 * dist * dist);
    col = mix(vec3(0.92, 0.95, 1.0), col, fog);
  }

  col = pow(col, vec3(0.92));
  gl_FragColor = vec4(col, 1.0);
}
`;

function createShader(gl, type, source) {
  const shader = gl.createShader(type);
  if (!shader) return null;
  gl.shaderSource(shader, source);
  gl.compileShader(shader);
  if (!gl.getShaderParameter(shader, gl.COMPILE_STATUS)) {
    console.error('WebGL shader compile error:', gl.getShaderInfoLog(shader));
    gl.deleteShader(shader);
    return null;
  }
  return shader;
}

function createProgram(gl, vsSource, fsSource) {
  const vs = createShader(gl, gl.VERTEX_SHADER, vsSource);
  const fs = createShader(gl, gl.FRAGMENT_SHADER, fsSource);
  if (!vs || !fs) return null;

  const program = gl.createProgram();
  if (!program) return null;
  gl.attachShader(program, vs);
  gl.attachShader(program, fs);
  gl.linkProgram(program);

  gl.deleteShader(vs);
  gl.deleteShader(fs);

  if (!gl.getProgramParameter(program, gl.LINK_STATUS)) {
    console.error('WebGL link error:', gl.getProgramInfoLog(program));
    gl.deleteProgram(program);
    return null;
  }
  return program;
}

function moodToFloat(mood) {
  if (mood === 'correct') return 1;
  if (mood === 'wrong') return 2;
  if (mood === 'timeout') return 3;
  return 0;
}

function expressionToFloat(expression) {
  if (expression === 'blink') return 1;
  if (expression === 'smile') return 2;
  if (expression === 'frown') return 3;
  return 0;
}

export default function WebGLMascot({
  className = '',
  mood = 'idle',
  expression = 'neutral',
  fallbackSrc = '',
}) {
  const canvasRef = useRef(null);
  const moodRef = useRef(moodToFloat(mood));
  const expressionRef = useRef(expressionToFloat(expression));
  const [webglFailed, setWebglFailed] = useState(false);

  useEffect(() => {
    moodRef.current = moodToFloat(mood);
  }, [mood]);

  useEffect(() => {
    expressionRef.current = expressionToFloat(expression);
  }, [expression]);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return undefined;
    setWebglFailed(false);

    const gl = canvas.getContext('webgl', { antialias: true, alpha: true, powerPreference: 'high-performance' });
    if (!gl) {
      setWebglFailed(true);
      return undefined;
    }

    const program = createProgram(gl, vertexShaderSource, fragmentShaderSource);
    if (!program) {
      setWebglFailed(true);
      return undefined;
    }

    const positionBuffer = gl.createBuffer();
    gl.bindBuffer(gl.ARRAY_BUFFER, positionBuffer);
    gl.bufferData(
      gl.ARRAY_BUFFER,
      new Float32Array([-1, -1, 1, -1, -1, 1, 1, 1]),
      gl.STATIC_DRAW,
    );

    const aPos = gl.getAttribLocation(program, 'a_pos');
    const uRes = gl.getUniformLocation(program, 'u_res');
    const uTime = gl.getUniformLocation(program, 'u_time');
    const uMood = gl.getUniformLocation(program, 'u_mood');
    const uExpression = gl.getUniformLocation(program, 'u_expression');

    let rafId = 0;
    let start = performance.now();

    const resize = () => {
      const dpr = Math.min(window.devicePixelRatio || 1, 2);
      const rect = canvas.getBoundingClientRect();
      const width = Math.max(1, Math.floor(rect.width * dpr));
      const height = Math.max(1, Math.floor(rect.height * dpr));
      if (canvas.width !== width || canvas.height !== height) {
        canvas.width = width;
        canvas.height = height;
      }
      gl.viewport(0, 0, canvas.width, canvas.height);
    };

    const render = (now) => {
      resize();
      const timeSec = (now - start) / 1000;

      gl.useProgram(program);
      gl.clearColor(0, 0, 0, 0);
      gl.clear(gl.COLOR_BUFFER_BIT);

      gl.bindBuffer(gl.ARRAY_BUFFER, positionBuffer);
      gl.enableVertexAttribArray(aPos);
      gl.vertexAttribPointer(aPos, 2, gl.FLOAT, false, 0, 0);

      if (uRes) gl.uniform2f(uRes, canvas.width, canvas.height);
      if (uTime) gl.uniform1f(uTime, timeSec);
      if (uMood) gl.uniform1f(uMood, moodRef.current);
      if (uExpression) gl.uniform1f(uExpression, expressionRef.current);

      gl.drawArrays(gl.TRIANGLE_STRIP, 0, 4);
      rafId = window.requestAnimationFrame(render);
    };

    const ro = new ResizeObserver(() => resize());
    ro.observe(canvas);
    rafId = window.requestAnimationFrame(render);

    return () => {
      window.cancelAnimationFrame(rafId);
      ro.disconnect();
      gl.deleteBuffer(positionBuffer);
      gl.deleteProgram(program);
    };
  }, []);

  return (
    <div className={`mascot-webgl ${className}`.trim()}>
      {!webglFailed && <canvas ref={canvasRef} aria-hidden="true" />}
      {webglFailed && fallbackSrc && (
        <img src={fallbackSrc} alt="Mascot" className="mascot-fallback" loading="lazy" />
      )}
    </div>
  );
}
