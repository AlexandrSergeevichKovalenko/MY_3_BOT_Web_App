# 00a — Frontend foundations: JS, JSX, data types, hooks, fetch

This file explains the language and the tools **once**. Every later frontend block assumes you
read this. It is not about our app yet — it is the vocabulary you need before our code means
anything. Read it top to bottom, then keep it open as a reference while you read block 01.

## 1. Two different things: JavaScript and JSX

**JavaScript (JS)** is the programming language that runs in the browser (and inside the Telegram
webview, which is just a browser). It has variables, functions, numbers, strings, arrays, objects,
`if`, loops — the logic. A file of pure JS ends in `.js`.

**JSX** is *not* a language. It is an extension of JS syntax that lets you write HTML-looking tags
directly inside JS code. It exists only because of React. A file that uses it ends in `.jsx`
(that's why our main UI file is `App.jsx`). This:

```jsx
const element = <button className="primary">Save</button>;
```

is **not** valid plain JS. Before it runs, a build tool (Vite, in our repo) compiles it into real
JS function calls:

```js
const element = React.createElement("button", { className: "primary" }, "Save");
```

So the mental model is: **JSX is sugar. It always turns into JS function calls that build UI
objects.** The `<button>` is not an HTML button yet — it's a JS object describing one, which React
later turns into a real DOM element. That is the entire difference: JS is the logic; JSX is a
compact way to write "what the screen should look like" inside that logic.

Two syntax facts you must know to read JSX:
- Attributes use camelCase and JS names, not HTML names: `className` (not `class`), `onClick`
  (not `onclick`).
- To drop a JS value into JSX you use curly braces: `<p>{score}/100</p>` means "render the value
  of the variable `score` here". Anything inside `{ }` in JSX is a JS expression.

## 2. Anatomy of a function in JS (bare, no app)

A **function** is a named block of logic that takes **inputs** (called *parameters* / *arguments*)
and produces one **output** (via `return`). There are two spellings you will see constantly.

### 2.1 Classic function declaration

```js
function add(a, b) {   // `function` keyword, name `add`, parameter list (a, b) in round brackets
  const sum = a + b;   // body lives inside curly braces { }. `const` declares a constant variable.
  return sum;          // `return` sends one value back to whoever called add(). Ends the function.
}                      // no return → the function returns `undefined`.

const x = add(2, 3);   // "calling"/"invoking" add with arguments 2 and 3. x is now 5.
```

Mandatory parts: the `function` keyword (for this form), a parameter list in `( )` (can be empty:
`()`), and a body in `{ }`. Statements inside usually end with `;`.

### 2.2 Arrow function (the form our React code uses everywhere)

```js
const add = (a, b) => {   // no `function` keyword. Parameters in ( ), then `=>`, then body { }.
  return a + b;           // arrow function stored in a const named `add`. Same behavior as above.
};

const square = n => n * n;   // shortcuts: 1 parameter → brackets optional; no { } → value is
                             // auto-returned. square(4) is 16. This "implicit return" is common.
```

Why two forms exist is not important yet. What matters: when you see `const handleClick = () => {
… }`, that is **a function named `handleClick` that takes no arguments**. `=>` is "arrow", it just
means "this is a function".

### 2.3 `async` / `await` (functions that wait for slow things)

Talking to a server takes time. JS does not freeze while it waits — it uses **Promises**. A Promise
is an object that means "a value that isn't here yet, but will be." Two keywords manage this:

```js
async function loadData() {          // `async` marks a function that contains waiting.
  const response = await fetch(url);  // `await` = "pause HERE until the Promise resolves, then
  const data = await response.json(); // continue." `response` is the resolved value, not a Promise.
  return data;                        // an async function ALWAYS returns a Promise to its caller.
}
```

Rule: `await` can only be used inside an `async` function. If you see `await`, the thing after it
is slow (a network call, reading a response body) and the code below it will not run until that
slow thing finishes.

## 3. Data types and data structures (what "the data" actually is)

Every value you pass around has a **type**. These are the only ones you need:

| Type | Written as | Example | What it is |
| --- | --- | --- | --- |
| string | text in quotes | `"Ich habe"` | a piece of text |
| number | digits | `7`, `100`, `2.5` | one number (JS has no separate int/float) |
| boolean | `true` / `false` | `true` | a yes/no flag |
| null / undefined | `null`, `undefined` | `null` | "no value" (null = deliberately empty; undefined = never set) |
| array | square brackets | `["a", "b", "c"]` | an **ordered list** of values, indexed from 0 |
| object | curly brackets | `{ id: 42, text: "hi" }` | a **key→value map** (a "dictionary" in Python terms) |

**Array** = ordered list. `items[0]` is the first element. It has a `.length`. You loop or
transform it with methods (section 5).

**Object** = a bag of named fields (keys) pointing to values. This is the single most important
structure, because every request body and every server response is an object. You read a field
with a dot: `user.id`, or with brackets: `user["id"]`. Values can themselves be arrays or objects,
so objects nest:

```js
const payload = {                       // an object with 3 keys
  initData: "user=...&hash=...",         // key `initData` → a string
  limit: 7,                              // key `limit` → a number
  translations: [                        // key `translations` → an array of objects
    { id_for_mistake_table: 42, translation: "Ich habe" },   // each element is an object
    { id_for_mistake_table: 43, translation: "Du bist" },
  ],
};
```

Read that structure carefully — it is literally the body we send to the server in block 01. When I
say "the request takes an array of objects", this is what that means: `translations` is a list, and
each element is a small object with exactly two keys.

## 4. JSON — the format data travels in

The network cannot send a JS object directly. It can only send **text**. **JSON** (JavaScript
Object Notation) is the agreed text format for objects and arrays. Two functions convert both ways:

```js
JSON.stringify(obj)   // JS object  → JSON string (text). Used when SENDING to the server.
JSON.parse(text)      // JSON string → JS object.        Used when READING a text response.
```

So `JSON.stringify({ limit: 7 })` produces the string `'{"limit":7}'`. That string is what
actually goes over the wire in the request body. On the server (Python/Flask) the reverse happens:
the JSON text is parsed back into a Python dict. **JSON is just the shared text encoding so two
different languages can exchange the same object.**

## 5. Array/object methods you will see (each described once)

A **method** is a function attached to a value, called with a dot: `value.methodName(args)`.

- `array.map(fn)` — takes a function, runs it on **every** element, returns a **new array** of the
  results. Same length as the input. Example: `[1,2,3].map(n => n * 10)` → `[10,20,30]`. We use it
  to turn a list of UI items into a list of request objects.
- `array.filter(fn)` — returns a new array with only the elements for which `fn` returned `true`.
- `object.field` / `object?.field` — read a field. The `?.` ("optional chaining") means "if the
  left side is null/undefined, stop and give `undefined` instead of crashing". `telegramApp?.initData`
  = "the `initData` of `telegramApp`, but don't crash if `telegramApp` doesn't exist."
- `a || b` — "or": gives `a` if `a` is truthy (not null/0/""/undefined), otherwise `b`. Used for
  defaults: `const name = input || "guest"`.
- `String(x)`, `Number(x)` — force-convert a value to a string / number.

## 6. React hooks — the methods that make the UI update

React is the library that turns our JS state into what you see on screen. The core rule of React:
**when a piece of "state" changes, React re-runs your component function and redraws the UI.** You
manage state with functions called **hooks**. A hook is just a function whose name starts with
`use`. Here are the four used in block 01, each with its exact signature.

### 6.1 `useState` — remember a value between redraws

```js
const [count, setCount] = useState(0);
//     ▲ current value  ▲ setter fn   ▲ initial value
```

`useState(initial)` returns an **array of exactly two elements**: `[currentValue, setterFunction]`.
The `const [a, b] = array` syntax is "destructuring" — it pulls element 0 into `a` and element 1
into `b`. You read the value from the first; you change it by calling the second:
`setCount(count + 1)`. Calling the setter tells React "this changed, redraw." You never write
`count = 5` directly — React wouldn't notice.

In our code you'll see: `const [initData, setInitData] = useState(telegramApp?.initData || '')`.
That means: state variable `initData`, initialized to Telegram's init string, or `''` (empty
string) if it's missing.

### 6.2 `useRef` — remember a value WITHOUT redrawing

```js
const tokenRef = useRef(0);   // returns an object: { current: 0 }
tokenRef.current = 5;         // you read/write .current; changing it does NOT trigger a redraw.
```

`useRef(initial)` returns an object with one field, `.current`. Use it for values that must
survive redraws but should not themselves cause one (e.g. a "which poll request is the latest"
counter). This is the difference from `useState`: state redraws the UI, a ref does not.

### 6.3 `useEffect` — run code as a side effect, after render

```js
useEffect(() => {          // arg 1: a function to run
  doSomething();
}, [x, y]);                // arg 2: the "dependency array"
```

`useEffect(fn, deps)` runs `fn` **after** the component renders, but only re-runs it when a value
in `deps` changed since last time. `[]` = run once on mount. No second arg = run after every
render. It's how you start timers, fetch data on open, subscribe to things.

### 6.4 `useMemo` — cache an expensive computation

```js
const sorted = useMemo(() => expensiveSort(list), [list]);
```

`useMemo(fn, deps)` runs `fn` and **remembers its result**, only recomputing when a `dep` changes.
Between renders where `list` didn't change, it returns the cached value instead of re-sorting. Pure
performance; it does not change what the value is.

## 7. `fetch` — how the frontend calls our server

`fetch` is the built-in function for making an HTTP request. This is the exact door between the
frontend and the backend, so learn its shape precisely.

```js
const response = await fetch(url, options);
```

- **Argument 1 `url`** — a string, the path to hit, e.g. `"/api/webapp/sentences"`.
- **Argument 2 `options`** — an object configuring the request. The fields we use:
  - `method` — a string: `"GET"` (read), `"POST"` (send data), etc.
  - `headers` — an object of metadata, e.g. `{ "Content-Type": "application/json" }` which tells the
    server "the body is JSON text."
  - `body` — a **string** (that's why we wrap our object in `JSON.stringify`). GET requests have no
    body; POST carries the data here.
- **Return value** — a Promise that resolves to a `Response` object. Because it's a Promise, we
  `await` it. The `Response` object's fields/methods we use:
  - `response.ok` — a boolean: `true` if the HTTP status was 200–299 (success).
  - `response.status` — the number, e.g. `401`, `429`.
  - `await response.json()` — reads the body text and `JSON.parse`s it into a JS object. Also slow
    (the body may still be streaming), so it's `await`ed.
  - `await response.text()` — reads the body as a raw string.

Put together, one full call looks like this (this is the *shape* of every server call in block 01):

```js
async function loadSentences(initData) {
  const response = await fetch("/api/webapp/sentences", {   // door 1: which endpoint
    method: "POST",                                          // we're sending data, so POST
    headers: { "Content-Type": "application/json" },         // "the body below is JSON"
    body: JSON.stringify({ initData, limit: 7 }),            // JS object → JSON text → the body
  });
  if (!response.ok) {                                         // 4xx/5xx → handle the error
    throw new Error("request failed");
  }
  const data = await response.json();                        // success → parse the JSON body to an object
  return data;                                               // `data` is now a normal JS object
}
```

Why the body is shaped `{ initData, limit: 7 }` and not something else: the **server** decides what
fields it reads. The frontend must send exactly the keys the backend looks for (`initData`,
`limit`, `session_id`, …). If you send a key the server ignores, nothing happens; if you omit a key
the server needs, you get a `400`. So the request structure is a **contract** dictated by the
backend handler — you'll see both sides side-by-side in block 01.

One more shorthand you'll see: `{ initData }` inside an object is the same as `{ initData: initData }`.
When the key name equals the variable name, JS lets you write it once. That's "shorthand property".

## 8. Self-check before block 01

1. In `const [initData, setInitData] = useState('')`, what type does `useState` return, how many
   elements does it have, and which one do you call to change the value?
2. We send `body: JSON.stringify({ initData, translations })`. Why can't we pass the plain object
   as `body` directly? What does `JSON.stringify` produce?
3. In `const response = await fetch(...)`, what is the type of `response` *before* the `await`
   resolves, and what is it *after*?

Answers are in sections 6.1, 4, and 7. If you can't answer these, reread those sections — block 01
uses all three in its first ten lines.
