// Isolated built-asset server. Auth and API traffic is intercepted by Playwright.
const http = require("node:http");
const fs = require("node:fs");
const crypto = require("node:crypto");
http
  .createServer((request, response) => {
    const files = {
      "/": "index.html",
      "/app.js": "app.js",
      "/style.css": "style.css",
    };
    const file = files[request.url];
    if (!file) {
      response.writeHead(404);
      response.end();
      return;
    }
    const nonce = crypto.randomBytes(32).toString("base64");
    response.setHeader(
      "Content-Security-Policy",
      `default-src 'none'; script-src 'self'; style-src 'self' 'nonce-${nonce}'; connect-src 'self' https://auth.i3alumba.ru; img-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'`,
    );
    response.setHeader(
      "Content-Type",
      file.endsWith(".js")
        ? "text/javascript"
        : file.endsWith(".css")
          ? "text/css"
          : "text/html",
    );
    response.setHeader("Cache-Control", "no-store");
    response.end(
      fs
        .readFileSync("dist/" + file, "utf8")
        .replace("__MONITOR_STYLE_NONCE__", nonce),
    );
  })
  .listen(4179, "127.0.0.1");
