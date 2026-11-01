import js from "@eslint/js";
import globals from "globals";
import tseslint from "typescript-eslint";
import hooks from "eslint-plugin-react-hooks";
export default tseslint.config({
  files: ["src/**/*.{ts,tsx}"],
  extends: [js.configs.recommended, ...tseslint.configs.recommended],
  languageOptions: { globals: globals.browser },
  plugins: { "react-hooks": hooks },
  rules: hooks.configs.recommended.rules,
});
