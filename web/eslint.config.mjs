import nextConfig from "eslint-config-next";
import tseslint from "typescript-eslint";

const config = [
  ...nextConfig.map((entry) => {
    if (entry.name !== "next") {
      return entry;
    }
    return {
      ...entry,
      languageOptions: {
        ...entry.languageOptions,
        parser: tseslint.parser,
      },
    };
  }),
  {
    settings: {
      react: {
        version: "19.2.4",
      },
    },
  },
];

export default config;
