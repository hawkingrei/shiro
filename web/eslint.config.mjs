import nextConfig from "eslint-config-next";

const config = [
  ...nextConfig,
  {
    settings: {
      react: {
        version: "19.2.4",
      },
    },
  },
];

export default config;
