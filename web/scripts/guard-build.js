const releaseFlags = new Set(["1", "true", "yes"]);
const releaseEnv =
  process.env.SHIRO_RELEASE || process.env.RELEASE || process.env.CI_RELEASE;

if (!releaseFlags.has(String(releaseEnv || "").toLowerCase())) {
  console.error(
    "Refusing to run `next build` without a release flag. " +
      "Use `npm run dev` for local development, or set SHIRO_RELEASE=1 to proceed."
  );
  process.exit(1);
}
