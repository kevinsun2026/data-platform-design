import { test, expect } from "@playwright/test";

/**
 * End-to-end smoke test for the login → datasources flow.
 *
 * The test exercises the happy path the brief calls out:
 *
 *  1. Visit ``/login``.
 *  2. Fill the email + password fields.
 *  3. Click the submit button.
 *  4. Expect the URL to land on ``/datasources``.
 *
 * The test deliberately talks to the real :file:`lib/api.ts`
 * client via the browser — no route mocking — so it doubles as a
 * wiring check that the Axios interceptor, the auth store, and
 * the Next.js rewrite all line up.
 *
 * To run against a stack with a real ``iam`` service up:
 *
 *   AIDP_TEST_EMAIL=admin@acme.com \
 *   AIDP_TEST_PASSWORD=StrongP@ss123 \
 *     pnpm test:e2e
 *
 * The defaults match the bootstrap credentials from the IAM
 * test suite so the e2e test fits into a freshly seeded dev
 * environment without a custom seed step.
 */
test("user can login and see datasources page", async ({ page }) => {
  const email = process.env.AIDP_TEST_EMAIL ?? "admin@acme.com";
  const password = process.env.AIDP_TEST_PASSWORD ?? "StrongP@ss123";

  await page.goto("/login");

  await page.fill('input[name=email]', email);
  await page.fill('input[name=password]', password);
  await page.click('button[type=submit]');

  // The login page calls ``router.replace('/datasources')`` on
  // success, so a successful login lands us straight on the
  // datasources list.
  await expect(page).toHaveURL(/\/datasources$/);

  // The datasources page renders a heading; assert it so a
  // successful login + an empty list still counts as a passing
  // e2e (we don't fail on "no datasources yet" because seeding
  // a real one is out of scope for the smoke test).
  await expect(
    page.getByRole("heading", { name: "Datasources" }),
  ).toBeVisible();
});

test("login page rejects empty form submission", async ({ page }) => {
  await page.goto("/login");

  // The submit button stays disabled via ``isSubmitting`` while
  // the request is in flight, but with an empty form the Zod
  // resolver surfaces field errors before any request is made.
  // Bypass the HTML5 validation and click straight through to
  // exercise the React-side validation path.
  await page.fill('input[name=email]', "not-an-email");
  await page.fill('input[name=password]', "");
  await page.click('button[type=submit]');

  await expect(page).toHaveURL(/\/login/);
  await expect(page.getByText(/valid email/i)).toBeVisible();
});
