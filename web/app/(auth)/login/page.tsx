"use client";

import * as React from "react";
import { useRouter } from "next/navigation";
import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { z } from "zod";
import { AxiosError } from "axios";

import { api } from "@/lib/api";
import { ApiError } from "@/lib/types";
import {
  type AuthTokens,
  type AuthUser,
  useAuthStore,
} from "@/lib/auth";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";

/**
 * Zod schema for the login form.
 *
 * Mirrors the platform's ``LoginRequest`` model:
 * - ``email`` is RFC-5321 shaped; we keep the field name ``email``
 *   to match the API (and the e2e test which fills
 *   ``input[name=email]``).
 * - ``password`` is at least 1 char (server enforces real strength).
 * - ``tenant_code`` is optional and surfaced as a secondary field;
 *   most logins won't need it.
 */
const loginSchema = z.object({
  email: z.string().email("Enter a valid email address"),
  password: z.string().min(1, "Password is required"),
  tenant_code: z.string().max(64).optional().or(z.literal("")),
});

type LoginInput = z.infer<typeof loginSchema>;

interface LoginResponse {
  token: AuthTokens;
  user: AuthUser;
}

export default function LoginPage() {
  const router = useRouter();
  const setSession = useAuthStore((s) => s.setSession);
  const [serverError, setServerError] = React.useState<string | null>(null);

  const {
    register,
    handleSubmit,
    formState: { errors, isSubmitting },
  } = useForm<LoginInput>({
    resolver: zodResolver(loginSchema),
    defaultValues: { email: "", password: "", tenant_code: "" },
  });

  const onSubmit = handleSubmit(async (values) => {
    setServerError(null);
    try {
      const body: Record<string, string> = {
        email: values.email,
        password: values.password,
      };
      if (values.tenant_code) {
        body.tenant_code = values.tenant_code;
      }
      const resp = await api.post<LoginResponse>("/auth/login", body);
      setSession({ token: resp.data.token, user: resp.data.user });
      router.replace("/datasources");
    } catch (err) {
      if (err instanceof ApiError) {
        setServerError(
          err.code === "UNAUTHORIZED"
            ? "Invalid email or password."
            : `${err.code}: ${err.message}`,
        );
        return;
      }
      if (err instanceof AxiosError) {
        setServerError(err.message);
        return;
      }
      setServerError("Unexpected error. Please try again.");
    }
  });

  return (
    <main className="flex min-h-screen items-center justify-center bg-muted/40 p-4">
      <Card className="w-full max-w-sm">
        <CardHeader>
          <CardTitle className="text-2xl">Sign in</CardTitle>
          <CardDescription>
            Use your AIDP account to access the admin console.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <form onSubmit={onSubmit} className="space-y-4" noValidate>
            <div className="space-y-1.5">
              <Label htmlFor="email">Email</Label>
              <Input
                id="email"
                type="email"
                autoComplete="username"
                {...register("email")}
              />
              {errors.email ? (
                <p className="text-xs text-destructive" role="alert">
                  {errors.email.message}
                </p>
              ) : null}
            </div>

            <div className="space-y-1.5">
              <Label htmlFor="password">Password</Label>
              <Input
                id="password"
                type="password"
                autoComplete="current-password"
                {...register("password")}
              />
              {errors.password ? (
                <p className="text-xs text-destructive" role="alert">
                  {errors.password.message}
                </p>
              ) : null}
            </div>

            <div className="space-y-1.5">
              <Label htmlFor="tenant_code">Tenant (optional)</Label>
              <Input
                id="tenant_code"
                type="text"
                autoComplete="off"
                placeholder="acme"
                {...register("tenant_code")}
              />
              {errors.tenant_code ? (
                <p className="text-xs text-destructive" role="alert">
                  {errors.tenant_code.message}
                </p>
              ) : null}
            </div>

            {serverError ? (
              <div
                className="rounded-md border border-destructive/40 bg-destructive/10 px-3 py-2 text-sm text-destructive"
                role="alert"
              >
                {serverError}
              </div>
            ) : null}

            <Button
              type="submit"
              className="w-full"
              disabled={isSubmitting}
              data-testid="login-submit"
            >
              {isSubmitting ? "Signing in…" : "Sign in"}
            </Button>
          </form>
        </CardContent>
      </Card>
    </main>
  );
}
