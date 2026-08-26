"use client";

import * as React from "react";
import { useRouter } from "next/navigation";
import { useForm, type Resolver } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { z } from "zod";

import { api } from "@/lib/api";
import { ApiError } from "@/lib/types";
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
 * Datasource-kind enum mirrored from the server.
 *
 * The server validates this list, so the client can either match
 * it exactly (we do) or live with a 400 response when a typo
 * sneaks in. The seven values cover the Phase 1 matrix: PG, MySQL,
 * Oracle, Hive, MongoDB, Doris, Kafka.
 */
const DATASOURCE_KINDS = [
  "postgresql",
  "mysql",
  "oracle",
  "hive",
  "mongodb",
  "doris",
  "kafka",
] as const;

const ENVS = ["dev", "staging", "prod", "test"] as const;

/**
 * Zod schema for the create form.
 *
 * Mirrors ``aidp_datasource.schemas.DatasourceCreateRequest`` and
 * the nested ``ConnectionConfig`` / ``CredentialsPayload``
 * models. The schema is intentionally permissive on the
 * connection / credentials sub-trees (free-form ``dict``) because
 * each connector validates its own knobs server-side.
 */
const createSchema = z.object({
  name: z
    .string()
    .min(1, "Name is required")
    .max(128, "Name must be 128 characters or fewer"),
  kind: z.enum(DATASOURCE_KINDS, {
    errorMap: () => ({ message: "Pick a datasource kind" }),
  }),
  env: z.enum(ENVS, {
    errorMap: () => ({ message: "Pick an environment" }),
  }),
  description: z
    .string()
    .max(512, "Description must be 512 characters or fewer")
    .optional()
    .or(z.literal("")),
  host: z.string().min(1, "Host is required").max(255),
  port: z.coerce
    .number()
    .int("Port must be an integer")
    .min(1, "Port must be ≥ 1")
    .max(65535, "Port must be ≤ 65535"),
  database: z.string().max(128).optional().or(z.literal("")),
  username: z.string().min(1, "Username is required").max(128),
  password: z.string().min(1, "Password is required").max(512),
  tags: z.string().optional().or(z.literal("")),
});

type CreateInput = z.infer<typeof createSchema>;

export default function NewDatasourcePage() {
  const router = useRouter();
  const [serverError, setServerError] = React.useState<string | null>(null);

  const {
    register,
    handleSubmit,
    formState: { errors, isSubmitting },
    watch,
  } = useForm<CreateInput>({
    resolver: zodResolver(createSchema) as Resolver<CreateInput>,
    defaultValues: {
      name: "",
      kind: "postgresql",
      env: "prod",
      description: "",
      host: "",
      port: 5432,
      database: "",
      username: "",
      password: "",
      tags: "",
    },
  });

  // Watching the ``kind`` lets the help text under the host field
  // stay kind-specific without a full state library. Re-renders are
  // cheap — only this component re-renders on a kind change.
  const kind = watch("kind");

  const onSubmit = handleSubmit(async (values) => {
    setServerError(null);
    const tags = (values.tags ?? "")
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);

    const body = {
      name: values.name,
      kind: values.kind,
      env: values.env,
      description: values.description ?? "",
      connection: {
        host: values.host,
        port: values.port,
        database: values.database ?? "",
        options: {},
      },
      credentials: {
        username: values.username,
        password: values.password,
      },
      tags,
      enabled: true,
    };

    try {
      await api.post("/datasources", body);
      router.replace("/datasources");
    } catch (err) {
      if (err instanceof ApiError) {
        setServerError(
          err.code === "CONFLICT"
            ? "A datasource with this name already exists."
            : `${err.code}: ${err.message}`,
        );
        return;
      }
      setServerError(
        err instanceof Error ? err.message : "Unexpected error.",
      );
    }
  });

  return (
    <div className="mx-auto max-w-2xl space-y-6">
      <div>
        <h1 className="text-3xl font-bold tracking-tight">New datasource</h1>
        <p className="text-sm text-muted-foreground">
          Register a new connection. Credentials are encrypted at rest and
          never echoed back.
        </p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Connection details</CardTitle>
          <CardDescription>
            The fields below match the server&apos;s
            {" "}<code className="font-mono text-xs">DatasourceCreateRequest</code>{" "}
            model.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <form onSubmit={onSubmit} className="space-y-4" noValidate>
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-1.5">
                <Label htmlFor="name">Name</Label>
                <Input
                  id="name"
                  type="text"
                  placeholder="warehouse-pg-1"
                  {...register("name")}
                />
                {errors.name ? (
                  <p className="text-xs text-destructive" role="alert">
                    {errors.name.message}
                  </p>
                ) : null}
              </div>

              <div className="space-y-1.5">
                <Label htmlFor="kind">Kind</Label>
                <select
                  id="kind"
                  className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
                  {...register("kind")}
                >
                  {DATASOURCE_KINDS.map((k) => (
                    <option key={k} value={k}>
                      {k}
                    </option>
                  ))}
                </select>
                {errors.kind ? (
                  <p className="text-xs text-destructive" role="alert">
                    {errors.kind.message}
                  </p>
                ) : null}
              </div>

              <div className="space-y-1.5">
                <Label htmlFor="env">Environment</Label>
                <select
                  id="env"
                  className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
                  {...register("env")}
                >
                  {ENVS.map((e) => (
                    <option key={e} value={e}>
                      {e}
                    </option>
                  ))}
                </select>
                {errors.env ? (
                  <p className="text-xs text-destructive" role="alert">
                    {errors.env.message}
                  </p>
                ) : null}
              </div>

              <div className="space-y-1.5">
                <Label htmlFor="tags">Tags (comma separated)</Label>
                <Input
                  id="tags"
                  type="text"
                  placeholder="prod, finance"
                  {...register("tags")}
                />
              </div>
            </div>

            <div className="space-y-1.5">
              <Label htmlFor="description">Description</Label>
              <Input
                id="description"
                type="text"
                placeholder="Primary finance warehouse"
                {...register("description")}
              />
            </div>

            <div className="grid grid-cols-3 gap-4">
              <div className="col-span-2 space-y-1.5">
                <Label htmlFor="host">Host</Label>
                <Input
                  id="host"
                  type="text"
                  placeholder="db.example.com"
                  {...register("host")}
                />
                {errors.host ? (
                  <p className="text-xs text-destructive" role="alert">
                    {errors.host.message}
                  </p>
                ) : null}
                <p className="text-xs text-muted-foreground">
                  {hintForKind(kind)}
                </p>
              </div>
              <div className="space-y-1.5">
                <Label htmlFor="port">Port</Label>
                <Input
                  id="port"
                  type="number"
                  {...register("port")}
                />
                {errors.port ? (
                  <p className="text-xs text-destructive" role="alert">
                    {errors.port.message}
                  </p>
                ) : null}
              </div>
            </div>

            <div className="space-y-1.5">
              <Label htmlFor="database">Database</Label>
              <Input
                id="database"
                type="text"
                placeholder={
                  kind === "kafka" || kind === "mongodb"
                    ? "optional"
                    : "finance"
                }
                {...register("database")}
              />
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-1.5">
                <Label htmlFor="username">Username</Label>
                <Input
                  id="username"
                  type="text"
                  autoComplete="off"
                  {...register("username")}
                />
                {errors.username ? (
                  <p className="text-xs text-destructive" role="alert">
                    {errors.username.message}
                  </p>
                ) : null}
              </div>
              <div className="space-y-1.5">
                <Label htmlFor="password">Password</Label>
                <Input
                  id="password"
                  type="password"
                  autoComplete="new-password"
                  {...register("password")}
                />
                {errors.password ? (
                  <p className="text-xs text-destructive" role="alert">
                    {errors.password.message}
                  </p>
                ) : null}
              </div>
            </div>

            {serverError ? (
              <div
                className="rounded-md border border-destructive/40 bg-destructive/10 px-3 py-2 text-sm text-destructive"
                role="alert"
              >
                {serverError}
              </div>
            ) : null}

            <div className="flex justify-end gap-2">
              <Button
                type="button"
                variant="ghost"
                onClick={() => router.replace("/datasources")}
                disabled={isSubmitting}
              >
                Cancel
              </Button>
              <Button type="submit" disabled={isSubmitting}>
                {isSubmitting ? "Creating…" : "Create datasource"}
              </Button>
            </div>
          </form>
        </CardContent>
      </Card>
    </div>
  );
}

/**
 * One-liner of the typical setup for a given connector. Kept in a
 * helper so the form's host-help text doesn't pile up next to the
 * field declarations.
 */
function hintForKind(kind: CreateInput["kind"]): string {
  switch (kind) {
    case "postgresql":
      return "Postgres host. Default port 5432.";
    case "mysql":
      return "MySQL host. Default port 3306.";
    case "oracle":
      return "Oracle host (use service_name in the database field). Default port 1521.";
    case "hive":
      return "HiveServer2 host. Default port 10000.";
    case "mongodb":
      return "MongoDB host. Default port 27017. Database is optional.";
    case "doris":
      return "Doris FE host. Default port 9030.";
    case "kafka":
      return "Kafka bootstrap host. Default port 9092. Database is unused.";
  }
}
