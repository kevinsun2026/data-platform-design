"use client";

import * as React from "react";
import Link from "next/link";
import { useQuery } from "@tanstack/react-query";
import { Plus } from "lucide-react";

import { api } from "@/lib/api";
import { ApiError } from "@/lib/types";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

/**
 * Datasource row — projection of the
 * ``aidp_datasource.schemas.DatasourceResponse`` model.
 *
 * Only the fields the list page renders are typed here; the
 * server is the source of truth, so anything extra is ignored
 * (axios doesn't enforce the type for extra fields).
 */
interface DatasourceRow {
  id: string;
  name: string;
  kind:
    | "postgresql"
    | "mysql"
    | "oracle"
    | "hive"
    | "mongodb"
    | "doris"
    | "kafka";
  env: string;
  description: string;
  tags: string[];
  enabled: boolean;
  created_at: string;
  updated_at: string;
}

interface DatasourceListResponse {
  items: DatasourceRow[];
}

export default function DatasourcesPage() {
  const { data, isLoading, error, refetch } = useQuery<
    DatasourceListResponse,
    ApiError
  >({
    queryKey: ["datasources"],
    queryFn: async () => {
      const resp = await api.get<DatasourceListResponse>("/datasources");
      return resp.data;
    },
  });

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Datasources</h1>
          <p className="text-sm text-muted-foreground">
            Connectors to the databases, warehouses, queues, and APIs
            the AIDP platform reads from.
          </p>
        </div>
        <Button asChild>
          <Link href="/datasources/new">
            <Plus className="mr-1.5 h-4 w-4" />
            New datasource
          </Link>
        </Button>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>All datasources</CardTitle>
          <CardDescription>
            One row per (tenant, name) pair. Click a name to view the
            connection details.
          </CardDescription>
        </CardHeader>
        <CardContent>
          {isLoading ? (
            <p className="text-sm text-muted-foreground">Loading…</p>
          ) : error ? (
            <div
              className="rounded-md border border-destructive/40 bg-destructive/10 px-3 py-2 text-sm text-destructive"
              role="alert"
            >
              <p>Failed to load datasources: {error.message}</p>
              {error.traceId ? (
                <p className="mt-1 font-mono text-xs">
                  trace_id: {error.traceId}
                </p>
              ) : null}
              <Button
                type="button"
                size="sm"
                variant="outline"
                className="mt-2"
                onClick={() => refetch()}
              >
                Retry
              </Button>
            </div>
          ) : !data || data.items.length === 0 ? (
            <p className="text-sm text-muted-foreground">
              No datasources yet.{" "}
              <Link
                href="/datasources/new"
                className="font-medium text-primary hover:underline"
              >
                Create your first one
              </Link>
              .
            </p>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Name</TableHead>
                  <TableHead>Kind</TableHead>
                  <TableHead>Env</TableHead>
                  <TableHead>Tags</TableHead>
                  <TableHead>Enabled</TableHead>
                  <TableHead>Updated</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {data.items.map((row) => (
                  <TableRow key={row.id}>
                    <TableCell className="font-medium">
                      <Link
                        href={`/datasources/${row.id}`}
                        className="hover:underline"
                      >
                        {row.name}
                      </Link>
                    </TableCell>
                    <TableCell className="font-mono text-xs">
                      {row.kind}
                    </TableCell>
                    <TableCell>{row.env}</TableCell>
                    <TableCell>
                      {row.tags.length > 0
                        ? row.tags.join(", ")
                        : "—"}
                    </TableCell>
                    <TableCell>{row.enabled ? "yes" : "no"}</TableCell>
                    <TableCell className="text-muted-foreground">
                      {new Date(row.updated_at).toLocaleString()}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
