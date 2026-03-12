# Group Changes

## Findings

1. Our pre-diff baseline returned only PG-joined members from `Group.members/2`, not registered DurableServer entries.
   - In the old implementation, `members/2` only called `Data.pg_members(...)` for exact matches and `Data.pg_members_by_prefix(...)` for prefix matches.
   - There was no `Data.registry_lookup(...)` or `Data.registry_lookup_by_prefix(...)` in `members/2`.

2. The commit that changed our vendored Group implementation to `process group entries only` was:
   - Commit: `386eb1805bdcf61878372bc8930b2b52aeefc474`
   - Short SHA: `386eb18`
   - Message: `Support member pattern`
   - Date: February 24, 2026

3. That commit changed `members/2` from:
   - registered + joined members
   to:
   - joined/PG members only

4. The current vendored update changes `members/2` back to returning both:
   - registered processes via `register/5`
   - joined processes via `join/5`

5. The current test failures in `test/group_test.exs` are therefore test expectation mismatches on our side, not a Group bug. The tests still assume the `386eb18` semantics.

## Full Diff Output

```diff
diff --git a/priv/group/README.md b/priv/group/README.md
index 9fcc3b8..896d3b3 100644
--- a/priv/group/README.md
+++ b/priv/group/README.md
@@ -61,6 +61,12 @@ members = Group.members(:my_app, "chat/room/42")
 ```
 
 `members/2` returns both registered processes and joined processes for a key.
+Keys ending with `"/"` perform a prefix query across all shards:
+
+```elixir
+# All members in rooms under "chat/"
+Group.members(:my_app, "chat/")
+```
 
 ### Monitoring
 
 diff --git a/priv/group/lib/group.ex b/priv/group/lib/group.ex
 index bd21125..2cd28cd 100644
--- a/priv/group/lib/group.ex
+++ b/priv/group/lib/group.ex
@@ -132,9 +132,9 @@ defmodule Group do
       # Join a key to be discoverable by other processes
       :ok = Group.join(MySup, "game/room/42", %{role: :spectator})
 
-      # Query all members of a key (joined processes only)
+      # Query all members of a key (DurableServers + joined processes)
       members = Group.members(MySup, "game/room/42")
-      # => [{#PID<0.200.0>, %{role: :spectator}}]
+      # => [{#PID<0.150.0>, %{module: GameRoom, ...}}, {#PID<0.200.0>, %{role: :spectator}}]
 
       # Leave when done
       :ok = Group.leave(MySup, "game/room/42")
@@ -358,6 +358,7 @@ defmodule Group do
 
   def register(name, key, meta, opts)
       when is_atom(name) and is_binary(key) and is_map(meta) and is_list(opts) do
+    validate_key!(key)
     cluster = Keyword.get(opts, :cluster)
     validate_cluster_connected!(name, cluster)
     shard = Replica.shard_for(name, cluster, key)
@@ -390,6 +391,7 @@ defmodule Group do
 
   def unregister(name, key, opts)
       when is_atom(name) and is_binary(key) and is_list(opts) do
+    validate_key!(key)
     cluster = Keyword.get(opts, :cluster)
     validate_cluster_connected!(name, cluster)
     shard = Replica.shard_for(name, cluster, key)
@@ -540,6 +542,7 @@ defmodule Group do
   def join(name, group, meta, opts)
       when is_atom(name) and is_binary(group) and is_map(meta) and
              is_list(opts) do
+    validate_key!(group)
     cluster = Keyword.get(opts, :cluster)
     validate_cluster_connected!(name, cluster)
     shard = Replica.shard_for(name, cluster, group)
@@ -569,6 +572,7 @@ defmodule Group do
 
   def leave(name, group, opts)
       when is_atom(name) and is_binary(group) and is_list(opts) do
+    validate_key!(group)
     cluster = Keyword.get(opts, :cluster)
     validate_cluster_connected!(name, cluster)
     shard = Replica.shard_for(name, cluster, group)
@@ -581,19 +585,19 @@ defmodule Group do
   # ===========================================================================
 
   @doc """
-  List all members of a group (process group entries only).
+  List all members of a group.
 
-  Returns processes that have joined via `join/3`. Registry entries
-  (via `register/3`) are not included — use `lookup/3` for those.
+  Returns both registered processes (via `register/5`) and
+  joined processes (via `join/5`).
 
-  Supports prefix matching: if `group` ends with `"/"`, returns all
-  members whose group key starts with that prefix. Prefix queries scan
-  all shards. Exact queries hit a single shard.
+  If `group` ends with `"/"`, performs a prefix query — returns all members
+  whose key starts with the given prefix. This scans all shards and is more
+  expensive than an exact key lookup.
 
   ## Parameters
 
   - `name` - The Group name
-  - `group` - The group to query (string). Trailing `"/"` triggers prefix match.
+  - `group` - The group to query (string). Append `"/"` for prefix matching.
   - `opts` - Keyword list of options
 
   ## Options
@@ -614,18 +618,42 @@ defmodule Group do
     num_shards = get_config(name).num_shards
 
     if String.ends_with?(group, "/") do
-      # Prefix query — scan all shards
-      Enum.flat_map(0..(num_shards - 1), fn shard ->
-        Data.pg_members_by_prefix(name, shard, cluster, group)
-        |> Enum.map(fn {pid, meta} -> {pid, extract_meta_fn.(meta)} end)
-      end)
+      members_by_prefix(name, num_shards, cluster, group, extract_meta_fn)
     else
-      # Exact match — single shard
-      shard = Replica.shard_index_for(cluster, group, num_shards)
+      members_exact(name, num_shards, cluster, group, extract_meta_fn)
+    end
+  end
 
+  defp members_exact(name, num_shards, cluster, group, extract_meta_fn) do
+    shard = Replica.shard_index_for(cluster, group, num_shards)
+
+    # DurableServer from registry (one or none)
+    registry_result =
+      case Data.registry_lookup(name, shard, cluster, group) do
+        {pid, meta, _time, _node} -> [{pid, extract_meta_fn.(meta)}]
+        nil -> []
+      end
+
+    # Joined pids from process group (zero or more)
+    group_members =
       Data.pg_members(name, shard, cluster, group)
       |> Enum.map(fn {pid, meta} -> {pid, extract_meta_fn.(meta)} end)
-    end
+
+    registry_result ++ group_members
+  end
+
+  defp members_by_prefix(name, num_shards, cluster, prefix, extract_meta_fn) do
+    Enum.reduce(0..(num_shards - 1), [], fn shard, acc ->
+      reg =
+        Data.registry_lookup_by_prefix(name, shard, cluster, prefix)
+        |> Enum.map(fn {pid, meta} -> {pid, extract_meta_fn.(meta)} end)
+
+      pg =
+        Data.pg_members_by_prefix(name, shard, cluster, prefix)
+        |> Enum.map(fn {pid, meta} -> {pid, extract_meta_fn.(meta)} end)
+
+      reg ++ pg ++ acc
+    end)
   end
 
   @doc """
@@ -830,6 +858,13 @@ defmodule Group do
   # Internal
   # ===========================================================================
 
+  defp validate_key!(key) do
+    if String.ends_with?(key, "/") do
+      raise ArgumentError,
+            "key #{inspect(key)} must not end with \"/\" — trailing slash is reserved for prefix queries"
+    end
+  end
+
   defp validate_cluster_connected!(_name, nil), do: :ok
 
   defp validate_cluster_connected!(name, cluster) do
```
