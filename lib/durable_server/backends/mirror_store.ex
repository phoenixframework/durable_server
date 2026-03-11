defmodule DurableServer.Backends.MirrorStore do
  @moduledoc """
  Dual-backend adapter for mirrored writes, fallback reads, and phased cutovers.

  This backend fronts two concrete backends:

  - `:primary`   - usually the current production backend
  - `:secondary` - usually the destination backend

  and lets reads/writes be directed independently while optionally mirroring and
  promoting data for an online cutover.

  ## Core Model

  Runtime behavior is driven by four switches:

  - `:read_preference` (`:primary | :secondary`) - where normal reads come from
  - `:write_target` (`:primary | :secondary`) - where authoritative writes go first
  - `:fallback_reads` (`boolean`) - if a read misses on preferred backend, try the other
  - `:mirror_writes` (`boolean`) - replicate writes/deletes to the non-authoritative backend

  Additional safety knobs:

  - `:promote_on_fallback` (`boolean`) - copy fallback read result into read-preferred backend
  - `:mirror_mode` (`:best_effort | :required`) - mirror failures ignored vs surfaced
  - `:secondary_required` (`boolean`) - whether `ensure_ready/1` must verify secondary

  ## Read Flow

  Reads always start from `read_preference`.

  ```text
  get(key)
    |
    v
  read_preference backend
    |-- hit -------------------------------> return object
    |
    |-- not_found & fallback_reads=true
           |
           v
        other backend
           |-- miss -----------------------> return miss/error
           |
           |-- hit
                 |
                 |-- promote_on_fallback=false -> return fallback object as-is
                 |
                 `-- promote_on_fallback=true
                        |
                        v
                     put into read_preference
                        |-- success/conflict-resolved -> return read_preference object
                        `-- transient failure          -> return {:error, {:promotion_failed, reason}}
  ```

  ### Why promotion matters

  ETags/CAS tokens are backend-local. Returning a token from fallback backend and
  then writing against read-preferred backend can conflict even when data matches.
  Promotion-on-fallback avoids that mismatch by returning tokens from the active
  read backend after promotion. If promotion fails, this backend now returns an
  error instead of the fallback object so callers do not accidentally CAS against
  the wrong backend token.

  ## Write Flow

  Writes (`put_object`, `try_claim`, `update_object`) and deletes start at
  `write_target`.

  ```text
  write/delete(key)
    |
    v
  write_target backend (authoritative for that operation)
    |-- failure ---------------------------> return error
    |
    `-- success
          |
          |-- mirror_writes=false ---------> return success
          |
          `-- mirror_writes=true
                 |
                 v
              other backend (mirror)
                 |-- success/not_found(delete) -> return success
                 |
                 `-- error
                       |-- mirror_mode=:best_effort -> return success
                       `-- mirror_mode=:required    -> return {:error, {:mirror_failed, reason}}
  ```

  Notes:

  - Mirrored `put` drops `:etag` from options intentionally.
    This avoids cross-backend CAS token coupling.
  - For `delete`, a mirror-side `:not_found` is treated as success.

  ## Source Of Truth

  There is no single permanent source of truth baked into this adapter.
  Source of truth is operationally defined by configuration at a point in time:

  - Read truth: `read_preference`
  - Write truth: `write_target`

  During a cutover, truth can intentionally shift phase by phase.
  `mirror_writes` + `promote_on_fallback` are the mechanisms that keep both
  sides converged while shifting those pointers.

  ## Recommended Cutover Sequence

  ```text
  Phase 1: Shadow
    read_preference=:primary, write_target=:primary
    mirror_writes=true, fallback_reads=true, promote_on_fallback=true

  Phase 2: Backfill
    copy historical objects into the secondary backend

  Phase 3: Combined Cutover
    read_preference=:secondary, write_target=:secondary
    keep mirror_writes=true for rollback safety, then disable

  Phase 4: Finalize
    switch to single-backend config (e.g. pure EKV)
  ```

  A read-only cutover (`read_preference=:secondary`, `write_target=:primary`)
  is not safe for existing DurableServer restarts because etags/vsns are
  backend-local. Existing-object restart reads the stored body and etag/vsn from
  `read_preference`, but lock acquisition and follow-up CAS writes go to
  `write_target`.

  ## API Semantics

  - `list_all_objects_stream/3` reads only from `read_preference`.
    It does not merge both backends.
  - `ensure_ready/1` always checks primary. Secondary readiness check is optional
    (`secondary_required: true`).
  """

  @behaviour DurableServer.StorageBackend

  alias DurableServer.StorageBackend

  @valid_state_opts [
    :primary,
    :secondary,
    :read_preference,
    :write_target,
    :fallback_reads,
    :promote_on_fallback,
    :mirror_writes,
    :mirror_mode,
    :secondary_required
  ]

  @mirror_modes [:best_effort, :required]
  @preference [:primary, :secondary]

  @type state :: %{
          required(:primary) => StorageBackend.t(),
          required(:secondary) => StorageBackend.t(),
          required(:read_preference) => :primary | :secondary,
          required(:write_target) => :primary | :secondary,
          required(:fallback_reads) => boolean(),
          required(:promote_on_fallback) => boolean(),
          required(:mirror_writes) => boolean(),
          required(:mirror_mode) => :best_effort | :required,
          required(:secondary_required) => boolean()
        }

  def normalize_opts(opts) when is_list(opts) do
    opts = Keyword.validate!(opts, @valid_state_opts)
    primary = Keyword.fetch!(opts, :primary)
    secondary = Keyword.fetch!(opts, :secondary)
    read_preference = Keyword.get(opts, :read_preference, :primary)
    write_target = Keyword.get(opts, :write_target, read_preference)
    mirror_mode = Keyword.get(opts, :mirror_mode, :best_effort)

    unless read_preference in @preference do
      raise ArgumentError, "mirror backend :read_preference must be :primary or :secondary"
    end

    unless write_target in @preference do
      raise ArgumentError, "mirror backend :write_target must be :primary or :secondary"
    end

    unless mirror_mode in @mirror_modes do
      raise ArgumentError,
            "mirror backend :mirror_mode must be :best_effort or :required"
    end

    %{
      primary: primary,
      secondary: secondary,
      read_preference: read_preference,
      write_target: write_target,
      fallback_reads: Keyword.get(opts, :fallback_reads, true),
      promote_on_fallback: Keyword.get(opts, :promote_on_fallback, true),
      mirror_writes: Keyword.get(opts, :mirror_writes, true),
      mirror_mode: mirror_mode,
      secondary_required: Keyword.get(opts, :secondary_required, false)
    }
  end

  @impl true
  def init_backend(opts) when is_map(opts), do: opts |> Map.to_list() |> init_backend()

  def init_backend(opts) when is_list(opts) do
    state = normalize_opts(opts)
    read_backend = backend(state, state.read_preference)

    {:ok,
     %{
       state: state,
       defaults: StorageBackend.defaults(read_backend),
       features: StorageBackend.features(read_backend)
     }}
  end

  @impl true
  def ensure_ready(%{} = state) do
    with :ok <- StorageBackend.ensure_ready(state.primary),
         :ok <- maybe_ensure_secondary(state) do
      :ok
    end
  end

  @impl true
  def get_object(%{} = state, key, opts) do
    read_backend = backend(state, state.read_preference)
    fallback_backend = backend(state, opposite(state.read_preference))

    case StorageBackend.get_object(read_backend, key, opts) do
      {:ok, obj} ->
        {:ok, obj}

      {:error, :not_found} when state.fallback_reads ->
        case StorageBackend.get_object(fallback_backend, key, opts) do
          {:ok, %{body: body} = obj} ->
            if state.promote_on_fallback do
              promote_fallback_object(read_backend, key, body, opts)
            else
              {:ok, obj}
            end

          other ->
            other
        end

      other ->
        other
    end
  end

  @impl true
  def list_all_objects_stream(%{} = state, prefix, opts) do
    StorageBackend.list_all_objects_stream(backend(state, state.read_preference), prefix, opts)
  end

  @impl true
  def put_object(%{} = state, key, data, opts) do
    write_backend = backend(state, state.write_target)
    mirror_backend = backend(state, opposite(state.write_target))

    case StorageBackend.put_object(write_backend, key, data, opts) do
      {:ok, %{} = result} ->
        case maybe_mirror_put(state, mirror_backend, key, data, opts) do
          :ok -> {:ok, result}
          {:error, reason} -> {:error, reason}
        end

      other ->
        other
    end
  end

  @impl true
  def delete_object(%{} = state, key) do
    write_backend = backend(state, state.write_target)
    mirror_backend = backend(state, opposite(state.write_target))

    case StorageBackend.delete_object(write_backend, key) do
      :ok ->
        case maybe_mirror_delete(state, mirror_backend, key) do
          :ok -> :ok
          {:error, reason} -> {:error, reason}
        end

      {:error, :not_found} = not_found ->
        if state.mirror_writes do
          _ = StorageBackend.delete_object(mirror_backend, key)
        end

        not_found

      other ->
        other
    end
  end

  @impl true
  def try_claim(%{} = state, key, body) do
    write_backend = backend(state, state.write_target)
    mirror_backend = backend(state, opposite(state.write_target))

    case StorageBackend.try_claim(write_backend, key, body) do
      {:ok, {:claimed, _etag} = claimed} ->
        case maybe_mirror_put(state, mirror_backend, key, body, []) do
          :ok -> {:ok, claimed}
          {:error, reason} -> {:error, reason}
        end

      other ->
        other
    end
  end

  @impl true
  def update_object(%{} = state, key, update_fn, opts) do
    write_backend = backend(state, state.write_target)
    mirror_backend = backend(state, opposite(state.write_target))

    case StorageBackend.update_object(write_backend, key, update_fn, opts) do
      {:ok, %{body: body} = obj} ->
        case maybe_mirror_put(state, mirror_backend, key, body, []) do
          :ok -> {:ok, obj}
          {:error, reason} -> {:error, reason}
        end

      other ->
        other
    end
  end

  @impl true
  def encode(%{} = state, data),
    do: StorageBackend.encode(backend(state, state.read_preference), data)

  @impl true
  def decode(%{} = state, data),
    do: StorageBackend.decode(backend(state, state.read_preference), data)

  @impl true
  def subscribe(%{} = state, subscriber, prefix, opts)
      when is_pid(subscriber) and is_binary(prefix) and is_list(opts) do
    read_preference = state.read_preference

    case StorageBackend.subscribe(backend(state, read_preference), subscriber, prefix, opts) do
      {:ok, subscription_ref} ->
        {:ok, {read_preference, subscription_ref}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def unsubscribe(%{} = state, {side, subscription_ref}) when side in @preference do
    StorageBackend.unsubscribe(backend(state, side), subscription_ref)
  end

  def unsubscribe(%{} = _state, _subscription_ref), do: :ok

  defp promote_fallback_object(read_backend, key, body, read_opts) do
    # Promote to the read backend so returned etag matches subsequent CAS writes.
    case StorageBackend.put_object(read_backend, key, body, max_retries: 3) do
      {:ok, promoted_obj} ->
        {:ok, promoted_obj}

      {:error, :conflict} ->
        StorageBackend.get_object(read_backend, key, read_opts)

      {:error, reason} ->
        {:error, {:promotion_failed, reason}}
    end
  end

  defp maybe_ensure_secondary(%{secondary_required: true, secondary: secondary}) do
    StorageBackend.ensure_ready(secondary)
  end

  defp maybe_ensure_secondary(_state), do: :ok

  defp maybe_mirror_put(%{mirror_writes: false}, _mirror_backend, _key, _data, _opts), do: :ok

  defp maybe_mirror_put(%{} = state, mirror_backend, key, data, opts) do
    mirror_opts = Keyword.drop(opts, [:etag])

    case StorageBackend.put_object(mirror_backend, key, data, mirror_opts) do
      {:ok, _} ->
        :ok

      {:error, reason} ->
        case state.mirror_mode do
          :best_effort -> :ok
          :required -> {:error, {:mirror_failed, reason}}
        end
    end
  end

  defp maybe_mirror_delete(%{mirror_writes: false}, _mirror_backend, _key), do: :ok

  defp maybe_mirror_delete(%{} = state, mirror_backend, key) do
    case StorageBackend.delete_object(mirror_backend, key) do
      :ok ->
        :ok

      {:error, :not_found} ->
        :ok

      {:error, reason} ->
        case state.mirror_mode do
          :best_effort -> :ok
          :required -> {:error, {:mirror_failed, reason}}
        end
    end
  end

  defp backend(%{primary: backend}, :primary), do: backend
  defp backend(%{secondary: backend}, :secondary), do: backend

  defp opposite(:primary), do: :secondary
  defp opposite(:secondary), do: :primary
end
