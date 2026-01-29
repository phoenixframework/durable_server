defmodule DurableServer.PubSub do
  @moduledoc """
  Pubsub system for DurableServer registry events.

  This module provides a pubsub interface for DurableServer lifecycle events,
  wrapping syn's event handler callbacks. It also allows non-DurableServer processes
  to join/leave keys to participate in the pubsub system and be discoverable
  cluster-wide.

  ## Core Concepts

  ### Subscriptions vs Memberships

  - **Subscriptions** (`subscribe/2`, `unsubscribe/2`): Receive events in your mailbox
    when DurableServers or other processes register/join matching keys anywhere in the
    cluster. Supports pattern matching on keys.

  - **Memberships** (`join/3`, `leave/2`): Make your process discoverable cluster-wide
    via `members/2`. Triggers `:joined`/`:left` events to subscribers.

  These are independent - joining a key does NOT automatically subscribe you to events,
  and subscribing does NOT make you discoverable via `members/2`.

  ## Event Types

  Events are delivered as messages to subscribed processes:

      {:durable_server, event_type, payload}

  | Event Type      | Trigger                                        | Extra Fields        |
  |-----------------|------------------------------------------------|---------------------|
  | `:registered`   | DurableServer started                          | -                   |
  | `:unregistered` | DurableServer stopped                          | `:reason`           |
  | `:updated`      | Metadata changed (DurableServer or member)     | `:previous_meta`    |
  | `:joined`       | Process joined via `join/3`                    | -                   |
  | `:left`         | Process left via `leave/2` or died             | `:reason`           |

  All payloads include: `:supervisor`, `:key`, `:pid`, `:meta`

  ## Pattern Types

  Subscriptions support three pattern types:

  - `"user/123"` - Exact match, only events for this specific key
  - `"user/"` - Prefix match, all keys starting with "user/"
  - `:all` - All events for this supervisor

  ## Self-Events

  A process that subscribes to a pattern and then joins a matching key will receive
  its own `:joined` event. Similarly for `:left` when leaving. Filter these in your
  handler if needed:

      def handle_info({:durable_server, :joined, %{pid: pid}}, state) when pid == self() do
        # Ignore our own join event
        {:noreply, state}
      end

  ## Examples

  ### Basic Subscription

      # Subscribe to all events for a specific key
      :ok = DurableServer.PubSub.subscribe(MySup, "user/123")

      # Subscribe to all keys under a prefix
      :ok = DurableServer.PubSub.subscribe(MySup, "chat/")

      # Subscribe to all events
      :ok = DurableServer.PubSub.subscribe(MySup, :all)

      # Handle events in a GenServer
      def handle_info({:durable_server, :registered, %{key: key, pid: pid}}, state) do
        IO.puts("DurableServer started: \#{key}")
        {:noreply, state}
      end

      def handle_info({:durable_server, :unregistered, %{key: key, reason: reason}}, state) do
        IO.puts("DurableServer stopped: \#{key}, reason: \#{inspect(reason)}")
        {:noreply, state}
      end

  ### Joining as a Member

      # Join a key to be discoverable by other processes
      :ok = DurableServer.PubSub.join(MySup, "game/room/42", %{role: :spectator})

      # Query all members of a key (DurableServers + joined processes)
      members = DurableServer.PubSub.members(MySup, "game/room/42")
      # => [{#PID<0.150.0>, %{module: GameRoom, ...}}, {#PID<0.200.0>, %{role: :spectator}}]

      # Leave when done
      :ok = DurableServer.PubSub.leave(MySup, "game/room/42")

  ## Usage Example

  ### Coordinating Phoenix Channels with DurableServers

  A common pattern is using Phoenix Channels to handle WebSocket connections while
  a DurableServer manages the persistent room state. This example shows:

  - Channel ensuring the DurableServer is started on join
  - Channel reacting to DurableServer going down (crash/shutdown)
  - Channel reacting to DurableServer coming back up
  - Bidirectional messaging between Channel and DurableServer

      # The DurableServer managing room state
      defmodule MyApp.ChatRoom do
        use DurableServer, vsn: 1

        def init(%{key: key} = state) do
          {:ok, Map.put(state, :messages, []), meta: %{room_id: key}}
        end

        def handle_call({:send_message, user, text}, _from, state) do
          message = %{user: user, text: text, at: DateTime.utc_now()}
          new_state = Map.update!(state, :messages, &[message | &1])

          # Broadcast to all members (channels + any other joined processes).
          DurableServer.PubSub.broadcast(MyApp.DurableSup, state.key, {:new_message, message})

          {:reply, :ok, new_state, :sync}
        end

        def handle_call(:get_messages, _from, state) do
          {:reply, Enum.reverse(state.messages), state}
        end
      end

      # The Phoenix Channel handling WebSocket connections
      defmodule MyAppWeb.RoomChannel do
        use Phoenix.Channel

        @sup MyApp.DurableSup

        def join("room:" <> room_id, _params, socket) do
          # Subscribe to lifecycle events for this room
          :ok = DurableServer.PubSub.subscribe(@sup, room_id)

          # Ensure the DurableServer is running
          {:ok, {room_pid, _}} = DurableServer.Supervisor.ensure_started_child(
            @sup,
            {MyApp.ChatRoom, %{key: room_id}}
          )

          # Join as a member so the DurableServer can broadcast to us
          :ok = DurableServer.PubSub.join(@sup, room_id, %{type: :channel})

          # Get recent messages
          messages = GenServer.call(room_pid, :get_messages)

          socket = socket
            |> assign(:room_id, room_id)
            |> assign(:room_pid, room_pid)

          {:ok, %{messages: messages}, socket}
        end

        # --- Handling messages FROM the DurableServer ---

        def handle_info({:new_message, message}, socket) do
          push(socket, "new_message", message)
          {:noreply, socket}
        end

        # --- Handling DurableServer lifecycle events ---

        # Room server went down - notify client and wait for it to come back
        def handle_info({:durable_server, :unregistered, %{key: key}}, socket)
            when key == socket.assigns.room_id do
          push(socket, "room_unavailable", %{})
          {:noreply, assign(socket, :room_pid, nil)}
        end

        # Room server came back up - reconnect and notify client
        def handle_info({:durable_server, :registered, %{key: key, pid: pid}}, socket)
            when key == socket.assigns.room_id do
          push(socket, "room_available", %{})
          {:noreply, assign(socket, :room_pid, pid)}
        end

        def handle_info({:durable_server, _, _}, socket), do: {:noreply, socket}

        # --- Handling messages FROM the client ---

        def handle_in("send_message", %{"text" => text}, socket) do
          case socket.assigns.room_pid do
            nil ->
              {:reply, {:error, %{reason: "room unavailable"}}, socket}

            pid ->
              :ok = GenServer.call(pid, {:send_message, socket.assigns.user, text})
              {:noreply, socket}
          end
        end
      end

  ## Architecture Notes

  - **Events are cluster-wide**: syn callbacks (`on_process_registered`, `on_process_joined`,
    etc.) fire on ALL nodes in the cluster. This means a subscriber on Node A receives
    events when a DurableServer registers on Node B.

  - **Subscriptions** are stored per-node in an Elixir `Registry`, enabling pattern matching
    and automatic cleanup when subscriber processes die.

  - **Memberships** use syn process groups for cluster-wide distribution and automatic
    cleanup when member processes die.
  """

  @registry DurableServer.PubSub.Registry

  @doc """
  Subscribe to DurableServer registry events matching the given pattern.

  The calling process will receive messages for matching keys:

      {:durable_server, event_type, %{
        supervisor: supervisor_name,
        key: key,
        pid: pid,
        meta: meta,
        ...
      }}

  ## Patterns

  - `"exact/key"` - exact key match
  - `"prefix/"` - all keys starting with "prefix/"
  - `:all` - all keys

  ## Returns

  - `:ok` on success
  - `{:error, reason}` on failure
  """
  def subscribe(supervisor_name, pattern_string)
      when is_atom(supervisor_name) and (is_binary(pattern_string) or pattern_string == :all) do
    pattern = parse_pattern(pattern_string)
    key = {supervisor_name, pattern}

    case Registry.register(@registry, key, nil) do
      {:ok, _} -> :ok
      {:error, {:already_registered, _}} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Unsubscribe from DurableServer registry events for the given pattern.

  ## Returns

  - `:ok` always (unsubscribing from a non-existent subscription is a no-op)
  """
  def unsubscribe(supervisor_name, pattern_string) when is_atom(supervisor_name) do
    pattern = parse_pattern(pattern_string)
    key = {supervisor_name, pattern}
    Registry.unregister(@registry, key)
    :ok
  end

  @doc """
  Join a key as a member.

  The calling process will:
  - Be discoverable via `members/2`
  - Trigger `:joined`/`:left` events to subscribers
  - Be automatically removed when it dies

  Note: Joining does NOT automatically subscribe the process to events.
  Call `subscribe/2` separately if you want to receive events.

  ## Parameters

  - `supervisor_name` - The DurableServer.Supervisor name
  - `key` - The exact key to join (patterns not supported for join)
  - `meta` - Optional metadata map (default: `%{}`)

  ## Returns

  - `:ok` on success
  - `{:error, :already_member}` if already a member (use `update/4` to change metadata)
  - `{:error, reason}` on other failures
  """
  def join(supervisor_name, key, meta \\ %{})
      when is_atom(supervisor_name) and is_binary(key) and is_map(meta) do
    scope = DurableServer.Supervisor.syn_scope(supervisor_name)
    pid = self()

    # Check if already a member - must use update/4 to change metadata
    case get_group_member_meta(scope, key, pid) do
      {:ok, _existing_meta} ->
        {:error, :already_member}

      :error ->
        case :syn.join(scope, key, pid, meta) do
          :ok -> :ok
          {:error, reason} -> {:error, reason}
        end
    end
  end

  @doc """
  Leave a key that was previously joined.

  ## Returns

  - `:ok` on success
  - `{:error, :not_in_group}` if not a member
  """
  def leave(supervisor_name, key) when is_atom(supervisor_name) and is_binary(key) do
    scope = DurableServer.Supervisor.syn_scope(supervisor_name)
    pid = self()

    case :syn.leave(scope, key, pid) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Update metadata for a process's membership in a key.

  This updates the metadata associated with the process in the given key's
  member list and broadcasts an `:updated` event to subscribers.

  Syn's process groups don't natively fire callbacks on metadata updates,
  so this function manually broadcasts the event after updating the metadata
  in syn. The order of operations is:

  1. Fetch the current metadata for the process
  2. Update the metadata in syn (via re-join)
  3. Broadcast `:updated` event to subscribers

  This ensures that `members/2` will return the new metadata before subscribers
  receive the event.

  Note: The `:updated` event is the same type used for DurableServer metadata
  changes. If you need to distinguish between DurableServer and member updates,
  you can check if the pid is registered in the syn registry (DurableServer) or
  only in the process group (member).

  ## Parameters

  - `supervisor_name` - The DurableServer.Supervisor name
  - `key` - The exact key the process has joined
  - `new_meta` - The new metadata map
  - `pid` - The pid to update (defaults to `self()`)

  ## Returns

  - `:ok` on success
  - `{:error, :not_a_member}` if the process is not a member of the key
  - `{:error, reason}` on other failures
  """
  def update(supervisor_name, key, new_meta, pid \\ self())
      when is_atom(supervisor_name) and is_binary(key) and is_map(new_meta) and is_pid(pid) do
    scope = DurableServer.Supervisor.syn_scope(supervisor_name)

    # Get current metadata before updating
    case get_group_member_meta(scope, key, pid) do
      {:ok, old_meta} ->
        # Update metadata in syn (re-join updates in place, no callback fired)
        case :syn.join(scope, key, pid, new_meta) do
          :ok ->
            # Broadcast the update event
            __broadcast__(supervisor_name, :updated, key, pid, new_meta, %{
              previous_meta: old_meta
            })

            :ok

          {:error, reason} ->
            {:error, reason}
        end

      :error ->
        {:error, :not_a_member}
    end
  end

  # Get metadata for a specific pid in a process group
  defp get_group_member_meta(scope, key, pid) do
    try do
      case Enum.find(:syn.members(scope, key), fn {p, _meta} -> p == pid end) do
        {^pid, meta} -> {:ok, meta}
        nil -> :error
      end
    rescue
      # syn raises ArgumentError if the group doesn't exist
      ArgumentError -> :error
    end
  end

  @doc """
  List all members (DurableServers + joined pids) for a specific key.

  Returns a list of `{pid, meta}` tuples for all processes registered or joined
  to the exact key.

  Note: Only exact key lookups are supported. Use `subscribe/2` with patterns
  to receive events for multiple keys.

  ## Returns

  - List of `{pid, meta}` tuples
  """
  def members(supervisor_name, key) when is_atom(supervisor_name) and is_binary(key) do
    scope = DurableServer.Supervisor.syn_scope(supervisor_name)

    # DurableServer from registry (one or none)
    registry_result =
      case :syn.lookup(scope, key) do
        {pid, meta} -> [{pid, extract_user_meta(meta)}]
        :undefined -> []
      end

    # Joined pids from process group (zero or more)
    group_members =
      try do
        :syn.members(scope, key)
      rescue
        # syn raises ArgumentError if the group doesn't exist
        ArgumentError -> []
      end

    registry_result ++ group_members
  end

  @doc """
  Broadcast a message to all members of a key.

  Sends `message` to all processes that have joined the key via `join/3`, as well as
  any DurableServer registered at that key. This is useful for application-level
  messaging between a DurableServer and connected clients (e.g., Phoenix Channels).

  ## Broadcast vs Subscribe

  There are two ways to receive messages in this module:

  - **`subscribe/2`** - Receive *lifecycle events* (`:registered`, `:unregistered`, etc.)
    when DurableServers or processes join/leave keys matching a pattern. These are
    system-generated events.

  - **`broadcast/3`** - Receive *application messages* sent explicitly by your code.
    Only members of the exact key receive the message.

  Use `subscribe` to react to lifecycle changes. Use `broadcast` to send your own
  messages to members.

  ## Filtering by Metadata

  `broadcast/3` sends to all members. If you need to filter by metadata (e.g., only
  send to members with `%{type: :channel}`), use `members/2` directly:

      for {pid, %{type: :channel}} <- DurableServer.PubSub.members(sup, key) do
        send(pid, message)
      end

  ## Returns

  - `:ok` always
  """
  def broadcast(supervisor_name, key, message)
      when is_atom(supervisor_name) and is_binary(key) do
    for {pid, _meta} <- members(supervisor_name, key) do
      send(pid, message)
    end

    :ok
  end

  @doc false
  def __broadcast__(supervisor_name, event_type, key, pid, meta, extra \\ %{}) do
    subscribers = get_subscribers_for_key(supervisor_name, key)

    payload =
      Map.merge(
        %{
          supervisor: supervisor_name,
          key: key,
          pid: pid,
          meta: extract_user_meta(meta)
        },
        extra
      )

    event = {:durable_server, event_type, payload}

    for subscriber_pid <- subscribers do
      send(subscriber_pid, event)
    end

    :ok
  end

  # Parse a pattern into an internal pattern representation
  # - :all matches all keys
  # - "prefix/" matches all keys starting with "prefix/"
  # - "exact/key" matches only that exact key
  defp parse_pattern(:all), do: :all

  defp parse_pattern(pattern) when is_binary(pattern) do
    if String.ends_with?(pattern, "/") do
      {:prefix, pattern}
    else
      {:exact, pattern}
    end
  end

  # Check if a pattern matches a key
  defp matches_pattern?(:all, _key), do: true
  defp matches_pattern?({:exact, pattern}, key), do: pattern == key
  defp matches_pattern?({:prefix, prefix}, key), do: String.starts_with?(key, prefix)

  # Get all local subscriber pids whose patterns match the given key
  defp get_subscribers_for_key(supervisor_name, key) do
    # Get all registrations and filter by pattern match
    # Registry keys are {supervisor_name, pattern}
    @registry
    |> Registry.select([
      {
        {{:"$1", :"$2"}, :"$3", :_},
        [{:==, :"$1", supervisor_name}],
        [{{:"$2", :"$3"}}]
      }
    ])
    |> Enum.filter(fn {pattern, _pid} -> matches_pattern?(pattern, key) end)
    |> Enum.map(fn {_pattern, pid} -> pid end)
    |> Enum.uniq()
  rescue
    # Registry doesn't exist yet during startup
    # (syn callbacks can fire before DurableServer.Application starts the registry)
    ArgumentError -> []
  end

  # Extract user_meta from DurableServer meta, or return the meta as-is for joined pids
  defp extract_user_meta(%{user_meta: user_meta}), do: user_meta
  defp extract_user_meta(meta) when is_map(meta), do: meta
end
