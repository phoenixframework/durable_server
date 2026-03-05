defmodule DurableServer.StorageBackend do
  @moduledoc false

  @enforce_keys [:adapter, :state]
  defstruct [:adapter, :state]

  @type t :: %__MODULE__{
          adapter: module(),
          state: term()
        }

  @type object :: %{
          required(:body) => binary(),
          required(:etag) => String.t()
        }

  @type list_object :: %{
          required(:key) => String.t(),
          required(:etag) => String.t(),
          optional(:size) => term(),
          optional(:last_modified) => term()
        }

  @type capabilities :: %{
          optional(:heartbeat_tracking_mode) => :poll | :subscribe,
          optional(:discovery_interval_ms) => pos_integer(),
          optional(:heartbeat_interval_ms) => pos_integer(),
          optional(:heartbeat_reconcile_interval_ms) => pos_integer()
        }

  @callback ensure_ready(state :: term()) :: :ok | {:error, term()}
  @callback capabilities(state :: term()) :: capabilities()
  @callback get_object(state :: term(), key :: String.t(), opts :: keyword()) ::
              {:ok, object()} | {:error, term()}
  @callback list_all_objects_stream(state :: term(), prefix :: String.t(), opts :: keyword()) ::
              Enumerable.t()
  @callback put_object(state :: term(), key :: String.t(), data :: binary(), opts :: keyword()) ::
              {:ok, object()} | {:error, term()}
  @callback delete_object(state :: term(), key :: String.t()) ::
              :ok | {:error, term()}
  @callback try_claim(state :: term(), key :: String.t(), body :: binary()) ::
              {:ok, {:claimed, String.t()}} | {:error, term()}
  @callback update_object(
              state :: term(),
              key :: String.t(),
              update_fn :: (object() -> {:ok, binary()} | {:error, term()}),
              opts :: keyword()
            ) ::
              {:ok, object()} | {:error, term()}
  @callback subscribe(
              state :: term(),
              subscriber :: pid(),
              prefix :: String.t(),
              opts :: keyword()
            ) ::
              {:ok, term()} | {:error, term()}
  @callback unsubscribe(state :: term(), subscription_ref :: term()) ::
              :ok | {:error, term()}

  @optional_callbacks capabilities: 1, subscribe: 4, unsubscribe: 2

  @spec new(module(), term()) :: t()
  def new(adapter, state) when is_atom(adapter) do
    %__MODULE__{adapter: adapter, state: state}
  end

  @spec ensure_ready(t()) :: :ok | {:error, term()}
  def ensure_ready(%__MODULE__{adapter: adapter, state: state}) do
    adapter.ensure_ready(state)
  end

  @spec capabilities(t()) :: capabilities()
  def capabilities(%__MODULE__{adapter: adapter, state: state}) do
    if function_exported?(adapter, :capabilities, 1) do
      adapter.capabilities(state)
    else
      %{heartbeat_tracking_mode: :poll}
    end
  end

  @spec get_object(t(), String.t(), keyword()) :: {:ok, object()} | {:error, term()}
  def get_object(%__MODULE__{adapter: adapter, state: state}, key, opts \\ [])
      when is_binary(key) and is_list(opts) do
    adapter.get_object(state, key, opts)
  end

  @spec list_all_objects_stream(t(), String.t(), keyword()) :: Enumerable.t()
  def list_all_objects_stream(%__MODULE__{adapter: adapter, state: state}, prefix, opts \\ [])
      when is_binary(prefix) and is_list(opts) do
    adapter.list_all_objects_stream(state, prefix, opts)
  end

  @spec put_object(t(), String.t(), binary(), keyword()) ::
          {:ok, object()} | {:error, term()}
  def put_object(%__MODULE__{adapter: adapter, state: state}, key, data, opts \\ [])
      when is_binary(key) and is_binary(data) and is_list(opts) do
    adapter.put_object(state, key, data, opts)
  end

  @spec delete_object(t(), String.t()) :: :ok | {:error, term()}
  def delete_object(%__MODULE__{adapter: adapter, state: state}, key) when is_binary(key) do
    adapter.delete_object(state, key)
  end

  @spec try_claim(t(), String.t(), binary()) :: {:ok, {:claimed, String.t()}} | {:error, term()}
  def try_claim(%__MODULE__{adapter: adapter, state: state}, key, body)
      when is_binary(key) and is_binary(body) do
    adapter.try_claim(state, key, body)
  end

  @spec update_object(
          t(),
          String.t(),
          (object() -> {:ok, binary()} | {:error, term()}),
          keyword()
        ) ::
          {:ok, object()} | {:error, term()}
  def update_object(%__MODULE__{adapter: adapter, state: state}, key, update_fn, opts \\ [])
      when is_binary(key) and is_function(update_fn, 1) and is_list(opts) do
    adapter.update_object(state, key, update_fn, opts)
  end

  @spec subscribe(t(), pid(), String.t(), keyword()) :: {:ok, term()} | {:error, term()}
  def subscribe(%__MODULE__{adapter: adapter, state: state}, subscriber, prefix, opts \\ [])
      when is_pid(subscriber) and is_binary(prefix) and is_list(opts) do
    if function_exported?(adapter, :subscribe, 4) do
      adapter.subscribe(state, subscriber, prefix, opts)
    else
      {:error, :unsupported}
    end
  end

  @spec unsubscribe(t(), term()) :: :ok | {:error, term()}
  def unsubscribe(%__MODULE__{adapter: adapter, state: state}, subscription_ref) do
    if function_exported?(adapter, :unsubscribe, 2) do
      adapter.unsubscribe(state, subscription_ref)
    else
      :ok
    end
  end
end
