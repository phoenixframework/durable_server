defmodule DurableServer.StorageBackend do
  @moduledoc false

  @enforce_keys [:adapter, :state, :defaults, :features]
  defstruct [:adapter, :state, :defaults, :features]

  @type t :: %__MODULE__{
          adapter: module(),
          state: term(),
          defaults: map(),
          features: map()
        }

  @type object :: %{
          required(:body) => term(),
          required(:etag) => String.t()
        }

  @type list_object :: %{
          required(:key) => String.t(),
          required(:etag) => String.t(),
          optional(:body) => term(),
          optional(:size) => term(),
          optional(:last_modified) => term()
        }

  @type defaults :: %{
          optional(:heartbeat_tracking_mode) => :poll | :subscribe,
          optional(:discovery_interval_ms) => pos_integer(),
          optional(:heartbeat_interval_ms) => pos_integer(),
          optional(:heartbeat_reconcile_interval_ms) => pos_integer()
        }

  @type features :: %{optional(atom()) => boolean()}

  @type init_result :: %{
          required(:state) => term(),
          optional(:defaults) => defaults(),
          optional(:features) => features()
        }

  @callback init_backend(raw_opts :: term()) :: {:ok, init_result()} | {:error, term()}
  @callback ensure_ready(state :: term()) :: :ok | {:error, term()}
  @callback get_object(state :: term(), key :: String.t(), opts :: keyword()) ::
              {:ok, object()} | {:error, term()}
  @callback list_all_objects_stream(state :: term(), prefix :: String.t(), opts :: keyword()) ::
              Enumerable.t()
  @callback put_object(state :: term(), key :: String.t(), data :: term(), opts :: keyword()) ::
              {:ok, object()} | {:error, term()}
  @callback delete_object(state :: term(), key :: String.t()) ::
              :ok | {:error, term()}
  @callback try_claim(state :: term(), key :: String.t(), body :: term()) ::
              {:ok, {:claimed, String.t()}} | {:error, term()}
  @callback update_object(
              state :: term(),
              key :: String.t(),
              update_fn :: (object() -> {:ok, term()} | {:error, term()}),
              opts :: keyword()
            ) ::
              {:ok, object()} | {:error, term()}
  @callback encode(state :: term(), data :: term()) :: {:ok, term()} | {:error, term()}
  @callback decode(state :: term(), data :: term()) :: {:ok, term()} | {:error, term()}
  @callback subscribe(
              state :: term(),
              subscriber :: pid(),
              prefix :: String.t(),
              opts :: keyword()
            ) ::
              {:ok, term()} | {:error, term()}
  @callback unsubscribe(state :: term(), subscription_ref :: term()) ::
              :ok | {:error, term()}

  @optional_callbacks subscribe: 4, unsubscribe: 2

  @known_default_keys [
    :heartbeat_tracking_mode,
    :discovery_interval_ms,
    :heartbeat_interval_ms,
    :heartbeat_reconcile_interval_ms
  ]

  @spec new(module(), term(), map(), map()) :: t()
  def new(adapter, state, defaults \\ %{}, features \\ %{})
      when is_atom(adapter) and is_map(defaults) and is_map(features) do
    %__MODULE__{adapter: adapter, state: state, defaults: defaults, features: features}
  end

  @spec init_backend(module(), term()) :: {:ok, t()} | {:error, term()}
  def init_backend(adapter, raw_opts) when is_atom(adapter) do
    case adapter.init_backend(raw_opts) do
      {:ok, init_result} ->
        {state, defaults, features} = validate_init_result(init_result)
        {:ok, new(adapter, state, defaults, features)}

      {:error, _reason} = error ->
        error

      other ->
        raise ArgumentError,
              "backend #{inspect(adapter)} init_backend/1 must return {:ok, map} or {:error, reason}, got: #{inspect(other)}"
    end
  end

  @spec ensure_ready(t()) :: :ok | {:error, term()}
  def ensure_ready(%__MODULE__{adapter: adapter, state: state}) do
    adapter.ensure_ready(state)
  end

  @spec defaults(t()) :: defaults()
  def defaults(%__MODULE__{defaults: defaults}), do: defaults

  @spec features(t()) :: features()
  def features(%__MODULE__{features: features}), do: features

  @spec supports?(t(), atom()) :: boolean()
  def supports?(%__MODULE__{features: features}, feature) when is_atom(feature) do
    Map.get(features, feature, false) == true
  end

  defp validate_init_result(%{} = init_result) do
    state = fetch_required(init_result, :state)
    defaults = validate_defaults(Map.get(init_result, :defaults, %{}))
    features = validate_features(Map.get(init_result, :features, %{}))
    {state, defaults, features}
  end

  defp validate_init_result(other) do
    raise ArgumentError, "backend init result must be a map, got: #{inspect(other)}"
  end

  defp fetch_required(map, key) when is_map(map) and is_atom(key) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> raise ArgumentError, "backend init result is missing required key #{inspect(key)}"
    end
  end

  defp validate_defaults(defaults) when is_map(defaults) do
    unknown_keys = Map.keys(defaults) -- @known_default_keys

    if unknown_keys != [] do
      raise ArgumentError, "backend init defaults contain unknown keys: #{inspect(unknown_keys)}"
    else
      validate_heartbeat_tracking_mode(defaults)
      validate_positive_default(defaults, :discovery_interval_ms)
      validate_positive_default(defaults, :heartbeat_interval_ms)
      validate_positive_default(defaults, :heartbeat_reconcile_interval_ms)
      defaults
    end
  end

  defp validate_defaults(other) do
    raise ArgumentError, "backend init defaults must be a map, got: #{inspect(other)}"
  end

  defp validate_heartbeat_tracking_mode(defaults) do
    case Map.get(defaults, :heartbeat_tracking_mode) do
      nil ->
        :ok

      :poll ->
        :ok

      :subscribe ->
        :ok

      other ->
        raise ArgumentError,
              "backend init default :heartbeat_tracking_mode must be :poll or :subscribe, got: #{inspect(other)}"
    end
  end

  defp validate_positive_default(defaults, key) when is_atom(key) do
    case Map.get(defaults, key) do
      nil ->
        :ok

      value when is_integer(value) and value > 0 ->
        :ok

      other ->
        raise ArgumentError,
              "backend init default #{inspect(key)} must be a positive integer, got: #{inspect(other)}"
    end
  end

  defp validate_features(features) when is_map(features) do
    invalid_key =
      Enum.find(Map.keys(features), fn
        key when is_atom(key) -> false
        _ -> true
      end)

    cond do
      invalid_key != nil ->
        raise ArgumentError,
              "backend init features must use atom keys, got: #{inspect(invalid_key)}"

      true ->
        invalid_pair =
          Enum.find(features, fn {_key, value} -> not is_boolean(value) end)

        case invalid_pair do
          nil ->
            features

          {key, value} ->
            raise ArgumentError,
                  "backend init feature #{inspect(key)} must be a boolean, got: #{inspect(value)}"
        end
    end
  end

  defp validate_features(other) do
    raise ArgumentError, "backend init features must be a map, got: #{inspect(other)}"
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

  @spec put_object(t(), String.t(), term(), keyword()) ::
          {:ok, object()} | {:error, term()}
  def put_object(%__MODULE__{adapter: adapter, state: state}, key, data, opts \\ [])
      when is_binary(key) and is_list(opts) do
    adapter.put_object(state, key, data, opts)
  end

  @spec delete_object(t(), String.t()) :: :ok | {:error, term()}
  def delete_object(%__MODULE__{adapter: adapter, state: state}, key) when is_binary(key) do
    adapter.delete_object(state, key)
  end

  @spec try_claim(t(), String.t(), term()) :: {:ok, {:claimed, String.t()}} | {:error, term()}
  def try_claim(%__MODULE__{adapter: adapter, state: state}, key, body) when is_binary(key) do
    adapter.try_claim(state, key, body)
  end

  @spec update_object(
          t(),
          String.t(),
          (object() -> {:ok, term()} | {:error, term()}),
          keyword()
        ) ::
          {:ok, object()} | {:error, term()}
  def update_object(%__MODULE__{adapter: adapter, state: state}, key, update_fn, opts \\ [])
      when is_binary(key) and is_function(update_fn, 1) and is_list(opts) do
    adapter.update_object(state, key, update_fn, opts)
  end

  @spec encode(t(), term()) :: {:ok, term()} | {:error, term()}
  def encode(%__MODULE__{adapter: adapter, state: state}, data) do
    adapter.encode(state, data)
  end

  @spec decode(t(), term()) :: {:ok, term()} | {:error, term()}
  def decode(%__MODULE__{adapter: adapter, state: state}, data) do
    adapter.decode(state, data)
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
