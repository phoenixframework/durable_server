defmodule DurableServer.StoredState do
  alias DurableServer.Meta

  defstruct key: nil,
            prefix: nil,
            state: nil,
            meta: nil,
            vsn: nil,
            etag: nil

  def to_storage_term(%__MODULE__{vsn: vsn, state: state, meta: %Meta{} = meta}) do
    %{
      vsn: vsn,
      state: state,
      meta: Meta.to_storage_term(meta)
    }
  end

  def to_object_store_term(%__MODULE__{vsn: vsn, state: state, meta: %Meta{} = meta}) do
    %{
      "vsn" => vsn,
      "state" => state,
      "meta" => Meta.encode_to_binary(meta)
    }
  end

  def from_storage_term(%__MODULE__{} = stored_state) do
    with {:ok, meta} <- normalize_meta(stored_state.meta) do
      {:ok,
       %__MODULE__{
         vsn: stored_state.vsn,
         state: stored_state.state,
         meta: meta
       }}
    end
  end

  def from_storage_term(%{vsn: vsn, state: state, meta: meta_term} = term)
      when map_size(term) == 3 do
    with {:ok, meta} <- normalize_meta(meta_term) do
      {:ok,
       %__MODULE__{
         vsn: vsn,
         state: state,
         meta: meta
       }}
    end
  end

  def from_storage_term(_), do: :not_stored_state

  def from_object_store_term(%{"vsn" => vsn, "state" => state, "meta" => meta_binary} = term)
      when map_size(term) == 3 and is_binary(meta_binary) do
    {:ok,
     %__MODULE__{
       vsn: vsn,
       state: state,
       meta: Meta.decode_from_binary(meta_binary, %{key: nil, prefix: nil})
     }}
  rescue
    error in [ArgumentError, RuntimeError] -> {:error, error}
  end

  def from_object_store_term(_), do: :not_stored_state

  defp normalize_meta(%Meta{} = meta) do
    {:ok, %{meta | key: nil, prefix: nil}}
  end

  defp normalize_meta(meta_term) when is_map(meta_term) do
    {:ok, Meta.from_storage_term(meta_term, %{key: nil, prefix: nil})}
  rescue
    error in [ArgumentError] -> {:error, error}
  end

  defp normalize_meta(other) do
    {:error, ArgumentError.exception("invalid stored meta term: #{inspect(other)}")}
  end
end
