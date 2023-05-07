defmodule SimpleSearch do
  @moduledoc """
  Documentation for `SimpleSearch`.
  """
  @stop_words_english File.read!("priv/english.txt") |> String.split()
  @stop_words_german File.read!("priv/german.txt") |> String.split()
  @stop_words @stop_words_english ++ @stop_words_german

  def index_new(idx_name, config) when is_atom(idx_name) and is_map(config) do
    :ets.new(idx_name, [:named_table, :public])

    Enum.each(config, fn {field_name, _field_config} ->
      t = :ets.new(idx_name, [:public, :ordered_set])
      :ets.insert(idx_name, {field_name, t})
    end)
  end

  def index_add_doc(idx_name, doc_id, doc) when is_atom(idx_name) and is_map(doc) do
    :ets.foldl(
      fn {field_name, t}, _acc ->
        Map.get(doc, field_name, "")
        |> split()
        |> reject_stop_words()
        |> Enum.each(fn value ->
          :ets.insert(t, {{value |> String.downcase(), doc_id}})
        end)
      end,
      :ignored_acc,
      idx_name
    )

    :ok
  end

  def index_remove_doc(idx_name, doc_id) when is_atom(idx_name) do
    :ets.foldl(
      fn {_field_name, t}, _acc ->
        :ets.match_delete(t, {{:_, doc_id}})
      end,
      :ignored_acc,
      idx_name
    )

    :ok
  end

  def search_index(idx_name, search_string) when is_atom(idx_name) and is_binary(search_string) do
    search_string
    |> split()
    |> Task.async_stream(fn search_substring ->
      search_substring = String.downcase(search_substring)

      :ets.foldl(
        fn {field_name, t}, acc ->
          [search_field_index_(t, field_name, search_substring) | acc]
        end,
        [],
        idx_name
      )
    end)
    |> Enum.flat_map(fn {:ok, res} -> res end)
    |> List.flatten()
    |> Enum.group_by(fn {doc_id, _} -> doc_id end, fn {_, v} -> v end)
  end

  def search_field_index(idx_name, field_name, search_string)
      when is_atom(idx_name) and is_binary(field_name) and is_binary(search_string) do
    case :ets.lookup(idx_name, field_name) do
      [{_field_name, t}] ->
        search_string
        |> split()
        |> Task.async_stream(fn search_substring ->
          search_substring = String.downcase(search_substring)
          search_field_index_(t, field_name, search_substring)
        end)
        |> Enum.flat_map(fn {:ok, res} -> res end)
        |> List.flatten()
        |> Enum.group_by(fn {doc_id, _} -> doc_id end, fn {_, v} -> v end)
    end
  end

  defp split(field_value) when is_binary(field_value) do
    String.split(field_value, [" ", "-", "/", "_", "."])
  end

  defp split(field_value) when is_list(field_value) do
    Enum.flat_map(field_value, fn fv -> split(fv) end)
  end

  defp split(field_value) when is_map(field_value) do
    split(Map.values(field_value))
  end

  defp split(_field_value) do
    []
  end

  defp reject_stop_words(field_values) do
    Enum.reject(field_values, fn v ->
      v in @stop_words
    end)
  end

  defp search_field_index_(t, field_name, search_substring) do
    search_field_index_(t, field_name, {search_substring, :_}, search_substring, [])
  end

  defp search_field_index_(t, field_name, key, search_substring, acc) do
    case :ets.next(t, key) do
      :"$end_of_table" ->
        acc

      {key_string, doc_id} = key ->
        if String.starts_with?(key_string, search_substring) do
          search_field_index_(t, field_name, key, search_substring, [
            {doc_id, {field_name, key_string}} | acc
          ])
        else
          acc
        end
    end
  end
end
